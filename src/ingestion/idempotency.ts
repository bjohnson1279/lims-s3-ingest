import { createHash } from "node:crypto";
import type { Prisma, PrismaClient } from "@prisma/client";
import type { JsonValue } from "../shared/json";

export interface S3Notification {
  bucket: string;
  key: string;
  eTag: string;
  sequencer: string;
}

/** sha256(bucket:key:eTag:sequencer) — deterministic across redeliveries of the same S3 event. */
export function deriveIdempotencyKey(n: S3Notification): string {
  return createHash("sha256").update(`${n.bucket}:${n.key}:${n.eTag}:${n.sequencer}`).digest("hex");
}

export type LeaseResult =
  | { state: "ACQUIRED" }
  /** Already finished: caller returns `responseData` with zero side effects. */
  | { state: "COMPLETED"; responseData: JsonValue | null }
  /** Live lease held by another worker; do not process, let the queue redeliver. */
  | { state: "IN_PROGRESS"; lockedUntil: Date };

/**
 * Lease state machine:
 *
 *   (absent) --acquire--> STARTED --complete--> COMPLETED   (terminal, cached response)
 *                            |  \--fail-------> FAILED      (re-acquirable)
 *                            \-- lockedUntil < now: re-acquirable (orphaned worker)
 */
export interface IdempotencyStore {
  acquire(key: string, leaseMs: number): Promise<LeaseResult>;
  complete(key: string, responseData: JsonValue): Promise<void>;
  fail(key: string, reason: string): Promise<void>;
}

export class PrismaIdempotencyStore implements IdempotencyStore {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly now: () => Date = () => new Date(),
  ) {}

  /**
   * One atomic statement decides the winner. Postgres evaluates the DO UPDATE ... WHERE
   * under the row lock taken by the conflicting insert, so exactly one concurrent caller
   * gets rowsAffected = 1. The takeover predicate only matches FAILED rows or STARTED
   * rows whose lease expired; COMPLETED rows are never touched.
   */
  async acquire(key: string, leaseMs: number): Promise<LeaseResult> {
    const now = this.now();
    const lockedUntil = new Date(now.getTime() + leaseMs);
    const affected = await this.prisma.$executeRaw`
      INSERT INTO "IngestionIdempotency" ("key", "status", "lockedUntil", "createdAt", "updatedAt")
      VALUES (${key}, 'STARTED'::"IdempotencyStatus", ${lockedUntil}, ${now}, ${now})
      ON CONFLICT ("key") DO UPDATE
        SET "status" = 'STARTED'::"IdempotencyStatus",
            "lockedUntil" = EXCLUDED."lockedUntil",
            "updatedAt" = EXCLUDED."updatedAt"
        WHERE "IngestionIdempotency"."status" = 'FAILED'::"IdempotencyStatus"
           OR ("IngestionIdempotency"."status" = 'STARTED'::"IdempotencyStatus"
               AND "IngestionIdempotency"."lockedUntil" < ${now})`;
    if (affected === 1) return { state: "ACQUIRED" };

    const row = await this.prisma.ingestionIdempotency.findUnique({ where: { key } });
    if (row === null) return this.acquire(key, leaseMs); // deleted between statements; retry once more
    if (row.status === "COMPLETED") {
      return { state: "COMPLETED", responseData: (row.responseData as JsonValue | null) ?? null };
    }
    return { state: "IN_PROGRESS", lockedUntil: row.lockedUntil };
  }

  /**
   * Guarded on status = STARTED so a worker whose lease already expired and was taken
   * over cannot clobber a newer COMPLETED record. (A full fencing token would be the
   * next step if lease windows are ever shorter than worst-case processing time.)
   */
  async complete(key: string, responseData: JsonValue): Promise<void> {
    await this.prisma.ingestionIdempotency.updateMany({
      where: { key, status: "STARTED" },
      data: { status: "COMPLETED", responseData: responseData as Prisma.InputJsonValue },
    });
  }

  async fail(key: string, reason: string): Promise<void> {
    await this.prisma.ingestionIdempotency.updateMany({
      where: { key, status: "STARTED" },
      data: { status: "FAILED", responseData: { error: reason } },
    });
  }
}
