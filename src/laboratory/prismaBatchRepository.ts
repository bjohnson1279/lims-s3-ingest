import { Prisma, type PrismaClient } from "@prisma/client";
import { OccCollisionError } from "../shared/errors";
import type { JsonObject } from "../shared/json";
import type { SampleInput } from "../ingestion/validator";
import { assertTransition, type BatchRepository, type BatchSnapshot, type BatchStatus } from "./batch";

const SAMPLE_CHUNK = 200;

export class PrismaBatchRepository implements BatchRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async findByBatchNumber(batchNumber: string): Promise<BatchSnapshot | null> {
    const row = await this.prisma.sampleBatch.findUnique({ where: { batchNumber } });
    return row && toSnapshot(row);
  }

  async createPending(batchNumber: string, metadata: JsonObject): Promise<BatchSnapshot> {
    try {
      const row = await this.prisma.sampleBatch.create({
        data: { batchNumber, metadata: metadata as Prisma.InputJsonObject },
      });
      return toSnapshot(row);
    } catch (err) {
      if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === "P2002") {
        return toSnapshot(await this.prisma.sampleBatch.findUniqueOrThrow({ where: { batchNumber } }));
      }
      throw err;
    }
  }

  async transition(snapshot: BatchSnapshot, to: BatchStatus, patch: JsonObject = {}): Promise<BatchSnapshot> {
    assertTransition(snapshot.status, to);
    return this.occUpdate(this.prisma, snapshot, to, patch);
  }

  async commitSamples(
    snapshot: BatchSnapshot,
    samples: readonly SampleInput[],
    to: BatchStatus,
    patch: JsonObject = {},
  ): Promise<BatchSnapshot> {
    assertTransition(snapshot.status, to);
    return this.prisma.$transaction(
      async (tx) => {
        // OCC gate FIRST: on collision we throw before writing a single sample, and any
        // later failure rolls the version bump back together with the samples.
        const next = await this.occUpdate(tx, snapshot, to, patch);
        for (let i = 0; i < samples.length; i += SAMPLE_CHUNK) {
          for (const s of samples.slice(i, i + SAMPLE_CHUNK)) {
            const results = s.results as Prisma.InputJsonObject;
            await tx.sample.upsert({
              where: { sampleCode: s.sampleCode },
              // `update` deliberately never reassigns batchId; a code owned by another batch is not stolen.
              update: { assayType: s.assayType, results, version: { increment: 1 } },
              create: { batchId: snapshot.id, sampleCode: s.sampleCode, assayType: s.assayType, results },
            });
          }
        }
        return next;
      },
      { timeout: 30_000, maxWait: 5_000 },
    );
  }

  /** UPDATE "SampleBatch" SET status=?, metadata=?, version=version+1 WHERE id=? AND version=? */
  private async occUpdate(
    db: Prisma.TransactionClient | PrismaClient,
    snapshot: BatchSnapshot,
    to: BatchStatus,
    patch: JsonObject,
  ): Promise<BatchSnapshot> {
    const metadata: JsonObject = { ...snapshot.metadata, ...patch };
    const res = await db.sampleBatch.updateMany({
      where: { id: snapshot.id, version: snapshot.version },
      data: { status: to, metadata: metadata as Prisma.InputJsonObject, version: { increment: 1 } },
    });
    if (res.count === 0) {
      throw new OccCollisionError(`Batch ${snapshot.batchNumber} moved past version ${snapshot.version}`);
    }
    return { ...snapshot, status: to, metadata, version: snapshot.version + 1 };
  }
}

function toSnapshot(row: {
  id: string;
  batchNumber: string;
  status: BatchStatus;
  version: number;
  metadata: Prisma.JsonValue;
}): BatchSnapshot {
  const metadata =
    row.metadata !== null && typeof row.metadata === "object" && !Array.isArray(row.metadata)
      ? (row.metadata as JsonObject)
      : {};
  return { id: row.id, batchNumber: row.batchNumber, status: row.status, version: row.version, metadata };
}
