import { Readable } from "node:stream";
import type { IdempotencyStore, LeaseResult, S3Notification } from "../../src/ingestion/idempotency";
import type { LedgerEntry, LedgerStore } from "../../src/ingestion/ledger";
import type { ObjectSource } from "../../src/ingestion/objectSource";
import type { SampleInput } from "../../src/ingestion/validator";
import { assertTransition, type BatchRepository, type BatchSnapshot, type BatchStatus } from "../../src/laboratory/batch";
import { OccCollisionError } from "../../src/shared/errors";
import type { JsonObject, JsonValue } from "../../src/shared/json";

/** In-memory batch repo with the exact OCC contract of the Prisma one (compare version, bump, else collide). */
export class MemoryBatchRepository implements BatchRepository {
  private readonly rows = new Map<string, BatchSnapshot>();
  readonly samples = new Map<string, SampleInput>();
  collisions = 0;
  writes = 0;
  private seq = 0;

  async findByBatchNumber(batchNumber: string): Promise<BatchSnapshot | null> {
    const row = this.rows.get(batchNumber);
    return row ? { ...row } : null;
  }

  async createPending(batchNumber: string, metadata: JsonObject): Promise<BatchSnapshot> {
    const existing = this.rows.get(batchNumber);
    if (existing) return { ...existing };
    const row: BatchSnapshot = {
      id: `batch-${++this.seq}`,
      batchNumber,
      status: "PENDING_INGESTION",
      version: 1,
      metadata,
    };
    this.rows.set(batchNumber, row);
    this.writes++;
    return { ...row };
  }

  async transition(s: BatchSnapshot, to: BatchStatus, patch: JsonObject = {}): Promise<BatchSnapshot> {
    assertTransition(s.status, to);
    return this.occ(s, to, patch);
  }

  async commitSamples(
    s: BatchSnapshot,
    samples: readonly SampleInput[],
    to: BatchStatus,
    patch: JsonObject = {},
  ): Promise<BatchSnapshot> {
    assertTransition(s.status, to);
    const next = this.occ(s, to, patch); // throws before any sample is written
    for (const sample of samples) {
      this.samples.set(sample.sampleCode, sample);
      this.writes++;
    }
    return next;
  }

  private occ(s: BatchSnapshot, to: BatchStatus, patch: JsonObject): BatchSnapshot {
    const row = this.rows.get(s.batchNumber);
    if (!row || row.version !== s.version) {
      this.collisions++;
      throw new OccCollisionError(`version mismatch on ${s.batchNumber}`);
    }
    const next: BatchSnapshot = {
      ...row,
      status: to,
      version: row.version + 1,
      metadata: { ...row.metadata, ...patch },
    };
    this.rows.set(s.batchNumber, next);
    this.writes++;
    return { ...next };
  }
}

export class MemoryIdempotencyStore implements IdempotencyStore {
  private readonly rows = new Map<
    string,
    { status: "STARTED" | "COMPLETED" | "FAILED"; lockedUntil: Date; responseData: JsonValue | null }
  >();
  constructor(private readonly now: () => Date = () => new Date()) {}

  async acquire(key: string, leaseMs: number): Promise<LeaseResult> {
    const row = this.rows.get(key);
    const now = this.now();
    if (row?.status === "COMPLETED") return { state: "COMPLETED", responseData: row.responseData };
    if (row?.status === "STARTED" && row.lockedUntil > now) return { state: "IN_PROGRESS", lockedUntil: row.lockedUntil };
    this.rows.set(key, { status: "STARTED", lockedUntil: new Date(now.getTime() + leaseMs), responseData: null });
    return { state: "ACQUIRED" };
  }
  async complete(key: string, responseData: JsonValue): Promise<void> {
    const row = this.rows.get(key);
    if (row?.status === "STARTED") this.rows.set(key, { ...row, status: "COMPLETED", responseData });
  }
  async fail(key: string, reason: string): Promise<void> {
    const row = this.rows.get(key);
    if (row?.status === "STARTED") this.rows.set(key, { ...row, status: "FAILED", responseData: { error: reason } });
  }
}

export class MemoryLedgerStore implements LedgerStore {
  private readonly rows = new Map<string, LedgerEntry>();
  async get(s3Key: string): Promise<LedgerEntry | null> {
    return this.rows.get(s3Key) ?? null;
  }
  async advance(entry: LedgerEntry, expectedPrevious: string | null): Promise<boolean> {
    const cur = this.rows.get(entry.s3Key);
    if ((cur?.lastSequencer ?? null) !== expectedPrevious) return false;
    this.rows.set(entry.s3Key, entry);
    return true;
  }
}

export class MemoryObjectSource implements ObjectSource {
  fetches = 0;
  private readonly bodies = new Map<string, string>();
  put(n: Pick<S3Notification, "bucket" | "key">, body: string): void {
    this.bodies.set(`${n.bucket}:${n.key}`, body);
  }
  async getObjectStream(n: S3Notification): Promise<Readable> {
    this.fetches++;
    const body = this.bodies.get(`${n.bucket}:${n.key}`);
    if (body === undefined) throw new Error(`NoSuchKey ${n.key}`);
    // Small chunks so tokens straddle chunk boundaries, exercising the streaming path.
    const chunks: string[] = [];
    for (let i = 0; i < body.length; i += 17) chunks.push(body.slice(i, i + 17));
    return Readable.from(chunks);
  }
}

export function makePayload(batchNumber: string, sampleCodes: string[] = ["S-1", "S-2", "S-3"]): string {
  return JSON.stringify({
    batchNumber,
    sourceInstrument: "HPLC-7",
    timestamp: "2026-11-09T12:00:00Z",
    samples: sampleCodes.map((c) => ({ sampleCode: c, assayType: "PCR", results: { ct: 21.5, flags: ["ok"] } })),
  });
}

