import { isNewerThanWatermark } from "../ingestion/sequencer";
import { deriveIdempotencyKey, type IdempotencyStore, type S3Notification } from "../ingestion/idempotency";
import { ledgerKey, type LedgerStore } from "../ingestion/ledger";
import type { ObjectSource } from "../ingestion/objectSource";
import { parseIngestionPayload } from "../ingestion/streamParser";
import { isClaimable, type BatchRepository, type BatchSnapshot, type BatchStatus } from "../laboratory/batch";
import {
  BatchBusyError,
  InvalidTransitionError,
  OccExhaustedError,
  PayloadValidationError,
  RetryableConcurrencyError,
} from "../shared/errors";
import type { JsonObject, JsonValue } from "../shared/json";
import { computeBackoffDelay, sleep as realSleep } from "./backoff";
import type { DlqHandler } from "./dlqHandler";

export interface WorkerConfig {
  /** Idempotency lease window; must exceed worst-case processing time. */
  leaseMs: number;
  /** Retries AFTER the first attempt (so total attempts = maxRetries + 1). */
  maxRetries: number;
  baseDelayMs: number;
  capDelayMs: number;
}

export const DEFAULT_WORKER_CONFIG: WorkerConfig = {
  leaseMs: 60_000,
  maxRetries: 3,
  baseDelayMs: 150,
  capDelayMs: 2_000,
};

export interface IngestWorkerDeps {
  idempotency: IdempotencyStore;
  ledger: LedgerStore;
  batches: BatchRepository;
  objects: ObjectSource;
  dlq: DlqHandler;
  config?: Partial<WorkerConfig>;
  clock?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  rng?: () => number;
}

export type IngestOutcome =
  | { kind: "COMPLETED"; batchId: string; batchNumber: string; sampleCount: number }
  /** Same S3 event already finished: cached response, zero writes. */
  | { kind: "DUPLICATE_REPLAY"; cached: JsonValue | null }
  /** Another worker holds a live lease; message should be redelivered later. */
  | { kind: "IN_FLIGHT"; lockedUntil: Date }
  /** Sequencer <= watermark: silently dropped. */
  | { kind: "STALE_DROPPED"; sequencer: string; watermark: string }
  /** Batch already VALIDATED/COMPLETED by a different notification; nothing to do. */
  | { kind: "SKIPPED_ALREADY_INGESTED"; batchNumber: string; status: BatchStatus }
  | { kind: "FAILED_DLQ"; reason: string };

/** Per-invocation scratch state. Never shared across messages, so workers are fully concurrent. */
interface RunContext {
  /** Set only after WE won the PENDING/FAILED -> PARSING OCC write. Gates failure cleanup. */
  owned: BatchSnapshot | null;
}

export class IngestWorker {
  private readonly cfg: WorkerConfig;
  private readonly now: () => Date;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly rng: () => number;

  constructor(private readonly deps: IngestWorkerDeps) {
    this.cfg = { ...DEFAULT_WORKER_CONFIG, ...deps.config };
    this.now = deps.clock ?? (() => new Date());
    this.sleep = deps.sleep ?? realSleep;
    this.rng = deps.rng ?? Math.random;
  }

  async process(n: S3Notification): Promise<IngestOutcome> {
    const idemKey = deriveIdempotencyKey(n);

    // 1. Idempotency lease. Replays return from here without touching anything else.
    const lease = await this.deps.idempotency.acquire(idemKey, this.cfg.leaseMs);
    if (lease.state === "COMPLETED") return { kind: "DUPLICATE_REPLAY", cached: lease.responseData };
    if (lease.state === "IN_PROGRESS") return { kind: "IN_FLIGHT", lockedUntil: lease.lockedUntil };

    const ctx: RunContext = { owned: null };
    try {
      return await this.run(n, idemKey, ctx);
    } catch (err) {
      return this.isolatePoisonPill(n, idemKey, ctx, err);
    }
  }

  private async run(n: S3Notification, idemKey: string, ctx: RunContext): Promise<IngestOutcome> {
    const wmKey = ledgerKey(n.bucket, n.key);

    // 2. Sequencer guard (read side). The watermark only advances at the end, so a crash
    //    mid-flight never causes the retry of the same event to be dropped as "stale".
    const ledgerRow = await this.deps.ledger.get(wmKey);
    const watermark = ledgerRow?.lastSequencer ?? null;
    if (watermark !== null && !isNewerThanWatermark(n.sequencer, watermark)) {
      await this.deps.idempotency.complete(idemKey, { status: "STALE_DROPPED", sequencer: n.sequencer, watermark });
      return { kind: "STALE_DROPPED", sequencer: n.sequencer, watermark };
    }

    // 3. Stream + validate. Nothing is mutated until the whole payload is known-good.
    const payload = await parseIngestionPayload(await this.deps.objects.getObjectStream(n));
    const { batchNumber } = payload;

    // 4. OCC claim: PENDING_INGESTION|FAILED -> PARSING. Exactly one racing worker wins;
    //    losers get OccCollisionError, back off, re-read, and observe the winner's state.
    const claim = await this.withOccRetry(async () => {
      const batch =
        (await this.deps.batches.findByBatchNumber(batchNumber)) ??
        (await this.deps.batches.createPending(batchNumber, {}));
      if (batch.status === "PARSING") throw new BatchBusyError(`Batch ${batchNumber} is being parsed by another worker`);
      if (!isClaimable(batch.status)) return { skipped: batch, owned: null };
      const owned = await this.deps.batches.transition(batch, "PARSING", {
        ingestion: { sourceInstrument: payload.sourceInstrument, sourceTimestamp: payload.timestamp, s3Key: n.key },
      });
      return { skipped: null, owned };
    });
    if (claim.skipped !== null) {
      await this.finish(n, idemKey, wmKey, watermark, {
        status: "SKIPPED_ALREADY_INGESTED",
        batchNumber,
        batchStatus: claim.skipped.status,
      });
      return { kind: "SKIPPED_ALREADY_INGESTED", batchNumber, status: claim.skipped.status };
    }
    ctx.owned = claim.owned;

    // 5. Samples + PARSING -> VALIDATED in a single transaction (all-or-nothing).
    ctx.owned = await this.withOccRetry((attempt) =>
      this.refreshOwned(ctx, "PARSING", attempt).then((snap) =>
        this.deps.batches.commitSamples(snap, payload.samples, "VALIDATED", {
          sampleCount: payload.samples.length,
        }),
      ),
    );

    // 6. VALIDATED -> COMPLETED, then advance watermark and release the lease (cache response).
    const done = await this.withOccRetry((attempt) =>
      this.refreshOwned(ctx, "VALIDATED", attempt).then((snap) =>
        this.deps.batches.transition(snap, "COMPLETED", { completedAt: this.now().toISOString() }),
      ),
    );
    ctx.owned = done;
    await this.finish(n, idemKey, wmKey, watermark, {
      status: "COMPLETED",
      batchId: done.id,
      batchNumber,
      sampleCount: payload.samples.length,
      version: done.version,
    });
    return { kind: "COMPLETED", batchId: done.id, batchNumber, sampleCount: payload.samples.length };
  }

  /** Advances the sequencer watermark (CAS), then marks the lease COMPLETED with the cached response. */
  private async finish(
    n: S3Notification,
    idemKey: string,
    wmKey: string,
    previousWatermark: string | null,
    response: JsonObject,
  ): Promise<void> {
    // A lost CAS means a newer event already advanced the watermark; that is fine.
    await this.deps.ledger.advance(
      { s3Key: wmKey, lastSequencer: n.sequencer, eTag: n.eTag, status: String(response["status"]) },
      previousWatermark,
    );
    await this.deps.idempotency.complete(idemKey, response);
  }

  /** First attempt trusts the in-memory snapshot (we just wrote it); retries re-read. */
  private async refreshOwned(ctx: RunContext, expected: BatchStatus, attempt: number): Promise<BatchSnapshot> {
    const owned = ctx.owned;
    if (owned === null) throw new InvalidTransitionError("UNOWNED", expected);
    if (attempt === 0) return owned;
    const fresh = await this.deps.batches.findByBatchNumber(owned.batchNumber);
    if (fresh === null || fresh.id !== owned.id || fresh.status !== expected) {
      throw new InvalidTransitionError(fresh?.status ?? "MISSING", expected);
    }
    return fresh;
  }

  /** Bounded retries (maxRetries) with exponential backoff + full jitter; only for concurrency errors. */
  private async withOccRetry<T>(fn: (attempt: number) => Promise<T>): Promise<T> {
    let last: Error | null = null;
    for (let attempt = 0; attempt <= this.cfg.maxRetries; attempt++) {
      try {
        return await fn(attempt);
      } catch (err) {
        if (!(err instanceof RetryableConcurrencyError)) throw err;
        last = err;
        if (attempt < this.cfg.maxRetries) {
          await this.sleep(computeBackoffDelay(attempt, this.cfg, this.rng));
        }
      }
    }
    throw new OccExhaustedError(this.cfg.maxRetries + 1, last ?? new Error("unknown"));
  }

  /**
   * Terminal-failure path. Order matters:
   *  a) quarantine the batch (FAILED + metadata) — only if we own it / it is untouched
   *  b) mark the lease FAILED so a deliberate replay can re-acquire it
   *  c) hand off to the DLQ
   * Each step is best-effort and isolated so a failing cleanup never throws out of the
   * worker (which would stall the consumer slot and trigger a hot redelivery loop).
   */
  private async isolatePoisonPill(
    n: S3Notification,
    idemKey: string,
    ctx: RunContext,
    err: unknown,
  ): Promise<IngestOutcome> {
    const error = err instanceof Error ? err : new Error(String(err));
    const batchNumber =
      ctx.owned?.batchNumber ?? (error instanceof PayloadValidationError ? error.batchNumber : undefined);
    const failure: JsonObject = {
      failure: { name: error.name, message: error.message, at: this.now().toISOString(), s3Key: n.key },
    };

    try {
      if (ctx.owned !== null) {
        // We hold PARSING/VALIDATED: the only legitimate writer, so FAILED is safe to force.
        await this.quarantine(ctx.owned.batchNumber, ["PARSING", "VALIDATED"], failure, false);
      } else if (error instanceof PayloadValidationError && batchNumber !== undefined) {
        // Never downgrade a batch that has other (good) data: only touch untouched PENDING rows.
        await this.quarantine(batchNumber, ["PENDING_INGESTION"], failure, true);
      }
    } catch {
      /* swallowed: see doc comment */
    }
    try {
      await this.deps.idempotency.fail(idemKey, `${error.name}: ${error.message}`);
    } catch {
      /* lease will simply expire */
    }
    try {
      await this.deps.dlq.publish({
        notification: n,
        idempotencyKey: idemKey,
        ...(batchNumber !== undefined ? { batchNumber } : {}),
        errorName: error.name,
        errorMessage: error.message,
        failedAt: this.now().toISOString(),
      });
    } catch {
      /* DLQ outage must not crash the consumer */
    }
    return { kind: "FAILED_DLQ", reason: `${error.name}: ${error.message}` };
  }

  private async quarantine(
    batchNumber: string,
    fromStates: readonly BatchStatus[],
    failure: JsonObject,
    createIfMissing: boolean,
  ): Promise<void> {
    await this.withOccRetry(async () => {
      let batch = await this.deps.batches.findByBatchNumber(batchNumber);
      if (batch === null && createIfMissing) batch = await this.deps.batches.createPending(batchNumber, {});
      if (batch === null || !fromStates.includes(batch.status)) return;
      await this.deps.batches.transition(batch, "FAILED", failure);
    });
  }
}
