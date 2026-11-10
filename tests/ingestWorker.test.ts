import { describe, expect, it } from "vitest";
import { IngestWorker } from "../src/engine/ingestWorker";
import { MockDlqHandler } from "../src/engine/dlqHandler";
import type { S3Notification } from "../src/ingestion/idempotency";
import type { BatchSnapshot } from "../src/laboratory/batch";
import {
  MemoryBatchRepository,
  MemoryIdempotencyStore,
  MemoryLedgerStore,
  MemoryObjectSource,
  makePayload,
} from "./support/memory";

const yieldMacrotask = (): Promise<void> => new Promise((r) => setImmediate(r));

function harness(batches = new MemoryBatchRepository()) {
  const objects = new MemoryObjectSource();
  const dlq = new MockDlqHandler();
  const idempotency = new MemoryIdempotencyStore();
  const ledger = new MemoryLedgerStore();
  const worker = new IngestWorker({
    idempotency,
    ledger,
    batches,
    objects,
    dlq,
    sleep: yieldMacrotask, // deterministic: no wall-clock waiting
    rng: () => 0.5,
  });
  return { batches, objects, dlq, worker };
}

const notif = (key: string, sequencer: string, eTag = "etag-1"): S3Notification => ({
  bucket: "lims-raw",
  key,
  eTag,
  sequencer,
});

describe("OCC concurrency", () => {
  /** Repo whose first N reads all park until N readers arrived, forcing identical starting versions. */
  class BarrierRepo extends MemoryBatchRepository {
    private arrived = 0;
    private release!: () => void;
    private readonly gate = new Promise<void>((r) => (this.release = r));
    constructor(private readonly parties: number) {
      super();
    }
    override async findByBatchNumber(n: string): Promise<BatchSnapshot | null> {
      const snap = await super.findByBatchNumber(n);
      if (this.arrived < this.parties) {
        this.arrived++;
        if (this.arrived === this.parties) this.release();
        await this.gate;
      }
      return snap;
    }
  }

  it("5 simultaneous workers on one batchNumber: exactly one winner, losers resolve cleanly", async () => {
    const repo = new BarrierRepo(5);
    // Pre-create so every worker reads the same row at version 1.
    await MemoryBatchRepository.prototype.createPending.call(repo, "BATCH-100", {});
    const { worker, objects, dlq } = harness(repo);

    const notifications = [1, 2, 3, 4, 5].map((i) => notif(`incoming/run-${i}.json`, `00A${i}`));
    for (const n of notifications) objects.put(n, makePayload("BATCH-100"));

    const outcomes = await Promise.all(notifications.map((n) => worker.process(n)));

    expect(outcomes.filter((o) => o.kind === "COMPLETED")).toHaveLength(1);
    expect(outcomes.filter((o) => o.kind === "SKIPPED_ALREADY_INGESTED")).toHaveLength(4);
    expect(outcomes.filter((o) => o.kind === "FAILED_DLQ")).toHaveLength(0);
    expect(dlq.entries).toHaveLength(0);

    expect(repo.collisions).toBe(4); // all four losers hit zero-rows-affected on the claim
    const final = await repo.findByBatchNumber("BATCH-100");
    expect(final?.status).toBe("COMPLETED");
    expect(final?.version).toBe(4); // 1 -> PARSING(2) -> VALIDATED(3) -> COMPLETED(4): one winner only
    expect(repo.samples.size).toBe(3); // upserted exactly once
  });

  it("exhausted retries against a stuck PARSING owner go to DLQ without failing the foreign batch", async () => {
    const repo = new MemoryBatchRepository();
    const b = await repo.createPending("BATCH-200", {});
    await repo.transition(b, "PARSING"); // someone else owns it and never finishes
    const { worker, objects, dlq } = harness(repo);
    const n = notif("incoming/x.json", "1");
    objects.put(n, makePayload("BATCH-200"));

    const out = await worker.process(n);

    expect(out.kind).toBe("FAILED_DLQ");
    expect(dlq.entries).toHaveLength(1);
    expect((await repo.findByBatchNumber("BATCH-200"))?.status).toBe("PARSING"); // untouched
  });
});

describe("idempotency", () => {
  it("replaying the identical notification performs zero writes and returns the cached response", async () => {
    const { worker, objects, batches } = harness();
    const n = notif("incoming/a.json", "00AB");
    objects.put(n, makePayload("BATCH-300"));

    const first = await worker.process(n);
    expect(first.kind).toBe("COMPLETED");
    const writesAfterFirst = batches.writes;

    const replay = await worker.process(n);
    expect(replay.kind).toBe("DUPLICATE_REPLAY");
    if (replay.kind === "DUPLICATE_REPLAY") {
      expect(replay.cached).toMatchObject({ status: "COMPLETED", batchNumber: "BATCH-300", sampleCount: 3 });
    }
    expect(batches.writes).toBe(writesAfterFirst);
    expect(objects.fetches).toBe(1); // payload not even re-read
  });
});

describe("sequencer guard", () => {
  it("silently drops notifications at or below the per-object watermark", async () => {
    const { worker, objects, batches } = harness();
    const newer = notif("incoming/o.json", "10", "e2");
    const older = notif("incoming/o.json", "9", "e1"); // lexicographically bigger, numerically smaller
    objects.put(newer, makePayload("BATCH-400"));
    objects.put(older, makePayload("BATCH-400", ["Z-9"]));

    expect((await worker.process(newer)).kind).toBe("COMPLETED");
    const writes = batches.writes;
    const out = await worker.process(older);

    expect(out.kind).toBe("STALE_DROPPED");
    expect(batches.writes).toBe(writes);
    expect(objects.fetches).toBe(1);
  });
});

describe("poison-pill isolation", () => {
  it("duplicate sample codes fail validation, mark the batch FAILED and route to the DLQ", async () => {
    const { worker, objects, batches, dlq } = harness();
    const n = notif("incoming/bad.json", "5");
    objects.put(n, makePayload("BATCH-500", ["S-1", "S-1"]));

    const out = await worker.process(n);

    expect(out.kind).toBe("FAILED_DLQ");
    expect(dlq.entries[0]?.errorName).toBe("PayloadValidationError");
    expect(dlq.entries[0]?.batchNumber).toBe("BATCH-500");
    const batch = await batches.findByBatchNumber("BATCH-500");
    expect(batch?.status).toBe("FAILED");
    expect(batch?.metadata["failure"]).toBeDefined();
    expect(batches.samples.size).toBe(0);
  });

  it("malformed JSON is isolated and the same worker keeps processing", async () => {
    const { worker, objects, dlq } = harness();
    const bad = notif("incoming/broken.json", "1");
    const good = notif("incoming/ok.json", "1");
    objects.put(bad, '{"batchNumber": "BATCH-600", "samples": [ {');
    objects.put(good, makePayload("BATCH-601"));

    expect((await worker.process(bad)).kind).toBe("FAILED_DLQ");
    expect((await worker.process(good)).kind).toBe("COMPLETED");
    expect(dlq.entries).toHaveLength(1);
  });
});

