import { PrismaClient } from "@prisma/client";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { MockDlqHandler } from "../src/engine/dlqHandler";
import { IngestWorker } from "../src/engine/ingestWorker";
import { PrismaIdempotencyStore, type S3Notification } from "../src/ingestion/idempotency";
import { PrismaLedgerStore } from "../src/ingestion/ledger";
import { PrismaBatchRepository } from "../src/laboratory/prismaBatchRepository";
import { MemoryObjectSource, makePayload } from "./support/memory";

/** Runs only when DATABASE_URL points at a migrated Postgres (docker compose provides one). */
const enabled = Boolean(process.env["DATABASE_URL"]);

describe.skipIf(!enabled)("Postgres integration", () => {
  const prisma = new PrismaClient();
  const idempotency = new PrismaIdempotencyStore(prisma);

  const build = () => {
    const objects = new MemoryObjectSource();
    const dlq = new MockDlqHandler();
    const worker = new IngestWorker({
      idempotency,
      ledger: new PrismaLedgerStore(prisma),
      batches: new PrismaBatchRepository(prisma),
      objects,
      dlq,
      config: { baseDelayMs: 300, capDelayMs: 3000 }, // give real DB transactions headroom during concurrency tests
    });
    return { objects, dlq, worker };
  };
  const notif = (key: string, sequencer: string): S3Notification => ({
    bucket: "lims-raw",
    key,
    eTag: "etag",
    sequencer,
  });

  beforeEach(async () => {
    await prisma.sample.deleteMany();
    await prisma.sampleBatch.deleteMany();
    await prisma.ingestionIdempotency.deleteMany();
    await prisma.s3IngestionLedger.deleteMany();
  });
  afterAll(async () => prisma.$disconnect());

  it("lease acquisition is atomic: 10 concurrent callers, exactly one ACQUIRED", async () => {
    const results = await Promise.all(Array.from({ length: 10 }, () => idempotency.acquire("k1", 60_000)));
    expect(results.filter((r) => r.state === "ACQUIRED")).toHaveLength(1);
    expect(results.filter((r) => r.state === "IN_PROGRESS")).toHaveLength(9);
  });

  it("an expired lease can be taken over, a FAILED one can be re-acquired, a COMPLETED one cannot", async () => {
    expect((await idempotency.acquire("k2", -1_000)).state).toBe("ACQUIRED"); // born expired
    expect((await idempotency.acquire("k2", 60_000)).state).toBe("ACQUIRED"); // takeover
    await idempotency.fail("k2", "boom");
    expect((await idempotency.acquire("k2", 60_000)).state).toBe("ACQUIRED");
    await idempotency.complete("k2", { ok: true });
    expect(await idempotency.acquire("k2", 60_000)).toEqual({ state: "COMPLETED", responseData: { ok: true } });
  });

  it("5 concurrent workers on one batchNumber: one COMPLETED, no DLQ, samples written once", async () => {
    const { worker, objects, dlq } = build();
    const ns = [1, 2, 3, 4, 5].map((i) => notif(`incoming/r${i}.json`, `0A${i}`));
    for (const n of ns) objects.put(n, makePayload("BATCH-PG-1"));

    const outcomes = await Promise.all(ns.map((n) => worker.process(n)));

    expect(outcomes.filter((o) => o.kind === "COMPLETED")).toHaveLength(1);
    expect(dlq.entries.map((e) => e.errorMessage)).toEqual([]); // surfaces the reason if this ever fails
    expect(outcomes.filter((o) => o.kind === "FAILED_DLQ")).toHaveLength(0);
    const batch = await prisma.sampleBatch.findUniqueOrThrow({ where: { batchNumber: "BATCH-PG-1" } });
    expect(batch.status).toBe("COMPLETED");
    expect(batch.version).toBe(4);
    expect(await prisma.sample.count()).toBe(3);
  });

  it("replay returns the cached response with no version bump", async () => {
    const { worker, objects } = build();
    const n = notif("incoming/a.json", "0AB");
    objects.put(n, makePayload("BATCH-PG-2"));
    expect((await worker.process(n)).kind).toBe("COMPLETED");
    const before = await prisma.sampleBatch.findUniqueOrThrow({ where: { batchNumber: "BATCH-PG-2" } });

    expect((await worker.process(n)).kind).toBe("DUPLICATE_REPLAY");

    const after = await prisma.sampleBatch.findUniqueOrThrow({ where: { batchNumber: "BATCH-PG-2" } });
    expect(after.version).toBe(before.version);
    expect(objects.fetches).toBe(1);
  });

  it("poison payload marks the batch FAILED with failure metadata", async () => {
    const { worker, objects, dlq } = build();
    const n = notif("incoming/bad.json", "1");
    objects.put(n, makePayload("BATCH-PG-3", ["S-1", "S-1"]));
    expect((await worker.process(n)).kind).toBe("FAILED_DLQ");
    const batch = await prisma.sampleBatch.findUniqueOrThrow({ where: { batchNumber: "BATCH-PG-3" } });
    expect(batch.status).toBe("FAILED");
    expect(batch.metadata).toHaveProperty("failure");
    expect(dlq.entries).toHaveLength(1);
  });
});

