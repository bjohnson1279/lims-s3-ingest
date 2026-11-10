# Event-Driven S3 Ingestion Engine with OCC & Idempotency

Production-grade prototype for an asynchronous, event-driven laboratory ingestion pipeline in TypeScript (Node.js) using Prisma ORM (PostgreSQL), Zod, and Vitest.

Processes high-volume S3 object creation notifications delivered via SQS, enforcing strict idempotency, out-of-order event sequencing protection, lock-free optimistic concurrency control (OCC), and dead-letter queue (DLQ) poison-pill isolation.

---

## Architectural Highlights

1. **Separation of Bounded Contexts**:
   - `IngestionContext`: S3 payload streaming, structural validation, idempotency leases, and event sequencing.
   - `LaboratoryContext`: Domain aggregates (`SampleBatch`, `Sample`), state-machine transitions, and transactional persistence.

2. **Deterministic Idempotency**:
   - Keys derived via `sha256(bucket + ":" + key + ":" + eTag + ":" + sequencer)`.
   - Atomic lease table (`IngestionIdempotency`) with `STARTED`, `COMPLETED`, and `FAILED` states plus a lease expiration window. Handled via raw atomic `INSERT ... ON CONFLICT DO UPDATE`.

3. **Out-of-Order Sequencer Protection**:
   - Safely parses S3 `sequencer` hex strings using `BigInt("0x" + sequencer)` to prevent lexicographical sorting bugs across varying string lengths.
   - Drops stale notifications with sequencer values $\le$ the recorded watermark for that key.

4. **Lock-Free Optimistic Concurrency Control (OCC)**:
   - Zero pessimistic row locks (`SELECT ... FOR UPDATE`).
   - Mutations on `SampleBatch` enforce `WHERE id = ? AND version = ?` with `version = version + 1`.
   - Collision triggers bounded retries (max 3) with exponential backoff and full jitter.

5. **Streaming Payload Parsing**:
   - `stream-json` SAX-style tokenizer feeds incremental Zod parsing. Holds only one sample object in memory at a time, protecting the runtime from OOM spikes under large payloads.

6. **Poison-Pill Isolation**:
   - Validation failures or exhausted OCC retries transition the batch to `FAILED`, record metadata, and route to a DLQ handler without crashing worker processes.

---

## Project Structure

```
├── Dockerfile
├── docker-compose.yml
├── prisma/
│   ├── schema.prisma
│   └── migrations/
│       └── 20261109000000_init/
│           └── migration.sql
├── src/
│   ├── engine/
│   │   ├── backoff.ts           # Full-jitter exponential backoff
│   │   ├── dlqHandler.ts        # Mock DLQ handler
│   │   └── ingestWorker.ts      # Core pipeline orchestrator
│   ├── ingestion/
│   │   ├── idempotency.ts       # Atomic lease store & key derivation
│   │   ├── ledger.ts            # S3 watermark ledger
│   │   ├── objectSource.ts      # S3 stream reader interface
│   │   ├── sequencer.ts         # BigInt S3 sequencer comparator
│   │   ├── streamParser.ts      # Streaming JSON tokenizer & validator
│   │   └── validator.ts         # Zod schemas for batches & samples
│   ├── laboratory/
│   │   ├── batch.ts             # State machine & domain interfaces
│   │   └── prismaBatchRepository.ts # OCC batch repository & sample upserts
│   └── shared/
│       ├── errors.ts            # Typed error hierarchy
│       └── json.ts              # Strict JSON types
└── tests/
    ├── ingestWorker.test.ts     # Concurrency & idempotency unit tests
    ├── postgres.integration.test.ts # End-to-end PostgreSQL tests
    ├── sequencer.test.ts        # BigInt ordering & jitter test suite
    └── support/
        └── memory.ts            # In-memory test doubles
```

---

## Running in Docker

```bash
# Run migrations, type checking, and all 18 unit + integration tests in Docker
docker-compose up --build --abort-on-container-exit --exit-code-from app

# Start PostgreSQL service in background (mapped to 127.0.0.1:5434)
docker-compose up -d db

# Tear down containers and network
docker-compose down
```

---

## Local Development

```bash
# Install dependencies
npm ci

# Generate Prisma client
npx prisma generate

# Typecheck
npm run typecheck

# Run unit tests
npm test
```
