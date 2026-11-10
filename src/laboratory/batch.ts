import { InvalidTransitionError } from "../shared/errors";
import type { JsonObject } from "../shared/json";
import type { SampleInput } from "../ingestion/validator";

export type BatchStatus = "PENDING_INGESTION" | "PARSING" | "VALIDATED" | "FAILED" | "COMPLETED";

/**
 * SampleBatch state machine:
 *
 *   PENDING_INGESTION --> PARSING --> VALIDATED --> COMPLETED
 *          |                |            |
 *          +----------------+------------+--> FAILED --> PARSING (operator/DLQ replay)
 *
 * COMPLETED is terminal. PARSING doubles as the soft "ownership" marker: because the
 * PENDING/FAILED -> PARSING step is itself an OCC write, exactly one worker can claim a batch.
 */
const ALLOWED: Readonly<Record<BatchStatus, readonly BatchStatus[]>> = {
  PENDING_INGESTION: ["PARSING", "FAILED"],
  PARSING: ["VALIDATED", "FAILED"],
  VALIDATED: ["COMPLETED", "FAILED"],
  FAILED: ["PARSING"],
  COMPLETED: [],
};

export function assertTransition(from: BatchStatus, to: BatchStatus): void {
  if (!ALLOWED[from].includes(to)) throw new InvalidTransitionError(from, to);
}

/** States from which a new ingestion run may claim the batch. */
export const isClaimable = (s: BatchStatus): boolean => s === "PENDING_INGESTION" || s === "FAILED";

export interface BatchSnapshot {
  id: string;
  batchNumber: string;
  status: BatchStatus;
  version: number;
  metadata: JsonObject;
}

/**
 * LaboratoryContext persistence port. Every mutating method is an OCC write against
 * `snapshot.version` and throws OccCollisionError when zero rows were affected.
 * No implementation may use SELECT ... FOR UPDATE.
 */
export interface BatchRepository {
  findByBatchNumber(batchNumber: string): Promise<BatchSnapshot | null>;
  /** Get-or-create; safe under concurrent creation (unique batchNumber). */
  createPending(batchNumber: string, metadata: JsonObject): Promise<BatchSnapshot>;
  transition(snapshot: BatchSnapshot, to: BatchStatus, metadataPatch?: JsonObject): Promise<BatchSnapshot>;
  /** Upserts all samples AND moves the batch to `to` in ONE transaction (all-or-nothing). */
  commitSamples(
    snapshot: BatchSnapshot,
    samples: readonly SampleInput[],
    to: BatchStatus,
    metadataPatch?: JsonObject,
  ): Promise<BatchSnapshot>;
}
