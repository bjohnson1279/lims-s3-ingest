/**
 * Error taxonomy. The worker routes on *class*, not on message text:
 *
 *  - RetryableConcurrencyError subclasses  -> bounded retry with backoff (OCC loop)
 *  - everything else                       -> terminal: FAILED + DLQ (poison-pill isolation)
 */

export class RetryableConcurrencyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** UPDATE ... WHERE id = ? AND version = ? matched zero rows: someone else won the race. */
export class OccCollisionError extends RetryableConcurrencyError {}

/** Another worker currently owns the batch (status PARSING). Transient by design. */
export class BatchBusyError extends RetryableConcurrencyError {}

/** Retry budget spent; terminal. */
export class OccExhaustedError extends Error {
  constructor(
    readonly attempts: number,
    readonly lastError: Error,
  ) {
    super(`OCC retries exhausted after ${attempts} attempts: ${lastError.message}`);
    this.name = "OccExhaustedError";
  }
}

export class InvalidTransitionError extends Error {
  constructor(from: string, to: string) {
    super(`Illegal batch transition ${from} -> ${to}`);
    this.name = "InvalidTransitionError";
  }
}

export class InvalidSequencerError extends Error {
  constructor(value: string) {
    super(`Invalid S3 sequencer (expected non-empty hex): "${value}"`);
    this.name = "InvalidSequencerError";
  }
}

/** Structural / semantic payload failure. Never retryable: replaying cannot fix the bytes. */
export class PayloadValidationError extends Error {
  constructor(
    message: string,
    readonly code: "MALFORMED_JSON" | "SCHEMA_VIOLATION" | "DUPLICATE_SAMPLE" | "PAYLOAD_TOO_LARGE",
    /** Best-effort, only set when the header's batchNumber itself was valid. */
    readonly batchNumber?: string,
  ) {
    super(message);
    this.name = "PayloadValidationError";
  }
}
