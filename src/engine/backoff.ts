export interface BackoffOptions {
  baseDelayMs: number;
  capDelayMs: number;
}

/**
 * Exponential backoff with FULL jitter:
 *   delay = random(0, min(cap, base * 2^attempt))
 * Full jitter (vs. equal/decorrelated) gives the best de-synchronisation of colliding
 * workers at the cost of occasionally retrying almost immediately.
 *
 * @param attempt zero-based index of the attempt that just failed
 * @param rng     injectable [0,1) source for deterministic tests
 */
export function computeBackoffDelay(attempt: number, opts: BackoffOptions, rng: () => number = Math.random): number {
  const ceiling = Math.min(opts.capDelayMs, opts.baseDelayMs * 2 ** attempt);
  return Math.floor(rng() * ceiling);
}

export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
