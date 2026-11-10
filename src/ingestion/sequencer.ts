import { InvalidSequencerError } from "../shared/errors";

const HEX = /^[0-9A-Fa-f]+$/;

/**
 * Parses an S3 sequencer into a BigInt.
 *
 * Sequencers are variable-length hex strings. Comparing them as strings is wrong
 * ("9" > "10" lexicographically); comparing as numbers is not, and BigInt has no
 * 2^53 precision cliff.
 */
export function parseS3Sequencer(sequencer: string): bigint {
  if (!HEX.test(sequencer)) throw new InvalidSequencerError(sequencer);
  return BigInt("0x" + sequencer);
}

/** Returns -1 if a < b, 0 if equal, 1 if a > b (numeric order). */
export function compareS3Sequencers(a: string, b: string): -1 | 0 | 1 {
  const left = parseS3Sequencer(a);
  const right = parseS3Sequencer(b);
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

/** True when `incoming` is strictly newer than the stored watermark (or none exists). */
export function isNewerThanWatermark(incoming: string, watermark: string | null): boolean {
  return watermark === null || compareS3Sequencers(incoming, watermark) > 0;
}
