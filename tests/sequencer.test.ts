import { describe, expect, it } from "vitest";
import { compareS3Sequencers, isNewerThanWatermark } from "../src/ingestion/sequencer";
import { InvalidSequencerError } from "../src/shared/errors";
import { computeBackoffDelay } from "../src/engine/backoff";

describe("compareS3Sequencers", () => {
  it("orders variable-length hex numerically, not lexicographically", () => {
    // Lexicographically "9" > "10"; numerically 0x9 < 0x10.
    expect(compareS3Sequencers("9", "10")).toBe(-1);
    expect(compareS3Sequencers("0055B1C2D3", "0055B1C2D4E5")).toBe(-1);
    expect(compareS3Sequencers("FFFFFFFFFFFFFFFFFF", "1000000000000000000")).toBe(-1);
  });
  it("returns 0 for identical tokens and is case-insensitive", () => {
    expect(compareS3Sequencers("0055AED6DCD90281E5", "0055AED6DCD90281E5")).toBe(0);
    expect(compareS3Sequencers("abc", "ABC")).toBe(0);
  });
  it("detects inverted order", () => {
    expect(compareS3Sequencers("10", "9")).toBe(1);
    expect(compareS3Sequencers("0055B1C2D4E5", "0055B1C2D3")).toBe(1);
  });
  it("rejects non-hex input", () => {
    expect(() => compareS3Sequencers("xyz", "1")).toThrow(InvalidSequencerError);
    expect(() => compareS3Sequencers("", "1")).toThrow(InvalidSequencerError);
  });
  it("watermark check is strictly-greater", () => {
    expect(isNewerThanWatermark("A", null)).toBe(true);
    expect(isNewerThanWatermark("A", "A")).toBe(false);
    expect(isNewerThanWatermark("9", "A")).toBe(false);
    expect(isNewerThanWatermark("B", "A")).toBe(true);
  });
});

describe("computeBackoffDelay (full jitter)", () => {
  const opts = { baseDelayMs: 100, capDelayMs: 1000 };
  it("is bounded by min(cap, base*2^attempt)", () => {
    expect(computeBackoffDelay(0, opts, () => 0.999)).toBeLessThan(100);
    expect(computeBackoffDelay(2, opts, () => 0.999)).toBeLessThan(400);
    expect(computeBackoffDelay(10, opts, () => 0.999)).toBeLessThan(1000);
  });
  it("spans zero to the ceiling", () => {
    expect(computeBackoffDelay(3, opts, () => 0)).toBe(0);
  });
});

