import { Transform, pipeline, type Readable, type TransformCallback } from "node:stream";
import { parser } from "stream-json";
import Assembler from "stream-json/Assembler";
import { PayloadValidationError } from "../shared/errors";
import {
  MAX_SAMPLES_PER_BATCH,
  batchNumberSchema,
  ingestionHeaderSchema,
  sampleSchema,
  type IngestionPayload,
  type SampleInput,
} from "./validator";

export interface StreamLimits {
  maxBytes: number;
  maxSamples: number;
}

export const DEFAULT_STREAM_LIMITS: StreamLimits = {
  maxBytes: 64 * 1024 * 1024,
  maxSamples: MAX_SAMPLES_PER_BATCH,
};

interface Token {
  name: string;
  value?: string;
}

/** Aborts the pipeline as soon as the byte budget is blown, instead of after buffering. */
class ByteLimit extends Transform {
  private seen = 0;
  constructor(private readonly maxBytes: number) {
    super();
  }
  override _transform(chunk: Buffer | string, _enc: BufferEncoding, cb: TransformCallback): void {
    this.seen += typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    if (this.seen > this.maxBytes) {
      cb(new PayloadValidationError(`Payload exceeds ${this.maxBytes} bytes`, "PAYLOAD_TOO_LARGE"));
      return;
    }
    cb(null, chunk);
  }
}

/**
 * Streams the S3 object body through a SAX-style tokenizer and validates incrementally.
 *
 * Memory profile: the raw body is never held; at any instant we hold the tokenizer
 * window plus ONE in-flight sample object. Each sample is Zod-validated and
 * duplicate-checked the moment it completes, so a poison payload fails fast
 * (after the first bad sample) rather than after a full parse.
 *
 * Trade-off: validated samples are still accumulated so the batch can be committed
 * atomically in one transaction (the retained set is bounded by `maxSamples`).
 * Committing in chunks would shrink the footprint but would expose partially
 * ingested batches, which the state machine forbids.
 *
 * Expected shape: { batchNumber, sourceInstrument, timestamp, samples: [ ... ] }
 * with keys in any order.
 */
export async function parseIngestionPayload(
  source: Readable,
  limits: StreamLimits = DEFAULT_STREAM_LIMITS,
): Promise<IngestionPayload> {
  // streamKeys/streamValues=false => only packed tokens (keyValue, stringValue, ...) are emitted.
  const tokenizer = parser({ streamKeys: false, streamValues: false });
  pipeline(source, new ByteLimit(limits.maxBytes), tokenizer, () => undefined);

  const header: Record<string, unknown> = {};
  const samples: SampleInput[] = [];
  const seenCodes = new Set<string>();
  let sawSamples = false;
  let depth = 0; // 0 = outside root object, 1 = inside it
  let key: string | null = null;
  let inSamples = false;
  let assembler: Assembler | null = null;

  const knownBatchNumber = (): string | undefined => {
    const parsed = batchNumberSchema.safeParse(header["batchNumber"]);
    return parsed.success ? parsed.data : undefined;
  };

  const acceptSample = (raw: unknown): void => {
    const parsed = sampleSchema.safeParse(raw);
    if (!parsed.success) {
      throw new PayloadValidationError(
        `samples[${samples.length}] invalid: ${parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ")}`,
        "SCHEMA_VIOLATION",
        knownBatchNumber(),
      );
    }
    if (seenCodes.has(parsed.data.sampleCode)) {
      throw new PayloadValidationError(
        `Duplicate sampleCode "${parsed.data.sampleCode}"`,
        "DUPLICATE_SAMPLE",
        knownBatchNumber(),
      );
    }
    if (samples.length >= limits.maxSamples) {
      throw new PayloadValidationError(`More than ${limits.maxSamples} samples`, "PAYLOAD_TOO_LARGE", knownBatchNumber());
    }
    seenCodes.add(parsed.data.sampleCode);
    samples.push(parsed.data);
  };

  /** Feeds a token to the active assembler; hands the finished value to its owner. */
  const feed = (token: Token): void => {
    assembler ??= new Assembler();
    assembler.consume(token);
    if (assembler.done) {
      const value: unknown = assembler.current;
      assembler = null;
      if (inSamples) acceptSample(value);
      else if (key !== null) header[key] = value;
    }
  };

  try {
    for await (const token of tokenizer as AsyncIterable<Token>) {
      if (assembler) {
        feed(token);
        continue;
      }
      if (depth === 0) {
        if (token.name !== "startObject") {
          throw new PayloadValidationError("Root must be a JSON object", "SCHEMA_VIOLATION");
        }
        depth = 1;
        continue;
      }
      if (inSamples) {
        if (token.name === "endArray") inSamples = false;
        else feed(token); // starts a fresh assembler for one sample
        continue;
      }
      if (token.name === "keyValue") {
        key = String(token.value);
      } else if (token.name === "endObject") {
        depth = 0;
      } else if (key === "samples" && token.name === "startArray") {
        sawSamples = true;
        inSamples = true;
      } else if (key === "samples") {
        throw new PayloadValidationError("samples must be an array", "SCHEMA_VIOLATION", knownBatchNumber());
      } else {
        feed(token);
      }
    }
  } catch (err) {
    tokenizer.destroy();
    source.destroy();
    if (err instanceof PayloadValidationError) throw err;
    throw new PayloadValidationError(
      `Malformed JSON: ${err instanceof Error ? err.message : String(err)}`,
      "MALFORMED_JSON",
      knownBatchNumber(),
    );
  }

  const parsedHeader = ingestionHeaderSchema.safeParse(header);
  if (!parsedHeader.success) {
    throw new PayloadValidationError(
      `Header invalid: ${parsedHeader.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ")}`,
      "SCHEMA_VIOLATION",
      knownBatchNumber(),
    );
  }
  if (!sawSamples || samples.length === 0) {
    throw new PayloadValidationError("samples must be a non-empty array", "SCHEMA_VIOLATION", parsedHeader.data.batchNumber);
  }
  return { ...parsedHeader.data, samples };
}
