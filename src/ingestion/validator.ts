import { z } from "zod";
import type { JsonValue } from "../shared/json";

export const MAX_SAMPLES_PER_BATCH = 50_000;

const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number().finite(),
    z.boolean(),
    z.null(),
    z.array(jsonValueSchema),
    z.record(jsonValueSchema),
  ]),
);

export const batchNumberSchema = z
  .string()
  .regex(/^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$/, "batchNumber must be 3-64 chars of [A-Za-z0-9_-]");

export const ingestionHeaderSchema = z
  .object({
    batchNumber: batchNumberSchema,
    sourceInstrument: z.string().trim().min(1).max(128),
    timestamp: z.string().datetime({ offset: true }),
  })
  .strict();

export const sampleSchema = z
  .object({
    sampleCode: z.string().trim().min(1).max(64),
    assayType: z.string().trim().min(1).max(64),
    results: z.record(jsonValueSchema),
  })
  .strict();

/** Full-document schema (used for buffered inputs; the stream parser applies the same parts incrementally). */
export const ingestionPayloadSchema = ingestionHeaderSchema.extend({
  samples: z
    .array(sampleSchema)
    .min(1)
    .max(MAX_SAMPLES_PER_BATCH)
    .superRefine((samples, ctx) => {
      const seen = new Set<string>();
      samples.forEach((s, index) => {
        if (seen.has(s.sampleCode)) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            path: [index, "sampleCode"],
            message: `Duplicate sampleCode "${s.sampleCode}"`,
          });
        }
        seen.add(s.sampleCode);
      });
    }),
});

export type IngestionHeader = z.infer<typeof ingestionHeaderSchema>;
export type SampleInput = z.infer<typeof sampleSchema>;
export type IngestionPayload = z.infer<typeof ingestionPayloadSchema>;
