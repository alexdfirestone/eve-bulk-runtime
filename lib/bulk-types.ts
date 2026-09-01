import { z } from "zod";

export const firmSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  context: z.record(z.string(), z.unknown()).default({}),
});

export const firmResultSchema = z.object({
  firmId: z.string(),
  status: z.enum(["succeeded", "failed", "needs_review"]),
  answer: z.string(),
  estimate: z.number().nullable(),
  currency: z.string().nullable(),
  assumptions: z.array(z.string()),
  evidence: z.array(z.string()),
  error: z.string().nullable(),
});

export type Firm = z.infer<typeof firmSchema>;
export type FirmResult = z.infer<typeof firmResultSchema>;

export const bulkRequestSchema = z.object({
  question: z.string().min(1),
  firms: z.array(firmSchema).min(1).max(10_000),
  concurrency: z.number().int().min(1).max(250).default(100),
});

export type BulkRequest = z.infer<typeof bulkRequestSchema>;
