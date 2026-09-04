import { z } from "zod";
import type { RunSettings } from "../workflows/bulk-run";

export const createRunSchema = z
  .object({
    question: z.string().trim().min(1).max(4_000),
    count: z.coerce.number().int().min(1).max(10_000).optional(),
    items: z
      .array(
        z.object({
          key: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
          context: z.unknown(),
        }),
      )
      .max(10_000)
      .optional(),
    targetConcurrency: z.coerce.number().int().min(1).max(250).optional(),
    concurrency: z.coerce.number().int().min(1).max(250).optional(),
    idempotencyKey: z.string().trim().min(1).max(200).optional(),
  })
  .refine((value) => value.items === undefined || value.count === undefined, {
    message: "Provide either items or count, not both.",
  })
  .refine((value) => value.items !== undefined || value.count !== undefined, {
    message: "Provide either items or count.",
  });

export type CreateRunBody = z.infer<typeof createRunSchema>;

const MAX_ITEM_CONTEXT_BYTES = 128 * 1024;
const MAX_TOTAL_CONTEXT_BYTES = 32 * 1024 * 1024;

export function normalizeCreateRun(body: unknown, idempotencyKeyHeader?: string | null) {
  const parsed = createRunSchema.parse(body);
  if (parsed.idempotencyKey && idempotencyKeyHeader && parsed.idempotencyKey !== idempotencyKeyHeader.trim()) {
    throw new Error("The body and Idempotency-Key header must match.");
  }
  const items = parsed.items ?? Array.from({ length: parsed.count ?? 100 }, (_, index) => ({
    key: `item-${String(index + 1).padStart(5, "0")}`,
    context: { index: index + 1, sampleValue: 1_000 + index * 25 },
  }));

  const seenKeys = new Set<string>();
  let totalBytes = 0;
  for (const item of items) {
    if (seenKeys.has(item.key)) throw new Error(`Duplicate item key: ${item.key}`);
    seenKeys.add(item.key);
    const serializedContext = JSON.stringify(item.context);
    if (serializedContext === undefined) throw new Error(`Context for ${item.key} must be a JSON value.`);
    const bytes = Buffer.byteLength(serializedContext);
    if (bytes > MAX_ITEM_CONTEXT_BYTES) {
      throw new Error(`Context for ${item.key} exceeds the 128 KiB limit.`);
    }
    totalBytes += bytes;
  }
  if (totalBytes > MAX_TOTAL_CONTEXT_BYTES) {
    throw new Error("Total item context exceeds the 32 MiB limit.");
  }

  const requestedConcurrency = parsed.targetConcurrency ?? parsed.concurrency ?? 25;
  const itemConcurrency = Math.min(25, requestedConcurrency);
  const activeChildren = Math.min(10, Math.ceil(requestedConcurrency / itemConcurrency));
  const settings: RunSettings = { activeChildren, itemConcurrency };

  return {
    question: parsed.question,
    items,
    settings,
    idempotencyKey: parsed.idempotencyKey ?? idempotencyKeyHeader ?? undefined,
  };
}

export function maxActiveRuns() {
  const value = Number(process.env.MAX_ACTIVE_RUNS ?? 2);
  return Number.isFinite(value) && value > 0 ? Math.trunc(value) : 2;
}
