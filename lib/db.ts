import { neon } from "@neondatabase/serverless";
import { logger, serializeError } from "./logger";

type SqlClient = ReturnType<typeof neon>;
type DbRow = Record<string, unknown>;

export type RunStatus =
  | "queued"
  | "running"
  | "completed"
  | "completed_with_errors"
  | "failed"
  | "cancelled";

export type ItemStatus = "queued" | "running" | "completed" | "failed" | "cancelled";
export type BatchStatus = "queued" | "running" | "completed" | "failed" | "cancelled";

export type RunSummary = {
  id: string;
  question: string;
  status: RunStatus;
  total: number;
  workflowRunId: string | null;
  parentRunId: string | null;
  retryCount: number;
  idempotencyKey: string | null;
  createdBy: string | null;
  createdAt: string | Date;
  updatedAt: string | Date;
  finishedAt: string | Date | null;
  succeeded: number;
  failed: number;
  cancelled: number;
  active: number;
  totalBatches: number;
  completedBatches: number;
};

export type RunItem = {
  itemIndex: number;
  itemKey: string;
  status: ItemStatus;
  sessionId: string | null;
  result: unknown;
  error: string | null;
  errorCode: string | null;
  attempts: number;
  startedAt: string | Date | null;
  finishedAt: string | Date | null;
};

export type CreateRunInput = {
  question: string;
  items: Array<{ key: string; context: unknown }>;
  idempotencyKey?: string;
  parentRunId?: string;
  retryCount?: number;
  createdBy?: string;
};

let sqlClient: SqlClient | undefined;

function getSql() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is required");
  sqlClient ??= neon(url);
  return sqlClient;
}

function isRetryableDatabaseError(error: unknown) {
  const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  return /fetch failed|connection|timeout|ECONNRESET|ECONNREFUSED|ETIMEDOUT|502|503|504/i.test(message);
}

async function query<T = DbRow>(operation: string, text: string, params: unknown[] = []): Promise<T[]> {
  const sql = getSql();
  let lastError: unknown;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      return await sql.query(text, params) as T[];
    } catch (error) {
      lastError = error;
      if (attempt === 4 || !isRetryableDatabaseError(error)) throw error;
      const delayMs = 250 * 2 ** (attempt - 1);
      logger.warn("database.query.retry", { operation, attempt, delayMs, error: serializeError(error, false) });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastError;
}

function uniqueViolation(error: unknown) {
  return typeof error === "object" && error !== null && (error as { code?: unknown }).code === "23505";
}

export function getBatchSize() {
  const value = Number(process.env.BULK_BATCH_SIZE ?? 200);
  if (!Number.isFinite(value)) return 200;
  return Math.min(Math.max(Math.trunc(value), 25), 500);
}

export async function getRunIdByIdempotencyKey(key: string) {
  const rows = await query<{ id: string }>(
    "run.get_by_idempotency_key",
    "SELECT id FROM bulk_runs WHERE idempotency_key = $1",
    [key],
  );
  return rows[0]?.id;
}

export async function createRun(input: CreateRunInput): Promise<{ id: string; existing: boolean }> {
  if (input.items.length === 0) throw new Error("A run must contain at least one item.");
  if (input.idempotencyKey) {
    const existing = await getRunIdByIdempotencyKey(input.idempotencyKey);
    if (existing) return { id: existing, existing: true };
  }

  const sql = getSql();
  const id = crypto.randomUUID();
  const now = new Date().toISOString();
  const batchSize = getBatchSize();
  const batchSpecs = Array.from({ length: Math.ceil(input.items.length / batchSize) }, (_, batchIndex) => {
    const startIndex = batchIndex * batchSize;
    return {
      id: crypto.randomUUID(),
      batchIndex,
      startIndex,
      size: Math.min(batchSize, input.items.length - startIndex),
    };
  });

  const batchIdByIndex = new Map(batchSpecs.map((batch) => [batch.batchIndex, batch.id]));
  const queries = [
    sql`INSERT INTO bulk_runs (
      id, question, status, total, parent_run_id, retry_count, idempotency_key,
      created_by, created_at, updated_at
    ) VALUES (
      ${id}, ${input.question}, 'queued', ${input.items.length},
      ${input.parentRunId ?? null}, ${input.retryCount ?? 0}, ${input.idempotencyKey ?? null},
      ${input.createdBy ?? null}, ${now}, ${now}
    )`,
  ];

  queries.push(sql`INSERT INTO run_batches (
    id, run_id, batch_index, start_index, size, status, created_at, updated_at
  ) SELECT * FROM unnest(
    ${batchSpecs.map((batch) => batch.id)}::text[],
    ${batchSpecs.map(() => id)}::text[],
    ${batchSpecs.map((batch) => batch.batchIndex)}::int[],
    ${batchSpecs.map((batch) => batch.startIndex)}::int[],
    ${batchSpecs.map((batch) => batch.size)}::int[],
    ${batchSpecs.map(() => "queued")}::text[],
    ${batchSpecs.map(() => now)}::timestamptz[],
    ${batchSpecs.map(() => now)}::timestamptz[]
  )`);

  // Items reference their batch, so batches must be inserted first within
  // this transaction even though both records are created together.
  for (let offset = 0; offset < input.items.length; offset += 500) {
    const chunk = input.items.slice(offset, offset + 500);
    const itemIds = chunk.map(() => crypto.randomUUID());
    const runIds = chunk.map(() => id);
    const batchIds = chunk.map((_, index) => batchIdByIndex.get(Math.floor((offset + index) / batchSize))!);
    const indexes = chunk.map((_, index) => offset + index);
    const keys = chunk.map((item) => item.key);
    const contexts = chunk.map((item) => JSON.stringify(item.context));
    const statuses = chunk.map(() => "queued");
    const timestamps = chunk.map(() => now);

    queries.push(sql`INSERT INTO run_items (
      id, run_id, batch_id, item_index, item_key, context_json, status, created_at, updated_at
    ) SELECT * FROM unnest(
      ${itemIds}::text[], ${runIds}::text[], ${batchIds}::text[], ${indexes}::int[],
      ${keys}::text[], ${contexts}::jsonb[], ${statuses}::text[],
      ${timestamps}::timestamptz[], ${timestamps}::timestamptz[]
    )`);
  }

  try {
    await sql.transaction(queries);
    return { id, existing: false };
  } catch (error) {
    if (!input.idempotencyKey || !uniqueViolation(error)) throw error;
    const existing = await getRunIdByIdempotencyKey(input.idempotencyKey);
    if (existing) return { id: existing, existing: true };
    throw error;
  }
}

const runSummarySelect = `
  SELECT
    r.id, r.question, r.status, r.total,
    r.workflow_run_id AS "workflowRunId",
    r.parent_run_id AS "parentRunId",
    r.retry_count AS "retryCount",
    r.idempotency_key AS "idempotencyKey",
    r.created_by AS "createdBy",
    r.created_at AS "createdAt",
    r.updated_at AS "updatedAt",
    r.finished_at AS "finishedAt",
    COALESCE(i.succeeded, 0) AS "succeeded",
    COALESCE(i.failed, 0) AS "failed",
    COALESCE(i.cancelled, 0) AS "cancelled",
    COALESCE(i.active, 0) AS "active",
    COALESCE(b.total_batches, 0) AS "totalBatches",
    COALESCE(b.completed_batches, 0) AS "completedBatches"
  FROM bulk_runs AS r
  LEFT JOIN (
    SELECT run_id,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS succeeded,
      COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
      COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
      COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active
    FROM run_items GROUP BY run_id
  ) AS i ON i.run_id = r.id
  LEFT JOIN (
    SELECT run_id,
      COUNT(*)::int AS total_batches,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_batches
    FROM run_batches GROUP BY run_id
  ) AS b ON b.run_id = r.id
`;

export async function listRuns(limit = 10): Promise<RunSummary[]> {
  await markStaleQueuedRunsFailed();
  const rows = await query<RunSummary>(
    "runs.list",
    `${runSummarySelect} ORDER BY r.created_at DESC LIMIT $1`,
    [Math.min(Math.max(limit, 1), 50)],
  );

  const result: RunSummary[] = [];
  for (const row of rows) {
    result.push(row.active === 0 && row.status === "running" ? await finalizeRun(row.id) ?? row : row);
  }
  return result;
}

export async function getRun(id: string): Promise<RunSummary | null> {
  const rows = await query<RunSummary>("run.get", `${runSummarySelect} WHERE r.id = $1`, [id]);
  const run = rows[0];
  if (!run) return null;
  if (run.status === "running" && run.active === 0) return await finalizeRun(id) ?? run;
  return run;
}

export async function getRunItems(
  runId: string,
  options: { page?: number; pageSize?: number; status?: ItemStatus } = {},
) {
  const page = Math.max(options.page ?? 1, 1);
  const pageSize = Math.min(Math.max(options.pageSize ?? 100, 1), 500);
  const status = options.status ?? null;
  const rows = await query<RunItem & { totalCount: number }>(
    "run_items.page",
    `SELECT
       item_index AS "itemIndex",
       item_key AS "itemKey",
       status,
       session_id AS "sessionId",
       result_json AS result,
       error,
       error_code AS "errorCode",
       attempts,
       started_at AS "startedAt",
       finished_at AS "finishedAt",
       COUNT(*) OVER() AS "totalCount"
     FROM run_items
     WHERE run_id = $1 AND ($2::text IS NULL OR status = $2::text)
     ORDER BY item_index
     LIMIT $3 OFFSET $4`,
    [runId, status, pageSize, (page - 1) * pageSize],
  );
  return {
    items: rows.map(({ totalCount, ...item }) => item),
    total: rows[0]?.totalCount ?? 0,
    page,
    pageSize,
  };
}

export async function countActiveRuns() {
  const rows = await query<{ count: number }>(
    "runs.active.count",
    "SELECT COUNT(*)::int AS count FROM bulk_runs WHERE status IN ('queued', 'running')",
  );
  return rows[0]?.count ?? 0;
}

export async function setWorkflowRunId(id: string, workflowRunId: string) {
  await query(
    "run.set_workflow_id",
    "UPDATE bulk_runs SET workflow_run_id = $1, updated_at = $2 WHERE id = $3",
    [workflowRunId, new Date().toISOString(), id],
  );
}

export async function markRunRunning(id: string) {
  await query(
    "run.mark_running",
    "UPDATE bulk_runs SET status = 'running', updated_at = $1 WHERE id = $2 AND status = 'queued'",
    [new Date().toISOString(), id],
  );
}

export async function markRunFailed(id: string, error = "The run failed before it could start.") {
  const now = new Date().toISOString();
  await query(
    "run_items.fail_before_start",
    `WITH failed_run AS (
       UPDATE bulk_runs
       SET status = 'failed', finished_at = $2::timestamptz, updated_at = $2::timestamptz
       WHERE id = $1 AND status IN ('queued', 'running')
       RETURNING id
     )
     UPDATE run_items
     SET status = 'failed', error = $3, error_code = 'RUN_FAILED', finished_at = $2::timestamptz, updated_at = $2::timestamptz
     WHERE run_id IN (SELECT id FROM failed_run) AND status IN ('queued', 'running')`,
    [id, now, error],
  );
}

export async function markRunCancelled(id: string) {
  const sql = getSql();
  const now = new Date().toISOString();
  await sql.transaction([
    sql`UPDATE bulk_runs
        SET status = 'cancelled', cancel_requested_at = ${now}, finished_at = ${now}, updated_at = ${now}
        WHERE id = ${id} AND status IN ('queued', 'running')`,
    sql`UPDATE run_items
        SET status = 'cancelled', finished_at = ${now}, updated_at = ${now}
        WHERE run_id = ${id} AND status IN ('queued', 'running')`,
    sql`UPDATE run_batches
        SET status = 'cancelled', finished_at = ${now}, updated_at = ${now}
        WHERE run_id = ${id} AND status IN ('queued', 'running')`,
  ]);
}

export async function getActiveBatchWorkflowIds(runId: string) {
  const rows = await query<{ workflowRunId: string }>(
    "run_batches.active_workflow_ids",
    "SELECT workflow_run_id AS \"workflowRunId\" FROM run_batches WHERE run_id = $1 AND status IN ('queued', 'running') AND workflow_run_id IS NOT NULL",
    [runId],
  );
  return rows.map((row) => row.workflowRunId);
}

export async function loadBatchPlan(runId: string) {
  const rows = await query<{ id: string }>(
    "run_batches.plan",
    "SELECT id FROM run_batches WHERE run_id = $1 ORDER BY batch_index",
    [runId],
  );
  return rows.map((row) => row.id);
}

export async function recordBatchStarted(batchId: string, workflowRunId: string) {
  const now = new Date().toISOString();
  await query(
    "run_batch.start",
    "UPDATE run_batches SET status = 'running', workflow_run_id = $1, updated_at = $2 WHERE id = $3",
    [workflowRunId, now, batchId],
  );
}

export async function loadBatchItemKeys(batchId: string) {
  const rows = await query<{ itemKey: string }>(
    "run_batch.items",
    `SELECT ri.item_key AS "itemKey"
     FROM run_items AS ri
     JOIN bulk_runs AS r ON r.id = ri.run_id
     WHERE ri.batch_id = $1 AND r.status <> 'cancelled'
     ORDER BY ri.item_index`,
    [batchId],
  );
  return rows.map((row) => row.itemKey);
}

export async function getExecutionItem(runId: string, itemKey: string) {
  const rows = await query<{
    question: string;
    context: unknown;
    status: ItemStatus;
  }>(
    "run_item.get_execution",
    `SELECT r.question, ri.context_json AS context, ri.status
     FROM bulk_runs AS r
     JOIN run_items AS ri ON ri.run_id = r.id
     WHERE r.id = $1 AND ri.item_key = $2`,
    [runId, itemKey],
  );
  return rows[0] ?? null;
}

export async function markItemRunning(runId: string, itemKey: string) {
  const now = new Date().toISOString();
  const rows = await query<{ sessionId: string | null; attempts: number }>(
    "run_item.mark_running",
    `UPDATE run_items AS i
     SET status = 'running', attempts = attempts + 1,
         started_at = COALESCE(started_at, $1), updated_at = $1
     WHERE i.run_id = $2 AND i.item_key = $3 AND i.status IN ('queued', 'running')
       AND EXISTS (
         SELECT 1 FROM bulk_runs AS r
         WHERE r.id = i.run_id AND r.status IN ('queued', 'running')
       )
     RETURNING session_id AS "sessionId", attempts`,
    [now, runId, itemKey],
  );
  return rows[0] ?? null;
}

export async function setItemSessionId(runId: string, itemKey: string, sessionId: string) {
  await query(
    "run_item.set_session_id",
    "UPDATE run_items SET session_id = $1, updated_at = $2 WHERE run_id = $3 AND item_key = $4",
    [sessionId, new Date().toISOString(), runId, itemKey],
  );
}

export async function clearItemSessionId(runId: string, itemKey: string) {
  await query(
    "run_item.clear_session_id",
    "UPDATE run_items SET session_id = NULL, updated_at = $1 WHERE run_id = $2 AND item_key = $3",
    [new Date().toISOString(), runId, itemKey],
  );
}

export async function markItemCompleted(runId: string, itemKey: string, result: unknown) {
  const now = new Date().toISOString();
  const rows = await query(
    "run_item.mark_completed",
    `UPDATE run_items
     SET status = 'completed', result_json = $1::jsonb, error = NULL, error_code = NULL,
         finished_at = $2::timestamptz, updated_at = $2::timestamptz
     WHERE run_id = $3 AND item_key = $4 AND status = 'running'
       AND EXISTS (
         SELECT 1 FROM bulk_runs AS r
         WHERE r.id = run_id AND r.status IN ('queued', 'running')
       )
     RETURNING item_key`,
    [JSON.stringify(result), now, runId, itemKey],
  );
  return rows.length > 0;
}

export async function markItemFailed(
  runId: string,
  itemKey: string,
  error: string,
  errorCode = "ITEM_FAILED",
) {
  const now = new Date().toISOString();
  await query(
    "run_item.mark_failed",
    `UPDATE run_items
     SET status = 'failed', error = $1, error_code = $2, finished_at = $3, updated_at = $3
     WHERE run_id = $4 AND item_key = $5 AND status IN ('queued', 'running')`,
    [error.slice(0, 4000), errorCode, now, runId, itemKey],
  );
}

export async function markBatchFailed(batchId: string, error: string) {
  const now = new Date().toISOString();
  await query(
    "run_batch.mark_failed",
    `WITH failed_batch AS (
       UPDATE run_batches
       SET status = 'failed', finished_at = $2::timestamptz, updated_at = $2::timestamptz
       WHERE id = $1 AND status IN ('queued', 'running')
       RETURNING id
     )
     UPDATE run_items
     SET status = 'failed', error = $3, error_code = 'BATCH_FAILED',
         finished_at = $2::timestamptz, updated_at = $2::timestamptz
     WHERE batch_id IN (SELECT id FROM failed_batch) AND status IN ('queued', 'running')`,
    [batchId, now, error.slice(0, 4000)],
  );
}

export async function finalizeBatch(batchId: string) {
  const now = new Date().toISOString();
  const rows = await query<{ succeeded: number; failed: number; status: BatchStatus }>(
    "run_batch.finalize",
    `UPDATE run_batches AS b
     SET
       succeeded = (SELECT COUNT(*) FROM run_items WHERE batch_id = b.id AND status = 'completed'),
       failed = (SELECT COUNT(*) FROM run_items WHERE batch_id = b.id AND status = 'failed'),
       status = CASE
         WHEN b.status = 'cancelled' THEN 'cancelled'
         WHEN EXISTS (
           SELECT 1 FROM run_items WHERE batch_id = b.id AND status IN ('queued', 'running')
         ) THEN 'running'
         ELSE 'completed'
       END,
       finished_at = CASE
         WHEN b.status = 'cancelled' THEN b.finished_at
         WHEN EXISTS (
           SELECT 1 FROM run_items WHERE batch_id = b.id AND status IN ('queued', 'running')
         ) THEN NULL
         ELSE $2::timestamptz
       END,
       updated_at = $2::timestamptz
     WHERE b.id = $1
     RETURNING succeeded, failed, status`,
    [batchId, now],
  );
  return rows[0] ?? null;
}

export async function finalizeRun(runId: string) {
  const now = new Date().toISOString();
  const rows = await query<RunSummary>(
    "run.finalize",
    `WITH final_run AS (
       UPDATE bulk_runs AS r
       SET
         status = CASE
           WHEN EXISTS (
             SELECT 1 FROM run_items WHERE run_id = r.id AND status IN ('queued', 'running')
           ) THEN 'running'
           WHEN EXISTS (
             SELECT 1 FROM run_items WHERE run_id = r.id AND status = 'failed'
           ) THEN 'completed_with_errors'
           ELSE 'completed'
         END,
         finished_at = CASE
           WHEN EXISTS (
             SELECT 1 FROM run_items WHERE run_id = r.id AND status IN ('queued', 'running')
           ) THEN NULL
           ELSE $2::timestamptz
         END,
         updated_at = $2::timestamptz
       WHERE r.id = $1 AND r.status = 'running'
       RETURNING id
     )
     SELECT
       r.id, r.question, r.status, r.total,
       r.workflow_run_id AS "workflowRunId",
       r.parent_run_id AS "parentRunId",
       r.retry_count AS "retryCount",
       r.idempotency_key AS "idempotencyKey",
       r.created_by AS "createdBy",
       r.created_at AS "createdAt",
       r.updated_at AS "updatedAt",
       r.finished_at AS "finishedAt",
       COALESCE(i.succeeded, 0) AS "succeeded",
       COALESCE(i.failed, 0) AS "failed",
       COALESCE(i.cancelled, 0) AS "cancelled",
       COALESCE(i.active, 0) AS "active",
       COALESCE(b.total_batches, 0) AS "totalBatches",
       COALESCE(b.completed_batches, 0) AS "completedBatches"
     FROM bulk_runs AS r
     LEFT JOIN (
       SELECT run_id,
         COUNT(*) FILTER (WHERE status = 'completed')::int AS succeeded,
         COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
         COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
         COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active
       FROM run_items GROUP BY run_id
     ) AS i ON i.run_id = r.id
     LEFT JOIN (
       SELECT run_id,
         COUNT(*)::int AS total_batches,
         COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_batches
       FROM run_batches GROUP BY run_id
     ) AS b ON b.run_id = r.id
     WHERE r.id IN (SELECT id FROM final_run) OR r.id = $1`,
    [runId, now],
  );
  return rows[0] ?? null;
}

export async function markStaleQueuedRunsFailed() {
  await query(
    "runs.reconcile_stale",
    `WITH stale_runs AS (
       UPDATE bulk_runs
       SET status = 'failed', finished_at = now(), updated_at = now()
       WHERE status = 'queued'
         AND workflow_run_id IS NULL
         AND created_at < now() - interval '2 minutes'
       RETURNING id
     )
     UPDATE run_items
     SET status = 'failed', error = 'Workflow start timed out.',
         error_code = 'WORKFLOW_START_TIMEOUT', finished_at = now(), updated_at = now()
     WHERE run_id IN (SELECT id FROM stale_runs) AND status = 'queued'`,
  );
}

export async function getFailedItemsForRetry(runId: string) {
  return query<{ key: string; context: unknown }>(
    "run_items.failed_for_retry",
    `SELECT item_key AS key, context_json AS context
     FROM run_items
     WHERE run_id = $1 AND status = 'failed'
     ORDER BY item_index`,
    [runId],
  );
}

export async function getRetryCount(runId: string) {
  const rows = await query<{ count: number }>(
    "run.retry_count",
    "SELECT COUNT(*)::int AS count FROM bulk_runs WHERE parent_run_id = $1",
    [runId],
  );
  return rows[0]?.count ?? 0;
}
