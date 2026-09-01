import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";

const databasePath = process.env.SQLITE_PATH ?? join(process.cwd(), ".data", "runs.db");
mkdirSync(dirname(databasePath), { recursive: true });

const db = new DatabaseSync(databasePath);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS bulk_runs (
    id TEXT PRIMARY KEY,
    question TEXT NOT NULL,
    status TEXT NOT NULL,
    total INTEGER NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    workflow_run_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS run_items (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    context_json TEXT NOT NULL,
    status TEXT NOT NULL,
    session_id TEXT,
    result_json TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, item_key)
  );
`);
try { db.exec("ALTER TABLE bulk_runs ADD COLUMN workflow_run_id TEXT"); } catch {}

export type RunStatus = "queued" | "running" | "completed" | "failed";
export type ItemStatus = "queued" | "running" | "completed" | "failed";

export function createRun(question: string, items: Array<{ key: string; context: unknown }>) {
  const id = crypto.randomUUID();
  const now = new Date().toISOString();
  db.prepare("INSERT INTO bulk_runs (id, question, status, total, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)")
    .run(id, question, "queued", items.length, now, now);
  const insert = db.prepare("INSERT INTO run_items (id, run_id, item_key, context_json, status, updated_at) VALUES (?, ?, ?, ?, ?, ?)");
  for (const item of items) insert.run(crypto.randomUUID(), id, item.key, JSON.stringify(item.context), "queued", now);
  return id;
}

export function getRun(id: string) {
  const run = db.prepare("SELECT * FROM bulk_runs WHERE id = ?").get(id) as Record<string, unknown> | undefined;
  if (!run) return null;
  const items = db.prepare("SELECT * FROM run_items WHERE run_id = ? ORDER BY rowid").all(id);
  return { ...run, items };
}

export function listRuns() {
  const runs = db.prepare("SELECT * FROM bulk_runs ORDER BY created_at DESC LIMIT 10").all() as Array<Record<string, unknown>>;
  const items = db.prepare("SELECT * FROM run_items WHERE run_id = ? ORDER BY rowid");
  return runs.map((run) => ({ ...run, items: items.all(String(run.id)) }));
}

export function setWorkflowRunId(id: string, workflowRunId: string) {
  db.prepare("UPDATE bulk_runs SET workflow_run_id = ?, updated_at = ? WHERE id = ?").run(workflowRunId, new Date().toISOString(), id);
}

export function markRunRunning(id: string) {
  db.prepare("UPDATE bulk_runs SET status = 'running', updated_at = ? WHERE id = ?").run(new Date().toISOString(), id);
}

export function markItemRunning(runId: string, itemKey: string, sessionId?: string) {
  db.prepare("UPDATE run_items SET status = 'running', session_id = COALESCE(?, session_id), attempts = attempts + 1, updated_at = ? WHERE run_id = ? AND item_key = ?")
    .run(sessionId ?? null, new Date().toISOString(), runId, itemKey);
  markRunRunning(runId);
}

export function markItemFinished(runId: string, itemKey: string, status: "completed" | "failed", result?: unknown, error?: string) {
  const now = new Date().toISOString();
  db.prepare("UPDATE run_items SET status = ?, result_json = ?, error = ?, updated_at = ? WHERE run_id = ? AND item_key = ?")
    .run(status, result ? JSON.stringify(result) : null, error ?? null, now, runId, itemKey);
  const remaining = db.prepare("SELECT COUNT(*) AS count FROM run_items WHERE run_id = ? AND status NOT IN ('completed', 'failed')").get(runId) as { count: number };
  const failed = db.prepare("SELECT COUNT(*) AS count FROM run_items WHERE run_id = ? AND status = 'failed'").get(runId) as { count: number };
  const nextStatus = remaining.count === 0 ? (failed.count > 0 ? "failed" : "completed") : "running";
  db.prepare("UPDATE bulk_runs SET completed = total - ?, status = ?, updated_at = ? WHERE id = ?")
    .run(remaining.count, nextStatus, now, runId);
}
