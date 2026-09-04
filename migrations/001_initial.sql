-- v2 bulk runtime schema. This project currently contains disposable test
-- data, so reset the legacy tables before creating the canonical schema.
DROP TABLE IF EXISTS run_items;
DROP TABLE IF EXISTS run_batches;
DROP TABLE IF EXISTS bulk_runs;

CREATE TABLE IF NOT EXISTS bulk_runs (
  id TEXT PRIMARY KEY,
  question TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'running', 'completed', 'completed_with_errors', 'failed', 'cancelled')
  ),
  total INTEGER NOT NULL CHECK (total >= 0),
  workflow_run_id TEXT,
  parent_run_id TEXT REFERENCES bulk_runs(id) ON DELETE SET NULL,
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  idempotency_key TEXT UNIQUE,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  cancel_requested_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS bulk_runs_created_at_idx ON bulk_runs (created_at DESC);
CREATE INDEX IF NOT EXISTS bulk_runs_status_idx ON bulk_runs (status);
CREATE INDEX IF NOT EXISTS bulk_runs_parent_run_id_idx ON bulk_runs (parent_run_id);

CREATE TABLE IF NOT EXISTS run_batches (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES bulk_runs(id) ON DELETE CASCADE,
  batch_index INTEGER NOT NULL CHECK (batch_index >= 0),
  start_index INTEGER NOT NULL CHECK (start_index >= 0),
  size INTEGER NOT NULL CHECK (size > 0),
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'running', 'completed', 'failed', 'cancelled')
  ),
  workflow_run_id TEXT,
  succeeded INTEGER NOT NULL DEFAULT 0 CHECK (succeeded >= 0),
  failed INTEGER NOT NULL DEFAULT 0 CHECK (failed >= 0),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS run_batches_run_index_idx ON run_batches (run_id, batch_index);
CREATE INDEX IF NOT EXISTS run_batches_run_status_idx ON run_batches (run_id, status);

CREATE TABLE IF NOT EXISTS run_items (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES bulk_runs(id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL REFERENCES run_batches(id) ON DELETE CASCADE,
  item_index INTEGER NOT NULL CHECK (item_index >= 0),
  item_key TEXT NOT NULL,
  context_json JSONB NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'running', 'completed', 'failed', 'cancelled')
  ),
  session_id TEXT,
  result_json JSONB,
  error TEXT,
  error_code TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS run_items_run_key_idx ON run_items (run_id, item_key);
CREATE INDEX IF NOT EXISTS run_items_run_index_idx ON run_items (run_id, item_index);
CREATE INDEX IF NOT EXISTS run_items_run_status_idx ON run_items (run_id, status);
CREATE INDEX IF NOT EXISTS run_items_batch_status_idx ON run_items (batch_id, status);
