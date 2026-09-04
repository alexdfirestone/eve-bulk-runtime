"use client";

import { useEffect, useMemo, useRef, useState } from "react";

type RunStatus =
  | "queued"
  | "running"
  | "completed"
  | "completed_with_errors"
  | "failed"
  | "cancelled";

type ItemStatus = "queued" | "running" | "completed" | "failed" | "cancelled";

type RunSummary = {
  id: string;
  question: string;
  status: RunStatus;
  total: number;
  workflowRunId: string | null;
  parentRunId: string | null;
  retryCount: number;
  createdAt: string;
  succeeded: number;
  failed: number;
  cancelled: number;
  active: number;
  totalBatches: number;
  completedBatches: number;
};

type RunItem = {
  itemIndex: number;
  itemKey: string;
  status: ItemStatus;
  sessionId: string | null;
  result: unknown;
  error: string | null;
  errorCode: string | null;
  attempts: number;
};

type ItemsResponse = {
  items: RunItem[];
  total: number;
  page: number;
  pageSize: number;
};

const itemStatuses: Array<ItemStatus | "all"> = ["all", "queued", "running", "completed", "failed", "cancelled"];

export default function Home() {
  const [question, setQuestion] = useState("What is the estimated value?");
  const [count, setCount] = useState(100);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [itemsResponse, setItemsResponse] = useState<ItemsResponse | null>(null);
  const [itemStatus, setItemStatus] = useState<ItemStatus | "all">("all");
  const [page, setPage] = useState(1);
  const [itemsVersion, setItemsVersion] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [busyRunIds, setBusyRunIds] = useState<string[]>([]);
  const activeStreams = useRef(new Set<string>());

  const selectedRun = useMemo(
    () => runs.find((run) => run.id === selectedRunId) ?? null,
    [runs, selectedRunId],
  );
  const hasActiveRuns = runs.some((run) => run.status === "queued" || run.status === "running");

  function headers(includeJson = false) {
    const result = new Headers();
    if (includeJson) result.set("content-type", "application/json");
    return result;
  }

  async function refreshRuns() {
    const response = await fetch("/api/runs", { cache: "no-store", headers: headers() });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error ?? "Unable to load runs.");
    setRuns(data as RunSummary[]);
    return data as RunSummary[];
  }

  async function loadItems() {
    if (!selectedRunId) {
      setItemsResponse(null);
      return;
    }
    const params = new URLSearchParams({ page: String(page), pageSize: "100" });
    if (itemStatus !== "all") params.set("status", itemStatus);
    const response = await fetch(`/api/runs/${selectedRunId}/items?${params}`, {
      cache: "no-store",
      headers: headers(),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error ?? "Unable to load items.");
    setItemsResponse(data as ItemsResponse);
  }

  useEffect(() => {
    refreshRuns()
      .then((initialRuns) => {
        if (initialRuns[0]) setSelectedRunId(initialRuns[0].id);
      })
      .catch((error) => setError(error.message));
  }, []);

  useEffect(() => {
    loadItems().catch((error) => setError(error.message));
  }, [selectedRunId, itemStatus, page, itemsVersion]);

  useEffect(() => {
    if (!hasActiveRuns) return;
    const interval = setInterval(() => {
      refreshRuns().catch(() => undefined);
      if (selectedRunId) loadItems().catch(() => undefined);
    }, 2_000);
    return () => clearInterval(interval);
  }, [hasActiveRuns, selectedRunId, itemStatus, page]);

  async function streamRun(runId: string, attempt = 0) {
    if (activeStreams.current.has(runId) || attempt >= 5) return;
    activeStreams.current.add(runId);

    try {
      const response = await fetch(`/api/runs/${runId}/stream`, { headers: headers() });
      if (response.status === 503) {
        activeStreams.current.delete(runId);
        setTimeout(() => void streamRun(runId, attempt + 1), 1_000);
        return;
      }
      if (!response.ok || !response.body) return;

      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let buffer = "";
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        buffer += chunk.value;
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines.filter(Boolean)) {
          try {
            const event = JSON.parse(line) as { type: string };
            if (event.type === "batches" || event.type === "run") {
              await refreshRuns().catch(() => undefined);
            }
          } catch {
            // Ignore an incomplete NDJSON line.
          }
        }
      }
    } finally {
      activeStreams.current.delete(runId);
      await refreshRuns().catch(() => undefined);
      setItemsVersion((version) => version + 1);
    }
  }

  useEffect(() => {
    for (const run of runs) {
      if (run.status === "queued" || run.status === "running") void streamRun(run.id);
    }
  }, [runs]);

  async function setBusy(runId: string, update: () => Promise<void>) {
    setBusyRunIds((current) => [...current, runId]);
    try {
      await update();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusyRunIds((current) => current.filter((id) => id !== runId));
    }
  }

  async function startRun() {
    setError(null);
    const idempotencyKey = crypto.randomUUID();
    try {
      const response = await fetch("/api/runs", {
        method: "POST",
        headers: headers(true),
      body: JSON.stringify({ question, count, idempotencyKey }),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "Unable to start the run.");
      const nextRuns = await refreshRuns();
      const run = (data.run ?? nextRuns[0]) as RunSummary | undefined;
      if (run) {
        setSelectedRunId(run.id);
        setPage(1);
        setItemsVersion((version) => version + 1);
        void streamRun(run.id);
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  function cancelRun(runId: string) {
    return setBusy(runId, async () => {
      const response = await fetch(`/api/runs/${runId}/cancel`, { method: "POST", headers: headers() });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "Unable to cancel the run.");
      await refreshRuns();
      setItemsVersion((version) => version + 1);
    });
  }

  function retryRun(runId: string) {
    return setBusy(runId, async () => {
      const response = await fetch(`/api/runs/${runId}/retry`, { method: "POST", headers: headers() });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "Unable to retry the run.");
      await refreshRuns();
      if (data.runId) {
        setSelectedRunId(data.runId as string);
        setPage(1);
        setItemsVersion((version) => version + 1);
        void streamRun(data.runId as string);
      }
    });
  }

  function rerunItem(runId: string, itemKey: string) {
    const busyKey = `${runId}:${itemKey}`;
    return setBusy(busyKey, async () => {
      const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/items/${encodeURIComponent(itemKey)}/retry`, {
        method: "POST",
        headers: headers(),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "Unable to rerun the item.");
      await refreshRuns();
      if (data.runId) {
        setSelectedRunId(data.runId as string);
        setPage(1);
        setItemsVersion((version) => version + 1);
        void streamRun(data.runId as string);
      }
    });
  }

  const totalPages = itemsResponse ? Math.max(1, Math.ceil(itemsResponse.total / itemsResponse.pageSize)) : 1;

  return <main>
    <h1>Bulk Runtime</h1>
    <p>One question, thousands of independent eve sessions, partitioned into small durable Workflow runs.</p>

    <div className="toolbar">
      <label className="control grow">
        <span>Question</span>
        <input value={question} onChange={(event) => setQuestion(event.target.value)} size={38} />
      </label>
      <label className="control">
        <span>Items</span>
        <input aria-label="item count" type="number" min={1} max={10000} value={count}
          onChange={(event) => setCount(Number(event.target.value))} />
      </label>
      <button onClick={startRun}>Start run</button>
    </div>

    {error && <div className="error">{error}</div>}

    {runs.map((run) => {
      const succeededRate = run.total === 0 ? 0 : Math.round((run.succeeded / run.total) * 100);
      const failedRate = run.total === 0 ? 0 : Math.round((run.failed / run.total) * 100);
      const isActive = run.status === "queued" || run.status === "running";
      const canRetry = !isActive && run.failed > 0;
      const isSelected = run.id === selectedRunId;

      return <section className="run" key={run.id}>
        <div className="run-head">
          <div>
            <strong>{run.question}</strong>
            <div className="muted">
              Run {run.id.slice(0, 8)}
              {run.parentRunId ? ` · retry of ${run.parentRunId.slice(0, 8)}` : ""}
              {run.workflowRunId ? ` · workflow ${run.workflowRunId.slice(0, 12)}` : ""}
            </div>
          </div>
          <span className={`pill ${run.status}`}>{run.status.replace(/_/g, " ")}</span>
        </div>

        <div className="stats">
          <span>{run.succeeded + run.failed + run.cancelled}/{run.total} finished</span>
          <span>Success {run.succeeded} ({succeededRate}%)</span>
          <span>Failed {run.failed} ({failedRate}%)</span>
          <span>Active {run.active}</span>
          <span>Batches {run.completedBatches}/{run.totalBatches}</span>
        </div>

        <div className="actions">
          <button className={isSelected ? "" : "secondary"} onClick={() => {
            setSelectedRunId(run.id);
            setPage(1);
            setItemsVersion((version) => version + 1);
          }}>View items</button>
          {isActive && <button className="secondary danger" disabled={busyRunIds.includes(run.id)}
            onClick={() => void cancelRun(run.id)}>Cancel</button>}
          {canRetry && <button className="secondary" disabled={busyRunIds.includes(run.id)}
            onClick={() => void retryRun(run.id)}>Retry failed</button>}
        </div>
      </section>;
    })}

    {selectedRun && <>
      <section className="run">
        <div className="run-head">
          <h2>Items</h2>
          <label className="inline-control">
            Status
            <select value={itemStatus} onChange={(event) => {
              setItemStatus(event.target.value as ItemStatus | "all");
              setPage(1);
            }}>
              {itemStatuses.map((status) => <option key={status} value={status}>{status}</option>)}
            </select>
          </label>
        </div>

        <table>
          <thead>
            <tr><th>Item</th><th>Status</th><th>Result</th><th>Error</th><th>Attempts</th><th>Action</th></tr>
          </thead>
          <tbody>
            {itemsResponse?.items.map((item) => <tr key={item.itemKey}>
              <td>{item.itemKey}</td>
              <td><span className={`pill ${item.status}`}>{item.status}</span></td>
              <td className="result">
                {item.result && typeof item.result === "object" && "answer" in item.result
                  ? String((item.result as { answer: unknown }).answer)
                  : "—"}
              </td>
              <td className="error-cell">{item.error ?? "—"}</td>
              <td>{item.attempts}</td>
              <td>
                {(item.status === "failed" || item.status === "completed") && <button className="secondary"
                  disabled={busyRunIds.includes(`${selectedRunId}:${item.itemKey}`)}
                  onClick={() => selectedRunId && void rerunItem(selectedRunId, item.itemKey)}>Rerun</button>}
              </td>
            </tr>)}
          </tbody>
        </table>

        <div className="pagination">
          <button className="secondary" disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button>
          <span>Page {page} of {totalPages} · {itemsResponse?.total ?? 0} items</span>
          <button className="secondary" disabled={page >= totalPages} onClick={() => setPage((value) => value + 1)}>Next</button>
        </div>
      </section>
    </>}
  </main>;
}
