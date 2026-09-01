"use client";

import { useEffect, useRef, useState } from "react";

type Run = { id: string; question: string; status: string; total: number; completed: number; items: Array<{ item_key: string; status: string; result_json: string | null; error: string | null }> };

export default function Home() {
  const [question, setQuestion] = useState("What is the estimated value?");
  const [count, setCount] = useState(10);
  const [concurrency, setConcurrency] = useState(5);
  const [runs, setRuns] = useState<Run[]>([]);
  const activeStreams = useRef(new Set<string>());

  async function refresh(): Promise<Run[]> {
    const response = await fetch("/api/runs", { cache: "no-store" });
    const data = await response.json() as Run[];
    setRuns(data);
    return data;
  }

  useEffect(() => {
    refresh().then((initialRuns) => {
      for (const run of initialRuns) {
        if (run.status === "queued" || run.status === "running") void streamRun(run.id);
      }
    });
  }, []);

  async function streamRun(runId: string) {
    if (activeStreams.current.has(runId)) return;
    activeStreams.current.add(runId);
    const response = await fetch(`/api/runs/${runId}/stream`);
    if (!response.ok || !response.body) {
      activeStreams.current.delete(runId);
      return;
    }
    const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
    let buffer = "";
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      buffer += chunk.value;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines.filter(Boolean)) {
        try {
          const event = JSON.parse(line) as { type: string; key?: string; status?: string; result?: unknown; error?: string };
          if (event.type === "item" && event.key && event.status) {
            setRuns((current) => current.map((run) => {
              if (run.id !== runId) return run;
              const items = run.items.map((item) => item.item_key !== event.key ? item : {
                ...item,
                status: event.status!,
                result_json: event.result === undefined ? item.result_json : JSON.stringify(event.result),
                error: event.error ?? (event.status === "failed" ? item.error : null),
              });
              const completed = items.filter((item) => item.status === "completed" || item.status === "failed").length;
              return { ...run, status: run.status === "queued" ? "running" : run.status, completed, items };
            }));
          }
        } catch {
          // Ignore an incomplete or non-JSON stream line.
        }
      }
    }
    activeStreams.current.delete(runId);
    await refresh();
  }

  async function startRun() {
    const response = await fetch("/api/runs", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ question, count, concurrency }) });
    const data = await response.json() as { runId: string };
    await refresh();
    void streamRun(data.runId);
  }

  return <main>
    <h1>Bulk Runtime</h1>
    <p>One question, many independent eve sessions, with context and status tracked per item.</p>
    <div className="toolbar"><input value={question} onChange={(e) => setQuestion(e.target.value)} size={38} /><label className="control"><span>Items</span><input aria-label="item count" type="number" min={1} max={10000} value={count} onChange={(e) => setCount(Number(e.target.value))} /></label><label className="control"><span>Concurrency</span><input aria-label="concurrency" type="number" min={1} max={250} value={concurrency} onChange={(e) => setConcurrency(Number(e.target.value))} /></label><button onClick={startRun}>Start run</button></div>
    {runs.map((run) => <section className="run" key={run.id}>
      {(() => {
        const succeeded = run.items.filter((item) => item.status === "completed").length;
        const failed = run.items.filter((item) => item.status === "failed").length;
        const active = run.total - succeeded - failed;
        const rate = (value: number) => `${Math.round((value / run.total) * 100)}%`;
        return <>
      <div className="run-head"><strong>{run.question}</strong><span className="pill">{run.status}</span></div>
      <div className="stats">
        <span>{run.completed}/{run.total} complete</span>
        <span>Success {succeeded} ({rate(succeeded)})</span>
        <span>Failed {failed} ({rate(failed)})</span>
        <span>Active {active}</span>
        <span>Run {run.id.slice(0, 8)}</span>
      </div>
      <table><thead><tr><th>Item</th><th>Status</th><th>Result</th><th>Error</th></tr></thead><tbody>{run.items.map((item) => <tr key={item.item_key}><td>{item.item_key}</td><td><span className="pill">{item.status}</span></td><td>{item.result_json ? JSON.parse(item.result_json).answer : "—"}</td><td>{item.error ?? "—"}</td></tr>)}</tbody></table>
        </>;
      })()}
    </section>)}
  </main>;
}
