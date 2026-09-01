import { NextResponse } from "next/server";
import { start } from "workflow/api";
import { createRun, getRun, listRuns, setWorkflowRunId } from "../../../lib/db";
import { bulkRun } from "../../../workflows/bulk-run";

export const runtime = "nodejs";

export async function GET() {
  return NextResponse.json(listRuns());
}

export async function POST(request: Request) {
  const body = await request.json() as { question?: string; count?: number; concurrency?: number };
  const question = body.question?.trim() || "What is the estimated value?";
  const count = Math.min(Math.max(body.count ?? 10, 1), 10_000);
  const concurrency = Math.min(Math.max(body.concurrency ?? 5, 1), 250);
  const items = Array.from({ length: count }, (_, index) => ({ key: `item-${String(index + 1).padStart(4, "0")}`, context: { index: index + 1, sampleValue: 1000 + index * 25 } }));
  const runId = createRun(question, items);
  const workflowRun = await start(bulkRun, [runId, question, items, concurrency]);
  setWorkflowRunId(runId, workflowRun.runId);
  return NextResponse.json({ runId, workflowRunId: workflowRun.runId, run: getRun(runId) }, { status: 202 });
}
