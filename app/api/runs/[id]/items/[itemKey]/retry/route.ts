import { NextResponse } from "next/server";
import { start } from "workflow/api";
import {
  countActiveRuns,
  createRun,
  getExecutionItem,
  getRun,
  markRunFailed,
  setWorkflowRunId,
} from "../../../../../../../lib/db";
import { logger } from "../../../../../../../lib/logger";
import { maxActiveRuns, normalizeCreateRun } from "../../../../../../../lib/run-contract";
import { ensureCoordinatorWorkflowWorld } from "../../../../../../../lib/workflow-runtime";
import { bulkRun } from "../../../../../../../workflows/bulk-run";

export const runtime = "nodejs";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ id: string; itemKey: string }> },
) {
  const { id, itemKey } = await params;
  let rerunId: string | undefined;

  try {
    const source = await getRun(id);
    if (!source) return NextResponse.json({ error: "Run not found." }, { status: 404 });

    const item = await getExecutionItem(id, itemKey);
    if (!item) return NextResponse.json({ error: "Item not found." }, { status: 404 });
    if (item.status === "queued" || item.status === "running") {
      return NextResponse.json({ error: "Wait for the item to finish before rerunning it." }, { status: 409 });
    }
    if (await countActiveRuns() >= maxActiveRuns()) {
      return NextResponse.json({ error: "Too many active runs." }, { status: 429 });
    }

    const input = normalizeCreateRun({
      question: source.question,
      items: [{ key: itemKey, context: item.context }],
      targetConcurrency: 1,
      idempotencyKey: crypto.randomUUID(),
    });
    const created = await createRun({
      question: input.question,
      items: input.items,
      idempotencyKey: input.idempotencyKey,
      parentRunId: id,
      retryCount: source.retryCount + 1,
    });
    rerunId = created.id;

    await ensureCoordinatorWorkflowWorld();
    const workflowRun = await start(bulkRun, [rerunId, input.settings]);
    await setWorkflowRunId(rerunId, workflowRun.runId);
    logger.info("api.item.rerun.accepted", {
      sourceRunId: id,
      sourceItemKey: itemKey,
      rerunId,
      workflowRunId: workflowRun.runId,
    });
    return NextResponse.json({ runId: rerunId, workflowRunId: workflowRun.runId, run: await getRun(rerunId) }, { status: 202 });
  } catch (error) {
    logger.error("api.item.rerun.failed", error, { sourceRunId: id, itemKey, rerunId });
    if (rerunId) await markRunFailed(rerunId).catch(() => undefined);
    return NextResponse.json({ error: "Unable to rerun the item." }, { status: 500 });
  }
}
