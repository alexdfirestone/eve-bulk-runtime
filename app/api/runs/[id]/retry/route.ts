import { NextResponse } from "next/server";
import { start } from "workflow/api";
import {
  countActiveRuns,
  createRun,
  getFailedItemsForRetry,
  getRetryCount,
  getRun,
  markRunFailed,
  setWorkflowRunId,
} from "../../../../../lib/db";
import { logger } from "../../../../../lib/logger";
import { maxActiveRuns, normalizeCreateRun } from "../../../../../lib/run-contract";
import { ensureCoordinatorWorkflowWorld } from "../../../../../lib/workflow-runtime";
import { bulkRun } from "../../../../../workflows/bulk-run";

export const runtime = "nodejs";

export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  let retryRunId: string | undefined;

  try {
    const source = await getRun(id);
    if (!source) return NextResponse.json({ error: "Run not found." }, { status: 404 });
    if (source.status === "queued" || source.status === "running") {
      return NextResponse.json({ error: "Wait for the run to finish before retrying." }, { status: 409 });
    }

    const failedItems = await getFailedItemsForRetry(id);
    if (failedItems.length === 0) {
      return NextResponse.json({ error: "This run has no failed items." }, { status: 400 });
    }
    if (await countActiveRuns() >= maxActiveRuns()) {
      return NextResponse.json({ error: "Too many active runs." }, { status: 429 });
    }

    const retryCount = await getRetryCount(id);
    const idempotencyKey = `${id}:retry:${retryCount + 1}`;
    const input = normalizeCreateRun({
      question: source.question,
      items: failedItems,
      targetConcurrency: 25,
      idempotencyKey,
    });

    const created = await createRun({
      question: input.question,
      items: input.items,
      idempotencyKey,
      parentRunId: id,
      retryCount: retryCount + 1,
    });
    retryRunId = created.id;
    if (created.existing) {
      return NextResponse.json({ runId: retryRunId, run: await getRun(retryRunId) }, { status: 200 });
    }

    await ensureCoordinatorWorkflowWorld();
    const workflowRun = await start(bulkRun, [retryRunId, input.settings]);
    await setWorkflowRunId(retryRunId, workflowRun.runId);

    logger.info("api.run.retry.accepted", {
      sourceRunId: id,
      retryRunId,
      workflowRunId: workflowRun.runId,
      itemCount: failedItems.length,
    });
    return NextResponse.json(
      {
        runId: retryRunId,
        workflowRunId: workflowRun.runId,
        run: await getRun(retryRunId),
        settings: input.settings,
      },
      { status: 202 },
    );
  } catch (error) {
    logger.error("api.run.retry.failed", error, { sourceRunId: id, retryRunId });
    if (retryRunId) await markRunFailed(retryRunId).catch(() => undefined);
    return NextResponse.json({ error: "Unable to retry the run." }, { status: 500 });
  }
}
