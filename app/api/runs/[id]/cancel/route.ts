import { NextResponse } from "next/server";
import { getRun as getWorkflowRun } from "workflow/api";
import { getActiveBatchWorkflowIds, getRun, markRunCancelled } from "../../../../../lib/db";
import { logger } from "../../../../../lib/logger";
import { ensureCoordinatorWorkflowWorld } from "../../../../../lib/workflow-runtime";

export const runtime = "nodejs";

export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  try {
    const run = await getRun(id);
    if (!run) return NextResponse.json({ error: "Run not found." }, { status: 404 });
    if (run.status !== "queued" && run.status !== "running") {
      return NextResponse.json({ run });
    }

    await ensureCoordinatorWorkflowWorld();
    const childWorkflowIds = await getActiveBatchWorkflowIds(id);

    if (run.workflowRunId) {
      await getWorkflowRun(run.workflowRunId).cancel({ cancelReason: "Cancelled from the bulk dashboard." });
    }
    await Promise.allSettled(
      childWorkflowIds.map((workflowId) =>
        getWorkflowRun(workflowId).cancel({ cancelReason: "Parent bulk run was cancelled." }),
      ),
    );

    await markRunCancelled(id);
    logger.info("api.run.cancelled", { runId: id, workflowRunId: run.workflowRunId });
    return NextResponse.json({ run: await getRun(id) });
  } catch (error) {
    logger.error("api.run.cancel.failed", error, { runId: id });
    return NextResponse.json({ error: "Unable to cancel the run." }, { status: 500 });
  }
}
