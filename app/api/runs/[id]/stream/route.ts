import { getRun as getWorkflowRun } from "workflow/api";
import { getRun } from "../../../../../lib/db";
import { logger } from "../../../../../lib/logger";
import { ensureCoordinatorWorkflowWorld } from "../../../../../lib/workflow-runtime";

export const runtime = "nodejs";

export async function GET(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  try {
    const run = await getRun(id);
    if (!run) return new Response("run not found\n", { status: 404 });
    if (!run.workflowRunId) {
      logger.warn("api.run.stream.starting", { runId: id, status: run.status });
      return new Response("run is still starting\n", {
        status: 503,
        headers: { "Retry-After": "1", "Content-Type": "text/plain; charset=utf-8" },
      });
    }

    await ensureCoordinatorWorkflowWorld();
    const workflowRun = getWorkflowRun(run.workflowRunId);
    return new Response(workflowRun.getReadable(), {
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
      },
    });
  } catch (error) {
    logger.error("api.run.stream.failed", error, { runId: id });
    return new Response("unable to stream run\n", { status: 500 });
  }
}
