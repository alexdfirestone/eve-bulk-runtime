import { getRun as getWorkflowRun } from "workflow/api";
import { getRun } from "../../../../../lib/db";

export const runtime = "nodejs";

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const run = getRun(id) as { workflow_run_id?: string } | null;
  if (!run?.workflow_run_id) return new Response("run is not ready\n", { status: 404 });

  const workflowRun = getWorkflowRun(run.workflow_run_id);
  return new Response(workflowRun.getReadable(), {
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-cache" },
  });
}
