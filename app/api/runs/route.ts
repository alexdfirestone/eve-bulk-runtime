import { NextResponse } from "next/server";
import { start } from "workflow/api";
import {
  countActiveRuns,
  createRun,
  getRun,
  getRunIdByIdempotencyKey,
  listRuns,
  markRunFailed,
  setWorkflowRunId,
} from "../../../lib/db";
import { logger } from "../../../lib/logger";
import { maxActiveRuns, normalizeCreateRun } from "../../../lib/run-contract";
import { ensureCoordinatorWorkflowWorld } from "../../../lib/workflow-runtime";
import { bulkRun } from "../../../workflows/bulk-run";

export const runtime = "nodejs";

export async function GET() {
  try {
    return NextResponse.json(await listRuns());
  } catch (error) {
    logger.error("api.runs.list.failed", error);
    return NextResponse.json({ error: "Unable to load runs." }, { status: 500 });
  }
}

export async function POST(request: Request) {
  const requestId = crypto.randomUUID();
  const startedAt = Date.now();
  let runId: string | undefined;

  try {
    const body = await request.json();
    const input = normalizeCreateRun(body, request.headers.get("idempotency-key"));
    runId = input.idempotencyKey ? await getRunIdByIdempotencyKey(input.idempotencyKey) : undefined;

    if (runId) {
      const run = await getRun(runId);
      return NextResponse.json({ runId, workflowRunId: run?.workflowRunId, run }, { status: 200 });
    }

    if (await countActiveRuns() >= maxActiveRuns()) {
      return NextResponse.json(
        { error: "Too many active runs. Try again after a run finishes." },
        { status: 429, headers: { "x-request-id": requestId } },
      );
    }

    logger.info("api.run.create.started", {
      requestId,
      itemCount: input.items.length,
      requestedConcurrency: input.settings.activeChildren * input.settings.itemConcurrency,
    });

    const created = await createRun({
      question: input.question,
      items: input.items,
      idempotencyKey: input.idempotencyKey,
    });
    runId = created.id;
    if (created.existing) {
      const run = await getRun(runId);
      return NextResponse.json({ runId, workflowRunId: run?.workflowRunId, run }, { status: 200 });
    }

    await ensureCoordinatorWorkflowWorld();
    const workflowRun = await start(bulkRun, [runId, input.settings]);
    await setWorkflowRunId(runId, workflowRun.runId);

    logger.info("api.run.create.accepted", {
      requestId,
      runId,
      workflowRunId: workflowRun.runId,
      itemCount: input.items.length,
      settings: input.settings,
      durationMs: Date.now() - startedAt,
    });

    return NextResponse.json(
      {
        runId,
        workflowRunId: workflowRun.runId,
        run: await getRun(runId),
        settings: input.settings,
      },
      { status: 202 },
    );
  } catch (error) {
    logger.error("api.run.create.failed", error, {
      requestId,
      runId,
      durationMs: Date.now() - startedAt,
    });
    if (runId) await markRunFailed(runId).catch(() => undefined);

    const message = error instanceof Error ? error.message : "Unable to start the bulk run.";
    return NextResponse.json(
      { error: message, requestId },
      { status: message.includes("Provide either") || message.includes("exceeds") ? 400 : 500 },
    );
  }
}
