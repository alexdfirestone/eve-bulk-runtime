import { getVercelOidcToken } from "@vercel/oidc";
import {
  FatalError,
  RetryableError,
  defineHook,
  getWritable,
  getWorkflowMetadata,
} from "workflow";
import { start } from "workflow/api";
import { z } from "zod";
import {
  clearItemSessionId,
  finalizeBatch,
  finalizeRun,
  getExecutionItem,
  loadBatchItemKeys,
  loadBatchPlan,
  markBatchFailed,
  markItemCompleted,
  markItemFailed,
  markItemRunning,
  markRunRunning,
  recordBatchStarted,
  setItemSessionId,
  setWorkflowRunId,
} from "../lib/db";
import { logger } from "../lib/logger";

const outputSchema = {
  type: "object",
  properties: {
    answer: { type: "string" },
    value: { anyOf: [{ type: "number" }, { type: "null" }] },
    assumptions: { type: "array", items: { type: "string" } },
    evidence: { type: "array", items: { type: "string" } },
  },
  required: ["answer", "value", "assumptions", "evidence"],
  additionalProperties: false,
} as const;

const structuredOutputSchema = z.object({
  answer: z.string(),
  value: z.number().nullable(),
  assumptions: z.array(z.string()),
  evidence: z.array(z.string()),
});

type EveOutput = z.infer<typeof structuredOutputSchema>;
type EveEvent = {
  type: string;
  data?: Record<string, unknown>;
  meta?: { id?: string };
};

export type RunSettings = {
  activeChildren: number;
  itemConcurrency: number;
};

type BatchResult = {
  status: "completed";
  succeeded: number;
  failed: number;
};

const batchCompletionHook = defineHook({
  schema: z.discriminatedUnion("status", [
    z.object({
      status: z.literal("completed"),
      value: z.unknown(),
    }),
    z.object({
      status: z.literal("failed"),
      error: z.string(),
    }),
  ]),
});

function completionToken(parentRunId: string, batchId: string) {
  return `bulk-batch-completion:${parentRunId}:${batchId}`;
}

async function resumeParentCompletion(
  token: string,
  result: { status: "completed"; value: BatchResult } | { status: "failed"; error: string },
) {
  "use step";
  await batchCompletionHook.resume(token, result);
}

async function withChildCompletionHook(
  runChild: () => Promise<BatchResult>,
  completionTokenArg: string,
) {
  let result: { status: "completed"; value: BatchResult } | { status: "failed"; error: string } | undefined;
  try {
    result = { status: "completed", value: await runChild() };
  } catch (error) {
    result = {
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    if (result) await resumeParentCompletion(completionTokenArg, result);
  }
}

function resolveEveHost() {
  const configured = process.env.EVE_AGENT_URL?.trim();
  if (configured) return new URL(configured).origin;

  const vercelHost = process.env.VERCEL_URL ?? process.env.VERCEL_PROJECT_PRODUCTION_URL;
  if (vercelHost) return new URL(`https://${vercelHost}`).origin;

  if (process.env.VERCEL) {
    throw new Error("Unable to resolve the eve service URL in Vercel. Set EVE_AGENT_URL or expose VERCEL_URL.");
  }
  return `http://127.0.0.1:${process.env.PORT ?? "3000"}`;
}

async function eveHeaders() {
  const headers = new Headers({ "content-type": "application/json" });
  if (process.env.VERCEL) {
    const token = await getVercelOidcToken();
    headers.set("authorization", `Bearer ${token}`);
    headers.set("x-vercel-trusted-oidc-idp-token", token);
  }
  return headers;
}

class EveSessionError extends Error {
  code: string;
  constructor(message: string, code: string) {
    super(message);
    this.name = "EveSessionError";
    this.code = code;
  }
}

function errorMessage(error: unknown) {
  if (!(error instanceof Error)) return String(error);
  const code = "code" in error && typeof error.code === "string" ? ` [${error.code}]` : "";
  const status = "status" in error && typeof error.status === "number" ? ` (HTTP ${error.status})` : "";
  const cause = error.cause instanceof Error ? `: ${error.cause.message}` : "";
  return `${error.name}${code}${status}: ${error.message}${cause}`;
}

async function requireOk(response: Response, operation: string) {
  if (response.ok) return;
  const body = (await response.text()).slice(0, 1_000);
  const error = new Error(`${operation} returned HTTP ${response.status}${body ? `: ${body}` : ""}`);
  Object.assign(error, { status: response.status });
  throw error;
}

function isRetryable(error: unknown) {
  if (error instanceof EveSessionError) {
    if (/no credentials|unauthorized|forbidden|invalid (request|schema)|bad request/i.test(error.message)) return false;
    return true;
  }
  if (!(error instanceof Error)) return false;
  const status = "status" in error && typeof error.status === "number" ? error.status : undefined;
  if (status === 408 || status === 425 || status === 429 || (status !== undefined && status >= 500)) return true;
  const name = `${error.name}: ${error.message}`.toLowerCase();
  return /aborterror|timeouterror|fetch failed|econnreset|econnrefused|etimedout|enotfound|bad gateway|service unavailable|gateway timeout/.test(name);
}

function workflowError(error: unknown) {
  if (FatalError.is(error)) return error;
  const message = errorMessage(error);
  if (isRetryable(error)) {
    return new RetryableError(message, { retryAfter: 5_000 });
  }
  return new FatalError(message);
}

function sessionTimeoutMs() {
  const value = Number(process.env.EVE_SESSION_TIMEOUT_MS ?? 300_000);
  return Number.isFinite(value) && value > 0 ? value : 300_000;
}

async function readEveResult(
  host: string,
  sessionId: string,
  headers: Headers,
  signal: AbortSignal,
) {
  let startIndex = 0;
  let structuredResult: EveOutput | undefined;
  let lastFailure: string | undefined;
  const eventIds = new Set<string>();

  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const response = await fetch(
        `${host}/eve/v1/session/${encodeURIComponent(sessionId)}/stream?startIndex=${startIndex}`,
        { headers, redirect: "error", signal },
      );
      await requireOk(response, "eve session stream");
      if (!response.body) throw new Error("eve session stream returned no response body");

      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let buffer = "";
      const processLine = (line: string) => {
        if (!line.trim()) return;
        const event = JSON.parse(line) as EveEvent;
        if (event.meta?.id && eventIds.has(event.meta.id)) return;
        if (event.meta?.id) eventIds.add(event.meta.id);
        startIndex += 1;

        if (event.type === "result.completed") {
          structuredResult = structuredOutputSchema.parse(event.data?.result);
        }
        if ((event.type === "step.failed" || event.type === "turn.failed") && typeof event.data?.message === "string") {
          lastFailure = event.data.message;
        }
        if (event.type === "session.failed") {
          const code = typeof event.data?.code === "string" ? event.data.code : "SESSION_FAILED";
          const message = typeof event.data?.message === "string" ? event.data.message : "unknown session failure";
          throw new EveSessionError(`eve session failed [${code}]: ${message}`, code);
        }
        if (event.type === "session.waiting" || event.type === "session.completed") {
          return true;
        }
        return false;
      };

      for (;;) {
        const { done, value } = await reader.read();
        buffer += value ?? "";
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (processLine(line)) return { data: structuredResult, lastFailure };
        }
        if (done) {
          if (processLine(buffer)) return { data: structuredResult, lastFailure };
          break;
        }
      }
      throw new Error("eve session stream ended before a terminal event");
    } catch (error) {
      if (attempt === 4 || error instanceof EveSessionError || signal.aborted) throw error;
      const delayMs = 250 * 2 ** (attempt - 1);
      logger.warn("eve.session.stream.retry", {
        eveSessionId: sessionId,
        attempt,
        delayMs,
        error: errorMessage(error),
      });
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw new Error("eve session stream retry budget exhausted");
}

async function cancelEveTurn(host: string, sessionId: string, headers: Headers) {
  try {
    const response = await fetch(`${host}/eve/v1/session/${encodeURIComponent(sessionId)}/cancel`, {
      method: "POST",
      headers,
    });
    await requireOk(response, "eve session cancel");
  } catch (error) {
    logger.warn("eve.session.cancel.failed", {
      eveSessionId: sessionId,
      error: errorMessage(error),
    });
  }
}

async function runItem(runId: string, batchId: string, itemKey: string) {
  "use step";
  const startedAt = Date.now();
  const execution = await getExecutionItem(runId, itemKey);
  if (!execution) throw new FatalError(`Item ${itemKey} was not found in run ${runId}.`);
  if (execution.status === "completed") return { itemKey, status: "completed" as const };
  if (execution.status === "cancelled") return { itemKey, status: "cancelled" as const };

  const running = await markItemRunning(runId, itemKey);
  if (!running) throw new FatalError(`Item ${itemKey} was no longer eligible to run.`);

  const host = resolveEveHost();
  const headers = await eveHeaders();
  const signal = AbortSignal.timeout(sessionTimeoutMs());
  let sessionId = running.sessionId;

  try {
    if (!sessionId) {
      const createResponse = await fetch(`${host}/eve/v1/session`, {
        method: "POST",
        headers,
        redirect: "error",
        signal,
        body: JSON.stringify({
          message: `Question: ${execution.question}\nAnalyze item ${itemKey}. Return only the requested structured result.`,
          clientContext: { itemKey, context: execution.context },
          operationId: `${runId}:${itemKey}:${running.attempts}`,
          outputSchema,
        }),
      });
      await requireOk(createResponse, "eve session create");
      const accepted = await createResponse.json() as { ok?: boolean; sessionId?: string; status?: string };
      if (!accepted.ok || !accepted.sessionId || accepted.status !== "accepted") {
        throw new FatalError(`eve session create returned an invalid response: ${JSON.stringify(accepted)}`);
      }
      sessionId = accepted.sessionId;
      await setItemSessionId(runId, itemKey, sessionId);
    }

    const result = await readEveResult(host, sessionId, headers, signal);
    if (!result.data) {
      const detail = result.lastFailure ? ` (${result.lastFailure})` : "";
      throw new Error(`eve session returned no structured result${detail}`);
    }
    const completed = await markItemCompleted(runId, itemKey, result.data);
    if (!completed) return { itemKey, status: "cancelled" as const };
    return { itemKey, status: "completed" as const };
  } catch (error) {
    if (sessionId) {
      await cancelEveTurn(host, sessionId, headers);
      await clearItemSessionId(runId, itemKey);
    }
    logger.error("bulk.item.failed", error, {
      runId,
      batchId,
      itemKey,
      durationMs: Date.now() - startedAt,
    });
    throw workflowError(error);
  }
}
runItem.maxRetries = 3;

async function markItemFailedStep(runId: string, itemKey: string, error: string) {
  "use step";
  await markItemFailed(runId, itemKey, error, "ITEM_FAILED");
}

async function loadBatchItemKeysStep(batchId: string) {
  "use step";
  return loadBatchItemKeys(batchId);
}

async function recordBatchStartedStep(batchId: string, workflowRunId: string) {
  "use step";
  await recordBatchStarted(batchId, workflowRunId);
}

async function finalizeBatchStep(batchId: string) {
  "use step";
  return finalizeBatch(batchId);
}

async function bulkBatch(
  runId: string,
  batchId: string,
  itemConcurrency: number,
  childWorkflowRunId: string,
) {
  "use workflow";
  const startedAt = Date.now();
  await recordBatchStartedStep(batchId, childWorkflowRunId);
  const itemKeys = await loadBatchItemKeysStep(batchId);

  for (let offset = 0; offset < itemKeys.length; offset += itemConcurrency) {
    const wave = itemKeys.slice(offset, offset + itemConcurrency);
    const outcomes = await Promise.allSettled(
      wave.map((itemKey) => runItem(runId, batchId, itemKey)),
    );

    for (const [index, outcome] of outcomes.entries()) {
      if (outcome.status === "rejected") {
        const error = outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason);
        await markItemFailedStep(runId, wave[index], error);
      }
    }
  }

  const summary = await finalizeBatchStep(batchId);
  logger.info("bulk.batch.completed", {
    runId,
    batchId,
    workflowRunId: childWorkflowRunId,
    durationMs: Date.now() - startedAt,
    succeeded: summary?.succeeded ?? 0,
    failed: summary?.failed ?? 0,
  });
  return {
    status: "completed" as const,
    succeeded: summary?.succeeded ?? 0,
    failed: summary?.failed ?? 0,
  };
}

export async function bulkBatchWithCompletion(
  runId: string,
  batchId: string,
  itemConcurrency: number,
  completionTokenArg: string,
) {
  "use workflow";
  const { workflowRunId } = getWorkflowMetadata();
  await withChildCompletionHook(
    () => bulkBatch(runId, batchId, itemConcurrency, workflowRunId),
    completionTokenArg,
  );
}

async function startAndWait(batchId: string, startChild: (token: string) => Promise<void>) {
  const { workflowRunId } = getWorkflowMetadata();
  const token = completionToken(workflowRunId, batchId);
  const hook = batchCompletionHook.create({ token });
  await startChild(token);
  const completion = await hook;
  if (completion.status === "failed") throw new Error(completion.error);
  return completion.value as BatchResult;
}

async function recordParentStartedStep(runId: string, workflowRunId: string) {
  "use step";
  await setWorkflowRunId(runId, workflowRunId);
  await markRunRunning(runId);
}

async function loadBatchPlanStep(runId: string) {
  "use step";
  return loadBatchPlan(runId);
}

async function markBatchFailedStep(batchId: string, error: string) {
  "use step";
  await markBatchFailed(batchId, error);
}

async function writeProgress(event: Record<string, unknown>) {
  "use step";
  const writer = getWritable<string>().getWriter();
  await writer.write(`${JSON.stringify(event)}\n`);
  writer.releaseLock();
}

async function closeProgress() {
  "use step";
  const writer = getWritable<string>().getWriter();
  await writer.close();
}

async function finalizeRunStep(runId: string) {
  "use step";
  return finalizeRun(runId);
}

export async function bulkRun(runId: string, settings: RunSettings) {
  "use workflow";
  const { workflowRunId } = getWorkflowMetadata();
  await recordParentStartedStep(runId, workflowRunId);
  const batches = await loadBatchPlanStep(runId);
  if (batches.length === 0) throw new FatalError(`Run ${runId} has no batches.`);

  let completedBatches = 0;
  for (let offset = 0; offset < batches.length; offset += settings.activeChildren) {
    const wave = batches.slice(offset, offset + settings.activeChildren);
    const outcomes = await Promise.allSettled(
      wave.map((batchId) =>
        startAndWait(batchId, (token) =>
          start(bulkBatchWithCompletion, [runId, batchId, settings.itemConcurrency, token]).then(() => undefined),
        ),
      ),
    );

    for (const [index, outcome] of outcomes.entries()) {
      if (outcome.status === "rejected") {
        const error = outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason);
        await markBatchFailedStep(wave[index], error);
      }
    }

    completedBatches += wave.length;
    await writeProgress({
      type: "batches",
      completed: completedBatches,
      total: batches.length,
    });
  }

  const summary = await finalizeRunStep(runId);
  await writeProgress({ type: "run", status: summary?.status ?? "completed" });
  await closeProgress();
  return { runId, status: summary?.status ?? "completed" };
}
