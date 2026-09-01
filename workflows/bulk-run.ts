import { Client } from "eve/client";
import { z } from "zod";
import { getWritable } from "workflow";
import { markItemFinished, markItemRunning } from "../lib/db";

const outputSchema = z.object({
  answer: z.string(),
  value: z.number().nullable(),
  assumptions: z.array(z.string()),
  evidence: z.array(z.string()),
});

type Item = { key: string; context: unknown };

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

async function runItem(runId: string, question: string, item: Item) {
  "use step";
  markItemRunning(runId, item.key);
  await writeProgress({ type: "item", key: item.key, status: "running" });
  try {
    const host = process.env.EVE_AGENT_URL ?? "http://127.0.0.1:2000";
    const client = new Client({ host, redirect: "error" });
    const { response } = await client.sessions.create({
      message: `Question: ${question}\nAnalyze item ${item.key}. Return only the requested structured result.`,
      clientContext: JSON.stringify({ itemKey: item.key, context: item.context }),
      outputSchema,
    });
    const result = await response.result();
    // A completed turn normally leaves the durable session in `waiting`, ready
    // for another message. The structured result is the authoritative success
    // signal for this one-shot batch item.
    if (result.status === "failed" || !result.data) {
      const eventTypes = result.events.map((event) => event.type).join(", ");
      const detail = [result.message, eventTypes ? `events: ${eventTypes}` : ""].filter(Boolean).join("; ");
      throw new Error(`eve session ${result.status}${detail ? ` (${detail})` : ""}`);
    }
    markItemFinished(runId, item.key, "completed", result.data);
    await writeProgress({ type: "item", key: item.key, status: "completed", result: result.data });
    return { key: item.key, status: "completed", sessionId: response.sessionId };
  } catch (error) {
    markItemFinished(runId, item.key, "failed", undefined, error instanceof Error ? error.message : String(error));
    await writeProgress({ type: "item", key: item.key, status: "failed", error: error instanceof Error ? error.message : String(error) });
    return { key: item.key, status: "failed" };
  }
}

export async function bulkRun(runId: string, question: string, items: Item[], concurrency = 50) {
  "use workflow";
  for (let offset = 0; offset < items.length; offset += concurrency) {
    await Promise.all(items.slice(offset, offset + concurrency).map((item) => runItem(runId, question, item)));
  }
  await writeProgress({ type: "run", status: "completed" });
  await closeProgress();
  return { runId };
}
