import { NextResponse } from "next/server";
import { getRunItems } from "../../../../../lib/db";
import { logger } from "../../../../../lib/logger";

export const runtime = "nodejs";

const statuses = new Set(["queued", "running", "completed", "failed", "cancelled"]);

export async function GET(request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const url = new URL(request.url);
  const status = url.searchParams.get("status");
  if (status && !statuses.has(status)) {
    return NextResponse.json({ error: "Invalid status filter." }, { status: 400 });
  }

  try {
    const result = await getRunItems(id, {
      page: Number(url.searchParams.get("page") ?? 1),
      pageSize: Number(url.searchParams.get("pageSize") ?? 100),
      status: status as "queued" | "running" | "completed" | "failed" | "cancelled" | undefined,
    });
    return NextResponse.json(result);
  } catch (error) {
    logger.error("api.run.items.failed", error, { runId: id });
    return NextResponse.json({ error: "Unable to load run items." }, { status: 500 });
  }
}
