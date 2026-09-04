import { NextResponse } from "next/server";
import { getRun } from "../../../../lib/db";
import { logger } from "../../../../lib/logger";

export const runtime = "nodejs";

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  try {
    const run = await getRun(id);
    if (!run) return NextResponse.json({ error: "Run not found." }, { status: 404 });
    return NextResponse.json(run);
  } catch (error) {
    logger.error("api.run.get.failed", error, { runId: id });
    return NextResponse.json({ error: "Unable to load the run." }, { status: 500 });
  }
}
