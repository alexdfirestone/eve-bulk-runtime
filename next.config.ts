import type { NextConfig } from "next";
import { withEve } from "eve/next";
import { withWorkflow } from "workflow/next";

const nextConfig: NextConfig = {
};

// Workflow transforms the bulk coordinator while eve mounts the agent as a
// sibling service at /eve/v1. The coordinator talks to that service over HTTP,
// keeping eve's internal Workflow runtime isolated from the parent workflow.
export default withEve(withWorkflow(nextConfig));
