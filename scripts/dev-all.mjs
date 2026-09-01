import { spawn } from "node:child_process";

const common = { stdio: "inherit", env: { ...process.env, EVE_AGENT_URL: "http://127.0.0.1:2000" } };
const children = [
  spawn("npm", ["run", "eve:dev", "--", "--no-ui"], common),
  spawn("npm", ["run", "dev"], common),
];

function shutdown() {
  for (const child of children) child.kill("SIGINT");
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
