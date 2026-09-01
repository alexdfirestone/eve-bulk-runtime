# eve bulk runtime

This repository is a lightweight two-process demo: an eve agent runtime and a
Next.js/Workflow coordinator with a SQLite status dashboard.

## Architecture

1. The coordinator validates a question, firm IDs, and a snapshot version, then starts a durable Workflow.
2. The Workflow executes firm IDs in bounded waves (start around 100).
3. Each firm is an independent Workflow step, so a transient failure retries only that firm.
4. Each step creates an eve session through `eve/client`, passes one firm snapshot as one-turn context, and requires the same structured output schema.
5. Each step writes its result idempotently to durable storage; the coordinator returns a run ID immediately and later exposes progress and aggregate results.

The bounded-wave implementation is the initial safe default. At 10,000 firms, pass IDs through the workflow instead of putting the full dataset and outputs in the event log. Keep the workflow return value as a small run summary.

## Local setup

Use Node 24, then configure:

```bash
export AI_GATEWAY_API_KEY=...
export BULK_INTERNAL_TOKEN=$(openssl rand -hex 32)
export EVE_AGENT_URL=http://127.0.0.1:2000
```

Run both runtimes with one command:

```bash
npm run dev:all
```

The dashboard will be available at `http://localhost:3000`.

The coordinator's request contract should look like:

```json
{
  "question": "What is estimated revenue?",
  "concurrency": 100,
  "firms": [
    {"id": "firm-001", "name": "Example Co", "context": {"revenue": 1200000}}
  ]
}
```

The coordinator calls the eve service at `EVE_AGENT_URL` (default
`http://127.0.0.1:2000`) with `Client.sessions.create()` for each item session.

## Vercel deployment

Deploy the eve runtime and Next.js coordinator as separate Vercel projects (or
two services in your deployment setup). The coordinator needs `EVE_AGENT_URL`
pointing at the eve deployment.

```bash
npm exec -- eve link
npm exec -- eve deploy
```

Set `AI_GATEWAY_API_KEY` (or use the Vercel OIDC model path), `BULK_INTERNAL_TOKEN`, and optionally `EVE_AGENT_URL`. Keep the token identical for the bulk route and the eve session calls.

For production, add a durable result store, request-level idempotency keys, a
firm snapshot version, a model/prompt version, and a re-drive path for failed
firms. Use `workflow inspect run <run-id>` and Vercel Agent Runs to inspect
workflow and eve traces.
