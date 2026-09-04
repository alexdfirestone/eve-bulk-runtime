# eve bulk runtime

A lightweight demo that runs one question across many independent contexts. A
Next.js dashboard starts a durable Vercel Workflow, each item creates its own
eve session, and Neon stores item status and structured results.

## Architecture

The project uses the official `withEve()` Next.js integration, composed with
`withWorkflow()`. Locally, Next.js starts eve automatically and proxies
`/eve/v1/**`. On Vercel, the Next.js app and eve runtime deploy as services in
the same project.

The coordinator processes items in bounded concurrent batches. Each item has
its own eve session, database row, progress event, result, and error. The
browser follows the parent Workflow stream rather than polling while a run is
active.

## Local development

Use Node 24. Pull the linked Vercel development environment, apply migrations,
and start the app:

```bash
vercel env pull .env.local --environment=development
npm run db:migrate
npm run dev
```

Open `http://localhost:3000`. No separate eve terminal or `EVE_AGENT_URL` is
needed. `npm run dev:all` remains an alias for the same one-command startup.

## Production deployment

Apply the migration to the production Neon database, then deploy the linked
project through eve:

```bash
vercel env pull .env.production.local --environment=production
MIGRATION_ENV=.env.production.local npm run db:migrate
npm run deploy -- --non-interactive --yes --project eve-bulk-runtime --team firestone
```

The bulk Workflow calls the colocated eve service with a Vercel OIDC token.
`EVE_AGENT_URL` is optional and should only be set when deliberately calling a
separate eve deployment.

## Production diagnostics

Application logs are structured JSON and correlate `requestId`, `runId`,
`workflowRunId`, `itemKey`, and `eveSessionId`. They do not log item context,
questions, tokens, or database credentials.

Watch the current production logs with:

```bash
vercel logs eve-bulk-runtime.vercel.app --follow
```

Useful events include `api.run.create.failed`, `database.query.retry`,
`eve.session.accepted`, `bulk.item.completed`, and `bulk.item.failed`. eve also
adds its own `$eve.*` Workflow tags, which appear in Vercel Agent Runs when that
view is enabled for the team.
