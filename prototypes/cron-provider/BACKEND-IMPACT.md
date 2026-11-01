# Backend impact: adopting HiveCast-as-cron-provider

Grounded in the prototype (`prototypes/cron-provider/`, commit `c6a0d4b5` — full
contract loop proven against a real Hermes gateway) and the RE findings
(`~/git/hermes-rea/re/hivecast-cloud-sync.md`). This is the "what changes in
hermes-cloud" answer, not a quicky.

## Today's scheduled-work architecture (the thing we'd invert)

```
workspace_routines (due idx)                          packages/lib/src/db/schema.ts:2052
  → Cloudflare scheduled Worker → POST /api/internal/routines/drain
    (apps/web/src/app/api/internal/routines/drain/route.ts; local dev:
     runLocalRoutineWorker, packages/lib/src/services/local-routine-worker.ts)
  → drainWorkspaceRoutines() claims a lease            workspace-routine-runner.ts:518
  → createWorkspaceRoutineRunner().admit()             workspace-routine-runner.ts:161
  → startHermesRuntimeRun() via hermes-runtime-proxy   (Cloudflare Access service token)
  → poll run → workspace_routine_runs + events → UI    workspace-routine-run-store.ts,
                                                        workspace-routine-events.ts
```

Cloud-driven execution: **the cloud is both the timer AND the executor-driver**.
Every fire needs a warm, Access-authenticated runtime. Chat-created agent crons
never appear here at all (the original pain).

## The inversion (proven in the prototype)

Cloud stays the **timer + registry + JWT minter** (the "NAS" role); the agent
becomes the **executor** of its own jobs, woken by an authenticated fire
webhook. One scheduler, one execution truth (the agent's jobs.json), and
scheduled work stops requiring a warm runtime — **scale-to-zero for runtimes**.

```
routines engine → arm registry (DB) → sweep → mint purpose=cron_fire JWT
  → POST runtime_gateway /api/cron/fire (verified via cloud JWKS, fail-closed)
  → agent run_job (its own CAS claim = at-most-once) → result ingestion → UI
chat with agent → cronjob_manage → jobs.json → provider register_job
  → arms appear in the cloud automatically (the gap closes)
```

## What changes, by area

### 1. Schema — one new migration (drizzle/0010_*)
- `runtime_cron_arms`: `(workspace_id, runtime_deployment_id FK, job_id,
  fire_at timestamptz, dedup_key, origin 'routine'|'agent', status
  pending|firing|fired|failed, attempts, last_error, callback_url)`,
  unique `(runtime_deployment_id, job_id)` = the upsert contract;
  partial index on pending + fire_at for the sweep.
- `runtime_cron_registrations` (optional, phase 2): agent-originated jobs
  (chat crons) mirrored read-only into the routines UI.
- Same-migration RLS per the canonical pattern: ENABLE RLS + restrictive scope
  policy via `hermes_rls_scope_expression(...)` + generic fail-closed gate.
  Runtime context needs **per-command** policies (SELECT own rows,
  INSERT/UPDATE own rows, DELETE denied — never a self-matchable FOR ALL on a
  table the runtime writes). Gate must TRIP, not just pass.
- JWKS signing keys never touch the DB (Worker secret/KV, kid-pinned).

### 2. New services (packages/lib/src/services/runtime-cron-*)
- `arm-service.ts` — port of the prototype's provision/cancel/list semantics
  (upsert by (deployment, job), dedup_key idempotency, idempotent cancel).
- `fire-service.ts` — sweep due arms → mint RS256 JWT (`iss` = cloud origin,
  `aud` = `agent:{runtime_deployment_id}`, `purpose=cron_fire`, exp ≤ 120s) →
  POST the runtime gateway via the existing Access transport
  (`runtimeAccessHeaders`) → retry non-2xx → at-most-once after 2xx.
- `jwks-service.ts` — key load/rotate, public JWKS document.

### 3. Routes (apps/web/src/app/api/runtime/cron/*)
- `provision|cancel|list` authenticated by the **runtime deployment
  credential** — this must be defined as bearer-only (no cookie fallthrough,
  distinct from browser sessions and from the internal drain signature).
  Today runtimes authenticate to Postgres via the runtime profile context;
  the API-level runtime bearer is a genuinely new credential seam.
- `GET /.well-known/jwks.json` — public, cacheable.

### 4. Routines engine rework (the biggest behavioral change)
- `workspace-routine-runner.ts` splits: UI-authored routines first
  **materialize as agent-side jobs.json entries** (provisioned through the
  runtime admin API at create/edit time), then the standard arm→fire loop
  executes them agent-side. `computeNextRoutineRun` (routine-schedule.ts)
  stays the cloud-side next-run authority for UI routines.
- The legacy execution path (`startHermesRuntimeRun` per due routine) becomes
  the migration fallback and is deleted in phase 3.
- Lease machinery shrinks: the agent's store CAS claim replaces
  routine-run leases/dispatch polling for agent-executed jobs.
- **Result ingestion is new**: agent-side run completion must land in
  `workspace_routine_runs` + publish `workspaceRoutineRunChanged` — a runtime
  → cloud ingestion endpoint (run status, output, usage, transcript ref).
  Options: direct POST from the agent's fire-completion path, or a hooks
  handler (`agent:end` event bus — RE finding F4) pushing run events.

### 5. Runtime image / agent-host packaging
- Ship the `hivecast` provider in the HermesCloud runtime image (affects all
  three providers: local-docker, incus, vps; image build pipeline gains
  provider + bootstrap config).
- Bootstrap writes `cron.provider: hivecast` + portal/callback/token/agent_id
  + verifier keys into the runtime's config.yaml — flows through
  `workspace-agent-runtime-profile` / deployment provisioning
  (`runtime-deployment-lock`, `runtime-image-replacement-service`).
- Short-term wart: the gateway verifier keys are `cron.chronos.*`-namespaced
  (see prototype README) — set them until the upstream generalization PR.
- Image pinning policy gains a step: re-diff the four seam files
  (scheduler_provider, chronos plugin, api_server fire handler, hooks.py) on
  every image upgrade — the CronScheduler interface is still EXPERIMENTAL.

### 6. What simplifies or dies
- **Warm-runtime-for-routines**: gone. The single biggest operational win —
  the readiness/stale-port/recovery class of problems
  (`workspace-runtime-recovery-service`, image replacement during idle) loses
  its scheduled-work trigger; recovery keeps handling the chat path.
- The chat-cron gap: chat-created crons auto-register (provider
  `register_job`) and surface in the UI via registrations.
- Run-state polling (`runtimeRunId` poll loops) → push ingestion.
- Delivery to humans stays agent-owned (`delivery_queue`) — cloud ingestion is
  for UI/history only; divergence risk documented, not fought.

### 7. Security review surface (expect findings here)
- New-table RLS migration (gate trip, per-command runtime policies,
  indirect-scope map if registrations references routines).
- Fire ingress: JWT fail-closed (proven), JWKS rotation story (kid pinning,
  overlap window), replay window (short exp + agent CAS).
- Runtime bearer: scopes minimal (provision/cancel/list only), 403-vs-5xx
  semantics for the provider's deterministic-identity fallback.
- at-most-once guarantees restated per hop (arm upsert / fire retry / agent CAS).

### 8. Testing
- Port the prototype's 12 contract tests into `packages/lib` (real store +
  RLS against scratch Postgres, not just in-memory maps).
- The e2e harness stays a dev tool (CI can't run Docker agents) — it is the
  live-loop proof against a real gateway.
- Provider (Python) needs a home + test story: vendor into the image build
  with a sync check, or upstream the provider and pin.

## Rollout (per-workspace flag, three phases)

1. **Cloud side lands inert** — arms/issuer/JWKS/routes + image with the
   provider present but `cron.provider` unset. Nothing changes operationally.
2. **Flag on (per workspace)** — new/edited routines materialize agent-side
   and fire via the loop; existing ones keep the legacy path; both write the
   same run store, UI unchanged.
3. **Agent-executed by default** — legacy execution path + lease machinery
   deleted; runtimes may scale to zero; scale-to-zero becomes a deployment
   policy option per workspace.

## Sizing (rough, honest)

- Cloud side (schema+RLS, services, routes, tests): the prototype already
  holds the semantics; real-store + RLS + ingestion endpoint is the bulk.
- Image/bootstrap packaging: small but touches three providers + image pinning.
- Result ingestion + UI surfacing of agent-created crons: the genuinely new
  product surface.
- The riskiest dependency is upstream (interface marked experimental) —
  mitigated by digest pinning + seam re-diff, and by the upstream
  verifier-key generalization PR.
