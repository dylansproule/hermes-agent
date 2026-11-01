# HiveCast ↔ Hermes Agent: cloud-sync architecture (reverse-engineering notes)

**Question:** how does Hermes Agent sync to a cloud control plane, and how can
HiveCast simplify its architecture instead of running a parallel scheduler?
Concretely: chatting with an agent lets it create crons that live only in the
agent's local store — they never reach HiveCast's routines/scheduling.

All findings verified against this tree (NousResearch/hermes-agent).

---

## Findings

### F1 — Cron already has a provider interface: "who decides *when* a job fires"
`cron/scheduler_provider.py` — `CronScheduler` ABC. Providers own **only the
trigger**; execution and delivery always stay in `cron.scheduler.run_job` /
`_deliver_result`. Contract rules baked into the docstring: growth must be
additive (optional methods with defaults), never a changed `start()` signature.
Interface surface:

- `start(stop_event, *, adapters, loop, interval)` — arm and RETURN (no loop required)
- `on_jobs_changed()` — the job set mutated; re-reconcile
- `register_job(job)` — first arm for a new job; **may raise** so creation can report
- `fire_claimed(job, ...)` — post-run hook (Chronos re-arms the next one-shot here)
- `reconcile()` — converge remote one-shots toward jobs.json
- `is_available()` — config/credential presence gate, **no network**

Marked EXPERIMENTAL "until a second provider validates it" — Chronos (F2) is
that second provider, in-tree. The interface is validated.

### F2 — Chronos is the complete reference implementation of cloud-owned scheduling
`plugins/cron_providers/chronos/` + `website/docs/developer-guide/chronos-managed-cron-contract.md`.

- Agent computes `next_run_at`, then arms **exactly one external one-shot per
  job** at that time (`provision(job_id, fire_at, dedup_key=job_id:fire_at)` —
  idempotent re-arm).
- At fire time the external scheduler calls the agent back:
  `POST {agent_callback_url}/api/cron/fire`, authenticated by a **short-lived
  JWT** (`aud=agent:{instance_id}`, `purpose=cron_fire`, verified via issuer
  JWKS, **fail-closed**), answered `202` + background run so a long turn never
  trips the caller's timeout; the **store CAS claim** guards double-fire on retry.
- After a run: re-arm the next one-shot (`fire_claimed`).
- **Scale-to-zero**: between fires the agent process can be fully stopped; it
  wakes only on a genuine fire. No 60s ticker, no periodic wake.
- Identity failure is handled: on deterministic 403 the provider flips to the
  built-in ticker for the process's fires so jobs keep firing (#97494 lesson).

The agent holds **no scheduler credentials** — it only knows three endpoints of
the control plane (`provision` / `cancel` / `list` + the fire relay).

### F3 — Chat-created crons flow through one seam
Chain (no cron can bypass it):

```
chat tool `cronjob_manage`            model_tools.py (toolset "cronjob_tools")
  └─ create_job_with_scheduler_registration()   cron/scheduler.py
       ├─ cron.jobs.create_job()      persist to jobs.json (single store)
       └─ provider.register_job(job)  first external arm
            └─ failure → CronSchedulerRegistrationError  (typed partial-failure:
               "job saved, scheduler not registered — do NOT re-create;
                pause/resume or edit to retry registration")
every later mutation (pause/resume/edit/delete/enable)
  └─ _notify_provider_jobs_changed()  cron/scheduler.py
       └─ provider.on_jobs_changed() → reconcile()
```

`reconcile()` converges the remote scheduler toward jobs.json: arm
missing/changed, cancel orphans. jobs.json is the agent-side source of truth;
the provider mirrors it outward.

### F4 — The "main event emitter" exists: `gateway/hooks.py`
A real event bus with `emit()` / `emit_collect()`, fired at gateway lifecycle
points:

- `gateway:startup`
- `session:start` / `session:end` / `session:reset`
- `agent:start` / `agent:step` (each tool-loop turn) / `agent:end`
- `command:*` (wildcard)

Context: `platform, user_id, chat_id, thread_id, chat_type, session_id,
message (500 chars)`; `agent:end` adds `response, model, provider`.
Handlers are drop-in directories `~/.hermes/hooks/<name>/{HOOK.yaml,handler.py}`
(`def handle(event_type, context)`, sync or async); **handler errors never
block the pipeline**. Emit sites: `run_startup.py`, `run_turn*.py`,
`run_inbound_hooks.py`, `slash_commands_session.py`.

This is a ready-made change-data-capture plane for a control plane.

### F5 — Provider resolution is config + plugin discovery, with safe fallback
`resolve_cron_scheduler()` (`cron/scheduler_provider.py`): reads `cron.provider`
from config.yaml → `plugins/cron_providers/` discovery → `is_available()` gate →
falls back to the built-in 60s ticker with a warning ("cron must never be left
without a trigger"). A provider is **inert unless configured**.

### F7 — The "NAS" role in detail (the part HiveCast would play)
NAS = Nous Account Service (`nous-account-service`), the hosted control plane.
In Chronos it is a **trusted timer + token minter, nothing more**. Four duties:

1. **Registry** — persist `(agent_id, job_id, schedule_id, agent_callback_url)`
   per armed one-shot. `provision` **upserts by `(agent_id, job_id)`**: re-arming
   the same fire is a no-op; a new `fire_at` replaces the prior arm. `cancel` is
   idempotent (unknown job = 200 no-op). `list` returns armed `{job_id,
   fire_at, schedule_id}` for agent-side reconcile.
2. **Timer** — arm exactly one one-shot at the agent-supplied `fire_at` (ISO
   8601, **second granularity — agent owns the time, no scheduler-side cron
   expressions cross the wire, no 1-minute floor**). The internal scheduler
   NAS uses is an implementation detail the agent never sees.
3. **Relay + JWT mint** — at fire time, scheduler → NAS `POST
   /api/agent-cron/relay` (request-signature auth, the trust boundary for the
   fire). NAS looks up the callback URL from its row, mints a short-lived JWT
   (`aud=agent:{instance_id}`, `iss=portal_url`, `purpose=cron_fire`,
   `exp≈60–120s`, asymmetric key published via JWKS), POSTs
   `{agent_callback_url}/api/cron/fire` with it. Agent 202 → NAS 2xx (scheduler
   stops retrying); non-2xx from the agent = retryable (agent store CAS makes
   retries at-most-once).
4. **Nothing else** — NAS does not compute schedules, does not advance
   recurrences (the agent advances `next_run_at` under its store lock and
   re-provisions), does not execute or deliver, and never wakes a sleeping
   agent periodically (that would negate scale-to-zero).

**Why NAS-mediated rather than scheduler→agent direct:** the scheduler signs
with NAS's keys, which the agent does not hold; the agent can only verify a
NAS-minted token — a trust path it already has. All scheduler credentials stay
inside NAS. No new agent secret: hop 1 reuses the portal token, hop 3 reuses
NAS-JWT verification.

**Identity nuance (hop 1):** NAS must resolve the calling agent's instance id
from EITHER an `agent:*` OAuth client OR the bootstrap-session token
(`hermes-cli-vps`, matched via `AgentInstance.bootstrapSessionId`, org-scoped) —
gating on `agent:*` alone 403s every real hosted agent. `403 invalid_client`
is deterministic → the agent stops calling NAS and runs the built-in ticker
(#97494). Transient 5xx never degrades; reconcile self-heals.


### F6 — Delivery is agent-owned and durable
`cron/scheduler_delivery.py` + `cron/delivery_queue.py`: target resolution
(origin/home/explicit/bot-chat), transcript mirroring, session seeding, and a
sqlite durable delivery queue with terminal states `delivered/failed/unknown/
suppressed` — losing a delivery is deliberately preferred over duplicating one.

---

## What this means for HiveCast

### The current pain, precisely
Chat with an agent → `cronjob_manage` → jobs.json in the runtime container →
the built-in ticker fires it locally. HiveCast's routines engine never hears
about it. Two schedulers, two sources of truth, and the cloud is blind to
agent-created schedules.

### Proposal A — HiveCast becomes a Chronos-style cron provider (primary)
One scheduler, one source of truth, minimal new surface:

- **Cloud side**: the routines engine plays the contract's control-plane role —
  implement the three management endpoints (`provision` / `cancel` / `list`,
  agent-authenticated) and mint fire JWTs (`purpose=cron_fire`,
  `aud=agent:{runtime_deployment_id}`) at fire time. HiveCast already owns
  runtime identity (`runtime_deployments`), so it is the natural JWT issuer +
  JWKS host.
- **Agent side**: either point Chronos at HiveCast endpoints via config
  (`cron.chronos.portal_url` → HiveCast), or ship a thin `hivecast` provider
  (~200 lines, Chronos is the template) in the HermesCloud runtime image.
- **Free wins**:
  - Chat-created crons reach the cloud **automatically** — creation always
    flows through `register_job` / `on_jobs_changed` (F3). No scraping, no
    second parser.
  - **Scale-to-zero**: between fires the runtime container needs no warm
    gateway loop — the warm-gateway/readiness/stale-port class of problems
    (Codex's reconciliation seam) largely evaporates for scheduled work.
  - The partial-failure contract (`job_saved: true, scheduler_registered:
    false, retry_create: false`) already defines correct UX for cloud blips.
  - Built-in ticker remains the automatic fallback — jobs never stop firing
    because the cloud is unreachable.

### Proposal B — a hooks handler as the event plane (complementary, cheap)
Drop `~/.hermes/hooks/hivecast-sync/` into the runtime image: subscribe to
`agent:end`, `session:*`, `command:*` and push lightweight events to the
control plane (F4). This replaces polling/readiness probing for *liveness and
activity* with a push stream, using a mechanism designed for exactly that.

### What NOT to do
- Don't build a second cloud scheduler that also parses jobs.json — the
  provider interface already separates trigger from execution.
- Don't sync by scraping transcripts or polling the gateway.
- Don't put scheduler credentials in the agent — the contract's trust model
  keeps the agent holding only its own identity token.

### Interface risks
- `CronScheduler` is marked EXPERIMENTAL (shape may change until a *third*
  provider validates it) — pin the agent image digest (already HiveCast
  policy) and re-verify on upgrade.
- The wire contract names NAS as the control plane; HiveCast implementing the
  same three endpoints is contract-compatible but should be flagged as a
  deliberate fork of the `agent-cron` API shape (or upstreamed as an
  alternative issuer).
- Fire ingress must stay fail-closed JWT-verified (the in-tree verifier is
  pluggable: `plugins/cron_providers/chronos/verify.py`).

### Live-runtime validation (REA tool + drift check)

Ran the `rea` CLI (morluto/rea, `rea-agents@5.0.0` under node v24.19.0 — default
node misses the engine floor) against the LIVE install `~/.hermes/hermes-agent`:

- REA's directory scan surfaced the embedded **Hermes Desktop Electron app**
  (`apps/desktop/release/mac-arm64/Hermes.app/Contents/Resources/app.asar`) and
  fail-closed on a real integrity delta: `node-pty` prebuild `pty.node`
  declared sha256 `e6457d66…` vs calculated `4dd84e55…` (unpacked). Likely
  arm64 slice-thinning of the fat binary at install time, but it is a genuine
  declared-vs-shipped mismatch and REA is correct to refuse past it (no bypass
  flag in v5). Static JS reconstruction of the desktop app stops there.
- REA's lanes are JS/Electron/.NET/native binaries — the Hermes runtime core is
  Python, so the seam validation below was done by direct live-vs-clone
  comparison instead.

**Seam drift, live install vs clone @ `c88ccd08c8`:**

| File | Verdict |
|---|---|
| `plugins/cron_providers/chronos/__init__.py` | **byte-identical** |
| `gateway/hooks.py` | **byte-identical** |
| `cron/scheduler_provider.py` | differs — additive refinement only (`profile_bot_platforms` threading, `has_credential_platform` guard on ticker fallback); ABC surface unchanged |
| `gateway/platforms/api_server.py` | differs — executor/delivery plumbing (`on_turn_executor`, profile threading); `/api/cron/fire` handler present in both |

**Conclusion: all documented seams (F1–F7) exist in the deployed runtime.**
Caveat: live is slightly behind clone HEAD; pin image digests and re-diff these
four files on upgrade (per the interface-risk note).

### Migration sketch
1. Ship the provider in the HermesCloud runtime image, `is_available()` gated
   on workspace/HiveCast credentials (mirrors Chronos' Nous-token check).
2. Default stays the builtin ticker; enable per workspace
   (`cron.provider: hivecast`) once fire ingress is verified end-to-end.
3. Existing cloud routines continue to run; agent-local jobs reconcile
   outward on first `start()` — the contract's reconcile handles adoption.
