# HiveCast-as-cron-provider prototype

Proves HiveCast can play the "NAS" role of Hermes Agent's Chronos managed-cron
contract (see `~/git/hermes-rea/re/hivecast-cloud-sync.md` for the RE findings):
**one scheduler, one source of truth** — chat-created or CLI-created agent crons
flow to the HiveCast cloud automatically, and the cloud owns every wake.

## What's here

- `server/` — the prototype cloud (zero deps: `node:http` + `node:crypto`):
  - `keys.ts` — RS256 keypair, JWKS, Chronos-shaped fire JWT
    (`iss`, `aud=agent:{id}`, `purpose=cron_fire`, exp ≈ 90s), verifier.
  - `cloud.ts` — `POST /api/agent-cron/provision` (upsert by (agent, job),
    idempotent by dedup_key), `POST /api/agent-cron/cancel` (idempotent),
    `GET /api/agent-cron/list`, `GET /.well-known/jwks.json`, and the fire
    loop: sweep due arms → mint JWT → POST `{callback}/api/cron/fire` →
    retry non-2xx → never double-fire after 2xx.
- `provider/` — the agent-side `hivecast` cron provider (Python, Chronos-shaped):
  config-gated (`cron.provider: hivecast`), arms one one-shot per job, reconciles
  on start/jobs-changed, re-arms after fire. Auth = the runtime's HiveCast token.
- `harness/run-e2e.ts` — siloed end-to-end proof (below).
- `tests/` — 12 unit/contract tests for the cloud side.

## The e2e loop (all real, no mocks in the loop itself)

```
hermes cron create 'in 1m' --no-agent --script …   (throwaway HERMES_HOME)
  → jobs.json → provider.register_job → cloud provision (armed)
  → cloud sweep at fire_at → RS256 JWT → POST /api/cron/fire
  → gateway verifies (JWKS + iss + aud + purpose) → 202
  → job runs → one-shot terminal (state=completed, enabled=false)
  → no re-arm; exactly one successful fire
```

Plus the negative: a forged bearer to `/api/cron/fire` fails closed with 401.

## Siloing guarantees (no port fights, no touching real state)

- **Every port is picked free at runtime** (`listen(0)`) — cloud and the
  gateway's api_server. Never 3000/8787/8790/8791/9230/9231.
- **Throwaway `HERMES_HOME`** (mkdtemp): its own config.yaml, cron store, jobs.
- The provider is injected as a **user plugin** (`$HERMES_HOME/plugins/hivecast/`)
  — the live install tree is never modified.
- `--no-agent` cron job runs a shell script — **no LLM, no external API**,
  fully hermetic and offline-capable.
- Nothing mounts the dev database; arms live in memory in the prototype cloud.

## Run it

```sh
# unit/contract tests (12)
node_modules/.bin/vitest run --config prototypes/cron-provider/vitest.config.ts

# e2e against the real Hermes gateway (node ≥ 24; ~75s, mostly waiting the 1m one-shot)
cd prototypes/cron-provider && node harness/run-e2e.ts
```

## Findings worth carrying into the real design

1. **The whole contract fits in ~300 lines of cloud + ~200 of provider.** The
   Chronos provider is an almost exact template — the `hivecast` provider is a
   config/identity port, not new architecture.
2. **Fire ingress needs zero agent-side code**: the gateway's shared
   `/api/cron/fire` handler verified our JWTs unchanged. BUT its verifier
   config keys are `cron.chronos.*` (`nas_jwks_url`, `expected_audience`,
   `portal_url`) — chronos-namespaced. Upstream should generalize these
   (e.g. `cron.fire_verifier.*`); until then a HiveCast provider must set the
   chronos keys. Documented in `provider/__init__.py`.
3. **Auth identity**: Chronos hop-1 auth is the Nous token resolved via
   `resolve_nous_access_token()`. The clean HiveCast analogue is the runtime
   deployment credential HiveCast already provisions (`runtime_deployments`),
   becoming the `agent:{id}` audience — one identity, both directions.
4. **403 semantics matter**: Chronos treats `403 invalid_client` as a
   deterministic identity rejection and falls back to the builtin ticker
   (#97494). The real HiveCast provider should mirror that; the prototype
   omits it deliberately (see provider docstring).
5. **Upsert semantics verified against the agent**: reconcile (arm
   missing/changed, cancel orphans) + dedup_key idempotency + agent-side store
   CAS make the whole loop safe to retry at every hop.
