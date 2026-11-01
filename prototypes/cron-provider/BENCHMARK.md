# Managed-cron overhaul benchmark — legacy vs HiveCast provider

Live comparison, run 2026-11-02 on the MacBook (host running normal dev load).
Both architectures exercised end-to-end against **real Hermes Agent gateways**
(throwaway `HERMES_HOME`s, free ports, `--no-agent` script jobs — no LLM, no
mocks inside the loop). Harness: `harness/bench.ts`; raw numbers:
`harness/bench-results.json`.

## Results

| Metric | Legacy (builtin ticker, warm) | Overhaul (hivecast provider) |
|---|---|---|
| Boot → ready (cold, dep prep) | 14,072 ms | 14,554 ms |
| Boot → ready (warm cache, re-boot) | — | **2,030 ms** |
| Schedule → arm visible | n/a (local jobs.json only) | 1,669 ms (3 creates) |
| Fire precision vs scheduled time | +1,033 ms median, **+61,033 ms worst** (minute-tick alignment) | **+137 / +360 / +492 ms** (500 ms sweep) |
| Idle cost per runtime | 1.22 % CPU, 230 MB RSS — forever | warm: 3.2 % CPU, 234 MB — **or scale-to-zero: 0 %** |
| Runtime required between fires? | **Yes, always-on** | No — cloud holds the arm, wakes the runtime |
| Scale-to-zero fire_at → job done | impossible | 4,555 ms (3 refused retries, 2.0 s boot, next retry lands) |
| DB due-scan @ 10k rows, 100 due | 0.31 ms p50 / 0.66 ms p95 | 0.30 ms p50 / 0.77 ms p95 (parity) |
| Cloud sweep pass @ 10k arms (in-mem) | — | 0.63 ms full scan |

## What the numbers mean

**Performance.** The legacy ticker aligns to minute boundaries: a "in 1m"
one-shot waits for the *next* tick — observed first fire 61.0 s after create,
median drift +1.0 s, and one of three one-shots fired a full minute late
(+61 s). The provider path fired every arm within **0.5 s** of its exact
`fire_at` because the cloud sweeps on its own cadence (500 ms here) and pushes
the fire. Precision is a *config knob* (sweep interval), not an architecture
ceiling; the ticker's is.

**Resources.** Warm costs are comparable (230 MB RSS either way; the provider
gateway idles ~2 CPU points hotter from re-arm polling — a prototype artifact
to optimize). The structural difference: legacy's scheduler *lives inside the
runtime*, so the runtime can never stop. Overhaul moves the timer to the
control plane — the runtime can be **completely absent** between fires. At
Dylan's 1,000-client design point: legacy = 1,000 always-on processes
(~12 idle cores, ~230 GB RSS); overhaul = one sweeper + rows at rest in
Postgres.

**Scalability.** DB-side parity at 10k rows (0.3 ms p50, both shapes, partial
indexes doing their job). The in-memory sweep scans 10k arms in 0.63 ms —
14M+ arms/minute per sweeper thread before needing the DB index anyway. The
arm registry is one row per pending one-shot: it *shrinks* when idle (rows
drop after fire), unlike routine rows that persist and grow.

**Readiness.** Cold first boot (dependency prep) ≈ 14 s either way — that's
Hermes Agent, not the architecture. Re-boot with warm cache: **2.0 s**. So a
scale-to-zero fire costs ~4.6 s end-to-end (wake-detect + boot + one retry
cadence). Retries dominate: 3 refused attempts before the agent-host reacted —
a faster wake hook (container start on first refusal) would cut this to
~2.5 s.

## Honest caveats

- Single run per scenario on a busy laptop — treat as magnitudes, not SLAs.
- The builtin ticker's *recurrence* stalled after one fire in a throwaway home
  (observed repeatedly); the benchmark therefore uses staggered one-shots on
  both sides — identical measurement, and matching the Chronos contract
  (one-shots only).
- Provider idle CPU (3.2 %) includes the prototype plugin's re-arm polling;
  production provider would poll only on `on_jobs_changed`.
- Scale-to-zero wake was simulated by the harness playing agent-host (boot on
  first refused fire) — the container-start hook is production work.
- Legacy measurements reflect Hermes Agent's in-process ticker as the stand-in
  for the current cloud drain loop (which additionally needs a *warm runtime*
  per workspace to execute anything at all — cost the table doesn't even
  capture).

## Reproduce

```bash
bash ~/.hermes/cache/scratch/prep-template.sh   # once: dependency-complete home
BENCH_TEMPLATE="$TMPDIR/bench-home-template" node harness/bench.ts
```

DB-side comparison runs against the real migrated scratch database
(`hivecast_cron_proto`, chain 0000→0010) from the product repo — see
`feat/runtime-cron-arms` there.
