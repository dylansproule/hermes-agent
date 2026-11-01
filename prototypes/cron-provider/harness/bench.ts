// Full-overhaul benchmark: legacy (builtin ticker, warm runtime, cloud-driven)
// vs overhaul (hivecast provider, cloud-armed, scale-to-zero capable).
//
// Measures: scheduling precision, end-to-end fire latency, idle process cost
// (CPU/RSS), cold-start (scale-to-zero → wake → fire) vs warm, and in-process
// sweep throughput at 10k arms. Siloed: free ports, throwaway HERMES_HOMEs
// cloned from a prepared template (dependency prep done once), --no-agent
// script jobs (no LLM). Run under node ≥ 24 (nvm v24.19.0).
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync, mkdirSync, cpSync, readFileSync, rmSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createCloudServer, type Cloud } from "../server/cloud.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const HERMES = join(process.env.HOME as string, ".hermes/hermes-agent/.hermes/bin/hermes");
const RUNTIME_ID = "rt-bench-1";
const RUNTIME_TOKEN = randomBytes(24).toString("hex");
const API_SERVER_KEY = randomBytes(24).toString("hex");
const ISSUER = "https://cron.proto.hivecast.local";

const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const port = (probe.address() as { port: number }).port;
      probe.close(() => resolve(port));
    });
    setTimeout(() => reject(new Error("port pick timeout")), 5_000);
  });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function run(cmd: string, args: string[], env: NodeJS.ProcessEnv, timeoutMs = 120_000) {
  return new Promise<{ code: number; out: string }>((resolve, reject) => {
    const child = spawn(cmd, args, { env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error(`timeout: ${cmd}\n${out.slice(-1500)}`)); }, timeoutMs);
    child.stdout!.on("data", (c) => (out += c));
    child.stderr!.on("data", (c) => (out += c));
    child.on("close", (code) => { clearTimeout(timer); resolve({ code: code ?? -1, out }); });
    child.on("error", reject);
  });
}

async function waitFor(what: string, probe: () => boolean | Promise<boolean>, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  let lastProgressLog = 0;
  while (Date.now() < deadline) {
    if (await probe()) return;
    if (Date.now() - lastProgressLog > 15_000) {
      lastProgressLog = Date.now();
      const detail = typeof progressDetail === "function" ? progressDetail() : "";
      console.log(`[bench] …waiting for ${what} (${Math.round((deadline - Date.now()) / 1000)}s left)${detail}`);
    }
    await sleep(500);
  }
  throw new Error(`timeout waiting for: ${what}`);
}
let progressDetail: (() => string) | null = null;

type Gateway = { proc: import("node:child_process").ChildProcess; pid: number; log: string; readyMs: number };

async function bootGateway(home: string, apiPort: number): Promise<Gateway> {
  const env = {
    ...process.env,
    HERMES_HOME: home,
    API_SERVER_PORT: String(apiPort),
    API_SERVER_KEY,
    API_SERVER_HOST: "127.0.0.1",
    PATH: `${process.env.HOME}/.hermes/hermes-agent/.hermes/bin:${process.env.PATH ?? ""}`,
  };
  const t0 = Date.now();
  const proc = spawn(HERMES, ["gateway", "run"], { env, stdio: ["ignore", "pipe", "pipe"] });
  let log = "";
  proc.stdout!.on("data", (c) => (log += c));
  proc.stderr!.on("data", (c) => (log += c));
  try {
    await waitFor("gateway api_server", async () => {
      try { await fetch(`http://127.0.0.1:${apiPort}/`, { signal: AbortSignal.timeout(1_000) }); return true; } catch { return false; }
    }, 240_000);
  } catch (error) {
    proc.kill("SIGKILL");
    throw new Error(`${error instanceof Error ? error.message : error}\n--- gateway log tail ---\n${log.slice(-3000)}`);
  }
  return { proc, pid: proc.pid!, log, readyMs: Date.now() - t0 };
}

function writeHome(from: string, apiPort: number, mode: "ticker" | "provider", cloud?: Cloud): string {
  const home = mkdtempSync(join(tmpdir(), `bench-${mode}-`));
  cpSync(from, home, { recursive: true });
  // The dependency-environment fingerprint is config-sensitive: a copied home
  // whose config.yaml is rewritten must boot as a FRESH data root (the recorded
  // env/lock for the old config makes the next boot's repair refuse). Drop the
  // recorded install state; first boot re-preps (~17s).
  rmSync(join(home, "installs"), { recursive: true, force: true });
  // Copied runtime IPC artifacts poison the CLI's gateway-liveness probe
  // (bash cp skips sockets; cpSync copies them as files). Start clean.
  rmSync(join(home, "gateway.sock"), { force: true });
  rmSync(join(home, "gateway.lock"), { force: true });
  rmSync(join(home, "gateway-starts.log"), { force: true });
  rmSync(join(home, "config.yaml"), { force: true });
  rmSync(join(home, "cron"), { recursive: true, force: true });
  const lines = [
    "agent:", "  name: Bench", "cron:",
  ];
  if (mode === "provider" && cloud) {
    lines.push(
      "  provider: hivecast", "  hivecast:",
      `    portal_url: ${cloud.url()}`,
      `    callback_url: http://127.0.0.1:${apiPort}`,
      `    token: ${RUNTIME_TOKEN}`, `    agent_id: ${RUNTIME_ID}`,
      "  chronos:",
      `    nas_jwks_url: ${cloud.url()}/.well-known/jwks.json`,
      `    expected_audience: agent:${RUNTIME_ID}`,
      `    portal_url: ${ISSUER}`,
    );
  }
  lines.push("platforms:", "  api_server:", "    enabled: true", `    port: ${apiPort}`);
  writeFileSync(join(home, "config.yaml"), lines.join("\n"));
  if (mode === "provider") {
    mkdirSync(join(home, "plugins"), { recursive: true });
    cpSync(join(HERE, "..", "provider"), join(home, "plugins", "hivecast"), { recursive: true });
  }
  mkdirSync(join(home, "scripts"), { recursive: true });
  // Every run appends a timestamp — the measured timeline (BSD date: seconds).
  writeFileSync(join(home, "scripts", "tick.sh"), "#!/bin/sh\ndate +%s >> \"" + join(home, "fire.log") + "\"\n", { mode: 0o755 });
  return home;
}

function sampleProcess(pid: number) {
  try {
    const out = execFileSync("/bin/ps", ["-o", "rss=,%cpu=", "-p", String(pid)], { encoding: "utf8" }).trim();
    const [rss, cpu] = out.split(/\s+/);
    return { rssKb: Number(rss), cpuPercent: Number(cpu) };
  } catch { return null; }
}

async function idleCost(pid: number, seconds = 20) {
  const samples: Array<{ rssKb: number; cpuPercent: number }> = [];
  const t0 = Date.now();
  let polls = 0;
  while (Date.now() - t0 < seconds * 1000) {
    const s = sampleProcess(pid);
    if (s) { samples.push(s); polls += 1; }
    await sleep(seconds * 1000 / 10);
  }
  const cpuAvg = samples.reduce((a, s) => a + s.cpuPercent, 0) / Math.max(samples.length, 1);
  return { sampleSeconds: seconds, polls, cpuAvgPercent: Number(cpuAvg.toFixed(2)), rssMaxKb: Math.max(...samples.map((s) => s.rssKb)) };
}

const readFireLog = (home: string) =>
  existsSync(join(home, "fire.log"))
    ? readFileSync(join(home, "fire.log"), "utf8").trim().split("\n").filter(Boolean).map(Number)
    : [];

const results: Record<string, unknown> = {};

async function scenarioTicker(template: string) {
  console.log("\n[bench] scenario 1: LEGACY — builtin ticker, warm runtime, gateway always on");
  const apiPort = await freePort();
  const home = writeHome(template, apiPort, "ticker");
  console.log(`[bench] ticker home: ${home} (api ${apiPort})`);
  const gw = await bootGateway(home, apiPort);
  results.legacy_boot_to_ready_ms = gw.readyMs;
  console.log(`[bench] boot→ready: ${gw.readyMs}ms`);
  try {
    const env = { ...process.env, HERMES_HOME: home, PATH: `${process.env.HOME}/.hermes/hermes-agent/.hermes/bin:${process.env.PATH ?? ""}` };
    // Staggered one-shots instead of a recurring job: the in-process ticker's
    // recurrence stalled after the first fire in a throwaway home (observed,
    // reported as a caveat) — one-shots are the reliable measurement vehicle
    // and match the overhaul scenario exactly.
    const schedules = ["in 1m", "in 2m", "in 3m"];
    const createT0 = Date.now();
    for (const [index, schedule] of schedules.entries()) {
      const created = await run(HERMES, ["cron", "create", schedule, "--no-agent", "--script", "tick.sh", "--deliver", "local", "--name", `bench-ticker-${index}`], env);
      if (created.code !== 0) throw new Error(`ticker create failed (${schedule}): ${created.out}`);
    }
    const jobsPath = join(home, "cron", "jobs.json");
    if (!existsSync(jobsPath)) throw new Error(`jobs.json missing in ${home} — create landed elsewhere. Gateway log tail:\n${gw.log.slice(-2000)}`);
    console.log(`[bench] 3 staggered one-shots created (1m/2m/3m)`);
    progressDetail = () => ` fires=${readFireLog(home).length}`;
    await waitFor("3 legacy runs", () => readFireLog(home).length >= 3, 360_000);
    const fires = readFireLog(home);
    // Per-fire latency: scheduled (+60/120/180s from create) → actual run.
    const latencies = fires.map((ranAt, i) => Number(((ranAt - createT0 / 1000 - [60, 120, 180][i]) * 1000).toFixed(0)));
    results.legacy_fire_latencies_ms = latencies;
    results.legacy_first_fire_ms = Number(((fires[0]! - createT0 / 1000) * 1000).toFixed(0));
    results.legacy_fire_times = fires;
    console.log(`[bench] first fire after create: ${results.legacy_first_fire_ms}ms; latencies vs schedule: ${latencies.join(", ")}ms`);
    const cost = await idleCost(gw.pid, 20);
    results.legacy_idle = cost;
    console.log(`[bench] idle: cpu=${cost.cpuAvgPercent}% rss=${cost.rssMaxKb}KB over ${cost.sampleSeconds}s`);
  } finally {
    progressDetail = null;
    gw.proc.kill("SIGKILL");
    rmSync(home, { recursive: true, force: true, maxRetries: 3 });
  }
}

async function scenarioProvider(template: string) {
  console.log("\n[bench] scenario 2: OVERHAUL — hivecast provider, cloud-armed, warm runtime");
  const apiPort = await freePort();
  const cloud: Cloud = await createCloudServer({ runtimeId: RUNTIME_ID, runtimeToken: RUNTIME_TOKEN, sweepMs: 500, issuer: ISSUER });
  const home = writeHome(template, apiPort, "provider", cloud);
  console.log(`[bench] provider home: ${home} (api ${apiPort})`);
  const gw = await bootGateway(home, apiPort);
  results.overhaul_boot_to_ready_ms = gw.readyMs;
  try {
  const env = { ...process.env, HERMES_HOME: home, API_SERVER_PORT: String(apiPort), API_SERVER_KEY, API_SERVER_HOST: "127.0.0.1", PATH: `${process.env.HOME}/.hermes/hermes-agent/.hermes/bin:${process.env.PATH ?? ""}` };
    const tCreate = Date.now();
    // Same staggered one-shots as the legacy scenario — identical measurement.
    const schedules = ["in 1m", "in 2m", "in 3m"];
    for (const [index, schedule] of schedules.entries()) {
      const created = await run(HERMES, ["cron", "create", schedule, "--no-agent", "--script", "tick.sh", "--deliver", "local", "--name", `bench-provider-${index}`], env);
      if (created.code !== 0) throw new Error(`provider create failed (${schedule}): ${created.out}`);
    }
    await waitFor("arms provisioned", () => cloud.arms().size >= 3, 30_000);
    results.overhaul_arm_latency_ms = Date.now() - tCreate; // create ×3 → 3 arms visible
    console.log(`[bench] create×3→3 arms: ${results.overhaul_arm_latency_ms}ms`);
    progressDetail = () => ` fires=${cloud.fireLog().filter((f) => f.ok).length} arms=${cloud.arms().size}`;
    await waitFor("3 overhaul runs", () => readFireLog(home).length >= 3, 360_000);
    const okFires = cloud.fireLog().filter((f) => f.ok);
    // Fire precision: accepted POST timestamp vs armed fire_at (ms late).
    const precisions = okFires
      .map((f) => Number((Date.parse(f.at) - Date.parse(f.fireAt)).toFixed(0)))
      .sort((a, b) => a - b);
    results.overhaul_fire_precision_ms = precisions;
    results.overhaul_fire_count = okFires.length;
    console.log(`[bench] accepted fires: ${okFires.length}; precision vs fire_at (ms late): ${precisions.join(", ")}`);
  const cost = await idleCost(gw.pid, 20);
  results.overhaul_idle = cost;
  console.log(`[bench] idle: cpu=${cost.cpuAvgPercent}% rss=${cost.rssMaxKb}KB over ${cost.sampleSeconds}s`);
  } finally {
  gw.proc.kill("SIGKILL");
  await cloud.close();
  rmSync(home, { recursive: true, force: true, maxRetries: 3 });
  }
  }

async function scenarioScaleToZero(template: string) {
  console.log("\n[bench] scenario 3: OVERHAUL scale-to-zero — runtime DOWN at arm time, woken by fire");
  const apiPort = await freePort();
  const cloud: Cloud = await createCloudServer({ runtimeId: RUNTIME_ID, runtimeToken: RUNTIME_TOKEN, sweepMs: 1_000, issuer: ISSUER, maxAttempts: 50 });
  const home = writeHome(template, apiPort, "provider", cloud);
  // Boot once to register the job, then HARD-STOP the runtime (scale to zero).
  const gw = await bootGateway(home, apiPort);
  const env = { ...process.env, HERMES_HOME: home, API_SERVER_PORT: String(apiPort), API_SERVER_KEY, API_SERVER_HOST: "127.0.0.1", PATH: `${process.env.HOME}/.hermes/hermes-agent/.hermes/bin:${process.env.PATH ?? ""}` };
  const created = await run(HERMES, ["cron", "create", "in 2m", "--no-agent", "--script", "tick.sh", "--deliver", "local", "--name", "bench-cold"], env);
  if (created.code !== 0) throw new Error(`cold create failed: ${created.out}`);
  await waitFor("arm provisioned", () => cloud.arms().size > 0, 30_000);
  const arm = [...cloud.arms().values()][0]!;
  gw.proc.kill("SIGKILL");
  await sleep(1_500);
  console.log(`[bench] runtime stopped; arm fires at ${arm.fireAt}. Playing agent-host: wake on first failed fire.`);
  const wakeAt = Date.now();
  const fireAt = Date.parse(arm.fireAt);
  // Agent-host behavior: poll the cloud's fire log; first failed attempt → boot the gateway.
  await waitFor("first refused fire", () => cloud.fireLog().length > 0, 200_000);
  const booted = await bootGateway(home, apiPort);
  const readyAt = Date.now();
  await waitFor("cold one-shot fired", () => cloud.fireLog().some((f) => f.ok), 200_000);
  const firedAt = Date.now();
  await waitFor("job terminal", () => readFireLog(home).length >= 1, 120_000);
  results.scale_to_zero = {
    fire_at_epoch_ms: fireAt,
    wake_started_epoch_ms: wakeAt,
    gateway_boot_to_ready_ms: booted.readyMs,
    ready_epoch_ms: readyAt,
    accepted_epoch_ms: firedAt,
    job_done_epoch_ms: Date.now(),
    fire_at_to_accepted_ms: firedAt - fireAt,
    refused_attempts_before_wake: cloud.fireLog().length - 1,
  };
  console.log(`[bench] boot→ready (warm cache): ${booted.readyMs}ms; fire_at→accepted: ${firedAt - fireAt}ms`);
  booted.proc.kill("SIGKILL");
  await cloud.close();
  rmSync(home, { recursive: true, force: true, maxRetries: 3 });
}

function scenarioSweepThroughput() {
  console.log("\n[bench] scenario 4: cloud sweep throughput (in-memory registry, 10k arms / 100 due)");
  const arms = new Map<string, { status: string; fireAt: number }>();
  const now = Date.now();
  for (let i = 0; i < 10_000; i += 1) {
    arms.set(`job-${i}`, { status: "pending", fireAt: now + (i < 100 ? -1_000 : i * 60) });
  }
  const t0 = performance.now();
  let due = 0;
  for (const arm of arms.values()) if (arm.status === "pending" && arm.fireAt <= now) due += 1;
  const passMs = performance.now() - t0;
  results.sweep_throughput = { arms: arms.size, due, full_scan_pass_ms: Number(passMs.toFixed(3)) };
  console.log(`[bench] full-scan sweep pass: ${passMs.toFixed(3)}ms over 10k arms (${due} due)`);
}

async function makeTemplate() {
  if (process.env.BENCH_TEMPLATE) {
    console.log(`[bench] using pre-prepared template: ${process.env.BENCH_TEMPLATE}`);
    return process.env.BENCH_TEMPLATE;
  }
  console.log("[bench] preparing home template (dependency prep once)…");
  const apiPort = await freePort();
  const template = mkdtempSync(join(tmpdir(), "bench-template-"));
  writeFileSync(join(template, "config.yaml"), ["agent:", "  name: Template", "platforms:", "  api_server:", "    enabled: true", `    port: ${apiPort}`].join("\n"));
  const gw = await bootGateway(template, apiPort);
  gw.proc.kill("SIGKILL");
  await sleep(500);
  return template;
}

async function main() {
  const template = await makeTemplate();
  try {
    await scenarioTicker(template);
    await scenarioProvider(template);
    await scenarioScaleToZero(template);
    scenarioSweepThroughput();
  } finally {
    if (!process.env.BENCH_TEMPLATE) rmSync(template, { recursive: true, force: true, maxRetries: 3 });
  }
  writeFileSync(join(HERE, "bench-results.json"), JSON.stringify(results, null, 2));
  console.log("\n[bench] results written to harness/bench-results.json");
  console.log(JSON.stringify(results, null, 2));
}

void main();
