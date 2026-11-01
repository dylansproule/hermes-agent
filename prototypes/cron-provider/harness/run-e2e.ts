// Siloed e2e: HiveCast prototype cloud + a REAL Hermes gateway in a throwaway
// HERMES_HOME. Every port is picked free at runtime (listen(0)) — this can run
// alongside a dev stack without fighting for ports. Nothing touches
// ~/.hermes/hermes-agent or the user's real home.
//
// Loop under test (Chronos contract, HiveCast as the control plane):
//   hermes cron create '30s' --no-agent  →  jobs.json  →  provider.register_job
//   → cloud provision (arm)  →  sweep  →  JWT fire POST /api/cron/fire
//   → gateway verifies JWT (JWKS)  →  202  →  job runs (no agent turn)
//   → one-shot completes; no re-arm.
// Plus negative: a forged bearer to /api/cron/fire must 401.
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync, mkdirSync, cpSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createCloudServer, type Cloud } from "../server/cloud.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const HERMES = join(process.env.HOME as string, ".hermes/hermes-agent/.hermes/bin/hermes");
const RUNTIME_ID = "rt-e2e-1";
const RUNTIME_TOKEN = randomBytes(24).toString("hex");
const API_SERVER_KEY = randomBytes(24).toString("hex");
const ISSUER = "https://cron.proto.hivecast.local";
const PORT_PICK_TIMEOUT = 5_000;

const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const port = (probe.address() as { port: number }).port;
      probe.close(() => resolve(port));
    });
    setTimeout(() => reject(new Error("port pick timeout")), PORT_PICK_TIMEOUT);
  });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function run(cmd: string, args: string[], env: NodeJS.ProcessEnv, timeoutMs = 60_000) {
  return new Promise<{ code: number; out: string }>((resolve, reject) => {
    const child = spawn(cmd, args, { env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`timeout: ${cmd} ${args.join(" ")}\n${out.slice(-2000)}`));
    }, timeoutMs);
    child.stdout!.on("data", (chunk) => (out += chunk));
    child.stderr!.on("data", (chunk) => (out += chunk));
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ code: code ?? -1, out });
    });
    child.on("error", reject);
  });
}

async function waitFor(what: string, probe: () => boolean | Promise<boolean>, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await sleep(500);
  }
  throw new Error(`timeout waiting for: ${what}`);
}

async function main() {
  console.log("[e2e] picking free ports…");
  const apiPort = await freePort();
  const home = mkdtempSync(join(tmpdir(), "hivecast-proto-home-"));
  const cloud: Cloud = await createCloudServer({
    runtimeId: RUNTIME_ID,
    runtimeToken: RUNTIME_TOKEN,
    sweepMs: 500,
    issuer: ISSUER, // the gateway's verifier requires iss == cron.chronos.portal_url (below)
  });
  console.log(`[e2e] cloud on ${cloud.url()} (jwks ${cloud.url()}/.well-known/jwks.json)`);
  console.log(`[e2e] hermes home: ${home}`);
  console.log(`[e2e] agent api_server will bind 127.0.0.1:${apiPort}`);

  // 1. Inject the provider as a USER plugin — $HERMES_HOME/plugins/hivecast/.
  mkdirSync(join(home, "plugins"), { recursive: true });
  cpSync(join(HERE, "..", "provider"), join(home, "plugins", "hivecast"), { recursive: true });

  // 2. Minimal config: cron provider + fire-verifier keys (chronos-namespaced —
  //    the shared handler's verifier reads cron.chronos.*; see provider README note).
  const callbackUrl = `http://127.0.0.1:${apiPort}`;
  const configYaml = [
    "agent:",
    "  name: HiveCast Proto",
    "cron:",
    "  provider: hivecast",
    "  hivecast:",
    `    portal_url: ${cloud.url()}`,
    `    callback_url: ${callbackUrl}`,
    `    token: ${RUNTIME_TOKEN}`,
    `    agent_id: ${RUNTIME_ID}`,
    "  chronos:",
    `    nas_jwks_url: ${cloud.url()}/.well-known/jwks.json`,
    `    expected_audience: agent:${RUNTIME_ID}`,
    `    portal_url: ${ISSUER}`,
    "platforms:",
    "  api_server:",
    "    enabled: true",
    `    port: ${apiPort}`,
  ].join("\n");
  writeFileSync(join(home, "config.yaml"), configYaml);

  const env = {
    ...process.env,
    HERMES_HOME: home,
    API_SERVER_PORT: String(apiPort),
    API_SERVER_KEY,
    API_SERVER_HOST: "127.0.0.1",
    PATH: `${process.env.HOME}/.hermes/hermes-agent/.hermes/bin:${process.env.PATH ?? ""}`,
  };

  // 3. Negative probe first (before the gateway boots): forged fire must not 401-passthrough later either.
  // 4. Boot the gateway in the foreground.
  console.log("[e2e] booting hermes gateway run …");
  const gateway = spawn(HERMES, ["gateway", "run"], { env, stdio: ["ignore", "pipe", "pipe"] });
  let gatewayLog = "";
  gateway.stdout!.on("data", (chunk) => (gatewayLog += chunk));
  gateway.stderr!.on("data", (chunk) => (gatewayLog += chunk));

  const failures: string[] = [];
  try {
    // Gateway up = api_server listening on our port.
    await waitFor("gateway api_server listening", async () => {
      try {
        await fetch(`${callbackUrl}/`, { signal: AbortSignal.timeout(1000) });
        return true;
      } catch {
        return false;
      }
    }, 90_000);
    console.log("[e2e] gateway is up");

    // 5. Forged JWT must fail closed.
    const forged = await fetch(`${callbackUrl}/api/cron/fire`, {
      method: "POST",
      headers: { authorization: "Bearer forged.token.here", "content-type": "application/json" },
      body: JSON.stringify({ job_id: "x", fire_at: new Date().toISOString() }),
      signal: AbortSignal.timeout(5000),
    });
    if (forged.status === 401) console.log("[e2e] forged fire JWT → 401 ✓");
    else failures.push(`forged JWT answered ${forged.status}, expected 401`);

    // 5b. A script for --no-agent mode (the script IS the job; no LLM anywhere).
    mkdirSync(join(home, "scripts"), { recursive: true });
    writeFileSync(join(home, "scripts", "proto-one-shot.sh"), "#!/bin/sh\necho \"hivecast proto one-shot ran\"\n", { mode: 0o755 });

    // 6. Create a one-shot cron job (no agent turn — hermetic).
    const created = await run(HERMES, ["cron", "create", "in 1m", "--no-agent", "--script", "proto-one-shot.sh", "--deliver", "local", "--name", "proto-one-shot"], env);
    if (created.code !== 0) throw new Error(`cron create failed:\n${created.out}`);
    console.log("[e2e] cron job created (in 1m one-shot, --no-agent)");

    // 7. Provider must have armed it with the cloud.
    await waitFor("provision (arm) to arrive at cloud", () => cloud.arms().size > 0, 30_000);
    const arm = [...cloud.arms().values()][0]!;
    console.log(`[e2e] armed: job=${arm.jobId} fire_at=${arm.fireAt} schedule=${arm.scheduleId}`);

    // 8. Fire lands, agent 202s, job runs.
    await waitFor("fire to be delivered and accepted", () => cloud.fireLog().some((entry) => entry.ok), 90_000);
    const fired = cloud.fireLog().find((entry) => entry.ok)!;
    console.log(`[e2e] fired ✓ agent=${fired.status} at=${fired.at}`);

    await waitFor("job to complete (one-shot terminal)", () => {
      try {
        const raw = JSON.parse(readFileSync(join(home, "cron", "jobs.json"), "utf8"));
        const list: unknown[] = Array.isArray(raw) ? raw : Array.isArray(raw?.jobs) ? raw.jobs : [];
        const job = list.find((entry) => (entry as { name?: string })?.name === "proto-one-shot") as
          | { state?: string; enabled?: boolean; next_run_at?: string | null }
          | undefined;
        // One-shot completion: enabled=False, state="completed", next_run_at=None (cron/jobs.py mark_job_run).
        return Boolean(job && job.state === "completed" && job.enabled === false);
      } catch {
        return false;
      }
    }, 90_000);
    console.log("[e2e] job completed in jobs.json ✓");

    // At-most-once: exactly one successful fire for this job.
    await sleep(3_000);
    const okFires = cloud.fireLog().filter((entry) => entry.ok && entry.jobId === arm.jobId);
    if (okFires.length === 1) console.log("[e2e] exactly one successful fire ✓");
    else failures.push(`expected 1 successful fire, saw ${okFires.length}`);
  } catch (error) {
    failures.push(error instanceof Error ? error.message : String(error));
    failures.push(`--- cloud arms ---\n${JSON.stringify([...cloud.arms().values()], null, 2)}`);
    failures.push(`--- cloud fire log ---\n${JSON.stringify(cloud.fireLog(), null, 2)}`);
    failures.push(`--- gateway log tail ---\n${gatewayLog.slice(-4000)}`);
  } finally {
    gateway.kill("SIGTERM");
    await cloud.close();
  }

  if (failures.length > 0) {
    console.error("\n[e2e] FAILURES:\n" + failures.join("\n"));
    try {
      rmSync(home, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
    } catch {
      console.error(`[e2e] (home left behind: ${home})`);
    }
    process.exit(1);
  }
  console.log("\n[e2e] ALL CHECKS PASSED");
  console.log(`[e2e] home kept for inspection: ${home}`);
}

void main();
