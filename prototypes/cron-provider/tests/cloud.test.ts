import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createServer, type Server } from "node:http";

import { createCloudServer, type Cloud } from "../server/cloud.ts";
import { loadOrCreateKeys, signFireJwt, verifyFireJwt } from "../server/keys.ts";

const TOKEN = "proto-runtime-token";
const RUNTIME_ID = "rt-proto-1";
const keys = loadOrCreateKeys();

let cloud: Cloud;
let agent: Server;
let agentPort: number;
const agentRequests: Array<{ auth: string; body: any }> = [];
let agentMode: { status: number } | "ok" = "ok";

beforeAll(async () => {
  cloud = await createCloudServer({ runtimeId: RUNTIME_ID, runtimeToken: TOKEN, sweepMs: 200, keys });
  agent = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      agentRequests.push({ auth: req.headers.authorization ?? "", body: JSON.parse(body || "{}") });
      const status = agentMode === "ok" ? 202 : agentMode.status;
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: status === 202 ? "accepted" : "error" }));
    });
  });
  agentPort = await new Promise<number>((resolve) => agent.listen(0, "127.0.0.1", () => resolve((agent.address() as { port: number }).port)));
});

afterAll(async () => {
  await cloud.close();
  agent.close();
});

const api = (path: string, init?: RequestInit) =>
  fetch(`${cloud.url()}${path}`, {
    ...init,
    headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json", ...(init?.headers ?? {}) },
  });

const provision = (jobId: string, fireAt: string, dedupKey = `${jobId}:${fireAt}`) =>
  api("/api/agent-cron/provision", {
    method: "POST",
    body: JSON.stringify({ job_id: jobId, fire_at: fireAt, agent_callback_url: `http://127.0.0.1:${agentPort}`, dedup_key: dedupKey }),
  });

describe("agent-cron management endpoints", () => {
  it("rejects a wrong bearer with 401", async () => {
    const response = await fetch(`${cloud.url()}/api/agent-cron/list`, { headers: { authorization: "Bearer wrong" } });
    expect(response.status).toBe(401);
  });

  it("provisions an arm and lists it", async () => {
    const future = new Date(Date.now() + 60_000).toISOString();
    const created = await provision("job-list", future);
    expect(created.status).toBe(200);
    expect(typeof (await created.json()).schedule_id).toBe("string");
    const list = await (await api("/api/agent-cron/list")).json();
    expect(list.armed).toHaveLength(1);
    expect(list.armed[0]).toMatchObject({ job_id: "job-list", fire_at: future });
  });

  it("re-provisioning the same (job, fire) is an idempotent no-op — one arm, not two", async () => {
    const future = new Date(Date.now() + 60_000).toISOString();
    await provision("job-idem", future);
    const again = await provision("job-idem", future);
    expect(again.status).toBe(200);
    expect([...cloud.arms().keys()].filter((key) => key.endsWith(":job-idem"))).toHaveLength(1);
  });

  it("a new fire_at for the same job replaces the prior arm (upsert by (agent, job))", async () => {
    const soon = new Date(Date.now() + 60_000).toISOString();
    const later = new Date(Date.now() + 120_000).toISOString();
    await provision("job-upsert", soon);
    await provision("job-upsert", later);
    const arm = cloud.arms().get(`${RUNTIME_ID}:job-upsert`)!;
    expect(arm.fireAt).toBe(later);
    expect([...cloud.arms().keys()].filter((key) => key.endsWith(":job-upsert"))).toHaveLength(1);
  });

  it("cancel is idempotent — unknown job is a 200 no-op", async () => {
    const first = await api("/api/agent-cron/cancel", { method: "POST", body: JSON.stringify({ job_id: "never-existed" }) });
    expect(first.status).toBe(200);
    expect(await first.json()).toEqual({ ok: true });
    const second = await api("/api/agent-cron/cancel", { method: "POST", body: JSON.stringify({ job_id: "never-existed" }) });
    expect(second.status).toBe(200);
  });

  it("rejects malformed bodies", async () => {
    const missing = await api("/api/agent-cron/provision", { method: "POST", body: JSON.stringify({ job_id: "x" }) });
    expect(missing.status).toBe(400);
    const badDate = await provision("job-bad", "not-a-date");
    expect(badDate.status).toBe(400);
  });
});

describe("fire JWT", () => {
  const keys = loadOrCreateKeys();

  it("round-trips with the Chronos claim shape and verifies", () => {
    const token = signFireJwt(keys, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}`, jobId: "j1", fireAt: "2026-11-01T00:00:00Z" });
    const result = verifyFireJwt(keys.publicKey, token, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}` });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.purpose).toBe("cron_fire");
      expect(result.payload.job_id).toBe("j1");
      expect(Number(result.payload.exp) - Number(result.payload.iat)).toBeLessThanOrEqual(120);
    }
  });

  it("fails closed on wrong audience, purpose, issuer, and expiry", () => {
    const base = { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}`, jobId: "j1", fireAt: "x" };
    const wrongAud = signFireJwt(keys, { ...base, aud: "agent:someone-else" });
    expect(verifyFireJwt(keys.publicKey, wrongAud, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}` })).toMatchObject({ ok: false, reason: "wrong_aud" });
    const wrongPurpose = signFireJwt(keys, { ...base, purpose: "general" });
    expect(verifyFireJwt(keys.publicKey, wrongPurpose, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}` })).toMatchObject({ ok: false, reason: "wrong_purpose" });
    const wrongIss = signFireJwt(keys, { ...base, iss: "evil" });
    expect(verifyFireJwt(keys.publicKey, wrongIss, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}` })).toMatchObject({ ok: false, reason: "wrong_iss" });
    const expired = signFireJwt(keys, { ...base, issuedAt: Math.floor(Date.now() / 1000) - 600, ttlSeconds: 60 });
    expect(verifyFireJwt(keys.publicKey, expired, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}`, leewaySeconds: 0 })).toMatchObject({ ok: false, reason: "expired" });
  });

  it("publishes an RS256 signing key via JWKS", async () => {
    const response = await fetch(`${cloud.url()}/.well-known/jwks.json`);
    const doc = await response.json();
    expect(doc.keys).toHaveLength(1);
    expect(doc.keys[0]).toMatchObject({ kty: "RSA", alg: "RS256", use: "sig" });
    expect(typeof doc.keys[0].n).toBe("string");
  });
});

describe("fire loop (due arm → agent webhook)", () => {
  it("fires a due arm exactly once with a valid JWT, then drops it from list", async () => {
    agentRequests.length = 0;
    const fireAt = new Date(Date.now() + 400).toISOString();
    await provision("job-fire-once", fireAt);

    await vi.waitFor(() => expect(cloud.fireLog().filter((entry) => entry.jobId === "job-fire-once" && entry.ok)).toHaveLength(1), { timeout: 10_000 });
    // at-most-once: give the sweeper several passes, still exactly one fire POST
    await new Promise((resolve) => setTimeout(resolve, 1200));
    expect(cloud.fireLog().filter((entry) => entry.jobId === "job-fire-once")).toHaveLength(1);
    expect(agentRequests.filter((request) => request.body.job_id === "job-fire-once")).toHaveLength(1);

    const seen = agentRequests.find((request) => request.body.job_id === "job-fire-once")!;
    const token = seen.auth.replace("Bearer ", "");
    const verified = verifyFireJwt(keys.publicKey, token, { iss: "proto-hivecast", aud: `agent:${RUNTIME_ID}` });
    expect(verified.ok).toBe(true);

    const list = await (await api("/api/agent-cron/list")).json();
    expect(list.armed.find((arm: { job_id: string }) => arm.job_id === "job-fire-once")).toBeUndefined();
  }, 15_000);

  it("retries a non-2xx agent response, then stops after a 2xx (no double fire)", async () => {
    agentRequests.length = 0;
    agentMode = { status: 503 };
    const fireAt = new Date(Date.now() + 300).toISOString();
    await provision("job-retry", fireAt);

    // A few failed attempts land first (agent 503).
    await vi.waitFor(() => expect(cloud.fireLog().filter((entry) => entry.jobId === "job-retry" && !entry.ok).length).toBeGreaterThanOrEqual(2), { timeout: 10_000 });
    agentMode = "ok";
    await vi.waitFor(() => expect(cloud.fireLog().filter((entry) => entry.jobId === "job-retry" && entry.ok)).toHaveLength(1), { timeout: 10_000 });
    await new Promise((resolve) => setTimeout(resolve, 1000));
    expect(cloud.fireLog().filter((entry) => entry.jobId === "job-retry" && entry.ok)).toHaveLength(1);
  }, 20_000);

  it("gives up after maxAttempts and marks the arm failed", async () => {
    agentMode = { status: 503 };
    const fireAt = new Date(Date.now() + 200).toISOString();
    await provision("job-doomed", fireAt);
    await vi.waitFor(() => expect(cloud.arms().get(`${RUNTIME_ID}:job-doomed`)?.status).toBe("failed"), { timeout: 20_000 });
    expect(cloud.arms().get(`${RUNTIME_ID}:job-doomed`)!.attempts).toBe(5);
    agentMode = "ok";
  }, 25_000);
});
