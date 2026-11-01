// HiveCast-as-NAS prototype cloud — node:http only, zero deps.
// Implements the Chronos wire contract from the agent's perspective:
//   POST /api/agent-cron/provision   (arm/re-arm one one-shot, upsert by (agent, job))
//   POST /api/agent-cron/cancel      (idempotent)
//   GET  /api/agent-cron/list        (armed one-shots for agent reconcile)
//   GET  /.well-known/jwks.json      (RS256 key the agent verifies fire JWTs against)
// plus the fire side: sweep due arms, mint a purpose=cron_fire JWT, POST the
// agent's /api/cron/fire, retry non-2xx, never double-fire after a 2xx.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { loadOrCreateKeys, jwksFor, signFireJwt, type KeyPair } from "./keys.ts";

export type Arm = {
  agentId: string;
  jobId: string;
  fireAt: string; // ISO 8601 — agent-computed
  callbackUrl: string;
  dedupKey: string;
  scheduleId: string;
  status: "pending" | "firing" | "fired" | "failed";
  attempts: number;
  lastError?: string;
};

export type FireAttempt = {
  jobId: string;
  fireAt: string;
  at: string;
  ok: boolean;
  status?: number;
  error?: string;
};

export type CloudOptions = {
  runtimeId: string;
  runtimeToken: string;
  keys?: KeyPair;
  sweepMs?: number;
  maxAttempts?: number;
  issuer?: string;
  /** Test hook: invoked before each fire POST. */
  onFireAttempt?: (attempt: FireAttempt) => void;
};

export type Cloud = {
  server: Server;
  port: number;
  url: () => string;
  arms: () => Map<string, Arm>;
  fireLog: () => FireAttempt[];
  close: () => Promise<void>;
};

export function createCloudServer(opts: CloudOptions): Promise<Cloud> {
  const keys = opts.keys ?? loadOrCreateKeys();
  const issuer = opts.issuer ?? "proto-hivecast";
  const maxAttempts = opts.maxAttempts ?? 5;
  const arms = new Map<string, Arm>(); // key: `${agentId}:${jobId}` — upsert semantics
  const fireLog: FireAttempt[] = [];
  let sweeping: Promise<void> | null = null;
  let timer: ReturnType<typeof setInterval> | null = null;

  const auth = (req: IncomingMessage, res: ServerResponse): string | null => {
    const header = req.headers.authorization ?? "";
    const token = header.startsWith("Bearer ") ? header.slice(7) : "";
    if (token !== opts.runtimeToken) {
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_token" }));
      return null;
    }
    return opts.runtimeId; // one provisioned runtime in the prototype
  };

  const readJson = (req: IncomingMessage) =>
    new Promise<Record<string, unknown>>((resolve, reject) => {
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk;
        if (body.length > 64 * 1024) reject(new Error("body_too_large"));
      });
      req.on("end", () => {
        try {
          resolve(body ? (JSON.parse(body) as Record<string, unknown>) : {});
        } catch {
          reject(new Error("invalid_json"));
        }
      });
      req.on("error", reject);
    });

  async function fireOne(arm: Arm): Promise<void> {
    arm.attempts += 1;
    const token = signFireJwt(keys, {
      iss: issuer,
      aud: `agent:${arm.agentId}`,
      jobId: arm.jobId,
      fireAt: arm.fireAt,
    });
    const attempt: FireAttempt = { jobId: arm.jobId, fireAt: arm.fireAt, at: new Date().toISOString(), ok: false };
    try {
      const response = await fetch(`${arm.callbackUrl.replace(/\/$/, "")}/api/cron/fire`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ job_id: arm.jobId, fire_at: arm.fireAt }),
        signal: AbortSignal.timeout(10_000),
      });
      attempt.status = response.status;
      attempt.ok = response.ok;
      if (response.ok) {
        arm.status = "fired";
      } else {
        arm.lastError = `agent answered ${response.status}`;
      }
    } catch (error) {
      attempt.error = error instanceof Error ? error.message : String(error);
      arm.lastError = attempt.error;
    }
    fireLog.push(attempt);
    opts.onFireAttempt?.(attempt);
    if (!attempt.ok && arm.attempts >= maxAttempts) arm.status = "failed";
  }

  async function sweep(): Promise<void> {
    const now = Date.now();
    for (const arm of arms.values()) {
      if (arm.status !== "pending") continue;
      if (Date.parse(arm.fireAt) > now) continue;
      await fireOne(arm); // sequential: prototype scale is one runtime
    }
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://proto");
    const send = (status: number, body: unknown) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };

    try {
      if (req.method === "GET" && url.pathname === "/.well-known/jwks.json") {
        return send(200, jwksFor(keys)); // public: the agent fetches this to verify fire JWTs
      }
      if (url.pathname.startsWith("/api/agent-cron/")) {
        const agentId = auth(req, res);
        if (!agentId) return;

        if (req.method === "POST" && url.pathname === "/api/agent-cron/provision") {
          const body = await readJson(req);
          const jobId = typeof body.job_id === "string" ? body.job_id : "";
          const fireAt = typeof body.fire_at === "string" ? body.fire_at : "";
          const callbackUrl = typeof body.agent_callback_url === "string" ? body.agent_callback_url : "";
          const dedupKey = typeof body.dedup_key === "string" ? body.dedup_key : "";
          if (!jobId || !fireAt || !callbackUrl || !dedupKey) {
            return send(400, { error: "job_id, fire_at, agent_callback_url, dedup_key required" });
          }
          if (Number.isNaN(Date.parse(fireAt))) return send(400, { error: "fire_at must be ISO 8601" });
          const key = `${agentId}:${jobId}`;
          const existing = arms.get(key);
          // Idempotent re-arm of the same fire: no-op success.
          if (existing && existing.dedupKey === dedupKey && existing.status !== "fired") {
            return send(200, { schedule_id: existing.scheduleId });
          }
          const arm: Arm = {
            agentId,
            jobId,
            fireAt,
            callbackUrl,
            dedupKey,
            scheduleId: randomUUID(),
            status: "pending",
            attempts: 0,
          };
          arms.set(key, arm); // upsert by (agent, job): new fire_at replaces the prior arm
          return send(200, { schedule_id: arm.scheduleId });
        }

        if (req.method === "POST" && url.pathname === "/api/agent-cron/cancel") {
          const body = await readJson(req);
          const jobId = typeof body.job_id === "string" ? body.job_id : "";
          if (!jobId) return send(400, { error: "job_id required" });
          arms.delete(`${agentId}:${jobId}`); // idempotent: unknown job is a 200 no-op
          return send(200, { ok: true });
        }

        if (req.method === "GET" && url.pathname === "/api/agent-cron/list") {
          const armed = [...arms.values()]
            .filter((arm) => arm.agentId === agentId && arm.status !== "fired" && arm.status !== "failed")
            .map(({ jobId, fireAt, scheduleId }) => ({ job_id: jobId, fire_at: fireAt, schedule_id: scheduleId }));
          return send(200, { armed });
        }

        return send(404, { error: "not_found" });
      }
      return send(404, { error: "not_found" });
    } catch (error) {
      return send(500, { error: error instanceof Error ? error.message : "internal" });
    }
  });

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      const runSweep = () => {
        if (sweeping) return;
        sweeping = sweep().finally(() => {
          sweeping = null;
        });
      };
      timer = setInterval(runSweep, opts.sweepMs ?? 1000);
      void runSweep();
      resolve({
        server,
        port,
        url: () => `http://127.0.0.1:${port}`,
        arms: () => arms,
        fireLog: () => fireLog,
        close: () =>
          new Promise<void>((done) => {
            if (timer) clearInterval(timer);
            server.close(() => done());
          }),
      });
    });
  });
}
