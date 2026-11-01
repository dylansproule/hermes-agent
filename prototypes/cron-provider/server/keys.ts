// RS256 key + JWT + JWKS helpers — node:crypto only, zero deps.
// Implements exactly the Chronos fire-JWT shape:
//   header  { alg: "RS256", kid }
//   payload { iss, aud: "agent:{id}", purpose: "cron_fire", job_id, fire_at, iat, nbf, exp }
import { createSign, createVerify, generateKeyPairSync, createPublicKey } from "node:crypto";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export type KeyPair = { publicKey: string; privateKey: string; kid: string };

const b64url = (buf: Buffer | string) =>
  Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

/** RSA 2048 keypair with a stable kid. Persists to `dir` when given (restart-stable JWKS). */
export function loadOrCreateKeys(dir?: string): KeyPair {
  if (dir && existsSync(join(dir, "private.pem"))) {
    return {
      privateKey: readFileSync(join(dir, "private.pem"), "utf8"),
      publicKey: readFileSync(join(dir, "public.pem"), "utf8"),
      kid: readFileSync(join(dir, "kid"), "utf8").trim(),
    };
  }
  const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const pair: KeyPair = {
    publicKey: publicKey.export({ type: "spki", format: "pem" }).toString(),
    privateKey: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
    kid: `hivecast-${Date.now().toString(36)}`,
  };
  if (dir) {
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "private.pem"), pair.privateKey);
    writeFileSync(join(dir, "public.pem"), pair.publicKey);
    writeFileSync(join(dir, "kid"), pair.kid);
  }
  return pair;
}

/** JWKS document for GET /.well-known/jwks.json (RS256 signing key only). */
export function jwksFor(pair: KeyPair) {
  const jwk = createPublicKey(pair.publicKey).export({ format: "jwk" }) as {
    kty: string; n: string; e: string;
  };
  return { keys: [{ kty: jwk.kty, n: jwk.n, e: jwk.e, kid: pair.kid, alg: "RS256", use: "sig" }] };
}

export type FireClaims = {
  iss: string;
  aud: string;
  jobId: string;
  fireAt: string;
  purpose?: string;
  ttlSeconds?: number;
  issuedAt?: number;
};

/** Mint a Chronos-shaped fire JWT. */
export function signFireJwt(pair: KeyPair, claims: FireClaims): string {
  const now = Math.floor(claims.issuedAt ?? Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT", kid: pair.kid };
  const payload = {
    iss: claims.iss,
    aud: claims.aud,
    purpose: claims.purpose ?? "cron_fire",
    job_id: claims.jobId,
    fire_at: claims.fireAt,
    iat: now,
    nbf: now - 30,
    exp: now + (claims.ttlSeconds ?? 90),
  };
  const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(payload))}`;
  const signature = createSign("RSA-SHA256").update(signingInput).sign(pair.privateKey);
  return `${signingInput}.${b64url(signature)}`;
}

export type VerifyResult = { ok: true; payload: Record<string, unknown> } | { ok: false; reason: string };

/** Verify a fire JWT against a JWKS public key (mirrors the agent's checks). */
export function verifyFireJwt(publicKeyPem: string, token: string, expect: { iss: string; aud: string; leewaySeconds?: number }): VerifyResult {
  const parts = token.split(".");
  if (parts.length !== 3) return { ok: false, reason: "malformed" };
  const signingInput = `${parts[0]}.${parts[1]}`;
  const signature = Buffer.from(parts[2]!.replace(/-/g, "+").replace(/_/g, "/"), "base64");
  const valid = createVerify("RSA-SHA256").update(signingInput).verify(publicKeyPem, signature);
  if (!valid) return { ok: false, reason: "bad_signature" };
  const payload = JSON.parse(Buffer.from(parts[1]!.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8")) as Record<string, unknown>;
  const leeway = expect.leewaySeconds ?? 30;
  const now = Math.floor(Date.now() / 1000);
  if (payload.iss !== expect.iss) return { ok: false, reason: "wrong_iss" };
  if (payload.aud !== expect.aud) return { ok: false, reason: "wrong_aud" };
  if (payload.purpose !== "cron_fire") return { ok: false, reason: "wrong_purpose" };
  if (Number(payload.exp) + leeway < now) return { ok: false, reason: "expired" };
  return { ok: true, payload };
}
