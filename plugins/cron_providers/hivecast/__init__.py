"""HiveCast managed-cron provider — PRODUCTION contract.

Chronos-shaped scheduler contract speaking the HiveCast control plane's
runtime-cron ingress routes:

- Auth: per-instance capability derived from the runtime's deployment API key
  (``hc_callback_<base64url hmac>``), identity in x-hermes-workspace-id /
  x-hermes-instance-id headers — never a static bearer, never body identity.
- Idempotency: caller-owned ``x-idempotency-key`` per arming intent.
- Routes: POST /api/internal/runtime-cron/provision, /cancel, /ack;
  GET /api/internal/runtime-cron/arms (the reconciliation pull).

Two-way sync (the halves that make this the runtime side of managed cron):

- PULLED DECISIONS: the reconciliation pull returns the control plane's
  recent DECISIONS (``removed`` / ``paused`` / ``resumed``). The provider
  mirrors them into jobs.json — 'removed' deletes the local job, 'paused'
  disables it, 'resumed' re-enables it — BEFORE arming, so a dashboard
  removal can never be undone by the re-arm loop.
- RUNTIME-SIDE ACK: when a cloud-armed fire completes, the provider acks the
  control plane by (jobId, fireAt) — the cycle-safe address. A 2xx delivery
  only proved receipt; the ack is what closes the arm to 'fired'/'failed',
  and it comes from the runtime that executed the job, not the cloud.

Config (cron.hivecast.*): portal_url, callback_url, runtime_api_key,
workspace_id, instance_id. Inert unless ``cron.provider: hivecast``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import threading
from typing import Any, Dict

from cron.scheduler_provider import CronScheduler

logger = logging.getLogger("cron.hivecast")

CAPABILITY_CONTEXT = b"hermescloud:trusted-runtime-callback:v1"


def _capability(runtime_api_key: str) -> str:
    digest = hmac.new(runtime_api_key.encode(), CAPABILITY_CONTEXT, hashlib.sha256).digest()
    return "hc_callback_" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _normalize_fire_at(fire_at: str) -> str:
    """Normalize any ISO-8601 variant Hermes emits to strict UTC '...Z'.

    The control plane's provision route validates fireAt with zod
    ``.datetime()`` (strict Z form) and the idempotency key with a safe
    charset — an offset form like '+00:00' would fail BOTH. The runtime-side
    client normalizes once, at its boundary, and every ack echoes the exact
    normalized string (the cycle address must round-trip byte-identical).
    """
    from datetime import UTC, datetime

    value = str(fire_at).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "T" not in value[:11]:
        value = value.replace(" ", "T", 1)
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _cfg(*keys: str, default: Any = "") -> Any:
    try:
        from hermes_cli.config import cfg_get, load_config
        return cfg_get(load_config(), *keys, default=default)
    except Exception:  # health: allow BLE001 -- config-read fallback boundary: any parse/env failure means "unset", never a crash
        return default


class HiveCastCronClient:
    """HTTP client for the production runtime-cron ingress routes."""

    def __init__(self, portal_url: str, runtime_api_key: str, workspace_id: str, instance_id: str,
                 *, timeout_seconds: float = 15.0) -> None:
        self.portal_url = portal_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {_capability(runtime_api_key)}",
            "x-hermes-workspace-id": workspace_id,
            "x-hermes-instance-id": instance_id,
            "Content-Type": "application/json",
        }
        self.timeout_seconds = timeout_seconds

    def _request(self, method: str, path: str, *, idempotency_key: str | None = None, **kwargs: Any) -> dict:
        import requests

        headers = dict(self.headers)
        if idempotency_key:
            headers["x-idempotency-key"] = idempotency_key
        response = requests.request(
            method, f"{self.portal_url}{path}", headers=headers,
            timeout=self.timeout_seconds, **kwargs)
        if response.status_code // 100 != 2:
            raise RuntimeError(f"{method} {path} returned {response.status_code}: {response.text[:200]}")
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as e:
            # A 2xx with an unparseable body is a control-plane contract
            # break: surface it to the caller's logging instead of silently
            # degrading to "no data".
            raise RuntimeError(
                f"{method} {path} returned 2xx with a non-JSON body: {e}") from e

    def provision(self, *, job_id: str, fire_at: str) -> dict:
        return self._request("POST", "/api/internal/runtime-cron/provision",
                             idempotency_key=f"{job_id}:{fire_at}",
                             json={"jobId": job_id, "fireAt": fire_at, "origin": "agent"})

    def cancel(self, *, job_id: str) -> dict:
        return self._request("POST", "/api/internal/runtime-cron/cancel", json={"jobId": job_id})

    def ack(self, *, job_id: str, fire_at: str, outcome: str, error: str | None = None) -> dict:
        payload: dict[str, Any] = {"jobId": job_id, "fireAt": fire_at, "outcome": outcome}
        if error:
            payload["error"] = str(error)[:500]
        # The ack is fire-and-forget telemetry: 404/409 answers are expected
        # (arm replaced, already settled) and must not raise into the gateway.
        return self._request("POST", "/api/internal/runtime-cron/ack", json=payload)

    def pull(self) -> dict:
        """The reconciliation pull: live arms + registrations + DECISIONS."""
        return self._request("GET", "/api/internal/runtime-cron/arms")

    def list_armed(self) -> list:
        data = self.pull()
        items = data.get("arms") if isinstance(data, dict) else None
        return items if isinstance(items, list) else []


class HiveCastCronScheduler(CronScheduler):
    """HiveCast-mediated external cron provider (Chronos contract)."""

    def __init__(self) -> None:
        self._armed: dict[str, str] = {}
        self._lock = threading.Lock()
        self._client: HiveCastCronClient | None = None

    @property
    def name(self) -> str:
        return "hivecast"

    def is_available(self) -> bool:
        return bool(
            _cfg("cron", "hivecast", "portal_url")
            and _cfg("cron", "hivecast", "callback_url")
            and _cfg("cron", "hivecast", "runtime_api_key")
            and _cfg("cron", "hivecast", "workspace_id")
            and _cfg("cron", "hivecast", "instance_id"))

    def _get_client(self) -> HiveCastCronClient:
        if self._client is None:
            self._client = HiveCastCronClient(
                str(_cfg("cron", "hivecast", "portal_url")),
                str(_cfg("cron", "hivecast", "runtime_api_key")),
                str(_cfg("cron", "hivecast", "workspace_id")),
                str(_cfg("cron", "hivecast", "instance_id")))
        return self._client

    def start(self, stop_event, *, adapters=None, loop=None, interval=60):
        self._reconcile_logged("start()")

    def stop(self) -> None:
        pass

    def on_jobs_changed(self) -> None:
        self._reconcile_logged("on_jobs_changed")

    def register_job(self, job: dict[str, Any]) -> None:
        self._arm_one_shot(job)

    def _arm_one_shot(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        fire_at = job.get("next_run_at")
        if not fire_at:
            return
        fire_at = _normalize_fire_at(fire_at)
        self._get_client().provision(job_id=job_id, fire_at=fire_at)
        with self._lock:
            self._armed[job_id] = fire_at

    def _arm_logged(self, job: dict[str, Any], what: str) -> None:
        try:
            self._arm_one_shot(job)
        except Exception as e:
            logger.warning("HiveCast failed to %s: %s", what, e, exc_info=True)

    def _cancel(self, job_id: str) -> None:
        try:
            self._get_client().cancel(job_id=job_id)
        finally:
            with self._lock:
                self._armed.pop(job_id, None)

    # -- two-way sync: apply the control plane's pulled DECISIONS -----------

    def _apply_decisions(self, cancellations: list) -> int:
        """Mirror control-plane decisions into jobs.json BEFORE arming.

        'removed' deletes the local job (the dashboard user removed the
        schedule), 'paused' disables it, 'resumed' re-enables it. Applying
        first means the desired-state loop below can never re-arm a job the
        dashboard just removed. A decision for an unknown job id is a no-op.
        """
        from cron.jobs import get_job, pause_job, remove_job, resume_job
        applied = 0
        for decision in cancellations or []:
            if not isinstance(decision, dict):
                continue
            job_id = str(decision.get("jobId") or "")
            action = str(decision.get("action") or "")
            if not job_id or action not in ("removed", "paused", "resumed"):
                continue
            try:
                if action == "removed":
                    if get_job(job_id) is not None and remove_job(job_id):
                        applied += 1
                elif action == "paused":
                    if get_job(job_id) is not None and pause_job(job_id, reason="hivecast: paused from dashboard"):
                        applied += 1
                elif action == "resumed":
                    if get_job(job_id) is not None and resume_job(job_id):
                        applied += 1
                with self._lock:
                    if action == "removed":
                        self._armed.pop(job_id, None)
            except Exception as e:
                logger.warning("HiveCast failed to apply decision %s %s: %s", action, job_id, e, exc_info=True)
        return applied

    def _list_armed(self) -> dict[str, str]:
        with self._lock:
            if self._armed:
                return dict(self._armed)
        try:
            observed = {item["jobId"]: item.get("fireAt", "")
                        for item in self._get_client().list_armed() if item.get("jobId")}
            with self._lock:
                self._armed.update(observed)
            return observed
        except Exception as e:  # health: allow BLE001 -- optional resync read: any failure degrades to "re-arm idempotently"
            logger.debug("HiveCast _list_armed failed (will re-arm idempotently): %s", e)
            return {}

    def reconcile(self) -> None:
        from cron.jobs import get_job, load_jobs

        # DECISIONS first: a dashboard removal must delete the local job
        # before the desired-state loop could re-arm it.
        try:
            decisions = self._get_client().pull().get("cancellations") or []
            applied = self._apply_decisions(decisions)
            if applied:
                logger.info("HiveCast applied %d control-plane decision(s) to local jobs", applied)
        except Exception as e:
            logger.warning("HiveCast decision pull failed: %s", e, exc_info=True)

        desired: dict[str, str] = {
            j["id"]: j["next_run_at"] for j in load_jobs()
            if j.get("enabled") and j.get("next_run_at") and j.get("state") != "paused"}
        observed = self._list_armed()
        for job_id, fire_at in desired.items():
            if observed.get(job_id) != fire_at and (job := get_job(job_id)):
                self._arm_logged(job, f"arm job {job_id}")
        for job_id in observed.keys() - desired.keys():
            try:
                self._cancel(job_id)
            except Exception as e:
                logger.warning("HiveCast failed to cancel orphan %s: %s", job_id, e, exc_info=True)

    def _reconcile_logged(self, what: str) -> None:
        try:
            self.reconcile()
        except Exception as e:
            logger.warning("HiveCast %s reconcile failed: %s", what, e, exc_info=True)

    # -- runtime-side ack: the completion trigger ----------------------------

    def _ack_logged(self, job_id: str, fire_at: str | None, *, success: bool, error: str | None) -> None:
        """Ack the control plane when a cloud-armed fire completes.

        Addressed by (jobId, fireAt) — the cycle-safe form: if the arm was
        replaced or already settled, the control plane answers honestly
        (404/'already') and that answer is logged, never raised. Only jobs
        this provider armed cloud-side are acked; a manual dashboard run has
        no cloud arm and is skipped by construction.
        """
        with self._lock:
            armed_fire_at = self._armed.get(job_id)
        fire_at = _normalize_fire_at(fire_at) if fire_at else armed_fire_at
        if not fire_at:
            return  # never cloud-armed from this process; nothing to ack
        try:
            result = self._get_client().ack(
                job_id=job_id, fire_at=fire_at,
                outcome="fired" if success else "failed", error=error)
            logger.info("HiveCast ack %s @ %s -> %s", job_id, fire_at, result.get("action", result))
        except Exception as e:
            logger.warning("HiveCast ack %s @ %s failed: %s", job_id, fire_at, e, exc_info=True)

    def _execution_outcome(self, claimed_job: dict) -> tuple[bool, str | None]:
        """Read the run's own execution record: (success, error)."""
        execution_id = claimed_job.get("execution_id")
        if execution_id:
            try:
                from cron.executions import get_execution
                record = get_execution(str(execution_id))
                if record and str(record.get("status")) in ("completed", "failed", "cancelled"):
                    return str(record.get("status")) == "completed", record.get("error")
            except Exception as e:  # health: allow BLE001 -- optional outcome read: any failure acks the delivery truth
                logger.debug("HiveCast execution outcome read failed: %s", e)
        # No terminal record readable: the fire itself ran; ack the delivery
        # truth rather than inventing a failure.
        return True, None

    def _cloud_armed_fire_at(self, job_id: str) -> str | None:
        """Resolve the cloud-side armed cycle for a job from the control plane.

        The arming process and the firing process can differ (the CLI arms on
        ``cron create``; the gateway fires): the local ``_armed`` map then
        misses a job that IS armed cloud-side. The control plane's arms
        listing (pending/firing/delivered — the reconciliation view) is the
        source of truth; its fireAt is the exact cycle address to ack. A pull
        failure degrades to "not cloud-armed" (logged) — never blocks a fire.
        """
        with self._lock:
            local = self._armed.get(job_id)
        if local:
            return local
        try:
            for item in self._get_client().list_armed():
                if item.get("jobId") == job_id:
                    fire_at = _normalize_fire_at(str(item.get("fireAt") or ""))
                    with self._lock:
                        self._armed[job_id] = fire_at
                    return fire_at
        except Exception as e:  # health: allow BLE001 -- optional read: any failure means "cannot confirm cloud-armed"
            logger.warning("HiveCast cloud-armed lookup for %s failed: %s", job_id, e, exc_info=True)
        return None

    def fire_claimed(
        self, claimed_job: dict, *, adapters: Any = None, loop: Any = None, cancel_event: Any = None
    ) -> bool:
        ran = super().fire_claimed(claimed_job, adapters=adapters, loop=loop, cancel_event=cancel_event)
        job_id = str(claimed_job.get("id") or "")
        if ran:
            # Cross-process safe: locally-armed fast path, then the control
            # plane's arms view (the CLI-armed/gateway-fired topology).
            cloud_fire_at = self._cloud_armed_fire_at(job_id)
            if cloud_fire_at:
                success, error = self._execution_outcome(claimed_job)
                self._ack_logged(job_id, claimed_job.get("_scheduled_instant"), success=success, error=error)
        if ran:
            from cron.jobs import get_job
            job = get_job(job_id)
            if job and job.get("enabled") and job.get("next_run_at"):
                self._arm_logged(job, f"re-arm job {job_id} after fire")
        return ran


def register(ctx) -> None:
    ctx.register_cron_scheduler(HiveCastCronScheduler())
