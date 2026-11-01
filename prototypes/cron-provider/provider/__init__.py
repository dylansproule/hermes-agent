"""HiveCast — cloud-owned managed cron provider (prototype).

Chronos-shaped (see plugins/cron_providers/chronos and
website/docs/developer-guide/chronos-managed-cron-contract.md) but the control
plane is HiveCast, not NAS. Differences from Chronos:

- Auth: the runtime's HiveCast token (``cron.hivecast.token``) — the identity
  HiveCast provisioned for this runtime deployment — instead of the Nous token.
- The agent-id is explicit (``cron.hivecast.agent_id``); it becomes the fire
  JWT audience (``agent:{agent_id}``).
- Fire ingress is UNCHANGED: the shared gateway handler ``/api/cron/fire``
  verifies via the Chronos verifier, so the verifier config keys
  (``cron.chronos.nas_jwks_url`` / ``expected_audience`` / ``portal_url``) are
  pointed at HiveCast values. (Upstreaming note: those keys should be
  provider-generic, not chronos-namespaced.)

Inert unless ``cron.provider: hivecast``.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict

from cron.scheduler_provider import CronScheduler

logger = logging.getLogger("cron.hivecast")


def _cfg(*keys: str, default: Any = "") -> Any:
    """Read a cron.hivecast.* config value (no network)."""
    try:
        from hermes_cli.config import cfg_get, load_config
        return cfg_get(load_config(), *keys, default=default)
    except Exception:
        return default


class HiveCastCronClient:
    """Minimal HTTP client for the HiveCast agent-cron endpoints (requests, bearer token)."""

    def __init__(self, portal_url: str, token: str, *, timeout_seconds: float = 15.0) -> None:
        self.portal_url = portal_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds

    def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        import requests

        response = requests.request(
            method, f"{self.portal_url}{path}",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            timeout=self.timeout_seconds, **kwargs)
        if response.status_code // 100 != 2:
            raise RuntimeError(f"{method} {path} returned {response.status_code}: {response.text[:200]}")
        try:
            return response.json() if response.content else {}
        except ValueError:
            return {}

    def provision(self, *, job_id: str, fire_at: str, agent_callback_url: str, dedup_key: str) -> dict:
        return self._request("POST", "/api/agent-cron/provision", json={
            "job_id": job_id, "fire_at": fire_at,
            "agent_callback_url": agent_callback_url, "dedup_key": dedup_key})

    def cancel(self, *, job_id: str) -> dict:
        return self._request("POST", "/api/agent-cron/cancel", json={"job_id": job_id})

    def list_armed(self) -> list:
        data = self._request("GET", "/api/agent-cron/list")
        items = data.get("armed") if isinstance(data, dict) else None
        return items if isinstance(items, list) else []


class HiveCastCronScheduler(CronScheduler):
    """HiveCast-mediated external cron provider (Chronos contract)."""

    def __init__(self) -> None:
        self._armed: dict[str, str] = {}
        self._lock = threading.Lock()
        self._client: HiveCastCronClient | None = None
        self._stop_event = None
        self._ticker_kwargs: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "hivecast"

    def is_available(self) -> bool:
        """Config presence only — NO network (mirrors Chronos)."""
        return bool(
            _cfg("cron", "hivecast", "portal_url")
            and _cfg("cron", "hivecast", "callback_url")
            and _cfg("cron", "hivecast", "token")
            and _cfg("cron", "hivecast", "agent_id"))

    def _get_client(self) -> HiveCastCronClient:
        if self._client is None:
            self._client = HiveCastCronClient(
                str(_cfg("cron", "hivecast", "portal_url")), str(_cfg("cron", "hivecast", "token")))
        return self._client

    def start(self, stop_event, *, adapters=None, loop=None, interval=60):
        """Arm all enabled jobs via HiveCast, then RETURN — no loop, no periodic wake."""
        self._ticker_kwargs = {"adapters": adapters, "loop": loop, "interval": interval}
        self._stop_event = stop_event
        self._reconcile_logged("start()")

    def stop(self) -> None:
        pass

    def on_jobs_changed(self) -> None:
        self._reconcile_logged("on_jobs_changed")

    def register_job(self, job: dict[str, Any]) -> None:
        """Arm the first one-shot for a new job; may raise so creation can report."""
        self._arm_one_shot(job)

    def _arm_one_shot(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        fire_at = job.get("next_run_at")
        if not fire_at:
            return
        self._get_client().provision(
            job_id=job_id, fire_at=fire_at, dedup_key=f"{job_id}:{fire_at}",
            agent_callback_url=str(_cfg("cron", "hivecast", "callback_url") or ""))
        with self._lock:
            self._armed[job_id] = fire_at

    def _arm_logged(self, job: dict[str, Any], what: str) -> None:
        """Best-effort arm: log a warning instead of raising (reconcile/fire must not die)."""
        try:
            self._arm_one_shot(job)
        except Exception as e:
            logger.warning("HiveCast failed to %s: %s", what, e)

    def _cancel(self, job_id: str) -> None:
        try:
            self._get_client().cancel(job_id=job_id)
        finally:
            with self._lock:
                self._armed.pop(job_id, None)

    def _list_armed(self) -> dict[str, str]:
        with self._lock:
            if self._armed:
                return dict(self._armed)
        try:
            observed = {item["job_id"]: item.get("fire_at", "")
                        for item in self._get_client().list_armed() if item.get("job_id")}
            with self._lock:
                self._armed.update(observed)
            return observed
        except Exception as e:
            logger.debug("HiveCast _list_armed failed (will re-arm idempotently): %s", e)
            return {}

    def reconcile(self) -> None:
        """Converge HiveCast one-shots toward jobs.json: arm missing/changed, cancel orphans."""
        from cron.jobs import get_job, load_jobs
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
                logger.warning("HiveCast failed to cancel orphan %s: %s", job_id, e)

    def _reconcile_logged(self, what: str) -> None:
        try:
            self.reconcile()
        except Exception as e:
            logger.warning("HiveCast %s reconcile failed: %s", what, e)

    def fire_claimed(
        self, claimed_job: dict, *, adapters: Any = None, loop: Any = None, cancel_event: Any = None
    ) -> bool:
        ran = super().fire_claimed(claimed_job, adapters=adapters, loop=loop, cancel_event=cancel_event)
        if ran:
            from cron.jobs import get_job
            job = get_job(claimed_job["id"])
            if job and job.get("enabled") and job.get("next_run_at"):
                self._arm_logged(job, f"re-arm job {claimed_job['id']} after fire")
        return ran


def register(ctx) -> None:
    """Plugin entrypoint — user-plugin discovery collects the provider here."""
    ctx.register_cron_scheduler(HiveCastCronScheduler())
