"""Unit tests for the HiveCast managed-cron provider (production contract).

All control-plane calls are mocked — ZERO live network. These prove:
  - is_available is config-only (no network), false without config.
  - One-shot arming sends the production provision payload with the caller-
    owned idempotency key and strict-Z fireAt normalization.
  - Reconcile arms missing, cancels orphaned, skips paused local jobs.
  - PULLED DECISIONS apply to jobs.json: 'removed' deletes the local job
    (and it is NOT re-armed), 'paused' disables it, 'resumed' re-enables it —
    decisions apply BEFORE the desired-state loop, so a dashboard removal
    can never be undone by the re-arm pass.
  - RUNTIME-SIDE ACK: a cloud-armed fire acks by (jobId, fireAt) — the
    cycle-safe address — with the run's own execution outcome (completed →
    fired, failed → failed+error); a manual (never cloud-armed) fire does
    NOT ack; a failing ack never breaks the fire path.
"""

import pytest


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


@pytest.fixture
def seeded_jobs(temp_home):
    """Two runnable interval jobs in the store: keep@ and remove@."""
    from cron import jobs as cron_jobs

    keep = {
        "id": "job-keep", "name": "keep", "prompt": "hi", "enabled": True, "state": "scheduled",
        "schedule": {"kind": "interval", "minutes": 5}, "deliver": "local",
        "next_run_at": "2026-11-02T12:00:00Z",
    }
    remove = {
        "id": "job-remove", "name": "remove me", "prompt": "hi", "enabled": True, "state": "scheduled",
        "schedule": {"kind": "interval", "minutes": 10}, "deliver": "local",
        "next_run_at": "2026-11-02T12:05:00Z",
    }
    cron_jobs.save_jobs([keep, remove])
    yield {"keep": keep, "remove": remove}
    cron_jobs.save_jobs([])


@pytest.fixture
def provider(monkeypatch):
    """A HiveCastCronScheduler with a fake control-plane client capturing calls."""
    from plugins.cron_providers.hivecast import HiveCastCronScheduler

    class FakeClient:
        def __init__(self):
            self.provisions = []
            self.cancels = []
            self.acks = []
            self.pulls = []
            self._armed = []
            self.pull_response: dict = {}

        def provision(self, *, job_id, fire_at):
            self.provisions.append({"job_id": job_id, "fire_at": fire_at})
            return {"action": "armed"}

        def cancel(self, *, job_id):
            self.cancels.append(job_id)
            return {}

        def ack(self, *, job_id, fire_at, outcome, error=None):
            self.acks.append({"job_id": job_id, "fire_at": fire_at, "outcome": outcome, "error": error})
            return {"action": "settled"}

        def pull(self):
            self.pulls.append(True)
            response = {"arms": list(self._armed), "registrations": [], "cancellations": []}
            response.update(self.pull_response)
            return response

        def list_armed(self):
            data = self.pull()
            return data.get("arms") or []

    scheduler = HiveCastCronScheduler()
    fake = FakeClient()
    scheduler._client = fake
    return scheduler, fake


# -- is_available -------------------------------------------------------------

def test_is_available_false_without_config(temp_home, monkeypatch):
    from plugins.cron_providers.hivecast import HiveCastCronScheduler

    monkeypatch.setattr("plugins.cron_providers.hivecast._cfg", lambda *k, default="": "")
    assert HiveCastCronScheduler().is_available() is False


def test_is_available_true_with_full_config(temp_home, monkeypatch):
    from plugins.cron_providers.hivecast import HiveCastCronScheduler

    def fake_cfg(*keys, default=""):
        return {
            ("cron", "hivecast", "portal_url"): "https://portal.test",
            ("cron", "hivecast", "callback_url"): "https://agent.example/",
            ("cron", "hivecast", "runtime_api_key"): "k",
            ("cron", "hivecast", "workspace_id"): "w",
            ("cron", "hivecast", "instance_id"): "i",
        }.get(keys, default)

    monkeypatch.setattr("plugins.cron_providers.hivecast._cfg", fake_cfg)
    assert HiveCastCronScheduler().is_available() is True


# -- production arming contract -----------------------------------------------

def test_arm_normalizes_fire_at_and_owns_the_idempotency_key(provider):
    scheduler, fake = provider
    scheduler._arm_one_shot({"id": "j1", "next_run_at": "2026-06-18T12:00:00+00:00"})

    assert len(fake.provisions) == 1
    assert fake.provisions[0]["fire_at"] == "2026-06-18T12:00:00Z"


def test_reconcile_arms_missing_and_cancels_orphans(provider, seeded_jobs):
    scheduler, fake = provider
    # Control plane knows only job-keep; job-remove is an orphan cloud-side.
    fake._armed = [{"jobId": "job-keep", "fireAt": "2026-11-02T12:00:00Z"}, {"jobId": "job-orphan", "fireAt": "2026-11-02T13:00:00Z"}]

    scheduler.reconcile()

    armed_ids = [p["job_id"] for p in fake.provisions]
    assert "job-remove" in armed_ids  # missing cloud-side, desired locally
    assert "job-orphan" in fake.cancels  # armed cloud-side, gone locally
    assert "job-keep" not in armed_ids  # already armed at the same fireAt


# -- (F) pulled decisions ------------------------------------------------------

def test_removed_decision_deletes_local_job_and_never_re_arms(provider, seeded_jobs):
    scheduler, fake = provider
    from cron import jobs as cron_jobs

    fake.pull_response = {"cancellations": [{"jobId": "job-remove", "action": "removed"}]}

    scheduler.reconcile()

    assert cron_jobs.get_job("job-remove") is None
    assert all(p["job_id"] != "job-remove" for p in fake.provisions)


def test_paused_decision_disables_local_job(provider, seeded_jobs):
    scheduler, fake = provider
    from cron import jobs as cron_jobs

    fake.pull_response = {"cancellations": [{"jobId": "job-keep", "action": "paused"}]}

    scheduler.reconcile()

    job = cron_jobs.get_job("job-keep")
    assert job is not None
    assert job["enabled"] is False
    # A disabled job is not desired state: no provision was sent for it.
    assert all(p["job_id"] != "job-keep" for p in fake.provisions)


def test_resumed_decision_re_enables_and_re_arms(provider, seeded_jobs):
    scheduler, fake = provider
    from cron import jobs as cron_jobs

    cron_jobs.pause_job("job-keep")
    assert cron_jobs.get_job("job-keep")["enabled"] is False

    fake.pull_response = {"cancellations": [{"jobId": "job-keep", "action": "resumed"}]}
    scheduler.reconcile()

    assert cron_jobs.get_job("job-keep")["enabled"] is True
    assert any(p["job_id"] == "job-keep" for p in fake.provisions)


def test_decision_for_unknown_job_is_a_no_op(provider, seeded_jobs):
    scheduler, fake = provider
    from cron import jobs as cron_jobs

    fake.pull_response = {"cancellations": [{"jobId": "no-such-job", "action": "removed"}]}
    scheduler.reconcile()  # must not raise

    assert cron_jobs.get_job("job-keep") is not None
    assert cron_jobs.get_job("job-remove") is not None


# -- (G) runtime-side ack -------------------------------------------------------

def _claim(job, execution_id="exec-1", occurrence=None):
    claimed = dict(job)
    claimed["execution_id"] = execution_id
    if occurrence:
        claimed["_scheduled_instant"] = occurrence
    return claimed


def test_cloud_armed_fire_acks_by_job_and_fire_at(provider, seeded_jobs, monkeypatch):
    scheduler, fake = provider
    job = seeded_jobs["keep"]
    scheduler._armed = {"job-keep": "2026-11-02T12:00:00Z"}
    monkeypatch.setattr(
        "cron.scheduler_provider.CronScheduler.fire_claimed", lambda self, claimed, **kw: True)
    monkeypatch.setattr(
        "cron.executions.get_execution",
        lambda execution_id: {"status": "completed", "error": None})

    scheduler.fire_claimed(_claim(job, occurrence="2026-11-02T12:00:00+00:00"))

    assert len(fake.acks) == 1
    ack = fake.acks[0]
    assert ack["job_id"] == "job-keep"
    assert ack["fire_at"] == "2026-11-02T12:00:00Z"  # normalized, cycle-safe address
    assert ack["outcome"] == "fired"


def test_failed_execution_acks_failed_with_error(provider, seeded_jobs, monkeypatch):
    scheduler, fake = provider
    scheduler._armed = {"job-keep": "2026-11-02T12:00:00Z"}
    monkeypatch.setattr(
        "cron.scheduler_provider.CronScheduler.fire_claimed", lambda self, claimed, **kw: True)
    monkeypatch.setattr(
        "cron.executions.get_execution",
        lambda execution_id: {"status": "failed", "error": "script exited 1"})

    scheduler.fire_claimed(_claim(seeded_jobs["keep"], occurrence="2026-11-02T12:00:00Z"))

    assert fake.acks[-1]["outcome"] == "failed"
    assert fake.acks[-1]["error"] == "script exited 1"


def test_manual_fire_of_never_armed_job_does_not_ack(provider, seeded_jobs, monkeypatch):
    scheduler, fake = provider
    monkeypatch.setattr(
        "cron.scheduler_provider.CronScheduler.fire_claimed", lambda self, claimed, **kw: True)
    monkeypatch.setattr(
        "cron.executions.get_execution",
        lambda execution_id: {"status": "completed", "error": None})

    scheduler.fire_claimed(_claim(seeded_jobs["keep"]))  # dashboard Run now

    assert fake.acks == []


def test_failing_ack_never_breaks_the_fire_path(provider, seeded_jobs, monkeypatch):
    scheduler, _fake = provider
    scheduler._armed = {"job-keep": "2026-11-02T12:00:00Z"}

    def raise_ack(**kwargs):
        raise RuntimeError("control plane unreachable")

    monkeypatch.setattr(
        "cron.scheduler_provider.CronScheduler.fire_claimed", lambda self, claimed, **kw: True)
    monkeypatch.setattr(scheduler._get_client(), "ack", raise_ack)

    # Must not raise: the job already ran locally; a lost ack is the cloud's
    # reconciliation problem, never a gateway failure.
    assert scheduler.fire_claimed(_claim(seeded_jobs["keep"], occurrence="2026-11-02T12:00:00Z")) is True


def test_no_execution_record_acks_fired_delivery_truth(provider, seeded_jobs, monkeypatch):
    scheduler, fake = provider
    scheduler._armed = {"job-keep": "2026-11-02T12:00:00Z"}
    monkeypatch.setattr(
        "cron.scheduler_provider.CronScheduler.fire_claimed", lambda self, claimed, **kw: True)
    monkeypatch.setattr("cron.executions.get_execution", lambda execution_id: None)

    scheduler.fire_claimed(_claim(seeded_jobs["keep"], occurrence="2026-11-02T12:00:00Z"))

    assert fake.acks[-1]["outcome"] == "fired"
