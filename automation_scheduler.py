from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

import automation_core
import background_jobs
import config
import price_pipeline  # noqa: F401 - imported for existing side effects / compatibility
import price_uploader
import promo_calendar  # noqa: F401 - imported for existing side effects / compatibility
import promo_executor
from safe_logs import log_event

POLL_SECONDS = max(5, int(getattr(config, "AUTOMATION_SCHEDULER_POLL_SECONDS", 30) or 30))
SCHEDULER_ENABLED = bool(getattr(config, "AUTOMATION_SCHEDULER_ENABLED", True))
SCHEDULER_EMBEDDED = bool(getattr(config, "AUTOMATION_SCHEDULER_EMBEDDED", True))
RUNNER_MODE = str(getattr(config, "AUTOMATION_RUNNER_MODE", "embedded") or "embedded").strip().lower() or "embedded"
PROMO_PLAN_KEY = "promo_daily"
PRICE_PLAN_KEY = "prices_daily"
SYSTEM_TENANT_ID = "_system"

_runtime_lock = threading.RLock()
_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _mark_started_local(state: Dict[str, Any], plan_key: str, now, job_id: str, source: str = "scheduler") -> None:
    plans = state.get("plans") if isinstance(state.get("plans"), dict) else {}
    bucket = plans.get(plan_key) if isinstance(plans.get(plan_key), dict) else {}
    bucket["last_run_date"] = now.strftime("%Y-%m-%d")
    bucket["last_started_at"] = now.astimezone(automation_core.timezone.utc).isoformat()
    bucket["last_job_id"] = str(job_id or "").strip()
    bucket["last_source"] = str(source or "scheduler").strip() or "scheduler"
    plans[plan_key] = bucket
    state["plans"] = plans


def _sync_plan_statuses(state: Dict[str, Any]) -> bool:
    plans = state.get("plans") if isinstance(state.get("plans"), dict) else {}
    changed = False
    for bucket in plans.values():
        if not isinstance(bucket, dict):
            continue
        job_id = str(bucket.get("last_job_id") or "").strip()
        if not job_id:
            continue
        job = background_jobs.get_job_public(job_id) or {}
        status = str(job.get("status") or "").strip()
        if not status:
            continue
        if bucket.get("last_status") != status:
            bucket["last_status"] = status
            changed = True
        if job.get("finished_at") and bucket.get("last_finished_at") != job.get("finished_at"):
            bucket["last_finished_at"] = job.get("finished_at")
            changed = True
        result = job.get("result") if isinstance(job.get("result"), dict) else {}
        report_path = str(result.get("report_path") or "").strip()
        if report_path and bucket.get("last_report_path") != report_path:
            bucket["last_report_path"] = report_path
            changed = True
    return changed


def _submit_plan_job(*, kind: str, label: str, target, kwargs: Dict[str, Any], unique_key: str, plan_key: str, schedule_time: str, state: Dict[str, Any], now) -> bool:
    job, created = background_jobs.submit_job(
        kind=kind,
        tenant_id=SYSTEM_TENANT_ID,
        label=label,
        target=target,
        kwargs=dict(kwargs),
        unique_key=unique_key,
    )
    if created:
        _mark_started_local(state, plan_key, now, job.get("job_id") or "", source="scheduler")
        log_event(
            "automation",
            "scheduler_submit",
            tenant_id=SYSTEM_TENANT_ID,
            plan_key=plan_key,
            job_id=job.get("job_id"),
            run_id=job.get("run_id") or job.get("job_id"),
            schedule_time=schedule_time,
            kind=kind,
        )
    return created


def scheduler_tick() -> Dict[str, Any]:
    settings = automation_core.load_settings()
    state = automation_core.load_state()
    scheduler_state = state.get("scheduler") if isinstance(state.get("scheduler"), dict) else {}
    scheduler_state["last_tick_at"] = automation_core.now_local(settings).isoformat(timespec="seconds")
    scheduler_state["status"] = "running"
    scheduler_state["worker_mode"] = RUNNER_MODE
    state["scheduler"] = scheduler_state
    changed = True
    changed = _sync_plan_statuses(state) or changed

    if settings.get("schedule_enabled", True):
        now = automation_core.now_local(settings)
        promo_settings = settings.get("promo") or {}
        if automation_core.is_plan_due(
            PROMO_PLAN_KEY,
            promo_settings.get("schedule_time") or "23:00",
            enabled=bool(promo_settings.get("enabled", False)),
            mode=str(promo_settings.get("mode") or "manual"),
            now=now,
            state=state,
            schedule_enabled=bool(settings.get("schedule_enabled", True)),
        ):
            created = _submit_plan_job(
                kind="promo_execute",
                label="Ночное снятие будущих акций",
                target=promo_executor.execute_future_promotions,
                kwargs={"run_source": RUNNER_MODE},
                unique_key="promo_execute",
                plan_key=PROMO_PLAN_KEY,
                schedule_time=promo_settings.get("schedule_time") or "23:00",
                state=state,
                now=now,
            )
            changed = created or changed

        price_settings = settings.get("prices") or {}
        if automation_core.is_plan_due(
            PRICE_PLAN_KEY,
            price_settings.get("schedule_time") or "01:00",
            enabled=bool(price_settings.get("enabled", False)),
            mode=str(price_settings.get("mode") or "manual"),
            now=now,
            state=state,
            schedule_enabled=bool(settings.get("schedule_enabled", True)),
        ):
            created = _submit_plan_job(
                kind="prices_upload",
                label="Ночная загрузка цен",
                target=price_uploader.run_price_upload_cycle,
                kwargs={"run_source": RUNNER_MODE, "rebuild": True},
                unique_key="prices_upload",
                plan_key=PRICE_PLAN_KEY,
                schedule_time=price_settings.get("schedule_time") or "01:00",
                state=state,
                now=now,
            )
            changed = created or changed

    if changed:
        automation_core.save_state(state)
    return state


def _build_health_snapshot() -> Dict[str, Any]:
    recent_jobs = background_jobs.list_jobs_public(tenant_id=SYSTEM_TENANT_ID, limit=60)
    counts = {"completed": 0, "error": 0, "running": 0, "queued": 0, "abandoned": 0}
    by_kind: Dict[str, Dict[str, Any]] = {}
    for job in recent_jobs:
        kind = str(job.get("kind") or "unknown").strip() or "unknown"
        status = str(job.get("status") or "unknown").strip() or "unknown"
        counts[status] = counts.get(status, 0) + 1
        bucket = by_kind.get(kind) if isinstance(by_kind.get(kind), dict) else {"kind": kind, "completed": 0, "error": 0, "running": 0, "queued": 0, "last_success_at": "", "last_error_at": "", "last_status": ""}
        bucket[status] = int(bucket.get(status) or 0) + 1
        bucket["last_status"] = status
        if status == "completed" and not bucket.get("last_success_at"):
            bucket["last_success_at"] = str(job.get("finished_at") or job.get("updated_at") or "")
        if status == "error" and not bucket.get("last_error_at"):
            bucket["last_error_at"] = str(job.get("finished_at") or job.get("updated_at") or "")
        by_kind[kind] = bucket
    summary = {
        "recent_total": len(recent_jobs),
        "status_counts": counts,
        "by_kind": list(by_kind.values()),
        "last_success_at": next((str(job.get("finished_at") or job.get("updated_at") or "") for job in recent_jobs if str(job.get("status") or "") == "completed"), ""),
        "last_error_at": next((str(job.get("finished_at") or job.get("updated_at") or "") for job in recent_jobs if str(job.get("status") or "") == "error"), ""),
        "running_jobs": [job for job in recent_jobs if str(job.get("status") or "") in {"queued", "running"}],
    }
    summary["healthy"] = not summary["status_counts"].get("error") or bool(summary.get("last_success_at"))
    return summary


def _scheduler_loop() -> None:
    automation_core.update_scheduler_state(
        started_at=automation_core.now_local(automation_core.load_settings()).isoformat(timespec="seconds"),
        status="running",
        last_error="",
        worker_mode=RUNNER_MODE,
    )
    while not _stop_event.wait(POLL_SECONDS):
        try:
            scheduler_tick()
        except Exception as exc:
            automation_core.update_scheduler_state(status="error", last_error=str(exc), worker_mode=RUNNER_MODE)
            log_event("automation", "scheduler_error", tenant_id=SYSTEM_TENANT_ID, level="error", error=str(exc), run_id="scheduler")


def run_forever() -> None:
    log_event("automation", "scheduler_worker_start", tenant_id=SYSTEM_TENANT_ID, run_id="scheduler", worker_mode=RUNNER_MODE)
    _stop_event.clear()
    _scheduler_loop()


def start_scheduler() -> bool:
    global _scheduler_thread
    if not SCHEDULER_ENABLED:
        automation_core.update_scheduler_state(status="disabled", worker_mode=RUNNER_MODE)
        return False
    if RUNNER_MODE == "external" or not SCHEDULER_EMBEDDED:
        automation_core.update_scheduler_state(status="external", worker_mode="external")
        log_event("automation", "scheduler_external_mode", tenant_id=SYSTEM_TENANT_ID, run_id="scheduler")
        return False
    with _runtime_lock:
        if _scheduler_thread is not None and _scheduler_thread.is_alive():
            return False
        _stop_event.clear()
        _scheduler_thread = threading.Thread(target=_scheduler_loop, name="wb-automation-scheduler", daemon=True)
        _scheduler_thread.start()
        return True


def stop_scheduler(timeout: float = 2.0) -> None:
    global _scheduler_thread
    _stop_event.set()
    thread = _scheduler_thread
    if thread is not None:
        try:
            thread.join(timeout=timeout)
        except Exception:
            pass
    _scheduler_thread = None
    automation_core.update_scheduler_state(status="stopped", worker_mode=RUNNER_MODE)
    log_event("automation", "scheduler_stopped", tenant_id=SYSTEM_TENANT_ID, run_id="scheduler", worker_mode=RUNNER_MODE)


def scheduler_status() -> Dict[str, Any]:
    settings = automation_core.load_settings()
    state = automation_core.load_state()
    runtime = {
        "enabled": SCHEDULER_ENABLED,
        "embedded": SCHEDULER_EMBEDDED,
        "worker_mode": RUNNER_MODE,
        "poll_seconds": POLL_SECONDS,
        "thread_alive": bool(_scheduler_thread and _scheduler_thread.is_alive()),
        "next_runs": automation_core.next_runs(settings),
    }
    return {
        "runtime_mode": RUNNER_MODE,
        "runtime": runtime,
        "state": state.get("scheduler") if isinstance(state.get("scheduler"), dict) else {},
        "plans": state.get("plans") if isinstance(state.get("plans"), dict) else {},
        "health": _build_health_snapshot(),
    }
