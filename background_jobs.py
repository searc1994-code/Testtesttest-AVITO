import threading
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import common
import config
import tenant_manager
from safe_logs import log_event

JobCallable = Callable[..., Any]

_MAX_WORKERS = int(getattr(config, "BACKGROUND_JOB_MAX_WORKERS", 4) or 4)
_STALE_SECONDS = int(getattr(config, "BACKGROUND_JOB_STALE_SECONDS", 6 * 60 * 60) or 21600)
_HISTORY_LIMIT = int(getattr(config, "BACKGROUND_JOB_HISTORY_LIMIT", 300) or 300)
_HEARTBEAT_INTERVAL = 5.0
_PROGRESS_LOG_LIMIT = 250
_SYSTEM_TENANT_ID = "_system"

_executor = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="wb-bg")
_runtime_lock = threading.RLock()
_runtime_futures: Dict[str, Future] = {}
_runtime_job_tenants: Dict[str, str] = {}
_tenant_locks: Dict[str, threading.RLock] = {}
_current_job_id_var: ContextVar[str] = ContextVar("wb_bg_current_job_id", default="")
_current_job_tenant_var: ContextVar[str] = ContextVar("wb_bg_current_job_tenant", default="")


def _now() -> str:
    return common.utc_now_iso()


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _normalize_tenant_id(value: Any) -> str:
    tenant_id = _clean(value)
    return tenant_id or _SYSTEM_TENANT_ID


def _tenant_lock(tenant_id: str) -> threading.RLock:
    tenant_id = _normalize_tenant_id(tenant_id)
    with _runtime_lock:
        lock = _tenant_locks.get(tenant_id)
        if lock is None:
            lock = threading.RLock()
            _tenant_locks[tenant_id] = lock
        return lock


def _jobs_file(tenant_id: str) -> Path:
    tenant_id = _normalize_tenant_id(tenant_id)
    if tenant_id == _SYSTEM_TENANT_ID:
        path = Path(common.SHARED_DIR) / "background_jobs_system.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            tenant_manager.write_json(path, [])
        return path
    paths = tenant_manager.ensure_tenant_dirs(tenant_id)
    return Path(paths["jobs_file"])


def _iter_known_job_tenants() -> List[str]:
    tenant_ids = [_SYSTEM_TENANT_ID]
    tenant_ids.extend(
        _clean(item.get("id"))
        for item in tenant_manager.load_tenants()
        if _clean(item.get("id"))
    )
    seen: set[str] = set()
    result: List[str] = []
    for tenant_id in tenant_ids:
        normalized = _normalize_tenant_id(tenant_id)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _job_key(kind: str, tenant_id: str, unique_key: str = "") -> str:
    base = f"{_clean(kind)}::{_normalize_tenant_id(tenant_id)}"
    unique_key = _clean(unique_key)
    if unique_key:
        base += f"::{unique_key}"
    return base


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return _clean(repr(value))


def _snapshot(job: Dict[str, Any]) -> Dict[str, Any]:
    return deepcopy({key: value for key, value in job.items() if key not in {"future"}})


def _public_result(value: Any) -> Any:
    if isinstance(value, dict):
        payload = {str(key): _json_safe(item) for key, item in value.items() if str(key) not in {"traceback", "debug_traceback", "internal_traceback"}}
        if any(str(key) in {"traceback", "debug_traceback", "internal_traceback"} for key in value.keys()):
            payload["traceback_available"] = True
        return payload
    return _json_safe(value)


def public_job_view(job: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = _snapshot(job) if isinstance(job, dict) else {}
    if not snapshot:
        return {}
    public_result = _public_result(snapshot.get("result"))
    snapshot["result"] = public_result
    if isinstance(public_result, dict):
        snapshot["traceback_available"] = bool(public_result.get("traceback_available"))
    progress = snapshot.get("progress") if isinstance(snapshot.get("progress"), dict) else {}
    snapshot["progress_stage"] = _clean(progress.get("stage"))
    snapshot["progress_percent"] = progress.get("percent")
    return snapshot


def get_job_public(job_id: str) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    return public_job_view(job) if job else None


def list_jobs_public(tenant_id: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    return [public_job_view(job) for job in list_jobs(tenant_id=tenant_id, limit=limit)]


def _parse_dt(value: Any) -> datetime:
    text = _clean(value)
    if not text:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def _is_stale(job: Dict[str, Any]) -> bool:
    ts = _parse_dt(job.get("last_heartbeat_at") or job.get("started_at") or job.get("created_at"))
    if ts.year < 1971:
        return False
    return (datetime.now(timezone.utc) - ts).total_seconds() > _STALE_SECONDS


def _load_jobs_for_tenant(tenant_id: str) -> List[Dict[str, Any]]:
    data = tenant_manager.read_json(_jobs_file(tenant_id), [])
    return data if isinstance(data, list) else []


def _save_jobs_for_tenant(tenant_id: str, jobs: List[Dict[str, Any]]) -> None:
    trimmed = list(jobs)[-_HISTORY_LIMIT:]
    tenant_manager.write_json(_jobs_file(tenant_id), trimmed)


def _cleanup_jobs_locked(tenant_id: str, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    changed = False
    for job in jobs:
        job_id = _clean(job.get("job_id"))
        status = _clean(job.get("status"))
        if status not in {"queued", "running"}:
            continue
        with _runtime_lock:
            future = _runtime_futures.get(job_id)
        if future is not None and not future.done():
            continue
        if future is None:
            job["status"] = "abandoned"
            job["finished_at"] = job.get("finished_at") or _now()
            job["updated_at"] = _now()
            job["last_message"] = "Задача помечена как незавершённая после рестарта или потери worker-а"
            changed = True
            continue
        if _is_stale(job):
            job["status"] = "abandoned"
            job["finished_at"] = job.get("finished_at") or _now()
            job["updated_at"] = _now()
            job["last_message"] = "Задача помечена как незавершённая после зависания"
            changed = True
    if changed:
        _save_jobs_for_tenant(tenant_id, jobs)
    return jobs


def _load_active_jobs(tenant_id: str) -> List[Dict[str, Any]]:
    with _tenant_lock(tenant_id):
        jobs = _load_jobs_for_tenant(tenant_id)
        return _cleanup_jobs_locked(tenant_id, jobs)


def _find_job_in_list(jobs: List[Dict[str, Any]], job_id: str) -> Optional[Dict[str, Any]]:
    job_id = _clean(job_id)
    for job in jobs:
        if _clean(job.get("job_id")) == job_id:
            return job
    return None


def _find_job_tenant(job_id: str) -> str:
    job_id = _clean(job_id)
    with _runtime_lock:
        tenant_id = _runtime_job_tenants.get(job_id)
        if tenant_id:
            return tenant_id
    for tenant_id in _iter_known_job_tenants():
        jobs = _load_active_jobs(tenant_id)
        if _find_job_in_list(jobs, job_id):
            return tenant_id
    return ""


def current_job_id() -> str:
    return _clean(_current_job_id_var.get())


def current_job_tenant_id() -> str:
    return _normalize_tenant_id(_current_job_tenant_var.get())


def update_job(job_id: str, **updates: Any) -> Dict[str, Any]:
    tenant_id = _find_job_tenant(job_id)
    if not tenant_id:
        raise KeyError(job_id)
    with _tenant_lock(tenant_id):
        jobs = _load_jobs_for_tenant(tenant_id)
        jobs = _cleanup_jobs_locked(tenant_id, jobs)
        job = _find_job_in_list(jobs, job_id)
        if not job:
            raise KeyError(job_id)
        if "updated_at" not in updates:
            updates["updated_at"] = _now()
        for key, value in updates.items():
            job[key] = _json_safe(value)
        _save_jobs_for_tenant(tenant_id, jobs)
        return _snapshot(job)


def touch_job(job_id: str, message: str = "", **metrics: Any) -> Dict[str, Any]:
    updates: Dict[str, Any] = {
        "last_heartbeat_at": _now(),
        "updated_at": _now(),
    }
    if message:
        updates["last_message"] = _clean(message)
    if metrics:
        progress = {str(k): _json_safe(v) for k, v in metrics.items()}
        updates["progress"] = progress
    return update_job(job_id, **updates)


def append_job_progress(job_id: str, stage: str, message: str = "", percent: Optional[float] = None, **data: Any) -> Dict[str, Any]:
    tenant_id = _find_job_tenant(job_id)
    if not tenant_id:
        raise KeyError(job_id)
    with _tenant_lock(tenant_id):
        jobs = _load_jobs_for_tenant(tenant_id)
        jobs = _cleanup_jobs_locked(tenant_id, jobs)
        job = _find_job_in_list(jobs, job_id)
        if not job:
            raise KeyError(job_id)
        progress = job.get("progress") if isinstance(job.get("progress"), dict) else {}
        progress_log = job.get("progress_log") if isinstance(job.get("progress_log"), list) else []
        row: Dict[str, Any] = {
            "ts": _now(),
            "stage": _clean(stage) or "progress",
        }
        if message:
            row["message"] = _clean(message)
        if percent is not None:
            try:
                row["percent"] = max(0.0, min(100.0, float(percent)))
            except Exception:
                pass
        for key, value in data.items():
            row[str(key)] = _json_safe(value)
        progress_log.append(row)
        progress_log = progress_log[-_PROGRESS_LOG_LIMIT:]
        progress.update({str(k): _json_safe(v) for k, v in data.items()})
        progress["stage"] = row["stage"]
        if "message" in row:
            progress["message"] = row["message"]
        if "percent" in row:
            progress["percent"] = row["percent"]
        job["progress"] = progress
        job["progress_log"] = progress_log
        job["updated_at"] = _now()
        job["last_heartbeat_at"] = job["updated_at"]
        if message:
            job["last_message"] = _clean(message)
        _save_jobs_for_tenant(tenant_id, jobs)
        return _snapshot(job)


def progress(stage: str, message: str = "", percent: Optional[float] = None, job_id: str = "", **data: Any) -> Dict[str, Any]:
    resolved_job_id = _clean(job_id) or current_job_id()
    if not resolved_job_id:
        return {}
    return append_job_progress(resolved_job_id, stage=stage, message=message, percent=percent, **data)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    tenant_id = _find_job_tenant(job_id)
    if not tenant_id:
        return None
    with _tenant_lock(tenant_id):
        jobs = _load_jobs_for_tenant(tenant_id)
        jobs = _cleanup_jobs_locked(tenant_id, jobs)
        job = _find_job_in_list(jobs, job_id)
        return _snapshot(job) if job else None


def list_jobs(tenant_id: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    limit = max(1, int(limit or 20))
    tenant_id = _clean(tenant_id)
    rows: List[Dict[str, Any]] = []
    tenant_ids = [_normalize_tenant_id(tenant_id)] if tenant_id else _iter_known_job_tenants()
    for tid in tenant_ids:
        for row in _load_active_jobs(tid):
            rows.append(_snapshot(row))
    rows.sort(key=lambda row: (_clean(row.get("updated_at")), _clean(row.get("created_at"))), reverse=True)
    return rows[:limit]


def list_latest_jobs_by_kind(tenant_id: str = "", limit: int = 12) -> List[Dict[str, Any]]:
    seen = set()
    rows: List[Dict[str, Any]] = []
    for job in list_jobs(tenant_id=tenant_id, limit=max(limit * 8, 50)):
        key = (_clean(job.get("tenant_id")), _clean(job.get("kind")))
        if key in seen:
            continue
        seen.add(key)
        rows.append(job)
        if len(rows) >= limit:
            break
    return rows


def abandon_running_jobs(
    kind: str,
    tenant_id: str = "",
    *,
    unique_key: str = "",
    older_than_seconds: int = 0,
    progress_at_least: Optional[float] = None,
    message: str = "",
) -> List[str]:
    kind = _clean(kind)
    unique_key = _clean(unique_key)
    if not kind:
        return []
    tenant_ids = [_normalize_tenant_id(tenant_id)] if _clean(tenant_id) else _iter_known_job_tenants()
    forced_message = _clean(message) or "Задача принудительно помечена как незавершённая перед новым ручным запуском"
    threshold = max(0, int(older_than_seconds or 0))
    min_percent: Optional[float] = None
    if progress_at_least is not None:
        try:
            min_percent = float(progress_at_least)
        except Exception:
            min_percent = None
    abandoned: List[str] = []
    now_dt = datetime.now(timezone.utc)

    for tid in tenant_ids:
        with _tenant_lock(tid):
            jobs = _load_jobs_for_tenant(tid)
            jobs = _cleanup_jobs_locked(tid, jobs)
            changed = False
            expected_singleton = _job_key(kind, tid, unique_key or kind)
            for job in jobs:
                if _clean(job.get("kind")) != kind:
                    continue
                if unique_key and _clean(job.get("singleton_key")) != expected_singleton:
                    continue
                if _clean(job.get("status")) not in {"queued", "running"}:
                    continue
                started = _parse_dt(job.get("started_at") or job.get("created_at"))
                age_seconds = max(0.0, (now_dt - started).total_seconds()) if started.year >= 1971 else 0.0
                if threshold and age_seconds < threshold:
                    continue
                if min_percent is not None:
                    progress = job.get("progress") if isinstance(job.get("progress"), dict) else {}
                    try:
                        percent_value = float(progress.get("percent") or 0.0)
                    except Exception:
                        percent_value = 0.0
                    if percent_value < min_percent:
                        continue
                job["status"] = "abandoned"
                job["finished_at"] = job.get("finished_at") or _now()
                job["updated_at"] = _now()
                job["last_heartbeat_at"] = job["updated_at"]
                job["last_message"] = forced_message
                changed = True
                abandoned.append(_clean(job.get("job_id")))
            if changed:
                _save_jobs_for_tenant(tid, jobs)
    return [job_id for job_id in abandoned if job_id]


def submit_job(
    *,
    kind: str,
    tenant_id: str,
    label: str,
    target: JobCallable,
    args: Tuple[Any, ...] = (),
    kwargs: Optional[Dict[str, Any]] = None,
    unique_key: str = "",
) -> Tuple[Dict[str, Any], bool]:
    tenant_id = _normalize_tenant_id(tenant_id)
    kind = _clean(kind)
    label = _clean(label) or kind or "job"
    unique_key = _clean(unique_key)
    kwargs = kwargs or {}
    singleton_key = _job_key(kind, tenant_id, unique_key or kind)

    with _tenant_lock(tenant_id):
        jobs = _load_jobs_for_tenant(tenant_id)
        jobs = _cleanup_jobs_locked(tenant_id, jobs)
        for job in reversed(jobs):
            if _clean(job.get("singleton_key")) != singleton_key:
                continue
            if _clean(job.get("status")) in {"queued", "running"}:
                return _snapshot(job), False

        job_id = uuid.uuid4().hex[:16]
        job = {
            "job_id": job_id,
            "run_id": job_id,
            "tenant_id": tenant_id,
            "kind": kind,
            "label": label,
            "unique_key": unique_key,
            "singleton_key": singleton_key,
            "status": "queued",
            "created_at": _now(),
            "started_at": "",
            "finished_at": "",
            "updated_at": _now(),
            "last_heartbeat_at": _now(),
            "last_message": "Поставлено в фон",
            "result": None,
            "error": "",
            "progress": {},
            "progress_log": [],
        }
        jobs.append(job)
        _save_jobs_for_tenant(tenant_id, jobs)
        log_event("jobs", "job_submitted", tenant_id=tenant_id, job_id=job_id, run_id=job_id, kind=kind, label=label, unique_key=unique_key, singleton_key=singleton_key)
        future = _executor.submit(_run_job, tenant_id, job_id, singleton_key, target, args, dict(kwargs))
        with _runtime_lock:
            _runtime_futures[job_id] = future
            _runtime_job_tenants[job_id] = tenant_id
        return _snapshot(job), True


def _heartbeat_worker(job_id: str, stop_event: threading.Event) -> None:
    while not stop_event.wait(_HEARTBEAT_INTERVAL):
        try:
            touch_job(job_id)
        except Exception:
            return


def _run_job(tenant_id: str, job_id: str, singleton_key: str, target: JobCallable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
    heartbeat_stop = threading.Event()
    heartbeat_thread = threading.Thread(target=_heartbeat_worker, args=(job_id, heartbeat_stop), name=f"wb-bg-heartbeat-{job_id}", daemon=True)
    tokens = None
    job_id_token = None
    job_tenant_token = None
    try:
        update_job(job_id, status="running", started_at=_now(), last_message="Выполняется", last_heartbeat_at=_now())
        log_event("jobs", "job_started", tenant_id=tenant_id, job_id=job_id, run_id=job_id, singleton_key=singleton_key)
        heartbeat_thread.start()
        if tenant_id != _SYSTEM_TENANT_ID:
            tenant = tenant_manager.get_tenant(tenant_id) or {}
            paths = tenant_manager.ensure_tenant_dirs(tenant_id) if tenant_id else {}
            tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
        else:
            common.reset_tenant_context(None)
        job_id_token = _current_job_id_var.set(job_id)
        job_tenant_token = _current_job_tenant_var.set(tenant_id)
        append_job_progress(job_id, stage="start", message="Задача запущена", percent=0)
        result = target(*args, **kwargs)
        safe_result = _json_safe(result)
        update_job(
            job_id,
            status="completed",
            finished_at=_now(),
            updated_at=_now(),
            last_heartbeat_at=_now(),
            last_message="Задача завершена",
            result=safe_result,
            error="",
        )
        append_job_progress(job_id, stage="done", message="Задача завершена", percent=100)
        log_event("jobs", "job_completed", tenant_id=tenant_id, job_id=job_id, run_id=job_id, singleton_key=singleton_key, result=_public_result(safe_result))
    except Exception as exc:
        try:
            error_code = _clean(exc.__class__.__name__).lower() or "unhandled_exception"
            debug_traceback = traceback.format_exc()[-8000:]
            public_result = {
                "error_code": error_code,
                "message": _clean(str(exc)),
                "debug_traceback": debug_traceback,
            }
            update_job(
                job_id,
                status="error",
                finished_at=_now(),
                updated_at=_now(),
                last_heartbeat_at=_now(),
                last_message="Задача завершилась с ошибкой",
                error=_clean(str(exc)),
                result=public_result,
            )
            append_job_progress(job_id, stage="error", message=_clean(str(exc)), percent=100, error_code=error_code)
            log_event("jobs", "job_failed", tenant_id=tenant_id, level="error", job_id=job_id, run_id=job_id, singleton_key=singleton_key, error=_clean(str(exc)), error_code=error_code, result=_public_result(public_result))
        except Exception:
            pass
    finally:
        heartbeat_stop.set()
        try:
            heartbeat_thread.join(timeout=1.0)
        except Exception:
            pass
        if job_tenant_token is not None:
            try:
                _current_job_tenant_var.reset(job_tenant_token)
            except Exception:
                pass
        if job_id_token is not None:
            try:
                _current_job_id_var.reset(job_id_token)
            except Exception:
                pass
        if tokens is not None:
            common.reset_tenant_context(tokens)
        elif tenant_id == _SYSTEM_TENANT_ID:
            common.reset_tenant_context(None)
        with _runtime_lock:
            _runtime_futures.pop(job_id, None)
            _runtime_job_tenants.pop(job_id, None)



def report_progress(message: str = "", *, stage: str = "", current: Any = None, total: Any = None, percent: Any = None, **data: Any) -> Dict[str, Any]:
    return progress(stage=stage, message=message, current=current, total=total, percent=percent, **data)
