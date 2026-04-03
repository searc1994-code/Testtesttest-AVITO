from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:  # Host app optional imports.
    import tenant_manager as _tenant_manager  # type: ignore
except Exception:  # pragma: no cover - optional dependency.
    _tenant_manager = None

try:
    import safe_logs as _safe_logs  # type: ignore
except Exception:  # pragma: no cover - optional dependency.
    _safe_logs = None

try:
    import common as _common  # type: ignore
except Exception:  # pragma: no cover - optional dependency.
    _common = None

try:
    import safe_files as _safe_files  # type: ignore
except Exception:  # pragma: no cover - optional dependency.
    _safe_files = None

try:
    import background_jobs as _background_jobs  # type: ignore
except Exception:  # pragma: no cover - optional dependency.
    _background_jobs = None



def clean_text(value: Any) -> str:
    if _common and hasattr(_common, "clean_text"):
        try:
            return str(_common.clean_text(value))
        except Exception:
            pass
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()



def utc_now_iso() -> str:
    if _common and hasattr(_common, "utc_now_iso"):
        try:
            return str(_common.utc_now_iso())
        except Exception:
            pass
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()



def resolve_tenant(tenant_id: str) -> Dict[str, Any]:
    tenant_id = clean_text(tenant_id)
    if _tenant_manager and hasattr(_tenant_manager, "get_tenant"):
        try:
            tenant = _tenant_manager.get_tenant(tenant_id)
            if isinstance(tenant, dict):
                return tenant
        except Exception:
            logger.exception("Failed to resolve tenant via tenant_manager")
    return {"id": tenant_id, "name": tenant_id or "default"}


_DEFAULT_ROOT = Path(os.environ.get("AVITO_MODULE_BASE_DIR", "/tmp/avito_module"))



def resolve_paths(tenant_id: str, base_dir: Optional[Path] = None) -> Dict[str, Path]:
    tenant_id = clean_text(tenant_id) or "default"
    if _tenant_manager and hasattr(_tenant_manager, "ensure_tenant_dirs"):
        try:
            paths = _tenant_manager.ensure_tenant_dirs(tenant_id)
            if isinstance(paths, dict) and paths.get("data_dir"):
                out: Dict[str, Path] = {}
                for key, value in paths.items():
                    try:
                        out[str(key)] = Path(str(value))
                    except Exception:
                        continue
                data_dir = out["data_dir"]
                auth_dir = out.get("auth_dir", data_dir)
                logs_dir = out.get("logs_dir", data_dir.parent / "logs")
                avito_logs_dir = logs_dir / "avito"
                channel_logs_dir = avito_logs_dir / "channels"
                run_logs_dir = avito_logs_dir / "runs"
                out.setdefault("avito_db_file", data_dir / "avito.sqlite3")
                out.setdefault("avito_settings_file", data_dir / "avito_settings.json")
                out.setdefault("avito_rules_file", data_dir / "avito_rules.json")
                out.setdefault("avito_browser_state_file", auth_dir / "avito_state.json")
                out.setdefault("avito_browser_profile_file", logs_dir / "avito_browser_profile.json")
                out.setdefault("avito_secret_file", auth_dir / "avito_secrets.json")
                out.setdefault("avito_guardian_state_file", auth_dir / "avito_guardian_state.json")
                out.setdefault("avito_exports_dir", avito_logs_dir / "exports")
                out.setdefault("avito_media_dir", data_dir / "avito_media")
                out.setdefault("avito_knowledge_dir", data_dir / "avito_knowledge")
                out.setdefault("avito_logs_dir", avito_logs_dir)
                out.setdefault("avito_channel_logs_dir", channel_logs_dir)
                out.setdefault("avito_run_logs_dir", run_logs_dir)
                out.setdefault("avito_run_index_file", avito_logs_dir / "runs_index.json")
                out.setdefault("avito_last_run_file", avito_logs_dir / "last_run.json")
                for key in ("avito_logs_dir", "avito_channel_logs_dir", "avito_run_logs_dir", "avito_exports_dir", "avito_media_dir", "avito_knowledge_dir"):
                    out[key].mkdir(parents=True, exist_ok=True)
                return out
        except Exception:
            logger.exception("Failed to resolve paths via tenant_manager")

    root = (base_dir or _DEFAULT_ROOT) / tenant_id
    data_dir = root / "data"
    auth_dir = root / "auth"
    safe_logs_dir = root / "safe_logs"
    logs_dir = root / "logs"
    avito_logs_dir = logs_dir / "avito"
    channel_logs_dir = avito_logs_dir / "channels"
    run_logs_dir = avito_logs_dir / "runs"
    for path in (root, data_dir, auth_dir, safe_logs_dir, logs_dir, avito_logs_dir, channel_logs_dir, run_logs_dir, data_dir / 'avito_media', data_dir / 'avito_knowledge'):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "tenant_root": root,
        "data_dir": data_dir,
        "auth_dir": auth_dir,
        "safe_logs_dir": safe_logs_dir,
        "logs_dir": logs_dir,
        "avito_logs_dir": avito_logs_dir,
        "avito_channel_logs_dir": channel_logs_dir,
        "avito_run_logs_dir": run_logs_dir,
        "avito_run_index_file": avito_logs_dir / "runs_index.json",
        "avito_last_run_file": avito_logs_dir / "last_run.json",
        "avito_db_file": data_dir / "avito.sqlite3",
        "avito_settings_file": data_dir / "avito_settings.json",
        "avito_rules_file": data_dir / "avito_rules.json",
        "avito_browser_state_file": auth_dir / "avito_state.json",
        "avito_browser_profile_file": logs_dir / "avito_browser_profile.json",
        "avito_secret_file": auth_dir / "avito_secrets.json",
        "avito_guardian_state_file": auth_dir / "avito_guardian_state.json",
        "avito_media_dir": data_dir / "avito_media",
        "avito_knowledge_dir": data_dir / "avito_knowledge",
        "avito_exports_dir": avito_logs_dir / "exports",
    }



def log_event(channel: str, event: str, tenant_id: Optional[str] = None, level: str = "info", **data: Any) -> None:
    if _safe_logs and hasattr(_safe_logs, "log_event"):
        try:
            _safe_logs.log_event(channel, event, tenant_id=tenant_id, level=level, **data)
            return
        except Exception:
            logger.exception("safe_logs.log_event failed")
    record = {
        "tenant_id": clean_text(tenant_id),
        "channel": clean_text(channel),
        "event": clean_text(event),
        "level": clean_text(level) or "info",
        "data": data,
    }
    getattr(logger, record["level"], logger.info)("%s", record)



def read_json(path: Path, default: Any) -> Any:
    if _common and hasattr(_common, "read_json"):
        try:
            return _common.read_json(path, default)
        except Exception:
            pass
    import json

    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default



def write_json(path: Path, payload: Any) -> None:
    if _common and hasattr(_common, "write_json"):
        try:
            _common.write_json(path, payload)
            return
        except Exception:
            pass
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _looks_sensitive_key(key: str) -> bool:
    low = clean_text(key).lower()
    return any(part in low for part in ("token", "secret", "password", "cookie", "authorization", "api_key", "client_secret", "phone"))



def sanitize_payload(data: Any, key_hint: str = "") -> Any:
    if _safe_logs and hasattr(_safe_logs, "sanitize"):
        try:
            return _safe_logs.sanitize(data, key_hint=key_hint)
        except Exception:
            logger.exception("safe_logs.sanitize failed")
    if _looks_sensitive_key(key_hint):
        return "[REDACTED]"
    if data is None or isinstance(data, (int, float, bool)):
        return data
    if isinstance(data, Path):
        return clean_text(str(data))
    if isinstance(data, str):
        text = clean_text(data)
        if len(text) > 1200:
            text = text[:1200] + "…"
        return text
    if isinstance(data, dict):
        return {clean_text(k): sanitize_payload(v, key_hint=str(k)) for k, v in data.items()}
    if isinstance(data, (list, tuple, set)):
        return [sanitize_payload(item, key_hint=key_hint) for item in list(data)]
    return clean_text(repr(data))



def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    payload = sanitize_payload(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    if _safe_files and hasattr(_safe_files, "append_jsonl"):
        try:
            _safe_files.append_jsonl(path, payload, ensure_ascii=False)
            return
        except Exception:
            logger.exception("safe_files.append_jsonl failed")
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")



def read_jsonl(path: Path, limit: int = 200) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return []
    rows.sort(key=lambda row: clean_text(row.get("ts")))
    return rows[-max(1, int(limit or 200)) :]



def current_background_job_id() -> str:
    if background_jobs_available() and hasattr(_background_jobs, "current_job_id"):
        try:
            return clean_text(_background_jobs.current_job_id())  # type: ignore[union-attr]
        except Exception:
            logger.exception("background_jobs.current_job_id failed")
    return ""



def current_background_job_tenant_id() -> str:
    if background_jobs_available() and hasattr(_background_jobs, "current_job_tenant_id"):
        try:
            return clean_text(_background_jobs.current_job_tenant_id())  # type: ignore[union-attr]
        except Exception:
            logger.exception("background_jobs.current_job_tenant_id failed")
    return ""



def background_jobs_available() -> bool:
    return bool(_background_jobs and hasattr(_background_jobs, "submit_job"))



def submit_background_job(
    *,
    kind: str,
    tenant_id: str,
    label: str,
    target: Any,
    args: Tuple[Any, ...] = (),
    kwargs: Optional[Dict[str, Any]] = None,
    unique_key: str = "",
) -> Tuple[Dict[str, Any], bool]:
    if not background_jobs_available():
        raise RuntimeError("background_jobs is unavailable")
    return _background_jobs.submit_job(  # type: ignore[union-attr]
        kind=clean_text(kind),
        tenant_id=clean_text(tenant_id),
        label=clean_text(label),
        target=target,
        args=args,
        kwargs=kwargs or {},
        unique_key=clean_text(unique_key),
    )



def background_progress(stage: str, message: str = "", percent: Optional[float] = None, **data: Any) -> Dict[str, Any]:
    if background_jobs_available() and hasattr(_background_jobs, "progress"):
        try:
            return _background_jobs.progress(stage=stage, message=message, percent=percent, **data)  # type: ignore[union-attr]
        except Exception:
            logger.exception("background_jobs.progress failed")
    return {}



def list_latest_jobs(tenant_id: str, limit: int = 8) -> List[Dict[str, Any]]:
    if background_jobs_available() and hasattr(_background_jobs, "list_latest_jobs_by_kind"):
        try:
            return list(_background_jobs.list_latest_jobs_by_kind(clean_text(tenant_id), limit=limit) or [])  # type: ignore[union-attr]
        except Exception:
            logger.exception("background_jobs.list_latest_jobs_by_kind failed")
    return []
