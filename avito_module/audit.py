from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .compat import (
    append_jsonl,
    background_progress,
    clean_text,
    current_background_job_id,
    log_event,
    read_json,
    sanitize_payload,
    utc_now_iso,
    write_json,
)
from .storage import AvitoStorage

_RUN_INDEX_LIMIT = 300


@dataclass(slots=True)
class AvitoAuditSnapshot:
    run_id: str
    tenant_id: str
    kind: str
    label: str
    status: str
    created_at: str
    updated_at: str
    finished_at: str
    job_id: str
    last_stage: str
    last_message: str
    last_percent: float
    steps_count: int
    duration_ms: int
    summary: Dict[str, Any]


class AvitoAuditLogger:
    """Detailed run logger for the Avito module.

    Compared with the host automation logs, this logger persists three layers:
    1. background_jobs progress timeline (when the current action runs in a job);
    2. per-channel JSONL logs under tenant/logs/avito/channels/;
    3. per-run timeline + run summary under tenant/logs/avito/runs/.

    The design is intentionally append-only and tolerant to partial failures: if one
    sink fails, the others still receive the event.
    """

    def __init__(self, storage: AvitoStorage, *, kind: str, label: str = "", source: str = "service") -> None:
        self.storage = storage
        self.tenant_id = storage.tenant_id
        self.kind = clean_text(kind) or "avito_run"
        self.label = clean_text(label) or self.kind
        self.source = clean_text(source) or "service"
        self.job_id = current_background_job_id()
        ts_token = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        self.run_id = f"{self.kind}-{ts_token}-{uuid.uuid4().hex[:8]}"
        self.created_monotonic = time.monotonic()
        self.run_meta: Dict[str, Any] = {
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "kind": self.kind,
            "label": self.label,
            "source": self.source,
            "job_id": self.job_id,
            "status": "running",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "finished_at": "",
            "last_stage": "",
            "last_message": "",
            "last_percent": 0.0,
            "steps_count": 0,
            "duration_ms": 0,
            "summary": {},
        }
        self.run_events_path = self.storage.paths.run_logs_dir / f"{self.run_id}.jsonl"
        self.run_summary_path = self.storage.paths.run_logs_dir / f"{self.run_id}.json"
        self._persist_meta()
        self._update_run_index()

    def stage(
        self,
        stage: str,
        message: str = "",
        *,
        channel: str = "ops",
        percent: Optional[float] = None,
        level: str = "info",
        mirror_safe_log: bool = True,
        **data: Any,
    ) -> Dict[str, Any]:
        stage = clean_text(stage) or "progress"
        channel = clean_text(channel) or self.kind
        message = clean_text(message)
        level = clean_text(level) or "info"
        row: Dict[str, Any] = {
            "ts": utc_now_iso(),
            "tenant_id": self.tenant_id,
            "run_id": self.run_id,
            "job_id": self.job_id,
            "kind": self.kind,
            "label": self.label,
            "source": self.source,
            "channel": channel,
            "stage": stage,
            "level": level,
        }
        if message:
            row["message"] = message
        if percent is not None:
            try:
                row["percent"] = max(0.0, min(100.0, float(percent)))
            except Exception:
                pass
        if data:
            row["data"] = sanitize_payload(data)

        append_jsonl(self.storage.paths.channel_logs_dir / f"{channel}.jsonl", row)
        append_jsonl(self.run_events_path, row)

        self.run_meta["updated_at"] = row["ts"]
        self.run_meta["last_stage"] = stage
        if message:
            self.run_meta["last_message"] = message
        if "percent" in row:
            self.run_meta["last_percent"] = row["percent"]
        self.run_meta["steps_count"] = int(self.run_meta.get("steps_count") or 0) + 1
        self.run_meta["duration_ms"] = int((time.monotonic() - self.created_monotonic) * 1000)
        self._persist_meta()
        self._update_run_index()

        try:
            background_progress(
                stage=stage,
                message=message,
                percent=row.get("percent"),
                run_id=self.run_id,
                avito_kind=self.kind,
                avito_channel=channel,
            )
        except Exception:
            pass

        if mirror_safe_log:
            safe_payload: Dict[str, Any] = {
                "run_id": self.run_id,
                "job_id": self.job_id,
                "kind": self.kind,
                "label": self.label,
                "source": self.source,
            }
            if "data" in row and isinstance(row["data"], dict):
                safe_payload.update(row["data"])
            log_event(f"avito_{channel}", stage, tenant_id=self.tenant_id, level=level, **safe_payload)
        return row

    def finish(self, status: str = "completed", message: str = "", *, level: str = "info", **summary: Any) -> Dict[str, Any]:
        status = clean_text(status) or "completed"
        final_stage = {
            "completed": "done",
            "error": "error",
            "warning": "warning",
            "abandoned": "abandoned",
        }.get(status, status)
        final_message = clean_text(message) or {
            "completed": "Операция Avito завершена",
            "error": "Операция Avito завершилась с ошибкой",
            "warning": "Операция Avito завершена с предупреждениями",
        }.get(status, "Операция Avito завершена")
        row = self.stage(final_stage, final_message, channel="ops", percent=100, level=level, **summary)
        finished_at = row.get("ts") or utc_now_iso()
        self.run_meta["status"] = status
        self.run_meta["finished_at"] = finished_at
        self.run_meta["updated_at"] = finished_at
        self.run_meta["duration_ms"] = int((time.monotonic() - self.created_monotonic) * 1000)
        self.run_meta["summary"] = sanitize_payload(summary) if summary else {}
        if final_message:
            self.run_meta["last_message"] = final_message
        self._persist_meta()
        self._update_run_index()
        return dict(self.run_meta)

    def fail(self, message: str, **summary: Any) -> Dict[str, Any]:
        return self.finish("error", message, level="error", **summary)

    def warn(self, stage: str, message: str, *, channel: str = "ops", percent: Optional[float] = None, **data: Any) -> Dict[str, Any]:
        return self.stage(stage, message, channel=channel, percent=percent, level="warning", **data)

    def info(self, stage: str, message: str, *, channel: str = "ops", percent: Optional[float] = None, **data: Any) -> Dict[str, Any]:
        return self.stage(stage, message, channel=channel, percent=percent, level="info", **data)

    def error(self, stage: str, message: str, *, channel: str = "ops", percent: Optional[float] = None, **data: Any) -> Dict[str, Any]:
        return self.stage(stage, message, channel=channel, percent=percent, level="error", **data)

    def _persist_meta(self) -> None:
        write_json(self.run_summary_path, self.run_meta)
        write_json(self.storage.paths.last_run_file, self.run_meta)

    def _update_run_index(self) -> None:
        rows = read_json(self.storage.paths.run_index_file, [])
        if not isinstance(rows, list):
            rows = []
        normalized: List[Dict[str, Any]] = [row for row in rows if isinstance(row, dict) and clean_text(row.get("run_id")) != self.run_id]
        normalized.append(dict(self.run_meta))
        normalized.sort(key=lambda row: clean_text(row.get("updated_at") or row.get("created_at")), reverse=True)
        normalized = normalized[:_RUN_INDEX_LIMIT]
        write_json(self.storage.paths.run_index_file, normalized)



def log_avito_event(
    storage: AvitoStorage,
    *,
    channel: str,
    stage: str,
    message: str = "",
    level: str = "info",
    kind: str = "avito_event",
    run_id: str = "",
    **data: Any,
) -> Dict[str, Any]:
    channel = clean_text(channel) or "ops"
    stage = clean_text(stage) or "event"
    row: Dict[str, Any] = {
        "ts": utc_now_iso(),
        "tenant_id": storage.tenant_id,
        "run_id": clean_text(run_id),
        "job_id": current_background_job_id(),
        "kind": clean_text(kind) or "avito_event",
        "channel": channel,
        "stage": stage,
        "level": clean_text(level) or "info",
    }
    if clean_text(message):
        row["message"] = clean_text(message)
    if data:
        row["data"] = sanitize_payload(data)
    append_jsonl(storage.paths.channel_logs_dir / f"{channel}.jsonl", row)
    safe_payload = {"kind": row["kind"], "run_id": row.get("run_id", ""), "job_id": row.get("job_id", "")}
    if isinstance(row.get("data"), dict):
        safe_payload.update(row["data"])
    log_event(f"avito_{channel}", stage, tenant_id=storage.tenant_id, level=row["level"], **safe_payload)
    return row



def list_recent_runs(storage: AvitoStorage, limit: int = 20) -> List[Dict[str, Any]]:
    rows = read_json(storage.paths.run_index_file, [])
    if not isinstance(rows, list):
        return []
    cleaned = [row for row in rows if isinstance(row, dict)]
    cleaned.sort(key=lambda row: clean_text(row.get("updated_at") or row.get("created_at")), reverse=True)
    return cleaned[: max(1, int(limit or 20))]



def load_run_summary(storage: AvitoStorage, run_id: str) -> Dict[str, Any]:
    run_id = clean_text(run_id)
    if not run_id:
        return {}
    path = storage.paths.run_logs_dir / f"{run_id}.json"
    payload = read_json(path, {})
    return payload if isinstance(payload, dict) else {}



def tail_jsonl(path: Path, limit: int = 200) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    import json

                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return []
    rows.sort(key=lambda row: clean_text(row.get("ts")))
    return rows[-max(1, int(limit or 200)) :]



def load_run_events(storage: AvitoStorage, run_id: str, limit: int = 300) -> List[Dict[str, Any]]:
    run_id = clean_text(run_id)
    if not run_id:
        return []
    path = storage.paths.run_logs_dir / f"{run_id}.jsonl"
    return tail_jsonl(path, limit=limit)



def load_channel_events(storage: AvitoStorage, channel: str, limit: int = 200) -> List[Dict[str, Any]]:
    channel = clean_text(channel) or "ops"
    path = storage.paths.channel_logs_dir / f"{channel}.jsonl"
    return tail_jsonl(path, limit=limit)



def trim_old_run_files(storage: AvitoStorage, *, keep: int = _RUN_INDEX_LIMIT) -> None:
    runs = list_recent_runs(storage, limit=max(keep, 1))
    keep_ids = {clean_text(row.get("run_id")) for row in runs if clean_text(row.get("run_id"))}
    for path in storage.paths.run_logs_dir.glob("*.json"):
        if path.name == storage.paths.last_run_file.name:
            continue
        run_id = path.stem
        if run_id in keep_ids:
            continue
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
    for path in storage.paths.run_logs_dir.glob("*.jsonl"):
        run_id = path.stem
        if run_id in keep_ids:
            continue
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
