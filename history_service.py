import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import common
import history_sync_worker
import tenant_manager
import safe_files


def clean_text(value: Any) -> str:
    return common.clean_text(value)


def utc_now_iso() -> str:
    return common.utc_now_iso()


def _current_tenant_id(tenant_id: Optional[str] = None) -> str:
    tenant = clean_text(tenant_id) or common.get_active_tenant_id()
    return clean_text(tenant)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def get_paths(tenant_id: Optional[str] = None) -> Dict[str, Path]:
    tid = _current_tenant_id(tenant_id)
    paths = history_sync_worker.get_paths(tid)
    return {
        "db": Path(paths["db"]),
        "meta": Path(paths["meta"]),
        "stop": Path(paths["stop"]),
        "log": Path(paths["meta"]).with_name("historical_sync_worker.log"),
    }


def load_meta(tenant_id: Optional[str] = None) -> Dict[str, Any]:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return {}
    data = history_sync_worker.load_meta(tid)
    return data if isinstance(data, dict) else {}


def save_meta(data: Dict[str, Any], tenant_id: Optional[str] = None) -> None:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return
    history_sync_worker.save_meta(tid, data if isinstance(data, dict) else {})


def update_meta(tenant_id: Optional[str] = None, **updates: Any) -> Dict[str, Any]:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return {}
    return history_sync_worker.update_meta(tid, **updates)


def _history_worker_script_path() -> Path:
    return Path(__file__).with_name("history_sync_worker.py")


def _history_worker_pid(meta: Dict[str, Any]) -> int:
    return _safe_int(meta.get("worker_pid"), 0)


def _pid_alive(pid: int) -> bool:
    pid = _safe_int(pid, 0)
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            import ctypes  # pragma: no cover
            handle = ctypes.windll.kernel32.OpenProcess(0x100000, 0, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _history_is_stale(meta: Dict[str, Any], stale_seconds: int = 240) -> bool:
    if clean_text(meta.get("status")) not in {"running", "starting", "stop_requested"}:
        return False
    heartbeat = clean_text(meta.get("last_heartbeat_at") or meta.get("started_at"))
    if not heartbeat:
        return False
    try:
        if heartbeat.endswith("Z"):
            heartbeat = heartbeat[:-1] + "+00:00"
        ts = time.time() - __import__("datetime").datetime.fromisoformat(heartbeat).timestamp()
        return ts > stale_seconds
    except Exception:
        return False


def _tail_log(log_path: Path, max_lines: int = 60) -> str:
    if not log_path.exists():
        return ""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception:
        return ""


def effective_meta(tenant_id: Optional[str] = None) -> Dict[str, Any]:
    tid = _current_tenant_id(tenant_id)
    meta = load_meta(tid)
    if not meta:
        return {"tenant_id": tid, "status": "idle"} if tid else {}
    meta = dict(meta)
    pid = _history_worker_pid(meta)
    alive = _pid_alive(pid)
    status = clean_text(meta.get("status")) or "idle"
    if status in {"running", "starting", "stop_requested"} and pid and not alive:
        meta["status"] = "stopped" if clean_text(meta.get("stop_requested")) in {"1", "true", "True"} else "error"
        meta.setdefault("finished_at", utc_now_iso())
        if not clean_text(meta.get("last_error")) and status != "stop_requested":
            log_tail = _tail_log(get_paths(tid)["log"], 20)
            if log_tail:
                meta["last_error"] = log_tail
        save_meta(meta, tid)
    meta["is_stale"] = _history_is_stale(meta)
    meta["pid_alive"] = alive
    meta["worker_pid"] = pid
    return meta


def ensure_db(tenant_id: Optional[str] = None) -> None:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return
    history_sync_worker.ensure_db(tid)


def history_conn(tenant_id: Optional[str] = None) -> sqlite3.Connection:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        raise ValueError("tenant_id is required")
    return history_sync_worker.history_conn(tid)


def get_counts(tenant_id: Optional[str] = None) -> Dict[str, int]:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return {"total": 0, "active": 0, "archive": 0, "needs_reply": 0, "empty_unanswered": 0}
    try:
        return history_sync_worker.get_counts(tid)
    except Exception:
        return {"total": 0, "active": 0, "archive": 0, "needs_reply": 0, "empty_unanswered": 0}


def db_has_data(tenant_id: Optional[str] = None) -> bool:
    counts = get_counts(tenant_id)
    return _safe_int(counts.get("total"), 0) > 0


def upsert_rows(rows: List[Dict[str, Any]], tenant_id: Optional[str] = None) -> int:
    tid = _current_tenant_id(tenant_id)
    if not tid or not rows:
        return 0
    ensure_db(tid)
    payload: List[tuple[Any, ...]] = []
    for row in rows:
        payload.append(
            (
                clean_text(row.get("review_id")),
                clean_text(row.get("source")) or "active",
                _safe_int(row.get("is_answered"), 0),
                clean_text(row.get("answer_text")),
                clean_text(row.get("answer_create_date")),
                _safe_int(row.get("stars"), 0),
                clean_text(row.get("created_date")),
                clean_text(row.get("user_name")),
                clean_text(row.get("subject_name")),
                clean_text(row.get("product_name")),
                clean_text(row.get("supplier_article")),
                clean_text(row.get("brand_name")),
                _safe_int(row.get("nm_id"), 0),
                clean_text(row.get("text")),
                clean_text(row.get("pros")),
                clean_text(row.get("cons")),
                clean_text(row.get("review_text")),
                _safe_int(row.get("has_media"), 0),
                _safe_int(row.get("is_empty_rating_only"), 0),
                clean_text(row.get("state")),
                clean_text(row.get("order_status")),
                clean_text(row.get("last_seen_at") or utc_now_iso()),
                clean_text(row.get("imported_at") or utc_now_iso()),
                clean_text(row.get("fingerprint")),
            )
        )
    with history_conn(tid) as conn:
        conn.executemany(
            """
            INSERT INTO reviews_history (
                review_id, source, is_answered, answer_text, answer_create_date, stars, created_date,
                user_name, subject_name, product_name, supplier_article, brand_name, nm_id,
                text, pros, cons, review_text, has_media, is_empty_rating_only, state, order_status,
                last_seen_at, imported_at, fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(review_id) DO UPDATE SET
                source=excluded.source,
                is_answered=excluded.is_answered,
                answer_text=excluded.answer_text,
                answer_create_date=excluded.answer_create_date,
                stars=excluded.stars,
                created_date=excluded.created_date,
                user_name=excluded.user_name,
                subject_name=excluded.subject_name,
                product_name=excluded.product_name,
                supplier_article=excluded.supplier_article,
                brand_name=excluded.brand_name,
                nm_id=excluded.nm_id,
                text=excluded.text,
                pros=excluded.pros,
                cons=excluded.cons,
                review_text=excluded.review_text,
                has_media=excluded.has_media,
                is_empty_rating_only=excluded.is_empty_rating_only,
                state=excluded.state,
                order_status=excluded.order_status,
                last_seen_at=excluded.last_seen_at,
                imported_at=excluded.imported_at,
                fingerprint=excluded.fingerprint
            """,
            payload,
        )
        conn.commit()
    counts = get_counts(tid)
    update_meta(
        tid,
        db_total_rows=counts["total"],
        db_active_rows=counts["active"],
        db_archive_rows=counts["archive"],
        db_needs_reply_rows=counts["needs_reply"],
        db_empty_unanswered=counts["empty_unanswered"],
        last_active_snapshot_at=utc_now_iso(),
    )
    return len(payload)


def upsert_active_snapshot(snapshot: Dict[str, Any], tenant_id: Optional[str] = None) -> int:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return 0
    feedbacks = (snapshot or {}).get("feedbacks") or []
    if not isinstance(feedbacks, list) or not feedbacks:
        return 0
    rows = [history_sync_worker.normalize_history_record(review, "active") for review in feedbacks]
    inserted = upsert_rows(rows, tid)
    meta = load_meta(tid)
    meta["last_active_snapshot_at"] = clean_text((snapshot or {}).get("fetched_at") or utc_now_iso())
    meta["expected_active_unanswered"] = _safe_int((snapshot or {}).get("count_unanswered"), _safe_int(meta.get("expected_active_unanswered"), 0))
    save_meta(meta, tid)
    return inserted


def get_row_by_id(review_id: str, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    tid = _current_tenant_id(tenant_id)
    review_id = clean_text(review_id)
    if not tid or not review_id or not db_has_data(tid):
        return None
    with history_conn(tid) as conn:
        row = conn.execute(
            """
            SELECT review_id, source, is_answered, answer_text, answer_create_date, stars, created_date,
                   user_name, subject_name, product_name, supplier_article, brand_name, nm_id,
                   text, pros, cons, review_text, is_empty_rating_only
            FROM reviews_history
            WHERE review_id=?
            """,
            (review_id,),
        ).fetchone()
    return dict(row) if row is not None else None


def list_rows(
    tenant_id: Optional[str] = None,
    sort_by: str = "newest",
    stars_filter: str = "all",
    search_query: str = "",
    source_filter: str = "all",
    content_filter: str = "all",
    answer_state: str = "needs_reply",
) -> List[Dict[str, Any]]:
    tid = _current_tenant_id(tenant_id)
    if not tid or not db_has_data(tid):
        return []
    ensure_db(tid)
    where = ["1=1"]
    params: List[Any] = []
    if source_filter == "active":
        where.append("source='active'")
    elif source_filter == "archive":
        where.append("source='archive'")
    if content_filter == "empty_only":
        where.append("is_empty_rating_only=1")
    elif content_filter == "with_content":
        where.append("is_empty_rating_only=0")
    if answer_state == "needs_reply":
        where.append("COALESCE(answer_text,'')='' ")
    elif answer_state == "answered_only":
        where.append("COALESCE(answer_text,'')<>'' ")
    if stars_filter not in {"", "all"}:
        where.append("stars=?")
        params.append(_safe_int(stars_filter))
    if search_query:
        q = f"%{clean_text(search_query).lower()}%"
        where.append("(lower(product_name) LIKE ? OR lower(supplier_article) LIKE ? OR CAST(nm_id AS TEXT) LIKE ? OR lower(user_name) LIKE ? OR lower(review_text) LIKE ?)")
        params.extend([q, q, q, q, q])
    order_sql = "created_date DESC"
    if sort_by == "oldest":
        order_sql = "created_date ASC"
    elif sort_by == "stars_low":
        order_sql = "stars ASC, created_date DESC"
    elif sort_by == "stars_high":
        order_sql = "stars DESC, created_date DESC"
    with history_conn(tid) as conn:
        rows = conn.execute(
            f"""
            SELECT review_id, source, is_answered, answer_text, stars, created_date, user_name, subject_name,
                   product_name, supplier_article, brand_name, nm_id, text, pros, cons, review_text,
                   is_empty_rating_only
            FROM reviews_history
            WHERE {' AND '.join(where)}
            ORDER BY {order_sql}
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def mark_replied(review_id: str, reply_text: str, tenant_id: Optional[str] = None) -> None:
    tid = _current_tenant_id(tenant_id)
    review_id = clean_text(review_id)
    if not tid or not review_id or not db_has_data(tid):
        return
    with history_conn(tid) as conn:
        conn.execute(
            "UPDATE reviews_history SET is_answered=1, answer_text=?, answer_create_date=?, last_seen_at=? WHERE review_id=?",
            (clean_text(reply_text), utc_now_iso(), utc_now_iso(), review_id),
        )
        conn.commit()
    counts = get_counts(tid)
    update_meta(
        tid,
        db_total_rows=counts["total"],
        db_active_rows=counts["active"],
        db_archive_rows=counts["archive"],
        db_needs_reply_rows=counts["needs_reply"],
        db_empty_unanswered=counts["empty_unanswered"],
    )


def start_sync(tenant_id: Optional[str] = None) -> bool:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return False
    if not tenant_manager.get_tenant(tid):
        raise ValueError("Кабинет не найден")
    meta = effective_meta(tid)
    if clean_text(meta.get("status")) in {"running", "starting", "stop_requested"} and _pid_alive(_history_worker_pid(meta)):
        return False
    paths = get_paths(tid)
    try:
        paths["stop"].unlink(missing_ok=True)
    except Exception:
        pass
    worker = _history_worker_script_path()
    if not worker.exists():
        raise FileNotFoundError(f"Не найден worker исторической синхронизации: {worker}")
    paths["log"].parent.mkdir(parents=True, exist_ok=True)
    out = paths["log"].open("a", encoding="utf-8")
    cmd = [sys.executable, str(worker), "--tenant", tid]
    kwargs: Dict[str, Any] = {"stdout": out, "stderr": subprocess.STDOUT, "cwd": str(worker.parent)}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(cmd, **kwargs)
    try:
        out.close()
    except Exception:
        pass
    update_meta(
        tid,
        tenant_id=tid,
        status="starting",
        phase="spawned",
        started_at=utc_now_iso(),
        finished_at="",
        last_error="",
        stop_requested=False,
        stopped_by_user=False,
        worker_pid=process.pid,
        last_heartbeat_at=utc_now_iso(),
        last_status_message="Запущен внешний worker исторической синхронизации.",
    )
    return True


def stop_sync(tenant_id: Optional[str] = None) -> bool:
    tid = _current_tenant_id(tenant_id)
    if not tid:
        return False
    meta = effective_meta(tid)
    paths = get_paths(tid)
    paths["stop"].parent.mkdir(parents=True, exist_ok=True)
    safe_files.write_text(paths["stop"], "stop\n", encoding="utf-8")
    pid = _history_worker_pid(meta)
    if pid and os.name != "nt" and _pid_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    update_meta(
        tid,
        tenant_id=tid,
        status="stop_requested",
        stop_requested=True,
        stopped_by_user=True,
        last_status_message="Запрошена остановка исторической синхронизации.",
    )
    return True


def job_payload(tenant_id: Optional[str] = None) -> Dict[str, Any]:
    tid = _current_tenant_id(tenant_id)
    meta = effective_meta(tid)
    counts = get_counts(tid)
    payload = {
        "tenant_id": tid,
        "meta": meta,
        "counts": counts,
        "can_start": clean_text(meta.get("status")) not in {"running", "starting", "stop_requested"} or not meta.get("pid_alive"),
        "db_exists": get_paths(tid)["db"].exists() if tid else False,
        "log_exists": get_paths(tid)["log"].exists() if tid else False,
    }
    return payload
