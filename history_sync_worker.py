import argparse
import json
import os
import re
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

import common
import tenant_manager
import safe_files


def clean_text(value: Any) -> str:
    try:
        return common.clean_text(value)
    except Exception:
        return re.sub(r"\s+", " ", str(value or "")).strip()


def utc_now_iso() -> str:
    try:
        return common.utc_now_iso()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return tenant_manager.read_json(path, default)
    except Exception:
        return safe_files.read_json(Path(path), default)


def write_json(path: Path, data: Any) -> None:
    try:
        tenant_manager.write_json(path, data)
        return
    except Exception:
        safe_files.write_json(Path(path), data, ensure_ascii=False, indent=2)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_dt(value: Any) -> datetime:
    s = clean_text(value)
    if not s:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def get_paths(tenant_id: str) -> Dict[str, Path]:
    raw = tenant_manager.get_tenant_paths(tenant_id)
    data_dir = Path(raw.get("data_dir") or Path(raw["tenant_root"]) / "data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return {
        "db": Path(raw.get("historical_db_file") or (data_dir / "reviews_history.sqlite3")),
        "meta": Path(raw.get("historical_sync_meta_file") or (data_dir / "historical_sync_meta.json")),
        "stop": Path(raw.get("historical_sync_stop_file") or (data_dir / "historical_sync_stop.flag")),
    }


def load_meta(tenant_id: str) -> Dict[str, Any]:
    return read_json(get_paths(tenant_id)["meta"], {}) if tenant_id else {}


def save_meta(tenant_id: str, data: Dict[str, Any]) -> None:
    write_json(get_paths(tenant_id)["meta"], data)


def update_meta(tenant_id: str, **updates: Any) -> Dict[str, Any]:
    meta = load_meta(tenant_id)
    meta.update(updates)
    if "last_heartbeat_at" not in updates:
        meta["last_heartbeat_at"] = utc_now_iso()
    save_meta(tenant_id, meta)
    return meta




def append_worker_log(tenant_id: str, message: str) -> None:
    try:
        log_path = get_paths(tenant_id)['meta'].with_name('historical_sync_worker.log')
        log_path.parent.mkdir(parents=True, exist_ok=True)
        safe_files.append_text(log_path, f"[{utc_now_iso()}] {message}\n")
    except Exception:
        pass


def reset_history_db(tenant_id: str, retries: int = 12, sleep_seconds: float = 1.0) -> None:
    """Safely clears tenant history DB without unlinking the sqlite file.
    This avoids WinError 32 when another process briefly holds the file.
    """
    ensure_db(tenant_id)
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            with history_conn(tenant_id) as conn:
                conn.execute('PRAGMA busy_timeout=10000')
                conn.execute('DELETE FROM reviews_history')
                conn.commit()
            append_worker_log(tenant_id, f'DB reset ok on attempt {attempt}')
            return
        except sqlite3.OperationalError as exc:
            last_exc = exc
            append_worker_log(tenant_id, f'DB reset locked on attempt {attempt}: {exc}')
            time.sleep(sleep_seconds)
        except Exception as exc:
            last_exc = exc
            append_worker_log(tenant_id, f'DB reset failed on attempt {attempt}: {exc}')
            time.sleep(sleep_seconds)
    raise RuntimeError(f'Не удалось очистить БД исторической синхронизации: {last_exc}')

def stop_requested(tenant_id: str) -> bool:
    return get_paths(tenant_id)["stop"].exists()


def clear_stop_flag(tenant_id: str) -> None:
    try:
        get_paths(tenant_id)["stop"].unlink(missing_ok=True)
    except Exception:
        pass


def history_conn(tenant_id: str) -> sqlite3.Connection:
    db_path = get_paths(tenant_id)["db"]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_db(tenant_id: str) -> None:
    with history_conn(tenant_id) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews_history (
                review_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                is_answered INTEGER NOT NULL DEFAULT 0,
                answer_text TEXT DEFAULT '',
                answer_create_date TEXT DEFAULT '',
                stars INTEGER NOT NULL DEFAULT 0,
                created_date TEXT DEFAULT '',
                user_name TEXT DEFAULT '',
                subject_name TEXT DEFAULT '',
                product_name TEXT DEFAULT '',
                supplier_article TEXT DEFAULT '',
                brand_name TEXT DEFAULT '',
                nm_id INTEGER DEFAULT 0,
                text TEXT DEFAULT '',
                pros TEXT DEFAULT '',
                cons TEXT DEFAULT '',
                review_text TEXT DEFAULT '',
                has_media INTEGER NOT NULL DEFAULT 0,
                is_empty_rating_only INTEGER NOT NULL DEFAULT 0,
                state TEXT DEFAULT '',
                order_status TEXT DEFAULT '',
                last_seen_at TEXT DEFAULT '',
                imported_at TEXT DEFAULT '',
                fingerprint TEXT DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_history_created_date ON reviews_history(created_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_history_answered ON reviews_history(is_answered)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_history_source ON reviews_history(source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_history_empty ON reviews_history(is_empty_rating_only)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_history_nm_id ON reviews_history(nm_id)")
        conn.commit()


def get_counts(tenant_id: str) -> Dict[str, int]:
    ensure_db(tenant_id)
    with history_conn(tenant_id) as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM reviews_history").fetchone()[0])
        active = int(conn.execute("SELECT COUNT(*) FROM reviews_history WHERE source='active'").fetchone()[0])
        archive = int(conn.execute("SELECT COUNT(*) FROM reviews_history WHERE source='archive'").fetchone()[0])
        needs_reply = int(conn.execute("SELECT COUNT(*) FROM reviews_history WHERE COALESCE(answer_text,'')=''").fetchone()[0])
        empty_unanswered = int(conn.execute("SELECT COUNT(*) FROM reviews_history WHERE COALESCE(answer_text,'')='' AND is_empty_rating_only=1").fetchone()[0])
    return {"total": total, "active": active, "archive": archive, "needs_reply": needs_reply, "empty_unanswered": empty_unanswered}


def normalize_history_record(raw_review: Dict[str, Any], source: str) -> Dict[str, Any]:
    try:
        review = common.normalize_review(raw_review)
    except Exception:
        review = raw_review
    answer = raw_review.get("answer")
    answer_text = ""
    answer_created = ""
    if isinstance(answer, dict):
        answer_text = clean_text(answer.get("text"))
        answer_created = clean_text(answer.get("createDate"))
    else:
        answer_text = clean_text(answer)
    has_media = bool(raw_review.get("photoLinks") or raw_review.get("video"))
    text = clean_text(review.get("text"))
    pros = clean_text(review.get("pros"))
    cons = clean_text(review.get("cons"))
    try:
        review_text = common.build_review_text(review)
    except Exception:
        review_text = " ".join([x for x in [text, pros, cons] if x]).strip() or "Покупатель поставил оценку без текста."
    try:
        fingerprint = common.review_signature(review)
    except Exception:
        fingerprint = f"{clean_text(review.get('id'))}:{_safe_int(review.get('productValuation'))}:{review_text[:120]}"
    is_empty = int(not text and not pros and not cons and not has_media)
    pd = review.get("productDetails") or {}
    return {
        "review_id": clean_text(review.get("id")),
        "source": source,
        "is_answered": int(bool(answer_text)),
        "answer_text": answer_text,
        "answer_create_date": answer_created,
        "stars": _safe_int(review.get("productValuation")),
        "created_date": clean_text(review.get("createdDate")),
        "user_name": clean_text(review.get("userName")),
        "subject_name": clean_text(review.get("subjectName")),
        "product_name": clean_text(pd.get("productName")),
        "supplier_article": clean_text(pd.get("supplierArticle")),
        "brand_name": clean_text(pd.get("brandName")),
        "nm_id": _safe_int(pd.get("nmId")),
        "text": text,
        "pros": pros,
        "cons": cons,
        "review_text": review_text,
        "has_media": int(has_media),
        "is_empty_rating_only": is_empty,
        "state": clean_text(raw_review.get("state")),
        "order_status": clean_text(raw_review.get("orderStatus")),
        "last_seen_at": utc_now_iso(),
        "imported_at": utc_now_iso(),
        "fingerprint": fingerprint,
    }


def upsert_rows(rows: List[Dict[str, Any]], tenant_id: str) -> None:
    if not rows:
        return
    ensure_db(tenant_id)
    with history_conn(tenant_id) as conn:
        conn.executemany(
            """
            INSERT INTO reviews_history (
                review_id, source, is_answered, answer_text, answer_create_date,
                stars, created_date, user_name, subject_name, product_name,
                supplier_article, brand_name, nm_id, text, pros, cons, review_text,
                has_media, is_empty_rating_only, state, order_status, last_seen_at, imported_at, fingerprint
            ) VALUES (
                :review_id, :source, :is_answered, :answer_text, :answer_create_date,
                :stars, :created_date, :user_name, :subject_name, :product_name,
                :supplier_article, :brand_name, :nm_id, :text, :pros, :cons, :review_text,
                :has_media, :is_empty_rating_only, :state, :order_status, :last_seen_at, :imported_at, :fingerprint
            )
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
            rows,
        )
        conn.commit()


def build_session(tenant_id: str) -> requests.Session:
    tenant = tenant_manager.get_tenant(tenant_id) or {}
    api_key = clean_text(tenant.get("wb_api_key"))
    if not api_key:
        raise RuntimeError("У кабинета не задан WB API key")
    s = requests.Session()
    s.headers.update({"Authorization": api_key})
    return s


def heartbeat(tenant_id: str, phase: str, message: str = "", **extra: Any) -> None:
    update_meta(tenant_id, phase=phase, last_heartbeat_at=utc_now_iso(), last_status_message=message, **extra)


def request_json(session: requests.Session, tenant_id: str, url: str, params: Dict[str, Any], phase: str, label: str, retries: int = 4) -> Dict[str, Any]:
    backoff = 2.0
    for attempt in range(1, retries + 1):
        if stop_requested(tenant_id):
            raise InterruptedError("Историческая синхронизация остановлена пользователем.")
        heartbeat(tenant_id, phase, f"{label}: запрос (attempt {attempt}, skip={params.get('skip', 0)})", retry_attempt=attempt)
        try:
            resp = session.get(url, params=params, timeout=(10, 60))
            if resp.status_code == 429:
                retry_after = resp.headers.get("X-Ratelimit-Retry") or resp.headers.get("Retry-After") or "2"
                try:
                    wait_seconds = max(1.0, float(retry_after))
                except Exception:
                    wait_seconds = backoff
                heartbeat(tenant_id, phase, f"{label}: 429, ждём {wait_seconds:.1f}s", retry_attempt=attempt)
                time.sleep(wait_seconds)
                backoff = min(backoff * 2, 20.0)
                continue
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("error"):
                raise RuntimeError(payload.get("errorText") or "WB API error")
            return payload
        except (requests.Timeout, requests.ConnectionError, RuntimeError, ValueError) as exc:
            if attempt >= retries:
                raise
            heartbeat(tenant_id, phase, f"{label}: retry {attempt}/{retries} после ошибки: {exc}", retry_attempt=attempt)
            time.sleep(backoff)
            backoff = min(backoff * 2, 20.0)
    raise RuntimeError(f"{label}: не удалось получить ответ WB")


def verify_and_finalize(session: requests.Session, tenant_id: str) -> None:
    counts = get_counts(tenant_id)
    meta = load_meta(tenant_id)
    expected_active = _safe_int(meta.get("expected_active_unanswered"))
    expected_archive = _safe_int(meta.get("expected_archive_total"))
    try:
        payload = request_json(
            session,
            tenant_id,
            common.WB_LIST_URL,
            {"isAnswered": False, "take": 1, "skip": 0, "order": "dateDesc"},
            "verify",
            "Проверка счётчиков",
            retries=2,
        )
        data = payload.get("data") or {}
        expected_active = _safe_int(data.get("countUnanswered"))
        expected_archive = _safe_int(data.get("countArchive"))
    except Exception as exc:
        meta["verify_error"] = str(exc)
    verified_active = counts["active"] == expected_active if expected_active >= 0 else False
    verified_archive = counts["archive"] == expected_archive if expected_archive >= 0 else False
    done = counts["active"] + counts["archive"]
    total = max(0, expected_active + expected_archive)
    meta.update({
        "expected_active_unanswered": expected_active,
        "expected_archive_total": expected_archive,
        "db_active_rows": counts["active"],
        "db_archive_rows": counts["archive"],
        "db_total_rows": counts["total"],
        "db_needs_reply": counts["needs_reply"],
        "db_empty_unanswered": counts["empty_unanswered"],
        "verified_active": verified_active,
        "verified_archive": verified_archive,
        "verified_all": bool(verified_active and verified_archive),
        "last_verified_at": utc_now_iso(),
        "progress_percent": round((done / total) * 100, 2) if total > 0 else 100,
        "status": "completed",
        "phase": "done",
        "finished_at": utc_now_iso(),
        "last_status_message": "Историческая синхронизация завершена",
        "retry_attempt": 0,
    })
    save_meta(tenant_id, meta)


def run_sync(tenant_id: str) -> None:
    paths = get_paths(tenant_id)
    clear_stop_flag(tenant_id)
    db_path = paths["db"]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    append_worker_log(tenant_id, f"Worker PID {os.getpid()} starting sync")
    reset_history_db(tenant_id)
    update_meta(
        tenant_id,
        status="running",
        phase="active",
        started_at=utc_now_iso(),
        finished_at="",
        last_error="",
        last_progress_at="",
        last_heartbeat_at=utc_now_iso(),
        last_status_message="Запуск синхронизации",
        active_imported=0,
        archive_imported=0,
        db_active_rows=0,
        db_archive_rows=0,
        db_total_rows=0,
        retry_attempt=0,
        last_batch_count=0,
        stop_requested=False,
        stopped_by_user=False,
        worker_pid=os.getpid(),
    )
    session = build_session(tenant_id)
    try:
        active_skip = 0
        expected_active = 0
        expected_archive = 0
        ACTIVE_BATCH = 250
        ARCHIVE_BATCH = 250

        append_worker_log(tenant_id, 'Starting active phase')
        while True:
            if stop_requested(tenant_id):
                raise InterruptedError("Историческая синхронизация остановлена пользователем.")
            payload = request_json(
                session,
                tenant_id,
                common.WB_LIST_URL,
                {"isAnswered": False, "take": ACTIVE_BATCH, "skip": active_skip, "order": "dateAsc"},
                "active-request",
                "Активные отзывы",
            )
            data = payload.get("data") or {}
            if active_skip == 0:
                expected_active = _safe_int(data.get("countUnanswered"))
                expected_archive = _safe_int(data.get("countArchive"))
                update_meta(tenant_id, expected_active_unanswered=expected_active, expected_archive_total=expected_archive)
            feedbacks = data.get("feedbacks") or []
            if not feedbacks:
                break
            rows = [normalize_history_record(fb, "active") for fb in feedbacks]
            upsert_rows(rows, tenant_id)
            active_skip += len(feedbacks)
            counts = get_counts(tenant_id)
            total_expected = max(1, expected_active + expected_archive)
            done = active_skip + _safe_int(load_meta(tenant_id).get("archive_imported"))
            update_meta(
                tenant_id,
                phase="active",
                active_imported=active_skip,
                db_active_rows=counts["active"],
                db_archive_rows=counts["archive"],
                db_total_rows=counts["total"],
                db_needs_reply=counts["needs_reply"],
                db_empty_unanswered=counts["empty_unanswered"],
                progress_percent=round((done / total_expected) * 100, 2) if total_expected > 0 else 0,
                last_progress_at=utc_now_iso(),
                last_batch_count=len(feedbacks),
                current_skip=active_skip,
                retry_attempt=0,
                last_status_message=f"Импортировано активных: {active_skip}/{expected_active}",
            )
            if len(feedbacks) < ACTIVE_BATCH:
                break
            time.sleep(0.4)

        archive_skip = 0
        heartbeat(tenant_id, "archive", f"Переход к архиву: 0/{expected_archive}")
        append_worker_log(tenant_id, 'Starting active phase')
        while True:
            if stop_requested(tenant_id):
                raise InterruptedError("Историческая синхронизация остановлена пользователем.")
            payload = request_json(
                session,
                tenant_id,
                f"{common.WB_LIST_URL}/archive",
                {"take": ARCHIVE_BATCH, "skip": archive_skip, "order": "dateAsc"},
                "archive-request",
                "Архивные отзывы",
            )
            data = payload.get("data") or {}
            feedbacks = data.get("feedbacks") or []
            if not feedbacks:
                break
            rows = [normalize_history_record(fb, "archive") for fb in feedbacks]
            upsert_rows(rows, tenant_id)
            archive_skip += len(feedbacks)
            counts = get_counts(tenant_id)
            total_expected = max(1, expected_active + expected_archive)
            done = _safe_int(load_meta(tenant_id).get("active_imported")) + archive_skip
            update_meta(
                tenant_id,
                phase="archive",
                archive_imported=archive_skip,
                db_active_rows=counts["active"],
                db_archive_rows=counts["archive"],
                db_total_rows=counts["total"],
                db_needs_reply=counts["needs_reply"],
                db_empty_unanswered=counts["empty_unanswered"],
                progress_percent=round((done / total_expected) * 100, 2) if total_expected > 0 else 0,
                last_progress_at=utc_now_iso(),
                last_batch_count=len(feedbacks),
                current_skip=archive_skip,
                retry_attempt=0,
                last_status_message=f"Импортировано архивных: {archive_skip}/{expected_archive}",
            )
            if len(feedbacks) < ARCHIVE_BATCH:
                break
            time.sleep(0.4)

        append_worker_log(tenant_id, 'Verifying and finalizing sync')
        verify_and_finalize(session, tenant_id)
        append_worker_log(tenant_id, 'Sync completed successfully')
    except InterruptedError as exc:
        append_worker_log(tenant_id, f'Sync interrupted: {exc}')
        update_meta(tenant_id, status="stopped", finished_at=utc_now_iso(), last_error=str(exc), stop_requested=True, stopped_by_user=True, last_status_message=str(exc))
    except Exception as exc:
        append_worker_log(tenant_id, f'Sync error: {exc} | {traceback.format_exc()}')
        update_meta(tenant_id, status="error", finished_at=utc_now_iso(), last_error=f"{exc}\n{traceback.format_exc()}", last_status_message=str(exc))
    finally:
        try:
            session.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    args = parser.parse_args()
    tenant_id = clean_text(args.tenant)
    if not tenant_manager.get_tenant(tenant_id):
        sys.stderr.write(f"Unknown tenant: {tenant_id}\n")
        return 2
    run_sync(tenant_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
