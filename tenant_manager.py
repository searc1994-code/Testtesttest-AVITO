import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

import safe_files

import config
import common

BASE_DIR = Path(__file__).resolve().parent
PRIVATE_ROOT = Path(getattr(config, "PRIVATE_ROOT", getattr(config, "WB_PRIVATE_DIR", str(Path.home() / "wb-ai-private")))).expanduser()

TENANTS_ROOT = PRIVATE_ROOT / "tenants"
REGISTRY_FILE = PRIVATE_ROOT / "tenants.json"
LEGACY_BACKUP_DIR = PRIVATE_ROOT / "legacy-backups"

TENANTS_ROOT.mkdir(parents=True, exist_ok=True)
LEGACY_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_EMOJIS = ["🏢", "🌿", "🌸", "📦", "💼", "🪴", "🎁", "🛒"]
HEALTH_TTL_SECONDS = 300
WB_LIST_URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"


TENANT_ID_PATTERN = re.compile(r"^[a-z0-9а-я_-]{1,80}$", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    return safe_files.read_json(Path(path), default)


def write_json(path: Path, data: Any) -> None:
    safe_files.write_json(Path(path), data, ensure_ascii=False, indent=2)


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def slugify(value: str) -> str:
    value = clean_text(value).lower()
    value = value.replace("ё", "e")
    value = re.sub(r"[^a-z0-9а-я_-]+", "-", value, flags=re.I)
    value = re.sub(r"-{2,}", "-", value).strip("-_")
    return value or "tenant"


def normalize_tenant_id(value: Any) -> str:
    tenant_id = clean_text(value)
    if not tenant_id:
        raise ValueError("Не указан tenant_id.")
    if not TENANT_ID_PATTERN.fullmatch(tenant_id):
        raise ValueError("Некорректный tenant_id.")
    return tenant_id


def _ensure_within_tenants_root(path: Path) -> Path:
    base = TENANTS_ROOT.resolve()
    resolved = Path(path).resolve()
    try:
        resolved.relative_to(base)
    except Exception as exc:
        raise ValueError("Путь кабинета выходит за пределы каталога tenants.") from exc
    return resolved


def _copy_default_if_missing(src: Path, dst: Path, fallback_text: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return
    if src.exists():
        shutil.copy2(src, dst)
    else:
        safe_files.write_text(dst, fallback_text, encoding="utf-8")


# --- registry ---
def load_tenants() -> List[Dict[str, Any]]:
    data = read_json(REGISTRY_FILE, [])
    tenants = data if isinstance(data, list) else []
    tenants = [t for t in tenants if isinstance(t, dict)]
    tenants.sort(key=lambda t: (0 if t.get("enabled", True) else 1, clean_text(t.get("name"))))
    return tenants


def save_tenants(tenants: List[Dict[str, Any]]) -> None:
    write_json(REGISTRY_FILE, tenants)


def get_tenant(tenant_id: str) -> Optional[Dict[str, Any]]:
    tenant_id = clean_text(tenant_id)
    for tenant in load_tenants():
        if clean_text(tenant.get("id")) == tenant_id:
            return tenant
    return None


def next_default_emoji(existing_count: int) -> str:
    return DEFAULT_EMOJIS[existing_count % len(DEFAULT_EMOJIS)]


def get_tenant_root(tenant_id: str) -> Path:
    safe_id = normalize_tenant_id(tenant_id)
    return _ensure_within_tenants_root(TENANTS_ROOT / safe_id)


def get_tenant_paths(tenant_id: str) -> Dict[str, Path]:
    root = get_tenant_root(tenant_id)
    auth_dir = root / "auth"
    logs_dir = root / "logs"
    complaints_dir = root / "complaints"
    screenshots_dir = complaints_dir / "screenshots"
    data_dir = root / "data"
    backups_dir = root / "backups"
    return {
        "tenant_root": root,
        "auth_dir": auth_dir,
        "logs_dir": logs_dir,
        "complaints_dir": complaints_dir,
        "screenshots_dir": screenshots_dir,
        "data_dir": data_dir,
        "backups_dir": backups_dir,
        "archive_file": data_dir / "reviews_archive.json",
        "drafts_file": data_dir / "draft_replies.json",
        "reply_queue_file": data_dir / "reply_queue.json",
        "reply_snapshot_file": data_dir / "reply_snapshot.json",
        "rules_file": data_dir / "business_rules.json",
        "system_prompt_file": data_dir / "system_prompt.txt",
        "complaint_prompt_file": data_dir / "complaint_prompt.txt",
        "ui_profile_file": data_dir / "wb_ui_profile.json",
        "auth_state_file": auth_dir / "wb_state.json",
        "auth_meta_file": auth_dir / "wb_state_meta.json",
        "complaint_drafts_file": complaints_dir / "complaint_drafts.json",
        "complaint_queue_file": complaints_dir / "complaint_queue.json",
        "complaint_results_file": complaints_dir / "complaint_results.jsonl",
        "low_rating_cache_file": complaints_dir / "low_rating_reviews_snapshot.json",
        "historical_db_file": data_dir / "reviews_history.sqlite3",
        "historical_sync_meta_file": data_dir / "historical_sync_meta.json",
        "historical_sync_stop_file": data_dir / "historical_sync_stop.flag",
        "historical_sync_log_file": data_dir / "historical_sync_worker.log",
        "question_snapshot_file": data_dir / "question_snapshot.json",
        "question_drafts_file": data_dir / "question_drafts.json",
        "question_queue_file": data_dir / "question_queue.json",
        "question_archive_file": data_dir / "question_archive.json",
        "question_clusters_file": data_dir / "question_clusters.json",
        "question_sync_meta_file": data_dir / "question_sync_meta.json",
        "question_ignored_file": data_dir / "question_ignored.json",
        "jobs_file": data_dir / "background_jobs.json",
        "health_file": data_dir / "health.json",
    }


def ensure_tenant_dirs(tenant_id: str) -> Dict[str, Path]:
    paths = get_tenant_paths(tenant_id)
    for key, path in paths.items():
        if key.endswith("_dir"):
            path.mkdir(parents=True, exist_ok=True)
    defaults = {
        "archive_file": [],
        "drafts_file": {},
        "reply_queue_file": [],
        "reply_snapshot_file": {},
        "complaint_drafts_file": {},
        "complaint_queue_file": [],
        "low_rating_cache_file": {},
        "historical_sync_meta_file": {},
        "question_snapshot_file": {},
        "question_drafts_file": {},
        "question_queue_file": [],
        "question_archive_file": [],
        "question_clusters_file": {},
        "question_sync_meta_file": {},
        "question_ignored_file": [],
        "jobs_file": [],
        "health_file": {},
    }
    for key, default in defaults.items():
        fp = paths[key]
        if not fp.exists():
            write_json(fp, default)
    if not paths["complaint_results_file"].exists():
        paths["complaint_results_file"].parent.mkdir(parents=True, exist_ok=True)
        paths["complaint_results_file"].touch()
    _copy_default_if_missing(BASE_DIR / "system_prompt.txt", paths["system_prompt_file"], "Пиши вежливо.")
    _copy_default_if_missing(BASE_DIR / "complaint_prompt.txt", paths["complaint_prompt_file"], "")
    _copy_default_if_missing(
        BASE_DIR / "business_rules.json",
        paths["rules_file"],
        json.dumps({"default_instructions": [], "special_cases": [], "cross_sell_catalog": []}, ensure_ascii=False, indent=2),
    )
    _copy_default_if_missing(BASE_DIR / "wb_ui_profile.json", paths["ui_profile_file"], "{}")
    return paths


# --- health ---
def _auth_cookies_alive(auth_state_file: Path) -> tuple[bool, str]:
    if not auth_state_file.exists():
        return False, "Файл авторизации не найден"
    try:
        payload = json.loads(auth_state_file.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"Не удалось прочитать cookies: {exc}"
    cookies = payload.get("cookies") or []
    if not isinstance(cookies, list):
        return False, "Некорректный формат storage_state"
    now_ts = time.time()
    found = False
    for cookie in cookies:
        domain = clean_text(cookie.get("domain")).lower()
        if "wildberries" not in domain:
            continue
        found = True
        expires = cookie.get("expires")
        if expires in (-1, None, 0, "", 0.0):
            return True, "session"
        try:
            if float(expires) > now_ts:
                return True, "cookie_alive"
        except Exception:
            continue
    if found:
        return False, "cookies_expired"
    return False, "cookies_for_wb_not_found"


def _api_health_probe(wb_api_key: str) -> tuple[bool, str, dict]:
    headers = {"Authorization": clean_text(wb_api_key)}
    params = {"isAnswered": False, "take": 1, "skip": 0, "order": "dateDesc"}
    try:
        response = requests.get(WB_LIST_URL, headers=headers, params=params, timeout=12)
        if response.status_code == 200:
            payload = response.json()
            if payload.get("error"):
                return False, clean_text(payload.get("errorText") or "WB API вернул ошибку"), {}
            data = payload.get("data") or {}
            return True, "OK", {
                "count_unanswered": int(data.get("countUnanswered") or 0),
                "count_archive": int(data.get("countArchive") or 0),
            }
        detail = clean_text(response.text)[:300]
        return False, f"HTTP {response.status_code}: {detail}", {}
    except Exception as exc:
        return False, str(exc), {}


def load_tenant_health(tenant_id: str) -> dict:
    paths = ensure_tenant_dirs(tenant_id)
    data = read_json(paths["health_file"], {})
    return data if isinstance(data, dict) else {}


def save_tenant_health(tenant_id: str, data: dict) -> None:
    paths = ensure_tenant_dirs(tenant_id)
    write_json(paths["health_file"], data)


def refresh_tenant_health(tenant_id: str, force: bool = False) -> dict:
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Кабинет не найден.")
    paths = ensure_tenant_dirs(tenant_id)
    cached = load_tenant_health(tenant_id)
    checked_at = cached.get("checked_at_ts") or 0
    if cached and not force and (time.time() - float(checked_at or 0) <= HEALTH_TTL_SECONDS):
        return cached

    cookies_ok, cookies_note = _auth_cookies_alive(paths["auth_state_file"])
    api_ok, api_note, api_meta = _api_health_probe(clean_text(tenant.get("wb_api_key")))
    health = {
        "tenant_id": tenant_id,
        "checked_at": utc_now_iso(),
        "checked_at_ts": time.time(),
        "api_ok": bool(api_ok),
        "api_note": api_note,
        "cookies_ok": bool(cookies_ok),
        "cookies_note": cookies_note,
        "api_meta": api_meta,
    }
    save_tenant_health(tenant_id, health)
    return health


# --- CRUD ---
def create_tenant(name: str, phone: str, wb_api_key: str, tenant_slug: str = "", emoji: str = "") -> Dict[str, Any]:
    name = clean_text(name)
    phone = clean_text(phone)
    wb_api_key = clean_text(wb_api_key)
    tenant_slug = slugify(tenant_slug or name)
    if not name:
        raise ValueError("Укажите название юрлица.")
    if not wb_api_key:
        raise ValueError("Укажите WB API key.")
    tenants = load_tenants()
    existing_ids = {clean_text(t.get("id")) for t in tenants}
    base_slug = tenant_slug
    suffix = 2
    while tenant_slug in existing_ids:
        tenant_slug = f"{base_slug}-{suffix}"
        suffix += 1
    tenant = {
        "id": tenant_slug,
        "name": name,
        "phone": phone,
        "emoji": clean_text(emoji) or next_default_emoji(len(tenants)),
        "wb_api_key": wb_api_key,
        "enabled": True,
        "created_at": utc_now_iso(),
        "last_login_at": "",
        "notes": "",
    }
    tenants.append(tenant)
    save_tenants(tenants)
    ensure_tenant_dirs(tenant_slug)
    return tenant


def update_tenant(tenant_id: str, **updates: Any) -> Dict[str, Any]:
    tenants = load_tenants()
    found = None
    for tenant in tenants:
        if clean_text(tenant.get("id")) == clean_text(tenant_id):
            for key, value in updates.items():
                if key in {"name", "phone", "emoji", "wb_api_key", "enabled", "notes", "last_login_at"}:
                    tenant[key] = value
            found = tenant
            break
    if not found:
        raise ValueError("Кабинет не найден.")
    save_tenants(tenants)
    return found


def delete_tenant_runtime_data(tenant_id: str, keep_auth: bool = False) -> None:
    paths = ensure_tenant_dirs(tenant_id)
    reset_json = {
        paths["archive_file"]: [],
        paths["drafts_file"]: {},
        paths["reply_queue_file"]: [],
        paths["reply_snapshot_file"]: {},
        paths["complaint_drafts_file"]: {},
        paths["complaint_queue_file"]: [],
        paths["low_rating_cache_file"]: {},
        paths["historical_sync_meta_file"]: {},
        paths["question_snapshot_file"]: {},
        paths["question_drafts_file"]: {},
        paths["question_queue_file"]: [],
        paths["question_archive_file"]: [],
        paths["question_clusters_file"]: {},
        paths["question_sync_meta_file"]: {},
        paths["question_ignored_file"]: [],
        paths["health_file"]: {},
    }
    for fp, default in reset_json.items():
        write_json(fp, default)
    safe_files.truncate_text(paths["complaint_results_file"], "", encoding="utf-8")
    for cleanup_key in ["historical_db_file", "historical_sync_stop_file", "historical_sync_log_file"]:
        fp = paths[cleanup_key]
        if fp.exists():
            try:
                fp.unlink()
            except Exception:
                pass
    for key in ["screenshots_dir", "logs_dir"]:
        d = paths[key]
        if d.exists():
            for child in d.iterdir():
                try:
                    if child.is_file():
                        child.unlink()
                    else:
                        shutil.rmtree(child)
                except Exception:
                    pass
    if not keep_auth:
        for key in ["auth_state_file", "auth_meta_file"]:
            fp = paths[key]
            if fp.exists():
                try:
                    fp.unlink()
                except Exception:
                    pass


def backup_tenant(tenant_id: str) -> Path:
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Кабинет не найден.")
    paths = ensure_tenant_dirs(tenant_id)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    zip_path = paths["backups_dir"] / f"{tenant_id}-backup-{stamp}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(paths["tenant_root"]):
            for file in files:
                fp = Path(root) / file
                zf.write(fp, fp.relative_to(paths["tenant_root"]).as_posix())
    return zip_path


# --- summaries ---
def summarize_tenant(tenant: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = clean_text(tenant.get("id"))
    paths = ensure_tenant_dirs(tenant_id)
    archive = read_json(paths["archive_file"], [])
    reply_queue = read_json(paths["reply_queue_file"], [])
    complaint_queue = read_json(paths["complaint_queue_file"], [])
    drafts = read_json(paths["drafts_file"], {})
    complaint_drafts = read_json(paths["complaint_drafts_file"], {})
    reply_snapshot = read_json(paths["reply_snapshot_file"], {})
    low_snapshot = read_json(paths["low_rating_cache_file"], {})
    question_snapshot = read_json(paths["question_snapshot_file"], {})
    question_queue = read_json(paths["question_queue_file"], [])
    question_drafts = read_json(paths["question_drafts_file"], {})
    question_archive = read_json(paths["question_archive_file"], [])
    question_ignored = read_json(paths["question_ignored_file"], [])
    history_meta = read_json(paths["historical_sync_meta_file"], {})
    auth_exists = paths["auth_state_file"].exists()
    auth_meta = read_json(paths["auth_meta_file"], {}) if auth_exists or paths["auth_meta_file"].exists() else {}
    complaint_results: Dict[str, int] = {}
    if paths["complaint_results_file"].exists():
        for line in paths["complaint_results_file"].read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                status = clean_text(json.loads(line).get("status")) or "unknown"
                complaint_results[status] = complaint_results.get(status, 0) + 1
            except Exception:
                continue
    health = load_tenant_health(tenant_id)
    return {
        **tenant,
        "auth_exists": auth_exists,
        "last_login_at": clean_text(auth_meta.get("saved_at")) or clean_text(tenant.get("last_login_at")),
        "reply_queue_open": len([x for x in reply_queue if clean_text(x.get("status")) not in {"sent", "success", "submitted"}]),
        "complaint_queue_open": len([x for x in complaint_queue if clean_text(x.get("status")) in {"queued", "processing", "failed"}]),
        "question_queue_open": len([x for x in question_queue if clean_text(x.get("status")) in {"queued", "processing", "failed"}]),
        "reply_drafts": len(drafts if isinstance(drafts, dict) else {}),
        "complaint_drafts": len(complaint_drafts if isinstance(complaint_drafts, dict) else {}),
        "question_drafts": len(question_drafts if isinstance(question_drafts, dict) else {}),
        "sent_replies": len(archive if isinstance(archive, list) else []),
        "sent_questions": len(question_archive if isinstance(question_archive, list) else []),
        "ignored_questions": len(question_ignored if isinstance(question_ignored, list) else []),
        "unanswered_cached": int((reply_snapshot or {}).get("count_unanswered") or 0),
        "low_rating_cached": len((low_snapshot or {}).get("feedbacks") or []),
        "questions_cached": int((question_snapshot or {}).get("count_unanswered") or len((question_snapshot or {}).get("questions") or [])),
        "complaint_results": complaint_results,
        "health": health,
        "history_sync_status": clean_text((history_meta or {}).get("status")) or "idle",
        "history_synced_total": int((history_meta or {}).get("db_total_rows") or 0),
        "history_db_exists": bool(paths["historical_db_file"].exists()),
        "tenant_root": str(paths["tenant_root"]),
    }


def collect_tenant_summaries() -> list[dict]:
    return [summarize_tenant(t) for t in load_tenants()]


# --- runtime context / workers ---
def apply_tenant_context(tenant_id: str, module_globals: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Кабинет не найден.")
    paths = ensure_tenant_dirs(tenant_id)
    common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
    if module_globals is not None:
        module_globals["PRIVATE_DIR"] = common.PRIVATE_DIR
        module_globals["ARCHIVE_FILE"] = common.ARCHIVE_FILE
        module_globals["DRAFTS_FILE"] = common.DRAFTS_FILE
        module_globals["SYSTEM_PROMPT_FILE"] = common.SYSTEM_PROMPT_FILE
        module_globals["REPLY_QUEUE_FILE"] = common.REPLY_QUEUE_FILE
        module_globals["REPLY_SNAPSHOT_FILE"] = common.REPLY_SNAPSHOT_FILE
        module_globals["HISTORICAL_DB_FILE"] = common.HISTORICAL_DB_FILE
        module_globals["HISTORICAL_SYNC_META_FILE"] = common.HISTORICAL_SYNC_META_FILE
        module_globals["QUESTION_SNAPSHOT_FILE"] = common.QUESTION_SNAPSHOT_FILE
        module_globals["QUESTION_DRAFTS_FILE"] = common.QUESTION_DRAFTS_FILE
        module_globals["QUESTION_QUEUE_FILE"] = common.QUESTION_QUEUE_FILE
        module_globals["QUESTION_ARCHIVE_FILE"] = common.QUESTION_ARCHIVE_FILE
        module_globals["QUESTION_CLUSTERS_FILE"] = common.QUESTION_CLUSTERS_FILE
        module_globals["QUESTION_IGNORED_FILE"] = common.QUESTION_IGNORED_FILE
        module_globals["WB_SESSION"] = common.WB_SESSION
    return {"tenant": tenant, "paths": paths}


def spawn_login_for_tenant(tenant_id: str) -> None:
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Кабинет не найден.")
    cmd = [sys.executable, str(BASE_DIR / "login_wb.py"), "--tenant", tenant_id]
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
        subprocess.Popen(cmd, creationflags=creationflags)
    else:
        subprocess.Popen(cmd)
