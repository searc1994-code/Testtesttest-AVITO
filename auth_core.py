from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import config
import safe_files

PBKDF2_ITERATIONS = 240_000
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 256
_LOCK_GUARD_RETENTION_DAYS = 30


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def admin_state_path() -> Path:
    return Path(getattr(config, "APP_ADMIN_STATE_FILE", str(Path(config.SECURITY_DIR) / "admin_auth.json")))


def login_guard_path() -> Path:
    return Path(getattr(config, "APP_ADMIN_LOGIN_GUARD_FILE", str(Path(config.SECURITY_DIR) / "admin_login_guard.json")))


def _env_username() -> str:
    return _clean(getattr(config, "APP_ADMIN_USERNAME", "admin") or "admin") or "admin"


def _env_password() -> str:
    return str(getattr(config, "APP_ADMIN_PASSWORD", "") or "")


def _legacy_env_password_allowed() -> bool:
    return bool(getattr(config, "APP_ALLOW_LEGACY_PLAINTEXT_ADMIN_PASSWORD", False))


def _migrate_env_password_to_hash() -> bool:
    return bool(getattr(config, "APP_ADMIN_PASSWORD_MIGRATE_TO_HASH", True))


def _env_password_hash() -> str:
    return _clean(getattr(config, "APP_ADMIN_PASSWORD_HASH", "") or "")


def _env_password_record() -> str:
    return str(getattr(config, "APP_ADMIN_PASSWORD_RECORD", "") or "")


def _hash_password(password: str, salt_hex: str, *, iterations: int = PBKDF2_ITERATIONS) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", str(password).encode("utf-8"), salt, max(1, int(iterations or PBKDF2_ITERATIONS)))
    return digest.hex()


def _parse_password_record(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    record = {
        "username": _clean(value.get("username") or _env_username()) or "admin",
        "salt": _clean(value.get("salt")),
        "hash": _clean(value.get("hash")),
        "iterations": int(value.get("iterations") or PBKDF2_ITERATIONS),
        "created_at": _clean(value.get("created_at") or utc_now_iso()),
        "updated_at": _clean(value.get("updated_at") or utc_now_iso()),
        "version": int(value.get("version") or 1),
    }
    return record if record["salt"] and record["hash"] else {}


def _parse_env_hash_payload(text: str) -> Dict[str, Any]:
    payload = _clean(text)
    if not payload:
        return {}
    if payload.startswith("{"):
        try:
            data = json.loads(payload)
        except Exception:
            return {}
        parsed = _parse_password_record(data)
        if parsed:
            parsed["source"] = "env_hash"
        return parsed
    parts = payload.split("$")
    if len(parts) != 4:
        return {}
    scheme, iterations_text, salt_hex, hash_hex = parts
    if _clean(scheme).lower() != "pbkdf2_sha256":
        return {}
    try:
        iterations = int(iterations_text)
    except Exception:
        return {}
    record = {
        "source": "env_hash",
        "username": _env_username(),
        "salt": _clean(salt_hex),
        "hash": _clean(hash_hex),
        "iterations": max(1, iterations),
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "version": 1,
    }
    return record if record["salt"] and record["hash"] else {}


def create_password_record(password: str, username: Optional[str] = None) -> Dict[str, Any]:
    username = _clean(username or _env_username()) or "admin"
    salt_hex = secrets.token_hex(16)
    now_text = utc_now_iso()
    return {
        "username": username,
        "salt": salt_hex,
        "hash": _hash_password(password, salt_hex),
        "iterations": PBKDF2_ITERATIONS,
        "created_at": now_text,
        "updated_at": now_text,
        "version": 1,
    }


def password_record_to_env_hash(record: Dict[str, Any]) -> str:
    parsed = _parse_password_record(record)
    if not parsed:
        raise ValueError("Некорректная запись пароля.")
    return f"pbkdf2_sha256${int(parsed.get('iterations') or PBKDF2_ITERATIONS)}${parsed.get('salt')}${parsed.get('hash')}"


def password_policy_errors(password: str, confirm_password: str = "") -> list[str]:
    errors: list[str] = []
    password = str(password or "")
    if len(password) < MIN_PASSWORD_LENGTH:
        errors.append(f"Пароль должен быть не короче {MIN_PASSWORD_LENGTH} символов.")
    if len(password) > MAX_PASSWORD_LENGTH:
        errors.append(f"Пароль должен быть не длиннее {MAX_PASSWORD_LENGTH} символов.")
    if confirm_password != "" and password != str(confirm_password or ""):
        errors.append("Пароль и подтверждение не совпадают.")
    return errors


def _bootstrap_from_env_password(env_password: str) -> Dict[str, Any]:
    record = create_password_record(env_password, username=_env_username())
    safe_files.write_json(admin_state_path(), record, ensure_ascii=False, indent=2)
    record = dict(record)
    record['source'] = 'file_bootstrapped_from_env'
    return record


def load_password_record() -> Dict[str, Any]:
    env_record_payload = _env_password_record()
    if env_record_payload:
        parsed_record = _parse_env_hash_payload(env_record_payload)
        if parsed_record:
            parsed_record["source"] = "env_hash"
            parsed_record["username"] = _clean(parsed_record.get("username") or _env_username()) or "admin"
            return parsed_record
    env_hash = _env_password_hash()
    if env_hash:
        parsed_hash = _parse_env_hash_payload(env_hash)
        if parsed_hash:
            parsed_hash["source"] = "env_hash"
            return parsed_hash
    data = safe_files.read_json(admin_state_path(), {})
    parsed = _parse_password_record(data)
    if parsed:
        parsed["source"] = "file"
        return parsed
    env_password = _env_password()
    if env_password and _migrate_env_password_to_hash():
        try:
            return _bootstrap_from_env_password(env_password)
        except Exception:
            pass
    if env_password and _legacy_env_password_allowed():
        return {
            "source": "env_plaintext",
            "username": _env_username(),
            "env_password": env_password,
        }
    return {}

def has_password_record() -> bool:
    data = load_password_record()
    if data.get("source") in {"env_hash", "env_plaintext"}:
        return True
    return bool(_clean(data.get("username")) and _clean(data.get("salt")) and _clean(data.get("hash")))


def needs_bootstrap() -> bool:
    if not bool(getattr(config, "APP_AUTH_ENABLED", True)):
        return False
    return not has_password_record()


def verify_credentials(username: str, password: str) -> bool:
    record = load_password_record()
    username = _clean(username)
    password = str(password or "")
    source = _clean(record.get("source"))
    if source == "env_plaintext":
        return hmac.compare_digest(username, _clean(record.get("username"))) and hmac.compare_digest(password, str(record.get("env_password") or ""))
    if not record:
        return False
    expected_user = _clean(record.get("username")) or _env_username()
    salt_hex = _clean(record.get("salt"))
    stored_hash = _clean(record.get("hash"))
    iterations = int(record.get("iterations") or PBKDF2_ITERATIONS)
    if not (expected_user and salt_hex and stored_hash):
        return False
    if not hmac.compare_digest(username, expected_user):
        return False
    try:
        computed = _hash_password(password, salt_hex, iterations=iterations)
    except Exception:
        return False
    return hmac.compare_digest(computed, stored_hash)


def bootstrap_admin_password(password: str, confirm_password: str = "", username: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
    source = _clean(load_password_record().get("source"))
    if source == "env_plaintext" and not force:
        raise ValueError("Пароль администратора уже задан через переменную окружения APP_ADMIN_PASSWORD. Включён legacy-режим.")
    if source == "env_hash" and not force:
        raise ValueError("Пароль администратора уже задан через переменную окружения APP_ADMIN_PASSWORD_HASH.")
    if has_password_record() and not needs_bootstrap() and not force:
        raise ValueError("Пароль администратора уже инициализирован.")
    errors = password_policy_errors(password, confirm_password)
    if errors:
        raise ValueError(" ".join(errors))
    record = create_password_record(password, username=username)
    safe_files.write_json(admin_state_path(), record, ensure_ascii=False, indent=2)
    return record


def describe_auth_state() -> Dict[str, Any]:
    record = load_password_record()
    source = _clean(record.get("source")) or ("file" if record else "missing")
    warnings: list[str] = []
    if source == "env_plaintext":
        warnings.append("Используется legacy-режим APP_ADMIN_PASSWORD. Для production лучше хранить только хешированный пароль.")
    if source == "file_bootstrapped_from_env":
        warnings.append("Пароль из APP_ADMIN_PASSWORD был автоматически перенесён в хешированный файл admin_auth.json.")
    payload = {
        "auth_enabled": bool(getattr(config, "APP_AUTH_ENABLED", True)),
        "needs_bootstrap": needs_bootstrap(),
        "source": source,
        "username": _clean(record.get("username")) or _env_username(),
        "state_path": str(admin_state_path()),
        "exists": admin_state_path().exists(),
        "uses_plaintext_env": source == "env_plaintext",
        "uses_hashed_env": source == "env_hash",
        "warnings": warnings,
    }
    return payload

def _load_login_guard_state() -> Dict[str, Any]:
    data = safe_files.read_json(login_guard_path(), {})
    if isinstance(data, dict):
        buckets = data.get("buckets") if isinstance(data.get("buckets"), dict) else {}
        data["buckets"] = buckets
        return data
    return {"buckets": {}}


def _save_login_guard_state(state: Dict[str, Any]) -> None:
    safe_files.write_json(login_guard_path(), state, ensure_ascii=False, indent=2)


def _login_bucket_key(username: str, remote_addr: str) -> str:
    user = _clean(username).lower() or "admin"
    addr = _clean(remote_addr).split(",", 1)[0].strip() or "unknown"
    return f"{user}::{addr}"


def _cleanup_guard_buckets(state: Dict[str, Any]) -> Dict[str, Any]:
    buckets = state.get("buckets") if isinstance(state.get("buckets"), dict) else {}
    threshold = _now() - timedelta(days=_LOCK_GUARD_RETENTION_DAYS)
    cleaned: Dict[str, Any] = {}
    for key, row in buckets.items():
        if not isinstance(row, dict):
            continue
        last_seen = _clean(row.get("last_seen") or row.get("locked_until") or row.get("first_failure_at"))
        if not last_seen:
            cleaned[_clean(key)] = row
            continue
        try:
            parsed = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
        except Exception:
            cleaned[_clean(key)] = row
            continue
        if parsed >= threshold:
            cleaned[_clean(key)] = row
    state["buckets"] = cleaned
    return state


def check_login_allowed(username: str, remote_addr: str) -> Dict[str, Any]:
    state = _cleanup_guard_buckets(_load_login_guard_state())
    key = _login_bucket_key(username, remote_addr)
    row = state["buckets"].get(key) if isinstance(state.get("buckets"), dict) else {}
    if not isinstance(row, dict):
        row = {}
    locked_until = _clean(row.get("locked_until"))
    remaining = 0
    allowed = True
    if locked_until:
        try:
            locked_dt = datetime.fromisoformat(locked_until.replace("Z", "+00:00"))
            delta = (locked_dt - _now()).total_seconds()
            if delta > 0:
                remaining = int(delta)
                allowed = False
        except Exception:
            allowed = True
    retry_after = max(0, remaining)
    return {
        "allowed": allowed,
        "remaining_lock_seconds": retry_after,
        "retry_after_seconds": retry_after,
        "failures": int(row.get("failures") or 0),
        "bucket_key": key,
    }


def register_login_failure(username: str, remote_addr: str) -> Dict[str, Any]:
    state = _cleanup_guard_buckets(_load_login_guard_state())
    buckets = state.get("buckets") if isinstance(state.get("buckets"), dict) else {}
    key = _login_bucket_key(username, remote_addr)
    row = buckets.get(key) if isinstance(buckets.get(key), dict) else {}
    now_text = utc_now_iso()
    failures = int(row.get("failures") or 0) + 1
    row.update(
        {
            "username": _clean(username).lower() or "admin",
            "remote_addr": _clean(remote_addr).split(",", 1)[0].strip() or "unknown",
            "failures": failures,
            "last_seen": now_text,
            "first_failure_at": _clean(row.get("first_failure_at") or now_text),
        }
    )
    max_attempts = max(1, int(getattr(config, "APP_ADMIN_LOGIN_MAX_ATTEMPTS", 5) or 5))
    lock_seconds = max(1, int(getattr(config, "APP_ADMIN_LOGIN_LOCK_SECONDS", 300) or 300))
    locked_until = ""
    if failures >= max_attempts:
        locked_until = (_now() + timedelta(seconds=lock_seconds)).isoformat()
        row["locked_until"] = locked_until
    else:
        row.pop("locked_until", None)
    buckets[key] = row
    state["buckets"] = buckets
    _save_login_guard_state(state)
    retry_after = lock_seconds if locked_until else 0
    return {
        "failures": failures,
        "locked_until": locked_until,
        "remaining_lock_seconds": retry_after,
        "retry_after_seconds": retry_after,
    }


def register_login_success(username: str, remote_addr: str) -> None:
    state = _cleanup_guard_buckets(_load_login_guard_state())
    buckets = state.get("buckets") if isinstance(state.get("buckets"), dict) else {}
    buckets.pop(_login_bucket_key(username, remote_addr), None)
    state["buckets"] = buckets
    _save_login_guard_state(state)
