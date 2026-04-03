from __future__ import annotations

import hmac
import secrets
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from flask import g, request, session
except Exception:  # pragma: no cover - fallback for offline smoke/import checks
    from types import SimpleNamespace
    g = SimpleNamespace()
    request = SimpleNamespace(form={}, headers={}, remote_addr='127.0.0.1')
    session = {}
from markupsafe import Markup, escape

import config
import safe_files

CSRF_SESSION_KEY = "_csrf_token"
CSRF_FIELD_NAME = "_csrf_token"


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def csrf_enabled() -> bool:
    return bool(getattr(config, "APP_CSRF_ENABLED", True))


def get_or_create_csrf_token() -> str:
    token = str(session.get(CSRF_SESSION_KEY) or "")
    if len(token) < 24:
        token = secrets.token_urlsafe(32)
        session[CSRF_SESSION_KEY] = token
        try:
            session.modified = True
        except Exception:
            pass
    return token


def rotate_csrf_token() -> str:
    token = secrets.token_urlsafe(32)
    session[CSRF_SESSION_KEY] = token
    try:
        session.modified = True
    except Exception:
        pass
    return token


def request_csrf_token() -> str:
    candidates = [
        request.form.get(CSRF_FIELD_NAME),
        request.form.get("csrf_token"),
        request.headers.get("X-CSRF-Token"),
        request.headers.get("X-CSRFToken"),
    ]
    for value in candidates:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def is_valid_csrf_request() -> bool:
    if not csrf_enabled():
        return True
    expected = str(session.get(CSRF_SESSION_KEY) or "")
    provided = request_csrf_token()
    return bool(expected and provided and hmac.compare_digest(expected, provided))


def csrf_input() -> Markup:
    token = get_or_create_csrf_token()
    return Markup(f'<input type="hidden" name="{escape(CSRF_FIELD_NAME)}" value="{escape(token)}">')


def get_csp_nonce() -> str:
    nonce = getattr(g, "csp_nonce", "")
    if not nonce:
        nonce = secrets.token_urlsafe(24)
        g.csp_nonce = nonce
    return nonce


def build_csp_header() -> str:
    nonce = get_csp_nonce()
    return "; ".join([
        "default-src 'self'",
        "base-uri 'self'",
        "form-action 'self'",
        "frame-ancestors 'self'",
        "object-src 'none'",
        "img-src 'self' data:",
        "font-src 'self' data:",
        "connect-src 'self'",
        f"script-src 'self' 'nonce-{nonce}'",
        "script-src-attr 'none'",
        f"style-src 'self' 'nonce-{nonce}'",
        "style-src-attr 'unsafe-inline'",
    ])


# -------------------------
# Login rate limiting
# -------------------------

def login_rate_state_path() -> Path:
    return Path(getattr(config, "LOGIN_RATE_STATE_FILE", str(Path(config.SECURITY_DIR) / "login_rate_limit.json")))


def _rate_window_seconds() -> int:
    return max(60, int(getattr(config, "LOGIN_RATE_WINDOW_SECONDS", 900) or 900))


def _rate_block_seconds() -> int:
    return max(60, int(getattr(config, "LOGIN_RATE_BLOCK_SECONDS", 900) or 900))


def _rate_max_attempts() -> int:
    return max(1, int(getattr(config, "LOGIN_RATE_MAX_ATTEMPTS", 5) or 5))


def _normalized_ip(remote_addr: str) -> str:
    ip = _clean(remote_addr)
    return ip or "unknown"


def _bucket_keys(username: str, remote_addr: str) -> List[str]:
    user = _clean(username).lower() or "anonymous"
    ip = _normalized_ip(remote_addr)
    return [f"ip:{ip}", f"userip:{user}@{ip}"]


def _load_rate_state() -> Dict[str, Any]:
    data = safe_files.read_json(login_rate_state_path(), {})
    if not isinstance(data, dict):
        return {"version": 1, "buckets": {}}
    buckets = data.get("buckets") if isinstance(data.get("buckets"), dict) else {}
    return {"version": int(data.get("version") or 1), "buckets": buckets}


def _save_rate_state(state: Dict[str, Any]) -> None:
    safe_files.write_json(login_rate_state_path(), state, ensure_ascii=False, indent=2)


def _prune_bucket(bucket: Dict[str, Any], now_ts: float) -> Dict[str, Any]:
    window = _rate_window_seconds()
    failures = []
    for value in bucket.get("failures") or []:
        try:
            ts = float(value)
        except Exception:
            continue
        if now_ts - ts <= window:
            failures.append(ts)
    block_until = 0.0
    try:
        block_until = float(bucket.get("block_until") or 0.0)
    except Exception:
        block_until = 0.0
    if block_until <= now_ts:
        block_until = 0.0
    cleaned: Dict[str, Any] = {"failures": failures}
    if block_until > 0:
        cleaned["block_until"] = block_until
    return cleaned


def login_rate_status(username: str, remote_addr: str, *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    now_ts = float(now_ts or time.time())
    state = _load_rate_state()
    buckets = state.get("buckets") or {}
    blocked_until = 0.0
    remaining_attempts = _rate_max_attempts()
    touched = False
    for key in _bucket_keys(username, remote_addr):
        bucket = _prune_bucket(buckets.get(key) or {}, now_ts)
        if bucket != (buckets.get(key) or {}):
            buckets[key] = bucket
            touched = True
        try:
            blocked_until = max(blocked_until, float(bucket.get("block_until") or 0.0))
        except Exception:
            pass
        failures = bucket.get("failures") or []
        remaining_attempts = min(remaining_attempts, max(0, _rate_max_attempts() - len(failures)))
    if touched:
        state["buckets"] = buckets
        _save_rate_state(state)
    return {
        "allowed": blocked_until <= now_ts,
        "retry_after_seconds": max(0, int(round(blocked_until - now_ts))) if blocked_until > now_ts else 0,
        "remaining_attempts": remaining_attempts,
    }


def record_login_failure(username: str, remote_addr: str, *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    now_ts = float(now_ts or time.time())
    state = _load_rate_state()
    buckets = state.get("buckets") or {}
    max_attempts = _rate_max_attempts()
    block_seconds = _rate_block_seconds()
    blocked_until = 0.0
    remaining_attempts = max_attempts
    for key in _bucket_keys(username, remote_addr):
        bucket = _prune_bucket(buckets.get(key) or {}, now_ts)
        failures = list(bucket.get("failures") or [])
        failures.append(now_ts)
        failures = failures[-max_attempts:]
        bucket["failures"] = failures
        if len(failures) >= max_attempts:
            bucket["block_until"] = max(float(bucket.get("block_until") or 0.0), now_ts + block_seconds)
        buckets[key] = bucket
        try:
            blocked_until = max(blocked_until, float(bucket.get("block_until") or 0.0))
        except Exception:
            pass
        remaining_attempts = min(remaining_attempts, max(0, max_attempts - len(failures)))
    state["buckets"] = buckets
    _save_rate_state(state)
    return {
        "allowed": blocked_until <= now_ts,
        "retry_after_seconds": max(0, int(round(blocked_until - now_ts))) if blocked_until > now_ts else 0,
        "remaining_attempts": remaining_attempts,
    }


def clear_login_failures(username: str, remote_addr: str) -> None:
    state = _load_rate_state()
    buckets = state.get("buckets") or {}
    changed = False
    for key in _bucket_keys(username, remote_addr):
        if key in buckets:
            buckets.pop(key, None)
            changed = True
    if changed:
        state["buckets"] = buckets
        _save_rate_state(state)
