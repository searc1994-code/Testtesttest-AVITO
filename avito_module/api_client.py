from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests

from .compat import clean_text, log_event, read_json, resolve_paths, utc_now_iso, write_json
from .config import AvitoModuleConfig


class AvitoApiError(RuntimeError):
    pass


class AvitoApiUnauthorized(AvitoApiError):
    pass


class AvitoApiCircuitOpen(AvitoApiError):
    pass


@dataclass(slots=True)
class OAuthToken:
    access_token: str
    expires_at_monotonic: float
    refresh_token: str = ""

    @property
    def expired(self) -> bool:
        return time.monotonic() >= self.expires_at_monotonic


@dataclass(slots=True)
class GuardianState:
    request_timestamps: List[float] = field(default_factory=list)
    last_request_ts: float = 0.0
    consecutive_failures: int = 0
    circuit_open_until: float = 0.0
    requests_total: int = 0
    refresh_total: int = 0
    auth_retry_total: int = 0
    rate_wait_total: int = 0
    circuit_open_total: int = 0
    last_refresh_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_timestamps": list(self.request_timestamps),
            "last_request_ts": self.last_request_ts,
            "consecutive_failures": self.consecutive_failures,
            "circuit_open_until": self.circuit_open_until,
            "requests_total": self.requests_total,
            "refresh_total": self.refresh_total,
            "auth_retry_total": self.auth_retry_total,
            "rate_wait_total": self.rate_wait_total,
            "circuit_open_total": self.circuit_open_total,
            "last_refresh_at": self.last_refresh_at,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "GuardianState":
        if not isinstance(payload, dict):
            return cls()
        return cls(
            request_timestamps=[float(x) for x in payload.get("request_timestamps") or [] if isinstance(x, (int, float))],
            last_request_ts=float(payload.get("last_request_ts") or 0.0),
            consecutive_failures=int(payload.get("consecutive_failures") or 0),
            circuit_open_until=float(payload.get("circuit_open_until") or 0.0),
            requests_total=int(payload.get("requests_total") or 0),
            refresh_total=int(payload.get("refresh_total") or 0),
            auth_retry_total=int(payload.get("auth_retry_total") or 0),
            rate_wait_total=int(payload.get("rate_wait_total") or 0),
            circuit_open_total=int(payload.get("circuit_open_total") or 0),
            last_refresh_at=clean_text(payload.get("last_refresh_at")),
        )


class TokenGuardian:
    def __init__(self, config: AvitoModuleConfig, tenant_id: str, event_writer: Optional[Callable[..., Any]] = None, base_dir=None) -> None:
        self.config = config
        self.tenant_id = clean_text(tenant_id) or "default"
        self.event_writer = event_writer
        self.base_dir = base_dir
        self.state_file = resolve_paths(self.tenant_id, base_dir=base_dir).get("avito_guardian_state_file")
        self.state = GuardianState.from_dict(read_json(self.state_file, {})) if self.state_file else GuardianState()

    def _emit(self, stage: str, message: str = "", *, level: str = "info", **data: Any) -> None:
        if callable(self.event_writer):
            try:
                self.event_writer(stage=stage, message=message, level=level, **data)
                return
            except Exception:
                pass
        log_event("avito_sync", stage, tenant_id=self.tenant_id, level=level, **data)

    def persist(self) -> None:
        if not self.state_file:
            return
        write_json(self.state_file, self.state.to_dict())
        try:
            self.state_file.chmod(0o600)
        except Exception:
            pass

    def before_request(self) -> None:
        now = time.time()
        if self.state.circuit_open_until and now < self.state.circuit_open_until:
            remaining = round(self.state.circuit_open_until - now, 2)
            self._emit(
                "api_circuit_open",
                "Circuit breaker Avito ещё открыт",
                level="warning",
                remaining_seconds=remaining,
            )
            raise AvitoApiCircuitOpen(f"Avito API circuit is open for {remaining}s")
        self.state.request_timestamps = [ts for ts in self.state.request_timestamps if now - ts < 60.0]
        max_per_minute = max(1, int(self.config.api_max_requests_per_minute or 60))
        min_interval = max(0, int(self.config.api_min_request_interval_ms or 0)) / 1000.0

        wait_for_rate = 0.0
        if len(self.state.request_timestamps) >= max_per_minute:
            oldest = min(self.state.request_timestamps)
            wait_for_rate = max(wait_for_rate, 60.0 - (now - oldest))
        if self.state.last_request_ts and min_interval > 0:
            wait_for_rate = max(wait_for_rate, min_interval - (now - self.state.last_request_ts))
        if wait_for_rate > 0:
            self.state.rate_wait_total += 1
            self.persist()
            self._emit(
                "api_rate_wait",
                "Token guardian сделал паузу перед запросом Avito",
                level="warning",
                wait_seconds=round(wait_for_rate, 3),
            )
            time.sleep(wait_for_rate)
            now = time.time()
        self.state.request_timestamps.append(now)
        self.state.last_request_ts = now
        self.state.requests_total += 1
        self.persist()

    def note_success(self) -> None:
        self.state.consecutive_failures = 0
        self.persist()

    def note_refresh(self, *, reason: str) -> None:
        self.state.refresh_total += 1
        self.state.auth_retry_total += 1
        self.state.last_refresh_at = utc_now_iso()
        self.persist()
        self._emit("oauth_refresh_success", "Avito token refresh выполнен", reason=reason, refresh_total=self.state.refresh_total)

    def note_failure(self, *, error_kind: str, status_code: int = 0) -> None:
        self.state.consecutive_failures += 1
        threshold = max(1, int(self.config.api_circuit_breaker_threshold or 5))
        if self.state.consecutive_failures >= threshold:
            cooldown = max(5, int(self.config.api_circuit_breaker_cooldown_seconds or 120))
            self.state.circuit_open_until = time.time() + cooldown
            self.state.circuit_open_total += 1
            self._emit(
                "api_circuit_open",
                "Circuit breaker Avito открыт после серии ошибок",
                level="error",
                error_kind=error_kind,
                status_code=status_code,
                consecutive_failures=self.state.consecutive_failures,
                cooldown_seconds=cooldown,
            )
        self.persist()

    def note_half_open(self) -> None:
        if self.state.circuit_open_until:
            self.state.circuit_open_until = 0.0
            self.state.consecutive_failures = 0
            self.persist()
            self._emit("api_circuit_half_open", "Circuit breaker Avito закрыт после cooldown", level="warning")


class AvitoApiClient:
    """Resilient Avito API client with token guardian.

    Practical behaviors:
    - refreshes token on time-based expiry;
    - retries on 401/403 with token refresh;
    - retries on 429/5xx with exponential backoff;
    - per-tenant rate limiting and circuit breaker.
    """

    def __init__(self, config: AvitoModuleConfig, tenant_id: str, event_writer: Optional[Callable[..., Any]] = None, base_dir=None) -> None:
        self.config = config
        self.tenant_id = clean_text(tenant_id) or "default"
        self.event_writer = event_writer
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.base_dir = base_dir
        self.guardian = TokenGuardian(config, self.tenant_id, event_writer=event_writer, base_dir=base_dir)
        self._token: Optional[OAuthToken] = None
        if config.access_token:
            self._token = OAuthToken(
                access_token=config.access_token,
                expires_at_monotonic=time.monotonic() + 30.0,
                refresh_token=config.refresh_token,
            )

    def _emit_event(self, stage: str, message: str = "", *, level: str = "info", **data: Any) -> None:
        if callable(self.event_writer):
            try:
                self.event_writer(stage=stage, message=message, level=level, **data)
                return
            except Exception:
                pass
        log_event("avito_sync", stage, tenant_id=self.tenant_id, level=level, **data)

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _token_url(self) -> str:
        return f"{self.config.api_base_url.rstrip('/')}/token"

    def _request_token(self, *, reason: str = "expired") -> OAuthToken:
        payload = {
            "grant_type": self.config.oauth_grant_type or "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
        }
        if payload["grant_type"] == "refresh_token" and self.config.refresh_token:
            payload["refresh_token"] = self.config.refresh_token
        response = self.session.post(self._token_url(), data=payload, timeout=30)
        if response.status_code >= 400:
            self._emit_event("oauth_failed", "OAuth Avito завершился ошибкой", level="error", status_code=response.status_code, body=response.text[:500])
            self.guardian.note_failure(error_kind="oauth", status_code=response.status_code)
            raise AvitoApiUnauthorized(f"OAuth failed: HTTP {response.status_code}")
        data = response.json()
        token = clean_text(data.get("access_token"))
        if not token:
            raise AvitoApiUnauthorized("OAuth failed: access_token missing")
        expires_in = int(data.get("expires_in") or 3600)
        refresh_token = clean_text(data.get("refresh_token") or self.config.refresh_token)
        self._token = OAuthToken(
            access_token=token,
            expires_at_monotonic=time.monotonic() + max(60, expires_in - 120),
            refresh_token=refresh_token,
        )
        self.config.access_token = token
        self.config.refresh_token = refresh_token
        try:
            self.config.persist()
        except Exception:
            pass
        self.guardian.note_refresh(reason=reason)
        self.guardian.note_success()
        self._emit_event("oauth_ok", "OAuth Avito обновлён", expires_in=expires_in, reason=reason)
        return self._token

    def ensure_token(self) -> str:
        if self.guardian.state.circuit_open_until and time.time() >= self.guardian.state.circuit_open_until:
            self.guardian.note_half_open()
        if self._token is None or self._token.expired:
            self._request_token(reason="expired")
        assert self._token is not None
        return self._token.access_token

    def _backoff_sleep(self, attempt_no: int, *, status_code: int = 0) -> None:
        base = max(0.05, float(self.config.api_backoff_base_seconds or 1.0))
        max_wait = max(base, float(self.config.api_backoff_max_seconds or 30.0))
        wait = min(max_wait, base * (2 ** max(0, attempt_no - 1)))
        self._emit_event(
            "api_backoff_wait",
            "Пауза перед повтором Avito API запроса",
            level="warning",
            attempt=attempt_no,
            status_code=status_code,
            wait_seconds=round(wait, 3),
        )
        time.sleep(wait)

    def request_json(self, method: str, endpoint: str, *, retry_auth: bool = True, **kwargs: Any) -> Any:
        attempts_budget = max(1, int(self.config.api_retry_budget or 3))
        request_kwargs = dict(kwargs)
        timeout = request_kwargs.pop("timeout", 30)
        url = f"{self.config.api_base_url.rstrip('/')}{endpoint}"
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts_budget + 1):
            self.guardian.before_request()
            token = self.ensure_token()
            headers = dict(request_kwargs.pop("headers", {}) or {})
            headers.setdefault("Authorization", f"Bearer {token}")
            headers.setdefault("Content-Type", "application/json")
            try:
                response = self.session.request(method.upper(), url, headers=headers, timeout=timeout, **request_kwargs)
            except requests.RequestException as exc:
                last_error = exc
                self.guardian.note_failure(error_kind="network")
                self._emit_event("api_request_failed", "Ошибка сети при запросе к Avito API", level="error", endpoint=endpoint, error=str(exc), attempt=attempt)
                if attempt < attempts_budget:
                    self._backoff_sleep(attempt)
                    continue
                raise AvitoApiError(f"Network error for {endpoint}: {exc}") from exc

            status_code = int(response.status_code or 0)
            if status_code in {401, 403}:
                self.guardian.note_failure(error_kind="auth", status_code=status_code)
                if retry_auth and attempt < attempts_budget:
                    self._emit_event("auth_retry", "Повторяю запрос Avito после обновления токена", level="warning", endpoint=endpoint, status_code=status_code, attempt=attempt)
                    self._request_token(reason=f"http_{status_code}")
                    self._backoff_sleep(attempt, status_code=status_code)
                    continue
                raise AvitoApiUnauthorized(f"HTTP {status_code}: {response.text[:200]}")
            if status_code == 429 or status_code >= 500:
                self.guardian.note_failure(error_kind="server", status_code=status_code)
                self._emit_event("api_retryable_error", "Avito API вернул retryable ошибку", level="warning", endpoint=endpoint, status_code=status_code, attempt=attempt)
                if attempt < attempts_budget:
                    self._backoff_sleep(attempt, status_code=status_code)
                    continue
                raise AvitoApiError(f"HTTP {status_code}: {response.text[:300]}")
            if status_code >= 400:
                self.guardian.note_failure(error_kind="http", status_code=status_code)
                raise AvitoApiError(f"HTTP {status_code}: {response.text[:300]}")

            self.guardian.note_success()
            if not response.content:
                return {}
            try:
                return response.json()
            except Exception as exc:
                raise AvitoApiError(f"Invalid JSON in response from {endpoint}: {exc}") from exc
        if last_error is not None:
            raise AvitoApiError(str(last_error))
        raise AvitoApiError(f"Retry budget exhausted for {endpoint}")

    @property
    def account_id(self) -> str:
        return clean_text(self.config.user_id)

    def list_chats(self, *, unread_only: bool = False, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        params = {"limit": max(1, limit), "offset": max(0, offset)}
        if unread_only:
            params["unread_only"] = True
        endpoint = f"/messenger/v2/accounts/{self.account_id}/chats"
        return self.request_json("GET", endpoint, params=params)

    def get_messages(self, chat_id: str, *, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        endpoint = f"/messenger/v3/accounts/{self.account_id}/chats/{clean_text(chat_id)}/messages"
        params = {"limit": max(1, limit), "offset": max(0, offset)}
        return self.request_json("GET", endpoint, params=params)

    def send_text_message(self, chat_id: str, text: str) -> Dict[str, Any]:
        endpoint = f"/messenger/v1/accounts/{self.account_id}/chats/{clean_text(chat_id)}/messages"
        payload = {"message": {"text": text}, "type": "text"}
        return self.request_json("POST", endpoint, json=payload)

    def mark_chat_as_read(self, chat_id: str) -> Dict[str, Any]:
        endpoint = f"/messenger/v1/accounts/{self.account_id}/chats/{clean_text(chat_id)}/read"
        return self.request_json("POST", endpoint, json={})

    def iter_chat_previews(self, *, unread_only: bool = False, limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        page_size = min(max(1, self.config.sync_page_limit), 100)
        remaining = limit if limit is not None else page_size * max(1, self.config.sync_max_pages)
        offset = 0
        page_no = 0
        while remaining > 0 and page_no < max(1, self.config.sync_max_pages):
            payload = self.list_chats(unread_only=unread_only, limit=min(page_size, remaining), offset=offset)
            chats = payload.get("chats") if isinstance(payload, dict) else []
            if not isinstance(chats, list):
                chats = []
            if not chats:
                break
            for raw_chat in chats:
                yield self._normalize_chat(raw_chat)
                remaining -= 1
                if remaining <= 0:
                    break
            page_no += 1
            offset += len(chats)
            if len(chats) < page_size:
                break

    def iter_messages(self, chat_id: str, *, limit: int = 200) -> Iterable[Dict[str, Any]]:
        page_size = min(100, max(1, limit))
        offset = 0
        remaining = limit
        while remaining > 0:
            payload = self.get_messages(chat_id, limit=min(page_size, remaining), offset=offset)
            messages = payload.get("messages") if isinstance(payload, dict) else []
            if not isinstance(messages, list) or not messages:
                break
            for raw_msg in messages:
                yield self._normalize_message(raw_msg)
                remaining -= 1
                if remaining <= 0:
                    break
            offset += len(messages)
            if len(messages) < page_size:
                break

    @staticmethod
    def _normalize_chat(raw_chat: Dict[str, Any]) -> Dict[str, Any]:
        context = raw_chat.get("context") or {}
        last_message = raw_chat.get("last_message") or raw_chat.get("lastMessage") or {}
        user = raw_chat.get("user") or raw_chat.get("client") or {}
        item = context.get("item") or raw_chat.get("item") or {}
        chat_id = clean_text(raw_chat.get("id") or raw_chat.get("chat_id"))
        return {
            "chat_id": chat_id,
            "id": chat_id,
            "title": clean_text(raw_chat.get("title") or item.get("title") or user.get("name") or chat_id),
            "client_name": clean_text(user.get("name") or raw_chat.get("name")),
            "item_id": clean_text(item.get("id") or raw_chat.get("item_id")),
            "item_title": clean_text(item.get("title") or context.get("title")),
            "unread_count": int(raw_chat.get("unread_count") or raw_chat.get("unread") or 0),
            "last_message_text": clean_text((last_message.get("content") or {}).get("text") or last_message.get("text")),
            "last_message_ts": clean_text(last_message.get("created") or raw_chat.get("updated") or raw_chat.get("updated_at")),
            "raw": raw_chat,
        }

    @staticmethod
    def _normalize_message(raw_message: Dict[str, Any]) -> Dict[str, Any]:
        content = raw_message.get("content") or {}
        return {
            "message_id": clean_text(raw_message.get("id")),
            "id": clean_text(raw_message.get("id")),
            "direction": clean_text(raw_message.get("direction") or raw_message.get("type") or "unknown"),
            "is_read": bool(raw_message.get("is_read")),
            "author_name": clean_text((raw_message.get("author") or {}).get("name") or raw_message.get("name")),
            "message_ts": clean_text(raw_message.get("created") or raw_message.get("created_at") or raw_message.get("updated")),
            "text": clean_text(content.get("text") or raw_message.get("text")),
            "attachments": raw_message.get("attachments") or [],
            "raw": raw_message,
        }
