import hashlib
import json
import os
import re
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

import safe_files
from safe_logs import log_event

try:
    from openai import OpenAI, RateLimitError
except Exception:  # pragma: no cover - graceful fallback for missing/broken dependency
    OpenAI = None  # type: ignore[assignment]

    class RateLimitError(Exception):
        pass

import config

BASE_DIR = Path(__file__).resolve().parent
PRIVATE_ROOT = Path(getattr(config, "PRIVATE_ROOT", getattr(config, "WB_PRIVATE_DIR", str(Path.home() / "wb-ai-private")))).expanduser()

PRIVATE_ROOT.mkdir(parents=True, exist_ok=True)
TENANTS_ROOT = PRIVATE_ROOT / "tenants"
TENANTS_ROOT.mkdir(parents=True, exist_ok=True)
SHARED_DIR = PRIVATE_ROOT / "shared"
SHARED_DIR.mkdir(parents=True, exist_ok=True)

_DEFAULT_PRIVATE_DIR = PRIVATE_ROOT
_DEFAULT_AUTH_DIR = _DEFAULT_PRIVATE_DIR / "auth"
_DEFAULT_LOGS_DIR = _DEFAULT_PRIVATE_DIR / "logs"
_DEFAULT_COMPLAINTS_DIR = _DEFAULT_PRIVATE_DIR / "complaints"
_DEFAULT_SCREENSHOTS_DIR = _DEFAULT_COMPLAINTS_DIR / "screenshots"
_DEFAULT_DATA_DIR = _DEFAULT_PRIVATE_DIR / "data"

for directory in [
    _DEFAULT_PRIVATE_DIR,
    _DEFAULT_AUTH_DIR,
    _DEFAULT_LOGS_DIR,
    _DEFAULT_COMPLAINTS_DIR,
    _DEFAULT_SCREENSHOTS_DIR,
    _DEFAULT_DATA_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)

REQUEST_TIMEOUT = int(getattr(config, "REQUEST_TIMEOUT", 20))
AI_MIN_INTERVAL_SEC = float(getattr(config, "AI_MIN_INTERVAL_SEC", 0.9))

OPENAI_BASE_URL = getattr(config, "OPENAI_BASE_URL", "https://api.hydraai.ru/v1")
OPENAI_MODEL = getattr(config, "OPENAI_MODEL", "gpt-4o-mini")
OPENAI_COMPLAINT_MODEL = getattr(config, "OPENAI_COMPLAINT_MODEL", OPENAI_MODEL)
FLASK_SECRET = getattr(config, "FLASK_SECRET", "wb-review-local-secret")
COMPLAINT_CONFIDENCE_THRESHOLD = float(getattr(config, "COMPLAINT_CONFIDENCE_THRESHOLD", 0.78))
MAX_GENERATE_PER_PAGE = int(getattr(config, "MAX_GENERATE_PER_PAGE", 5))

WB_LIST_URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
WB_ANSWER_URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks/answer"
WB_NEW_ITEMS_URL = "https://feedbacks-api.wildberries.ru/api/v1/new-feedbacks-questions"
WB_QUESTIONS_LIST_URL = "https://feedbacks-api.wildberries.ru/api/v1/questions"
WB_QUESTIONS_COUNT_URL = "https://feedbacks-api.wildberries.ru/api/v1/questions/count"
WB_QUESTIONS_COUNT_UNANSWERED_URL = "https://feedbacks-api.wildberries.ru/api/v1/questions/count-unanswered"
WB_QUESTION_URL = "https://feedbacks-api.wildberries.ru/api/v1/question"

WB_SELLER_BASE_URL = getattr(config, "WB_SELLER_BASE_URL", "https://seller.wildberries.ru")
WB_REVIEWS_URL = getattr(config, "WB_REVIEWS_URL", "https://seller.wildberries.ru/reviews")

PLAYWRIGHT_HEADLESS = bool(getattr(config, "PLAYWRIGHT_HEADLESS", False))
PLAYWRIGHT_BROWSER_CHANNEL = getattr(config, "PLAYWRIGHT_BROWSER_CHANNEL", None)
PLAYWRIGHT_SLOW_MO_MS = int(getattr(config, "PLAYWRIGHT_SLOW_MO_MS", 150))

QUESTION_PROMPT_TEMPLATE_FILE = BASE_DIR / "question_prompt.txt"
QUESTION_RULES_TEMPLATE_FILE = BASE_DIR / "question_rules.json"
QUESTION_PROMPT_FILE = SHARED_DIR / "question_prompt.txt"
QUESTION_RULES_FILE = SHARED_DIR / "question_rules.json"

_path_factories: Dict[str, Callable[[], Path]] = {
    "tenant_root": lambda: _DEFAULT_PRIVATE_DIR,
    "auth_dir": lambda: _DEFAULT_AUTH_DIR,
    "logs_dir": lambda: _DEFAULT_LOGS_DIR,
    "complaints_dir": lambda: _DEFAULT_COMPLAINTS_DIR,
    "screenshots_dir": lambda: _DEFAULT_SCREENSHOTS_DIR,
    "data_dir": lambda: _DEFAULT_DATA_DIR,
    "archive_file": lambda: BASE_DIR / "reviews_archive.json",
    "drafts_file": lambda: BASE_DIR / "draft_replies.json",
    "rules_file": lambda: BASE_DIR / "business_rules.json",
    "system_prompt_file": lambda: BASE_DIR / "system_prompt.txt",
    "complaint_prompt_file": lambda: BASE_DIR / "complaint_prompt.txt",
    "ui_profile_file": lambda: BASE_DIR / "wb_ui_profile.json",
    "auth_state_file": lambda: _DEFAULT_AUTH_DIR / "wb_state.json",
    "auth_meta_file": lambda: _DEFAULT_AUTH_DIR / "wb_state_meta.json",
    "complaint_drafts_file": lambda: _DEFAULT_COMPLAINTS_DIR / "complaint_drafts.json",
    "complaint_queue_file": lambda: _DEFAULT_COMPLAINTS_DIR / "complaint_queue.json",
    "complaint_results_file": lambda: _DEFAULT_COMPLAINTS_DIR / "complaint_results.jsonl",
    "reply_queue_file": lambda: _DEFAULT_PRIVATE_DIR / "reply_queue.json",
    "reply_snapshot_file": lambda: _DEFAULT_PRIVATE_DIR / "reply_snapshot.json",
    "low_rating_cache_file": lambda: _DEFAULT_PRIVATE_DIR / "low_rating_reviews_snapshot.json",
    "historical_db_file": lambda: _DEFAULT_PRIVATE_DIR / "reviews_history.sqlite3",
    "historical_sync_meta_file": lambda: _DEFAULT_PRIVATE_DIR / "historical_sync_meta.json",
    "historical_sync_stop_file": lambda: _DEFAULT_PRIVATE_DIR / "historical_sync_stop.flag",
    "historical_sync_log_file": lambda: _DEFAULT_PRIVATE_DIR / "historical_sync_worker.log",
    "question_snapshot_file": lambda: _DEFAULT_PRIVATE_DIR / "question_snapshot.json",
    "question_drafts_file": lambda: _DEFAULT_PRIVATE_DIR / "question_drafts.json",
    "question_queue_file": lambda: _DEFAULT_PRIVATE_DIR / "question_queue.json",
    "question_archive_file": lambda: _DEFAULT_PRIVATE_DIR / "question_archive.json",
    "question_clusters_file": lambda: _DEFAULT_PRIVATE_DIR / "question_clusters.json",
    "question_sync_meta_file": lambda: _DEFAULT_PRIVATE_DIR / "question_sync_meta.json",
    "question_ignored_file": lambda: _DEFAULT_PRIVATE_DIR / "question_ignored.json",
}

_tenant_id_var: ContextVar[str] = ContextVar("wb_tenant_id", default="")
_tenant_var: ContextVar[Dict[str, Any]] = ContextVar("wb_tenant", default={})
_tenant_paths_var: ContextVar[Dict[str, Path]] = ContextVar("wb_tenant_paths", default={})
_wb_api_key_var: ContextVar[str] = ContextVar("wb_api_key", default=clean_text(getattr(config, "WB_API_KEY", "")) if 'clean_text' in globals() else str(getattr(config, "WB_API_KEY", "")))
_wb_session_var: ContextVar[requests.Session | None] = ContextVar("wb_session", default=None)

_LAST_AI_CALL_TS = 0.0
_OPENAI_CLIENT: Any = None
_OPENAI_CLIENT_KEY: str = ""
_OPENAI_CLIENT_BASE_URL: str = ""


class TenantPathProxy(os.PathLike[str]):
    def __init__(self, key: str):
        self.key = key

    def resolve(self) -> Path:
        paths = _tenant_paths_var.get() or {}
        value = paths.get(self.key)
        if value is not None:
            return Path(value)
        factory = _path_factories[self.key]
        return Path(factory())

    def __fspath__(self) -> str:
        return os.fspath(self.resolve())

    def __str__(self) -> str:
        return str(self.resolve())

    def __repr__(self) -> str:
        return f"TenantPathProxy({self.key!r}, path={self.resolve()!s})"

    def __getattr__(self, name: str) -> Any:
        return getattr(self.resolve(), name)

    def __truediv__(self, other: Any) -> Path:
        return self.resolve() / other

    def __rtruediv__(self, other: Any) -> Path:
        return Path(other) / self.resolve()


class TenantSessionProxy:
    def _resolve(self) -> requests.Session:
        session = _wb_session_var.get()
        api_key = get_active_api_key()
        if session is None:
            session = requests.Session()
            if api_key:
                session.headers.update({"Authorization": api_key})
            _wb_session_var.set(session)
            return session
        if clean_text(session.headers.get("Authorization")) != clean_text(api_key):
            try:
                session.close()
            except Exception:
                pass
            session = requests.Session()
            if api_key:
                session.headers.update({"Authorization": api_key})
            _wb_session_var.set(session)
        return session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolve(), name)

    def close(self) -> None:
        session = _wb_session_var.get()
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
            _wb_session_var.set(None)


PRIVATE_DIR = TenantPathProxy("tenant_root")
AUTH_DIR = TenantPathProxy("auth_dir")
LOGS_DIR = TenantPathProxy("logs_dir")
COMPLAINTS_DIR = TenantPathProxy("complaints_dir")
SCREENSHOTS_DIR = TenantPathProxy("screenshots_dir")
DATA_DIR = TenantPathProxy("data_dir")

ARCHIVE_FILE = TenantPathProxy("archive_file")
DRAFTS_FILE = TenantPathProxy("drafts_file")
RULES_FILE = TenantPathProxy("rules_file")
SYSTEM_PROMPT_FILE = TenantPathProxy("system_prompt_file")
COMPLAINT_PROMPT_FILE = TenantPathProxy("complaint_prompt_file")
UI_PROFILE_FILE = TenantPathProxy("ui_profile_file")
AUTH_STATE_FILE = TenantPathProxy("auth_state_file")
AUTH_META_FILE = TenantPathProxy("auth_meta_file")
COMPLAINT_DRAFTS_FILE = TenantPathProxy("complaint_drafts_file")
COMPLAINT_QUEUE_FILE = TenantPathProxy("complaint_queue_file")
COMPLAINT_RESULTS_FILE = TenantPathProxy("complaint_results_file")
REPLY_QUEUE_FILE = TenantPathProxy("reply_queue_file")
REPLY_SNAPSHOT_FILE = TenantPathProxy("reply_snapshot_file")
LOW_RATING_CACHE_FILE = TenantPathProxy("low_rating_cache_file")
HISTORICAL_DB_FILE = TenantPathProxy("historical_db_file")
HISTORICAL_SYNC_META_FILE = TenantPathProxy("historical_sync_meta_file")
HISTORICAL_SYNC_STOP_FILE = TenantPathProxy("historical_sync_stop_file")
HISTORICAL_SYNC_LOG_FILE = TenantPathProxy("historical_sync_log_file")
QUESTION_SNAPSHOT_FILE = TenantPathProxy("question_snapshot_file")
QUESTION_DRAFTS_FILE = TenantPathProxy("question_drafts_file")
QUESTION_QUEUE_FILE = TenantPathProxy("question_queue_file")
QUESTION_ARCHIVE_FILE = TenantPathProxy("question_archive_file")
QUESTION_CLUSTERS_FILE = TenantPathProxy("question_clusters_file")
QUESTION_SYNC_META_FILE = TenantPathProxy("question_sync_meta_file")
QUESTION_IGNORED_FILE = TenantPathProxy("question_ignored_file")

WB_SESSION = TenantSessionProxy()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


# set correct default now that clean_text exists
_wb_api_key_var = ContextVar("wb_api_key", default=clean_text(getattr(config, "WB_API_KEY", "")))


def bind_tenant_context(
    tenant_id: str,
    tenant: Optional[Dict[str, Any]] = None,
    paths: Optional[Dict[str, Any]] = None,
    wb_api_key: str = "",
) -> Dict[str, Any]:
    session = _wb_session_var.get()
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
        _wb_session_var.set(None)

    normalized_paths: Dict[str, Path] = {}
    if isinstance(paths, dict):
        for key, value in paths.items():
            if value is None:
                continue
            try:
                normalized_paths[key] = Path(str(value))
            except Exception:
                continue
    tenant_payload = tenant if isinstance(tenant, dict) else {}
    api_key = clean_text(wb_api_key or tenant_payload.get("wb_api_key") or "")
    tokens = {
        "tenant_id": _tenant_id_var.set(clean_text(tenant_id)),
        "tenant": _tenant_var.set(tenant_payload),
        "paths": _tenant_paths_var.set(normalized_paths),
        "api_key": _wb_api_key_var.set(api_key),
    }
    return tokens


def reset_tenant_context(tokens: Optional[Dict[str, Any]] = None) -> None:
    session = _wb_session_var.get()
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
        _wb_session_var.set(None)
    if not tokens:
        _tenant_id_var.set("")
        _tenant_var.set({})
        _tenant_paths_var.set({})
        _wb_api_key_var.set(clean_text(getattr(config, "WB_API_KEY", "")))
        return
    for key in ["api_key", "paths", "tenant", "tenant_id"]:
        token = tokens.get(key)
        if token is not None:
            try:
                {
                    "api_key": _wb_api_key_var,
                    "paths": _tenant_paths_var,
                    "tenant": _tenant_var,
                    "tenant_id": _tenant_id_var,
                }[key].reset(token)
            except Exception:
                pass


def get_active_tenant_id() -> str:
    return clean_text(_tenant_id_var.get())


def get_active_tenant() -> Dict[str, Any]:
    tenant = _tenant_var.get() or {}
    return tenant if isinstance(tenant, dict) else {}


def get_active_api_key() -> str:
    return clean_text(_wb_api_key_var.get())


def get_current_tenant_paths() -> Dict[str, Path]:
    paths = _tenant_paths_var.get() or {}
    return {key: Path(str(value)) for key, value in paths.items()}


def resolve_path(key: str) -> Path:
    proxy = globals().get({
        "tenant_root": "PRIVATE_DIR",
        "auth_dir": "AUTH_DIR",
        "logs_dir": "LOGS_DIR",
        "complaints_dir": "COMPLAINTS_DIR",
        "screenshots_dir": "SCREENSHOTS_DIR",
        "archive_file": "ARCHIVE_FILE",
        "drafts_file": "DRAFTS_FILE",
        "rules_file": "RULES_FILE",
        "system_prompt_file": "SYSTEM_PROMPT_FILE",
        "complaint_prompt_file": "COMPLAINT_PROMPT_FILE",
        "ui_profile_file": "UI_PROFILE_FILE",
        "auth_state_file": "AUTH_STATE_FILE",
        "auth_meta_file": "AUTH_META_FILE",
        "complaint_drafts_file": "COMPLAINT_DRAFTS_FILE",
        "complaint_queue_file": "COMPLAINT_QUEUE_FILE",
        "complaint_results_file": "COMPLAINT_RESULTS_FILE",
        "reply_queue_file": "REPLY_QUEUE_FILE",
        "reply_snapshot_file": "REPLY_SNAPSHOT_FILE",
        "low_rating_cache_file": "LOW_RATING_CACHE_FILE",
        "historical_db_file": "HISTORICAL_DB_FILE",
        "historical_sync_meta_file": "HISTORICAL_SYNC_META_FILE",
        "historical_sync_stop_file": "HISTORICAL_SYNC_STOP_FILE",
        "historical_sync_log_file": "HISTORICAL_SYNC_LOG_FILE",
        "question_snapshot_file": "QUESTION_SNAPSHOT_FILE",
        "question_drafts_file": "QUESTION_DRAFTS_FILE",
        "question_queue_file": "QUESTION_QUEUE_FILE",
        "question_archive_file": "QUESTION_ARCHIVE_FILE",
        "question_clusters_file": "QUESTION_CLUSTERS_FILE",
        "question_sync_meta_file": "QUESTION_SYNC_META_FILE",
        "question_ignored_file": "QUESTION_IGNORED_FILE",
    }.get(key, ""))
    if isinstance(proxy, TenantPathProxy):
        return proxy.resolve()
    factory = _path_factories[key]
    return Path(factory())


def get_wb_headers() -> Dict[str, str]:
    api_key = get_active_api_key()
    return {"Authorization": api_key} if api_key else {}


def get_wb_session() -> requests.Session:
    return WB_SESSION._resolve()


def __getattr__(name: str) -> Any:
    if name == "ACTIVE_TENANT_ID":
        return get_active_tenant_id()
    if name == "ACTIVE_TENANT":
        return get_active_tenant()
    if name == "WB_HEADERS":
        return get_wb_headers()
    raise AttributeError(name)


def clean_text_preserve_lines(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def read_json(path: Path, default: Any) -> Any:
    return safe_files.read_json(Path(path), default)


def write_json(path: Path, data: Any) -> None:
    safe_files.write_json(Path(path), data, ensure_ascii=False, indent=2)


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    safe_files.append_jsonl(Path(path), row, ensure_ascii=False)


def _ensure_shared_seed(dst: Path, src: Path, fallback: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return
    if src.exists():
        safe_files.write_text(dst, src.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        safe_files.write_text(dst, fallback, encoding="utf-8")


def build_review_text(review: Dict[str, Any]) -> str:
    parts: List[str] = []
    text = clean_text_preserve_lines(review.get("text"))
    pros = clean_text_preserve_lines(review.get("pros"))
    cons = clean_text_preserve_lines(review.get("cons"))

    if text:
        parts.append(f"Текст: {text}")
    if pros:
        parts.append(f"Плюсы: {pros}")
    if cons:
        parts.append(f"Минусы: {cons}")

    if not parts:
        return "Покупатель поставил оценку без текста."

    return "\n".join(parts)


def review_signature(review: Dict[str, Any]) -> str:
    payload = {
        "id": clean_text(review.get("id")),
        "text": clean_text(review.get("text")),
        "pros": clean_text(review.get("pros")),
        "cons": clean_text(review.get("cons")),
        "stars": int(review.get("productValuation", 0) or 0),
        "product": clean_text(review.get("productDetails", {}).get("productName")),
        "article": clean_text(review.get("productDetails", {}).get("supplierArticle")),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_review(review: Dict[str, Any]) -> Dict[str, Any]:
    product = review.get("productDetails", {}) or {}
    return {
        "id": clean_text(review.get("id")),
        "text": clean_text_preserve_lines(review.get("text")),
        "pros": clean_text_preserve_lines(review.get("pros")),
        "cons": clean_text_preserve_lines(review.get("cons")),
        "productValuation": int(review.get("productValuation", 0) or 0),
        "createdDate": clean_text(review.get("createdDate")),
        "userName": clean_text(review.get("userName")),
        "subjectName": clean_text(review.get("subjectName")),
        "productDetails": {
            "productName": clean_text(product.get("productName")),
            "supplierArticle": clean_text(product.get("supplierArticle")),
            "brandName": clean_text(product.get("brandName")),
            "nmId": int(product.get("nmId", 0) or 0),
        },
    }


def normalize_question(question: Dict[str, Any]) -> Dict[str, Any]:
    product = question.get("productDetails", {}) or {}
    answer = question.get("answer") or {}
    return {
        "id": clean_text(question.get("id")),
        "text": clean_text_preserve_lines(question.get("text")),
        "createdDate": clean_text(question.get("createdDate")),
        "state": clean_text(question.get("state")),
        "subjectName": clean_text(question.get("subjectName")),
        "answer": {
            "text": clean_text_preserve_lines(answer.get("text")),
            "state": clean_text(answer.get("state")),
            "editable": bool(answer.get("editable")),
            "createDate": clean_text(answer.get("createDate")),
        }
        if answer
        else None,
        "productDetails": {
            "imtId": int(product.get("imtId", 0) or 0),
            "nmId": int(product.get("nmId", 0) or 0),
            "productName": clean_text(product.get("productName")),
            "supplierArticle": clean_text(product.get("supplierArticle")),
            "supplierName": clean_text(product.get("supplierName")),
            "brandName": clean_text(product.get("brandName")),
            "size": clean_text(product.get("size")),
        },
        "wasViewed": bool(question.get("wasViewed")),
        "isWarned": bool(question.get("isWarned")),
    }


def restore_review_from_form(form: Any) -> Dict[str, Any]:
    return normalize_review(
        {
            "id": clean_text(form.get("review_id")),
            "text": clean_text_preserve_lines(form.get("text")),
            "pros": clean_text_preserve_lines(form.get("pros")),
            "cons": clean_text_preserve_lines(form.get("cons")),
            "productValuation": int(form.get("stars", 0) or 0),
            "createdDate": clean_text(form.get("created_date")),
            "userName": clean_text(form.get("user_name")),
            "subjectName": clean_text(form.get("subject_name")),
            "productDetails": {
                "productName": clean_text(form.get("product_name")),
                "supplierArticle": clean_text(form.get("supplier_article")),
                "brandName": clean_text(form.get("brand_name")),
                "nmId": int(form.get("nm_id", 0) or 0),
            },
        }
    )


def load_rules() -> Dict[str, Any]:
    return read_json(RULES_FILE, {"default_instructions": [], "special_cases": [], "cross_sell_catalog": []})


def load_system_prompt() -> str:
    if SYSTEM_PROMPT_FILE.exists():
        return SYSTEM_PROMPT_FILE.read_text(encoding="utf-8")
    return "Ты опытный менеджер по отзывам продавца на Wildberries. Отвечай по-русски, кратко, спокойно и без канцелярита."


def load_complaint_prompt() -> str:
    if COMPLAINT_PROMPT_FILE.exists():
        return COMPLAINT_PROMPT_FILE.read_text(encoding="utf-8")
    return ""


def load_question_prompt() -> str:
    _ensure_shared_seed(
        QUESTION_PROMPT_FILE,
        QUESTION_PROMPT_TEMPLATE_FILE,
        "Ты отвечаешь на вопросы покупателей по товарам на Wildberries. Отвечай подробно, полезно для покупателя и с приоритетом инструкции менеджера.",
    )
    return QUESTION_PROMPT_FILE.read_text(encoding="utf-8")


def load_question_rules() -> Dict[str, Any]:
    _ensure_shared_seed(
        QUESTION_RULES_FILE,
        QUESTION_RULES_TEMPLATE_FILE,
        json.dumps({"default_instructions": [], "auto_queue_confidence": 0.92, "rules": []}, ensure_ascii=False, indent=2),
    )
    data = read_json(QUESTION_RULES_FILE, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("default_instructions", [])
    data.setdefault("auto_queue_confidence", 0.92)
    data.setdefault("rules", [])
    return data


def save_question_prompt(text: str) -> None:
    _ensure_shared_seed(
        QUESTION_PROMPT_FILE,
        QUESTION_PROMPT_TEMPLATE_FILE,
        "Ты отвечаешь на вопросы покупателей по товарам на Wildberries. Отвечай подробно, полезно для покупателя и с приоритетом инструкции менеджера.",
    )
    safe_files.write_text(Path(QUESTION_PROMPT_FILE), clean_text_preserve_lines(text) or load_question_prompt(), encoding="utf-8")


def save_question_rules(data: Dict[str, Any]) -> None:
    _ensure_shared_seed(
        QUESTION_RULES_FILE,
        QUESTION_RULES_TEMPLATE_FILE,
        json.dumps({"default_instructions": [], "auto_queue_confidence": 0.92, "rules": []}, ensure_ascii=False, indent=2),
    )
    write_json(QUESTION_RULES_FILE, data)


def load_ui_profile() -> Dict[str, Any]:
    return read_json(UI_PROFILE_FILE, {})


def _wb_json_get(url: str, params: dict) -> dict:
    response = WB_SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(payload.get("errorText") or "WB API вернул ошибку")
    return payload


def _wb_json_patch(url: str, payload: dict) -> dict:
    response = WB_SESSION.patch(url, json=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    if not response.content:
        return {"data": None, "error": False, "errorText": "", "additionalErrors": None}
    data = response.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(data.get("errorText") or "WB API вернул ошибку")
    return data


def fetch_feedbacks_page(
    is_answered: bool,
    skip: int = 0,
    take: int = 50,
    order: str = "dateDesc",
    date_from: int | None = None,
    date_to: int | None = None,
    nm_id: int | None = None,
) -> tuple[list[dict], int, int, dict]:
    params = {
        "isAnswered": bool(is_answered),
        "take": take,
        "skip": skip,
        "order": order,
    }
    if date_from is not None:
        params["dateFrom"] = int(date_from)
    if date_to is not None:
        params["dateTo"] = int(date_to)
    if nm_id:
        params["nmId"] = int(nm_id)
    payload = _wb_json_get(WB_LIST_URL, params)
    data = payload.get("data") or {}
    feedbacks = data.get("feedbacks") or []
    count_unanswered = int(data.get("countUnanswered") or 0)
    count_archive = int(data.get("countArchive") or 0)
    return feedbacks, count_unanswered, count_archive, data


def fetch_archive_feedbacks_page(skip: int = 0, take: int = 1000, order: str = "dateAsc", nm_id: int | None = None) -> tuple[list[dict], dict]:
    params = {
        "take": take,
        "skip": skip,
        "order": order,
    }
    if nm_id:
        params["nmId"] = int(nm_id)
    payload = _wb_json_get(f"{WB_LIST_URL}/archive", params)
    data = payload.get("data") or {}
    feedbacks = data.get("feedbacks") or []
    return feedbacks, data


def fetch_pending_reviews(skip: int = 0, take: int = 50) -> tuple[list[dict], int, int]:
    feedbacks, count_unanswered, count_archive, _ = fetch_feedbacks_page(False, skip=skip, take=take, order="dateDesc")
    normalized = [normalize_review(fb) for fb in feedbacks]
    return normalized, count_unanswered, count_archive


def fetch_unseen_feedbacks_questions() -> Dict[str, bool]:
    payload = _wb_json_get(WB_NEW_ITEMS_URL, {})
    data = payload.get("data") or {}
    return {
        "hasNewQuestions": bool(data.get("hasNewQuestions")),
        "hasNewFeedbacks": bool(data.get("hasNewFeedbacks")),
    }


def fetch_questions_unanswered_counts() -> Dict[str, int]:
    payload = _wb_json_get(WB_QUESTIONS_COUNT_UNANSWERED_URL, {})
    data = payload.get("data") or {}
    return {
        "countUnanswered": int(data.get("countUnanswered") or 0),
        "countUnansweredToday": int(data.get("countUnansweredToday") or 0),
    }


def fetch_questions_count(
    is_answered: bool | None = None,
    date_from: int | None = None,
    date_to: int | None = None,
) -> int:
    params: Dict[str, Any] = {}
    if is_answered is not None:
        params["isAnswered"] = bool(is_answered)
    if date_from is not None:
        params["dateFrom"] = int(date_from)
    if date_to is not None:
        params["dateTo"] = int(date_to)
    payload = _wb_json_get(WB_QUESTIONS_COUNT_URL, params)
    return int((payload.get("data") or 0) or 0)


def fetch_questions_page(
    is_answered: bool,
    skip: int = 0,
    take: int = 50,
    order: str = "dateDesc",
    date_from: int | None = None,
    date_to: int | None = None,
    nm_id: int | None = None,
) -> tuple[list[dict], int, int, dict]:
    params = {
        "isAnswered": bool(is_answered),
        "take": int(take),
        "skip": int(skip),
        "order": order,
    }
    if date_from is not None:
        params["dateFrom"] = int(date_from)
    if date_to is not None:
        params["dateTo"] = int(date_to)
    if nm_id:
        params["nmId"] = int(nm_id)
    payload = _wb_json_get(WB_QUESTIONS_LIST_URL, params)
    data = payload.get("data") or {}
    questions = data.get("questions") or []
    count_unanswered = int(data.get("countUnanswered") or 0)
    count_archive = int(data.get("countArchive") or 0)
    return questions, count_unanswered, count_archive, data


def fetch_question_by_id(question_id: str) -> Dict[str, Any]:
    payload = _wb_json_get(WB_QUESTION_URL, {"id": clean_text(question_id)})
    return normalize_question((payload.get("data") or {}))


def patch_question(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _wb_json_patch(WB_QUESTIONS_LIST_URL, payload)


def extract_json_object(text: str) -> dict:
    text = clean_text_preserve_lines(text)
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.S)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}
    return {}


def to_unix_timestamp(value: datetime | str | int | float | None) -> int:
    if value is None:
        raise ValueError("Не передано значение даты для преобразования в Unix timestamp")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = clean_text(value)
        if not text:
            raise ValueError("Пустая строка даты")
        for candidate in [text, text.replace("Z", "+00:00")]:
            try:
                dt = datetime.fromisoformat(candidate)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                continue
        raise ValueError(f"Не удалось разобрать дату: {text}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _get_openai_api_key() -> str:
    primary_value = clean_text(getattr(config, "OPENAI_API_KEY", ""))
    if primary_value:
        return primary_value
    for candidate in ["OPENAI_API_KEY", "HYDRAAI_API_KEY", "HYDRA_API_KEY", "OPENAI_KEY"]:
        value = clean_text(os.getenv(candidate, ""))
        if value:
            return value
    return ""


def get_ai_runtime_diagnostics() -> Dict[str, Any]:
    return {
        "key_present": bool(_get_openai_api_key()),
        "key_source": clean_text(getattr(config, "OPENAI_API_KEY_SOURCE", "")),
        "base_url": clean_text(OPENAI_BASE_URL),
        "base_url_source": clean_text(getattr(config, "OPENAI_BASE_URL_SOURCE", "")),
        "model": clean_text(OPENAI_MODEL),
        "model_source": clean_text(getattr(config, "OPENAI_MODEL_SOURCE", "")),
        "complaint_model": clean_text(OPENAI_COMPLAINT_MODEL),
        "complaint_model_source": clean_text(getattr(config, "OPENAI_COMPLAINT_MODEL_SOURCE", "")),
        "fallback_models": _configured_openai_fallback_models(),
        "client_library_present": bool(OpenAI is not None),
        "private_root": str(PRIVATE_ROOT),
        "security_dir": clean_text(getattr(config, "SECURITY_DIR", "")),
    }


def _configured_openai_fallback_models() -> List[str]:
    raw_value = getattr(config, "OPENAI_FALLBACK_MODELS", os.getenv("OPENAI_FALLBACK_MODELS", ""))
    if isinstance(raw_value, (list, tuple, set)):
        raw_items = list(raw_value)
    else:
        raw_items = re.split(r"[,;\n]+", str(raw_value or ""))
    models: List[str] = []
    for item in raw_items:
        model_name = clean_text(item)
        if model_name and model_name not in models:
            models.append(model_name)
    return models


def _build_ai_model_candidates(preferred_model: Optional[str] = None) -> List[str]:
    candidates: List[str] = []
    for item in [preferred_model, OPENAI_MODEL, OPENAI_COMPLAINT_MODEL, *_configured_openai_fallback_models()]:
        model_name = clean_text(item)
        if model_name and model_name not in candidates:
            candidates.append(model_name)
    return candidates or ["gpt-4o-mini"]


def describe_ai_failure(error: Exception, model: str = "") -> Dict[str, str]:
    raw_message = clean_text(str(error))
    lower = raw_message.lower()
    model_name = clean_text(model)
    model_hint = f" Модель: {model_name}." if model_name else ""
    code = "ai_unavailable"
    public_message = "AI недоступен. Проверьте ключ, модель и доступ к провайдеру."

    if "openai>=1.0" in lower or ("пакет openai" in lower and "недоступен" in lower):
        code = "dependency_missing"
        public_message = "Пакет openai недоступен. Установите зависимость openai>=1.0."
    elif any(token in lower for token in ["authentication", "unauthorized", "401", "invalid api key", "incorrect api key", "forbidden", "permission"]):
        code = "auth_failed"
        public_message = "AI API отклонил ключ доступа. Проверьте OPENAI_API_KEY и base URL."
    elif any(token in lower for token in ["openai_api_key", "api key", "apikey", "api-key"]) and any(token in lower for token in ["not set", "missing", "required", "не задан", "не указан"]):
        code = "config_missing"
        public_message = "Не задан ключ AI. Заполните OPENAI_API_KEY / HYDRAAI_API_KEY или сохраните ключ в security/openai_api_key.txt."
    elif any(token in lower for token in ["model", "модель"]) and any(token in lower for token in ["not found", "does not exist", "unsupported", "unknown", "unavailable", "not available", "404"]):
        code = "model_unavailable"
        public_message = f"У провайдера недоступна выбранная AI-модель.{model_hint} Проверьте OPENAI_MODEL и OPENAI_COMPLAINT_MODEL."
    elif any(token in lower for token in ["rate limit", "429", "too many requests"]):
        code = "rate_limit"
        public_message = "AI API временно ограничил запросы. Повторите попытку чуть позже."
    elif any(token in lower for token in ["context length", "maximum context", "token limit", "too many tokens", "prompt is too long"]):
        code = "prompt_too_large"
        public_message = f"Запрос к AI оказался слишком большим.{model_hint} Сократите промпт или правила."
    elif any(token in lower for token in ["connection", "timed out", "timeout", "dns", "temporarily unavailable", "service unavailable", "network", "api connection", "bad gateway", "gateway timeout", "remote disconnected"]):
        code = "network"
        public_message = "Нет устойчивого соединения с AI API. Проверьте сеть, base URL и доступность провайдера."
    elif raw_message:
        public_message = f"AI вызов завершился ошибкой: {raw_message}"

    return {
        "code": code,
        "public_message": public_message,
        "raw_message": raw_message,
        "model": model_name,
    }


def classify_ai_error(exc: Exception) -> Dict[str, Any]:
    info = describe_ai_failure(exc)
    return {"type": info.get("code") or "unknown", "message": info.get("raw_message") or clean_text(exc)}


def _ai_error_allows_model_fallback(error: Exception, model: str = "") -> bool:
    info = describe_ai_failure(error, model=model)
    return info.get("code") == "model_unavailable"


def _extract_ai_response_text(response: Any) -> str:
    try:
        content = response.choices[0].message.content
    except Exception as exc:
        raise RuntimeError("AI вернул неожиданный ответ без текста.") from exc

    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                fragment = item.get("text") or item.get("content") or item.get("value")
                if fragment:
                    parts.append(str(fragment))
                continue
            fragment = getattr(item, "text", None) or getattr(item, "content", None)
            if fragment:
                parts.append(str(fragment))
        text = "\n".join(parts)
    else:
        text = str(content or "")

    text = clean_text_preserve_lines(text)
    if not text:
        raise RuntimeError("AI вернул пустой ответ.")
    return text


def _get_openai_client() -> Any:
    global _OPENAI_CLIENT, _OPENAI_CLIENT_KEY, _OPENAI_CLIENT_BASE_URL
    if OpenAI is None:
        raise RuntimeError("Пакет openai недоступен. Установите зависимость openai>=1.0.")
    api_key = _get_openai_api_key()
    if not api_key:
        raise RuntimeError("Не задан OPENAI_API_KEY. Укажите ключ в окружении или config.py.")
    base_url = clean_text(OPENAI_BASE_URL)
    if _OPENAI_CLIENT is not None and _OPENAI_CLIENT_KEY == api_key and _OPENAI_CLIENT_BASE_URL == base_url:
        return _OPENAI_CLIENT
    _OPENAI_CLIENT = OpenAI(api_key=api_key, base_url=OPENAI_BASE_URL)
    _OPENAI_CLIENT_KEY = api_key
    _OPENAI_CLIENT_BASE_URL = base_url
    return _OPENAI_CLIENT


def call_ai(messages: list[dict], model: Optional[str] = None, temperature: float = 0.25) -> str:
    global _LAST_AI_CALL_TS

    model_candidates = _build_ai_model_candidates(model)
    now = time.monotonic()
    sleep_for = AI_MIN_INTERVAL_SEC - (now - _LAST_AI_CALL_TS)
    if sleep_for > 0:
        time.sleep(sleep_for)

    last_error: Optional[Exception] = None
    client = _get_openai_client()
    attempted_models: List[str] = []
    for target_model in model_candidates:
        for attempt in range(3):
            attempted_models.append(target_model)
            try:
                response = client.chat.completions.create(
                    model=target_model,
                    messages=messages,
                    temperature=temperature,
                )
                _LAST_AI_CALL_TS = time.monotonic()
                content = _extract_ai_response_text(response)
                log_event('ai', 'call_success', tenant_id=get_active_tenant_id(), model=target_model, attempt=attempt + 1, response_len=len(content), base_url=OPENAI_BASE_URL)
                return content
            except RateLimitError as exc:
                last_error = exc
                info = classify_ai_error(exc)
                log_event('ai', 'call_retry', tenant_id=get_active_tenant_id(), level='warning', model=target_model, attempt=attempt + 1, error_type=info['type'], error=info['message'], base_url=OPENAI_BASE_URL)
                time.sleep(2 + attempt * 2)
                continue
            except Exception as exc:
                last_error = exc
                failure = describe_ai_failure(exc, model=target_model)
                log_event('ai', 'call_failed', tenant_id=get_active_tenant_id(), level='error', model=target_model, attempt=attempt + 1, error_type=failure['code'], error=failure['raw_message'], public_message=failure['public_message'], base_url=OPENAI_BASE_URL)
                if _ai_error_allows_model_fallback(exc, model=target_model):
                    break
                raise exc
        else:
            continue
        if last_error is not None and _ai_error_allows_model_fallback(last_error, model=target_model):
            continue
        break

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Не удалось получить ответ от AI. Проверенные модели: {', '.join(attempted_models) or ', '.join(model_candidates)}")
