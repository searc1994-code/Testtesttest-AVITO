from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse, urlunparse

import common
import safe_files
from complaint_core import append_result, load_complaint_queue, save_complaint_queue, reconcile_complaint_queue
from safe_logs import log_event

try:
    import background_jobs
except Exception:  # pragma: no cover - optional during isolated checks
    background_jobs = None

clean_text = common.clean_text
read_json = common.read_json
write_json = common.write_json
utc_now_iso = common.utc_now_iso

PRIVATE_DIR = common.PRIVATE_DIR
AUTH_STATE_FILE = common.AUTH_STATE_FILE
AUTH_META_FILE = common.AUTH_META_FILE
SCREENSHOTS_DIR = common.SCREENSHOTS_DIR
COMPLAINTS_DIR = common.COMPLAINTS_DIR
PLAYWRIGHT_HEADLESS = bool(getattr(common, "PLAYWRIGHT_HEADLESS", False))
PLAYWRIGHT_SLOW_MO_MS = int(getattr(common, "PLAYWRIGHT_SLOW_MO_MS", 250) or 250)
PLAYWRIGHT_BROWSER_CHANNEL = getattr(common, "PLAYWRIGHT_BROWSER_CHANNEL", None)
WB_SELLER_BASE_URL = getattr(common, "WB_SELLER_BASE_URL", "https://seller.wildberries.ru")
PLAYWRIGHT_DEFAULT_TIMEOUT_MS = int(getattr(getattr(common, "config", object()), "PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 15000) or 15000)
PLAYWRIGHT_NAVIGATION_TIMEOUT_MS = int(getattr(getattr(common, "config", object()), "PLAYWRIGHT_NAVIGATION_TIMEOUT_MS", 45000) or 45000)
WB_BROWSER_ALLOWED_NAV_HOSTS = tuple(clean_text(x).lower() for x in (getattr(getattr(common, "config", object()), "WB_BROWSER_ALLOWED_NAV_HOSTS", ["seller.wildberries.ru"]) or ["seller.wildberries.ru"]))
WB_BROWSER_ALLOWED_REQUEST_SUFFIXES = tuple(clean_text(x).lower() for x in (getattr(getattr(common, "config", object()), "WB_BROWSER_ALLOWED_REQUEST_SUFFIXES", ["wildberries.ru", "wb.ru", "wbstatic.net"]) or ["wildberries.ru", "wb.ru", "wbstatic.net"]))
_load_ui_profile = getattr(common, "load_ui_profile", None)

try:
    from zoneinfo import ZoneInfo
    MOSCOW_TZ = ZoneInfo("Europe/Moscow")
except Exception:
    MOSCOW_TZ = timezone(timedelta(hours=3))

CATEGORY_ID_MAP = {
    "Отзыв оставили конкуренты": "12",
    "Отзыв не относится к товару": "11",
    "Спам-реклама в тексте": "13",
    "Нецензурная лексика": "16",
    "Отзыв с политическим контекстом": "18",
    "Угрозы, оскорбления": "20",
    "Другое": "19",
}

BROWSER_FAILURE_BREAKER_THRESHOLD = max(2, int(getattr(getattr(common, "config", object()), "BROWSER_FAILURE_BREAKER_THRESHOLD", 3) or 3))
BROWSER_FAILURE_BREAKER_WINDOW_SECONDS = max(300, int(getattr(getattr(common, "config", object()), "BROWSER_FAILURE_BREAKER_WINDOW_SECONDS", 6 * 60 * 60) or 21600))

DEFAULT_PROFILE = {
    "search_urls": [
        "https://seller.wildberries.ru/feedbacks/feedbacks-tab/not-answered?feedbacks-module_searchValue={search_value}&feedbacks-module_createdPeriod={created_period}&feedbacks-module_valuations={valuation}&feedbacks-module_content={content}",
        "https://seller.wildberries.ru/feedbacks/feedbacks-tab/answered?feedbacks-module_searchValue={search_value}&feedbacks-module_createdPeriod={created_period}&feedbacks-module_valuations={valuation}&feedbacks-module_content={content}",
    ],
    "complaint_menu_texts": ["Пожаловаться на отзыв", "Пожаловаться"],
    "submit_button_texts": ["Отправить"],
    "success_hints": [
        "Жалоба отправлена",
        "Спасибо, мы рассмотрим",
        "отправлена",
        "принята",
        "Мои жалобы",
    ],
    "overlay_close_texts": ["Закрыть", "Понятно", "Понял", "×"],
}


class BrowserBotError(RuntimeError):
    pass


def _job_progress(message: str = "", *, stage: str = "", current: Any = None, total: Any = None, percent: Any = None, **metrics: Any) -> None:
    if background_jobs is None:
        return
    try:
        background_jobs.report_progress(message, stage=stage, current=current, total=total, percent=percent, **metrics)
    except Exception:
        return


def _body_debug_snippet(page, limit: int = 500) -> str:
    try:
        text = _collapse_spaces(page.locator("body").inner_text(timeout=1200))
    except Exception:
        try:
            text = _collapse_spaces(page.locator("body").inner_text())
        except Exception:
            text = ""
    return text[:limit]


def _allowed_host(url: str) -> str:
    try:
        parsed = urlparse(str(url or "").strip())
        return clean_text(parsed.hostname).lower()
    except Exception:
        return ""


def _is_allowed_navigation_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"https", "http"}:
        return False
    host = clean_text(parsed.hostname).lower()
    if not host:
        return False
    return host in WB_BROWSER_ALLOWED_NAV_HOSTS


def _is_allowed_request_url(url: str) -> bool:
    raw = str(url or "").strip()
    if raw.startswith(("data:", "blob:", "about:")):
        return True
    parsed = urlparse(raw)
    if parsed.scheme not in {"https", "http"}:
        return False
    host = clean_text(parsed.hostname).lower()
    if not host:
        return False
    if host in WB_BROWSER_ALLOWED_NAV_HOSTS:
        return True
    return any(host.endswith(suffix) for suffix in WB_BROWSER_ALLOWED_REQUEST_SUFFIXES)


def _assert_allowed_navigation_url(url: str) -> None:
    if not _is_allowed_navigation_url(url):
        raise BrowserBotError(f"Запрещён переход по URL вне разрешённых адресов WB: {clean_text(url)}")


def _install_context_guards(context) -> None:
    try:
        context.set_default_timeout(PLAYWRIGHT_DEFAULT_TIMEOUT_MS)
    except Exception:
        pass
    try:
        context.set_default_navigation_timeout(PLAYWRIGHT_NAVIGATION_TIMEOUT_MS)
    except Exception:
        pass

    # В жалобах не перехватываем и не режем фоновые запросы.
    # После ужесточения guards у WB могли не подгружаться строки отзывов,
    # потому что seller-интерфейс тянет данные и статику не только с seller.wildberries.ru.
    # Защиту top-level navigation оставляем в _safe_page_goto(), а subrequests не ломаем.
    return None

def _safe_page_goto(page, url: str) -> None:
    _assert_allowed_navigation_url(url)
    page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAVIGATION_TIMEOUT_MS)


def _close_context(browser=None, context=None) -> None:
    try:
        if context is not None:
            try:
                context.clear_cookies()
            except Exception:
                pass
            context.close()
    except Exception:
        pass
    try:
        if browser is not None:
            browser.close()
    except Exception:
        pass


OUTCOME_ACCEPTED_PATTERNS = [
    "жалоба принята",
    "жалоба удовлетворена",
    "жалоба одобрена",
    "принята",
]
OUTCOME_REJECTED_PATTERNS = [
    "жалоба отклонена",
    "жалоба отклонено",
    "отклонена",
    "отклонено",
]
OUTCOME_PENDING_PATTERNS = [
    "жалоба отправлена",
    "на рассмотрении",
    "на проверке",
    "жалоба рассматривается",
    "рассматривается",
]


def _detect_row_outcome(row) -> str | None:
    text = _row_text(row).lower()
    if not text:
        return None
    if any(p in text for p in OUTCOME_REJECTED_PATTERNS):
        return "rejected"
    if any(p in text for p in OUTCOME_ACCEPTED_PATTERNS):
        return "accepted"
    if any(p in text for p in OUTCOME_PENDING_PATTERNS):
        return "pending"
    return None


def _status_for_queue_and_result(raw_status: str) -> str:
    raw = clean_text(raw_status)
    if raw in {"submitted", "submitted_click_only", "submitted_verified", "success", "already_complained", "already_submitted"}:
        return "pending"
    return raw


def _load_profile() -> Dict[str, Any]:
    profile = dict(DEFAULT_PROFILE)
    if callable(_load_ui_profile):
        try:
            custom = _load_ui_profile() or {}
            if isinstance(custom, dict):
                for key, value in custom.items():
                    if value:
                        profile[key] = value
        except Exception:
            pass
    return profile


def _safe_name(value: Any) -> str:
    text = clean_text(value) or "artifact"
    chunks: list[str] = []
    for char in text:
        if char.isalnum() or char in {"-", "_", "."}:
            chunks.append(char)
        else:
            chunks.append("-")
    return "".join(chunks).strip("-._") or "artifact"


def _profile_version(profile: Dict[str, Any]) -> str:
    explicit = clean_text(profile.get("version"))
    if explicit:
        return explicit
    try:
        payload = json.dumps(profile, ensure_ascii=False, sort_keys=True).encode("utf-8")
    except Exception:
        payload = repr(sorted(profile.items())).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:12]


def _page_kind_from_url(url: str) -> str:
    lowered = clean_text(url).lower()
    if "feedbacks-tab/not-answered" in lowered:
        return "not_answered"
    if "feedbacks-tab/answered" in lowered:
        return "answered"
    if "/reviews" in lowered:
        return "reviews"
    if not lowered:
        return "unknown"
    return "other"


def _failure_tracker_file() -> Path:
    return Path(str(COMPLAINTS_DIR)) / "browser_failure_tracker.json"


def _load_failure_tracker() -> Dict[str, Any]:
    data = read_json(_failure_tracker_file(), {})
    if not isinstance(data, dict):
        return {"items": {}}
    items = data.get("items") if isinstance(data.get("items"), dict) else {}
    data["items"] = items
    return data


def _save_failure_tracker(data: Dict[str, Any]) -> None:
    write_json(_failure_tracker_file(), data)


def _failure_tracker_key(review_id: str, error_code: str, profile_version: str) -> str:
    return f"{clean_text(review_id) or 'unknown'}::{clean_text(error_code) or 'unknown'}::{clean_text(profile_version) or 'default'}"


def _breaker_preflight(review_id: str, profile_version: str) -> Dict[str, Any]:
    tracker = _load_failure_tracker()
    items = tracker.get("items") if isinstance(tracker.get("items"), dict) else {}
    now_ts = time.time()
    prefix = f"{clean_text(review_id) or 'unknown'}::"
    suffix = f"::{clean_text(profile_version) or 'default'}"
    for key, row in items.items():
        if not str(key).startswith(prefix) or not str(key).endswith(suffix) or not isinstance(row, dict):
            continue
        last_failed_ts = float(row.get("last_failed_ts") or 0.0)
        failures = int(row.get("failures") or 0)
        if failures < BROWSER_FAILURE_BREAKER_THRESHOLD:
            continue
        if last_failed_ts and now_ts - last_failed_ts > BROWSER_FAILURE_BREAKER_WINDOW_SECONDS:
            continue
        return {
            "key": key,
            "error_code": clean_text(row.get("error_code")),
            "failures": failures,
            "last_error": clean_text(row.get("last_error")),
            "last_page_kind": clean_text(row.get("last_page_kind")),
            "last_profile_version": clean_text(row.get("profile_version")),
            "last_forensics_path": clean_text(row.get("last_forensics_path")),
        }
    return {}


def _reset_failure_tracker(review_id: str, profile_version: str) -> None:
    tracker = _load_failure_tracker()
    items = tracker.get("items") if isinstance(tracker.get("items"), dict) else {}
    prefix = f"{clean_text(review_id) or 'unknown'}::"
    suffix = f"::{clean_text(profile_version) or 'default'}"
    changed = False
    for key in list(items.keys()):
        if str(key).startswith(prefix) and str(key).endswith(suffix):
            items.pop(key, None)
            changed = True
    if changed:
        tracker["items"] = items
        _save_failure_tracker(tracker)


def _register_failure(review_id: str, error_code: str, profile_version: str, *, page_kind: str = "", error_text: str = "", forensics_path: str = "", run_id: str = "") -> Dict[str, Any]:
    tracker = _load_failure_tracker()
    items = tracker.get("items") if isinstance(tracker.get("items"), dict) else {}
    key = _failure_tracker_key(review_id, error_code, profile_version)
    row = items.get(key) if isinstance(items.get(key), dict) else {}
    now_text = utc_now_iso()
    now_ts = time.time()
    previous_failed_ts = float(row.get("last_failed_ts") or 0.0)
    failures = int(row.get("failures") or 0)
    if not previous_failed_ts or now_ts - previous_failed_ts > BROWSER_FAILURE_BREAKER_WINDOW_SECONDS:
        failures = 0
    failures += 1
    row.update({
        "review_id": clean_text(review_id),
        "error_code": clean_text(error_code),
        "profile_version": clean_text(profile_version),
        "last_page_kind": clean_text(page_kind),
        "last_error": clean_text(error_text),
        "last_forensics_path": clean_text(forensics_path),
        "last_run_id": clean_text(run_id),
        "last_failed_at": now_text,
        "last_failed_ts": now_ts,
        "failures": failures,
    })
    items[key] = row
    tracker["items"] = items
    _save_failure_tracker(tracker)
    return dict(row)


def _capture_failure_artifacts(page, *, review_id: str, run_id: str, error_code: str, profile_version: str, page_kind: str, note: str = "") -> Dict[str, str]:
    base_dir = Path(str(COMPLAINTS_DIR)) / "forensics" / _safe_name(review_id or "unknown")
    base_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{_safe_name(review_id or 'unknown')}__{_safe_name(run_id or utc_now_iso())}__{_safe_name(error_code or 'error')}"
    screenshot_path = base_dir / f"{stem}.png"
    html_path = base_dir / f"{stem}.html"
    meta_path = base_dir / f"{stem}.json"
    url = ""
    title = ""
    body_snippet = ""
    if page is not None:
        try:
            page.screenshot(path=str(screenshot_path), full_page=True)
        except Exception:
            pass
        try:
            safe_files.write_text(html_path, page.content(), encoding="utf-8")
        except Exception:
            pass
        try:
            url = clean_text(getattr(page, "url", ""))
        except Exception:
            url = ""
        try:
            title = clean_text(page.title())
        except Exception:
            title = ""
        try:
            body_snippet = _body_debug_snippet(page, limit=1200)
        except Exception:
            body_snippet = ""
    write_json(meta_path, {
        "captured_at": utc_now_iso(),
        "review_id": clean_text(review_id),
        "run_id": clean_text(run_id),
        "error_code": clean_text(error_code),
        "profile_version": clean_text(profile_version),
        "page_kind": clean_text(page_kind),
        "note": clean_text(note),
        "url": url,
        "title": title,
        "body_snippet": body_snippet,
        "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
        "html_path": str(html_path) if html_path.exists() else "",
    })
    return {
        "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
        "html_path": str(html_path) if html_path.exists() else "",
        "meta_path": str(meta_path),
    }


def _classify_browser_error(exc: Exception, page=None) -> str:
    text = clean_text(exc).lower()
    url = clean_text(getattr(page, "url", "") if page is not None else "").lower()
    try:
        if page is not None and _page_requests_auth(page):
            return "auth_expired"
    except Exception:
        pass
    if "has been closed" in text or "target page, context or browser has been closed" in text:
        return "browser_closed"
    if "timed out" in text or "timeout" in text:
        return "network_timeout"
    if "не удалось нажать кнопку" in text or "не удалось выбрать категорию" in text or "не удалось вставить текст причины" in text:
        return "selector_missing"
    if "на странице нет видимых строк отзывов" in text or "не удалось найти нужный отзыв" in text or "таблиц" in text:
        return "empty_filtered_table"
    if "авториза" in text or "вход" in text or "login" in url:
        return "auth_expired"
    if "не удалось обработать жалобу через браузер" in text:
        return "wb_layout_changed"
    return "browser_submit_error"


def has_saved_auth() -> bool:
    return AUTH_STATE_FILE.exists()


def get_auth_status() -> Dict[str, Any]:
    meta = read_json(AUTH_META_FILE, {})
    return {
        "exists": AUTH_STATE_FILE.exists(),
        "state_path": str(AUTH_STATE_FILE),
        "meta_path": str(AUTH_META_FILE),
        "saved_at": clean_text(meta.get("saved_at")),
        "seller_url": clean_text(meta.get("seller_url")),
    }


def interactive_login() -> Dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise BrowserBotError(
            "Не установлен Playwright. Выполните: pip install playwright && playwright install"
        ) from exc

    AUTH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    log_event("browser", "interactive_login_start")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel=PLAYWRIGHT_BROWSER_CHANNEL,
            slow_mo=PLAYWRIGHT_SLOW_MO_MS,
        )
        context = browser.new_context()
        _install_context_guards(context)
        page = context.new_page()
        _safe_page_goto(page, WB_SELLER_BASE_URL)
        print("Открылся браузер Wildberries Seller.")
        print("Войдите вручную по телефону/SMS, откройте кабинет и нажмите ENTER здесь.")
        input()
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        try:
            context.storage_state(path=str(AUTH_STATE_FILE), indexed_db=True)
        except TypeError:
            context.storage_state(path=str(AUTH_STATE_FILE))
        meta = {
            "saved_at": utc_now_iso(),
            "seller_url": page.url,
        }
        write_json(AUTH_META_FILE, meta)
        _close_context(browser=browser, context=context)
    status = get_auth_status()
    log_event("browser", "interactive_login_saved", saved_at=status.get("saved_at"), state_path=status.get("state_path"))
    return status


def _safe_screenshot(page, review_id: str, suffix: str) -> str:
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    rid = clean_text(review_id) or "unknown"
    path = SCREENSHOTS_DIR / f"{rid}_{suffix}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
    except Exception:
        pass
    return str(path)


def _parse_iso(iso_text: str) -> Optional[datetime]:
    iso_text = clean_text(iso_text)
    if not iso_text:
        return None
    try:
        dt = datetime.fromisoformat(iso_text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _format_date(iso_text: str) -> str:
    dt = _parse_iso(iso_text)
    if not dt:
        return clean_text(iso_text)[:10]
    return dt.astimezone(MOSCOW_TZ).strftime("%d.%m.%Y")


def _format_datetime_msk(iso_text: str) -> str:
    dt = _parse_iso(iso_text)
    if not dt:
        return ""
    return dt.astimezone(MOSCOW_TZ).strftime("%d.%m.%Y в %H:%M")


def _build_created_period(iso_text: str) -> str:
    dt = _parse_iso(iso_text)
    if not dt:
        return ""
    local_dt = dt.astimezone(MOSCOW_TZ)
    start_local = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1) - timedelta(milliseconds=1)
    start_ms = int(start_local.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end_local.astimezone(timezone.utc).timestamp() * 1000)
    return f"{start_ms}-{end_ms}"


def _collapse_spaces(text: str) -> str:
    return " ".join(clean_text(text).split())


def _build_search_value(item: Dict[str, Any]) -> str:
    search_value = clean_text(item.get("search_value"))
    if search_value:
        return search_value
    review = item.get("review", {}) or {}
    nm_id = clean_text(review.get("nm_id"))
    if nm_id and nm_id != "0":
        return nm_id
    article = clean_text(review.get("supplier_article"))
    if article:
        return article
    return clean_text(item.get("review_id"))


def _render_search_url(template: str, *, search_value: str, created_period: str, valuation: str, content: str) -> str:
    url = str(template)
    # If old profile template lacks new placeholders, append the missing params.
    if "{search_value}" not in url and "feedbacks-module_searchValue=" not in url:
        sep = "&" if "?" in url else "?"
        url += f"{sep}feedbacks-module_searchValue={{search_value}}"
    if "{created_period}" not in url and "feedbacks-module_createdPeriod=" not in url and created_period:
        url += "&feedbacks-module_createdPeriod={created_period}"
    if "{valuation}" not in url and "feedbacks-module_valuations=" not in url and valuation:
        url += "&feedbacks-module_valuations={valuation}"
    if "{content}" not in url and "feedbacks-module_content=" not in url and content:
        url += "&feedbacks-module_content={content}"

    try:
        rendered = url.format(
            search_value=search_value,
            created_period=created_period,
            valuation=valuation,
            content=content,
        )
        parsed = urlparse(rendered)
        if parsed.query:
            query_items = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if clean_text(value) != ""]
            rendered = urlunparse(parsed._replace(query=urlencode(query_items, doseq=True)))
        return rendered
    except Exception:
        return url


def _build_candidate_urls(item: Dict[str, Any], profile: Dict[str, Any]) -> List[str]:
    search_value = quote_plus(_build_search_value(item))
    review = item.get("review", {}) or {}
    created_period = _build_created_period(review.get("created_date"))
    valuation = clean_text(review.get("stars"))
    content = "all"

    templates = list(profile.get("search_urls") or DEFAULT_PROFILE["search_urls"])
    legacy_templates = [
        f"{WB_SELLER_BASE_URL}/feedbacks/feedbacks-tab/not-answered?feedbacks-module_searchValue={{search_value}}",
        f"{WB_SELLER_BASE_URL}/feedbacks/feedbacks-tab/answered?feedbacks-module_searchValue={{search_value}}",
    ]

    urls: List[str] = []

    def _append(url: str) -> None:
        if url and url not in urls:
            urls.append(url)

    # Сначала даём старые «широкие» адреса, на которых модуль жалоб у пользователя работал стабильно.
    for template in legacy_templates:
        try:
            _append(str(template).format(search_value=search_value))
        except Exception:
            continue

    variants = [
        {"created_period": "", "valuation": "", "content": ""},
        {"created_period": "", "valuation": "", "content": content},
        {"created_period": "", "valuation": valuation, "content": content},
        {"created_period": created_period, "valuation": valuation, "content": content},
    ]
    for template in templates:
        for variant in variants:
            url = _render_search_url(
                template,
                search_value=search_value,
                created_period=variant["created_period"],
                valuation=variant["valuation"],
                content=variant["content"],
            )
            _append(url)
    return urls

def _first_visible(locator, *, limit: int = 6):
    try:
        count = min(locator.count(), max(1, int(limit or 1)))
    except Exception:
        count = 1
    for index in range(count):
        try:
            item = locator.nth(index)
        except Exception:
            item = locator
        try:
            if item.is_visible():
                return item
        except Exception:
            continue
    return None


def _has_visible_selector(page, selectors: List[str]) -> bool:
    for selector in selectors:
        selector = clean_text(selector)
        if not selector:
            continue
        try:
            locator = page.locator(selector)
            if _first_visible(locator) is not None:
                return True
        except Exception:
            continue
    return False


def _has_visible_exact_text(page, labels: List[str]) -> bool:
    for label in labels:
        label = _collapse_spaces(label)
        if not label:
            continue
        try:
            locator = page.get_by_text(label, exact=True)
            if _first_visible(locator, limit=4) is not None:
                return True
        except Exception:
            continue
    return False


def _auth_text_score(page) -> int:
    score = 0
    try:
        snippet = _body_debug_snippet(page, limit=2400).lower()
    except Exception:
        snippet = ""
    for marker in [
        'авторизация',
        'вход в кабинет',
        'номер телефона',
        'получить код',
        'введите код',
        'код из смс',
        'пароль',
        'одноразовый код',
    ]:
        if marker in snippet:
            score += 1
    return score


AUTH_INPUT_SELECTORS = [
    "input[type='password']",
    "input[autocomplete='current-password']",
    "input[autocomplete='one-time-code']",
    "input[inputmode='tel']",
    "input[type='tel']",
    "input[name*='phone']",
    "input[name*='login']",
    "input[autocomplete='username']",
]

AUTH_EXACT_TEXTS = [
    'Войти',
    'Вход',
    'Авторизация',
    'Вход в кабинет',
    'Получить код',
    'Введите код',
    'Номер телефона',
    'Пароль',
]


def _page_requests_auth(page) -> bool:
    auth_inputs_visible = _has_visible_selector(page, AUTH_INPUT_SELECTORS)
    exact_auth_text_visible = _has_visible_exact_text(page, AUTH_EXACT_TEXTS)
    auth_score = _auth_text_score(page)
    if auth_inputs_visible and (exact_auth_text_visible or auth_score >= 2):
        return True
    if exact_auth_text_visible and auth_score >= 2:
        return True
    return False


def _ensure_logged_in(page) -> None:
    raw_url = clean_text(getattr(page, "url", ""))
    parsed = urlparse(raw_url)
    host = clean_text(parsed.hostname).lower()
    path = clean_text(parsed.path).lower()
    auth_path_prefixes = (
        '/login',
        '/auth',
        '/signin',
        '/passport',
    )
    if host and ('seller-auth' in host or 'passport' in host):
        raise BrowserBotError("Сессия WB недействительна. Снова выполните python login_wb.py")
    if any(path == prefix or path.startswith(prefix + '/') for prefix in auth_path_prefixes):
        raise BrowserBotError("Сессия WB недействительна. Снова выполните python login_wb.py")
    if _page_requests_auth(page):
        raise BrowserBotError("WB просит авторизацию. Снова выполните python login_wb.py")


def _dismiss_overlays(page, profile: Dict[str, Any]) -> None:
    # Close detail panel / extra info if accidentally opened.
    for _ in range(3):
        closed_any = False
        try:
            modal = page.locator("#Portal-modal-extend-info")
            if modal.count() > 0 and modal.first.is_visible():
                try:
                    modal.get_by_role("button").last.click(timeout=1000)
                    closed_any = True
                except Exception:
                    try:
                        page.keyboard.press("Escape")
                        closed_any = True
                    except Exception:
                        pass
        except Exception:
            pass

        for label in profile.get("overlay_close_texts") or []:
            try:
                btn = page.get_by_text(label, exact=False).first
                if btn.is_visible():
                    btn.click(timeout=800)
                    closed_any = True
            except Exception:
                continue
        if not closed_any:
            break
        try:
            page.wait_for_timeout(250)
        except Exception:
            pass


def _wait_page_ready(page) -> None:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass


def _open_candidate_page(page, url: str, profile: Dict[str, Any]) -> None:
    log_event("browser", "open_candidate_page", url=url)
    _safe_page_goto(page, url)
    _wait_page_ready(page)
    _ensure_logged_in(page)
    _dismiss_overlays(page, profile)


def _snippet_markers(item: Dict[str, Any]) -> List[str]:
    review = item.get("review", {}) or {}
    snippets: List[str] = []
    for raw in [review.get("text"), review.get("cons"), review.get("pros"), review.get("review_text")]:
        val = _collapse_spaces(raw)
        if not val or val == _collapse_spaces("Покупатель поставил оценку без текста."):
            continue
        # Use shorter fragments that still survive UI truncation.
        if len(val) > 90:
            val = val[:90].strip()
        if len(val) >= 12:
            snippets.append(val)
    uniq: List[str] = []
    for s in snippets:
        if s and s not in uniq:
            uniq.append(s)
    return uniq[:6]


def _text_markers(item: Dict[str, Any]) -> List[str]:
    review = item.get("review", {}) or {}
    markers: List[str] = []

    date_marker = _format_date(review.get("created_date"))
    if date_marker:
        markers.append(date_marker)

    article = _collapse_spaces(review.get("supplier_article"))
    if article:
        markers.append(article)

    nm_id = _collapse_spaces(review.get("nm_id"))
    if nm_id and nm_id != "0":
        markers.append(nm_id)

    product_name = _collapse_spaces(review.get("product_name"))
    if product_name:
        markers.append(product_name[:100].strip())

    for raw in [review.get("text"), review.get("cons"), review.get("pros"), review.get("review_text")]:
        val = _collapse_spaces(raw)
        if not val or val == _collapse_spaces("Покупатель поставил оценку без текста."):
            continue
        if len(val) > 80:
            val = val[:80].strip()
        if len(val) >= 8:
            markers.append(val)

    uniq: List[str] = []
    for marker in markers:
        if marker and marker not in uniq:
            uniq.append(marker)
    return uniq[:10]


def _ancestor_container(locator):
    xpaths = [
        "xpath=ancestor::tr[1]",
        "xpath=ancestor::li[1]",
        "xpath=ancestor::article[1]",
    ] + [f"xpath=ancestor::div[{i}]" for i in range(1, 10)]

    for xp in xpaths:
        try:
            cand = locator.locator(xp).first
            if cand.count() < 1:
                continue
            if not cand.is_visible():
                continue
            try:
                buttons = cand.locator("button")
                if buttons.count() >= 1:
                    return cand
            except Exception:
                continue
        except Exception:
            continue
    return None


def _legacy_find_review_container(page, item: Dict[str, Any]):
    search_value = _build_search_value(item)
    article = clean_text((item.get("review") or {}).get("supplier_article"))

    for marker in _text_markers(item):
        variants: List[str] = [marker]
        for size in (70, 50, 30):
            if len(marker) > size:
                prefix = marker[:size].strip()
                if prefix and prefix not in variants:
                    variants.append(prefix)
        for variant in variants:
            if len(clean_text(variant)) < 6:
                continue
            try:
                loc = page.get_by_text(variant, exact=False).first
                loc.wait_for(timeout=2200)
                if not loc.is_visible():
                    continue
                container = _ancestor_container(loc)
                if container is not None:
                    return container, f"text:{variant[:40]}"
            except Exception:
                continue

    for sel in ["tr", "li", "article", "div"]:
        try:
            candidates = page.locator(sel)
            count = min(candidates.count(), 60)
            for i in range(count):
                cand = candidates.nth(i)
                if not cand.is_visible():
                    continue
                text = _collapse_spaces(cand.inner_text())
                if not text:
                    continue
                if search_value and search_value not in text:
                    if not article or article not in text:
                        continue
                if cand.locator("button").count() >= 1:
                    return cand, f"fallback:{sel}"
        except Exception:
            continue

    raise BrowserBotError(
        "Не удалось найти нужный отзыв в списке по текстовым признакам и артикулу."
    )


def _click_text_option(scope, labels: List[str], timeout_ms: int = 2500) -> bool:
    for label in labels:
        label = clean_text(label)
        if not label:
            continue
        locators = [
            scope.get_by_role("button", name=label),
            scope.get_by_role("radio", name=label),
            scope.get_by_label(label),
            scope.get_by_text(label, exact=False),
        ]
        for locator in locators:
            try:
                target = locator.first
                target.wait_for(timeout=timeout_ms)
                try:
                    target.scroll_into_view_if_needed(timeout=timeout_ms)
                except Exception:
                    pass
                target.click(timeout=timeout_ms)
                return True
            except Exception:
                continue
    return False


def _legacy_click_row_menu(container, page) -> None:
    tried = []
    try:
        buttons = container.locator("button")
        count = buttons.count()
        for idx in range(count - 1, -1, -1):
            btn = buttons.nth(idx)
            try:
                if btn.is_visible():
                    try:
                        btn.scroll_into_view_if_needed(timeout=1000)
                    except Exception:
                        pass
                    btn.click(timeout=2500)
                    return
            except Exception as exc:
                tried.append(str(exc))
                continue
    except Exception as exc:
        tried.append(str(exc))

    for sel in ["button[aria-haspopup='menu']", "button[aria-expanded]", "button"]:
        try:
            btn = container.locator(sel).last
            if btn.is_visible():
                btn.click(timeout=2000)
                return
        except Exception:
            continue

    raise BrowserBotError(f"Не удалось открыть меню отзыва. Подробности: {' | '.join(tried[:4])}")


def _count_active_stars(row) -> int:
    selectors = [
        "div[class*='Rating--active']",
        "div[class*='Rating__'] div[class*='active']",
    ]
    for sel in selectors:
        try:
            count = row.locator(sel).count()
            if count > 0:
                return int(count)
        except Exception:
            continue
    return 0


def _row_text(row) -> str:
    try:
        return _collapse_spaces(row.inner_text(timeout=1200))
    except Exception:
        try:
            return _collapse_spaces(row.inner_text())
        except Exception:
            return ""


def _score_row(row, item: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    review = item.get("review", {}) or {}
    text = _row_text(row)
    score = 0
    facts: Dict[str, Any] = {
        "text": text,
        "matched": [],
        "has_rejected_chip": "Жалоба отклонена" in text,
        "has_refund_chip": "Отказ" in text,
    }

    date_only = _format_date(review.get("created_date"))
    date_time = _format_datetime_msk(review.get("created_date"))
    if date_time and date_time in text:
        score += 40
        facts["matched"].append("date_time")
    elif date_only and date_only in text:
        score += 25
        facts["matched"].append("date")
    else:
        score -= 50

    nm_id = clean_text(review.get("nm_id"))
    if nm_id and nm_id != "0":
        if nm_id in text:
            score += 20
            facts["matched"].append("nm_id")
        else:
            score -= 5

    article = _collapse_spaces(review.get("supplier_article"))
    if article:
        if article in text:
            score += 12
            facts["matched"].append("article")
        else:
            score -= 2

    product_name = _collapse_spaces(review.get("product_name"))
    if product_name:
        short_product = product_name[:45].strip()
        if short_product and short_product in text:
            score += 8
            facts["matched"].append("product")

    expected_stars = int(review.get("stars", 0) or 0)
    found_stars = _count_active_stars(row)
    facts["found_stars"] = found_stars
    if expected_stars and found_stars:
        if expected_stars == found_stars:
            score += 30
            facts["matched"].append("stars")
        else:
            score -= 45

    snippet_hits = 0
    for snippet in _snippet_markers(item):
        prefixes = [snippet, snippet[:70].strip(), snippet[:50].strip(), snippet[:30].strip()]
        if any(pref and pref in text for pref in prefixes):
            snippet_hits += 1
            score += 18 if snippet_hits == 1 else 6
    if snippet_hits:
        facts["matched"].append(f"snippet:{snippet_hits}")

    if facts["has_rejected_chip"]:
        score -= 60
    return score, facts


def _feedback_rows_locator(page):
    selectors = [
        "tr.Table-item",
        "tbody tr.Table-item",
        "table tbody tr",
        "[class*='Table-item']",
        "[data-name='Table'] tbody tr",
        "[role='row']",
    ]
    deadline = time.monotonic() + 8.0
    last_selector = selectors[0]
    last_count = 0
    while time.monotonic() < deadline:
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = locator.count()
            except Exception:
                continue
            if count > 0:
                return locator, selector, count
            last_selector = selector
            last_count = count
        try:
            page.wait_for_timeout(350)
        except Exception:
            break
    return page.locator(last_selector), last_selector, last_count

def _find_review_container(page, item: Dict[str, Any]):
    modern_error: Optional[Exception] = None
    rows, selector, count = _feedback_rows_locator(page)

    if count >= 1:
        try:
            scored: List[Tuple[int, Any, Dict[str, Any]]] = []
            for i in range(count):
                row = rows.nth(i)
                try:
                    if not row.is_visible():
                        continue
                except Exception:
                    pass
                score, facts = _score_row(row, item)
                scored.append((score, row, facts))

            if not scored:
                raise BrowserBotError("На странице нет видимых строк отзывов для анализа.")

            scored.sort(key=lambda x: x[0], reverse=True)
            best_score, best_row, best_facts = scored[0]
            matched = best_facts.get("matched", [])
            if best_score < 15 or not any(m in matched for m in ["date", "date_time"]):
                diagnostic = "; ".join(
                    [f"score={s}, matched={f.get('matched')}, stars={f.get('found_stars')}, rejected={f.get('has_rejected_chip')}" for s, _, f in scored[:3]]
                )
                raise BrowserBotError(
                    "Не удалось однозначно определить нужный отзыв среди найденных строк. "
                    f"Диагностика: {diagnostic}"
                )

            if len(scored) > 1:
                second_score = scored[1][0]
                if best_score - second_score <= 8 and "snippet:1" not in matched and "stars" not in matched:
                    raise BrowserBotError(
                        "По текущему артикулу найдено несколько похожих отзывов, и система не может безопасно выбрать нужный автоматически. "
                        "Сузьте фильтр вручную или перепроверьте дату/оценку."
                    )

            matched_by = ",".join(best_facts.get("matched", [])) or f"score={best_score}"
            log_event(
                "browser",
                "row_matched",
                review_id=clean_text((item.get("review") or {}).get("id") or item.get("review_id")),
                matched_by=matched_by,
                best_score=best_score,
                selector=selector,
                diagnostics={
                    "found_stars": best_facts.get("found_stars"),
                    "matched": best_facts.get("matched"),
                    "has_rejected_chip": best_facts.get("has_rejected_chip"),
                },
            )
            return best_row, matched_by
        except Exception as exc:
            modern_error = exc
    else:
        snippet = _body_debug_snippet(page)
        modern_error = BrowserBotError(
            "На странице не найдены строки отзывов после применения фильтра. "
            f"Селектор: {selector}. URL: {clean_text(getattr(page, 'url', ''))}. "
            f"Фрагмент страницы: {snippet}"
        )

    try:
        legacy_container, legacy_marker = _legacy_find_review_container(page, item)
        review_id = clean_text((item.get("review") or {}).get("id") or item.get("review_id"))
        log_event(
            "browser",
            "row_matched_legacy",
            review_id=review_id,
            matched_by=legacy_marker,
            modern_error=clean_text(modern_error) if modern_error else "",
        )
        return legacy_container, f"legacy:{legacy_marker}"
    except Exception as legacy_exc:
        modern_text = clean_text(modern_error) if modern_error else ""
        legacy_text = clean_text(legacy_exc)
        url = clean_text(getattr(page, 'url', ''))
        snippet = _body_debug_snippet(page)
        if modern_text and legacy_text and modern_text != legacy_text:
            raise BrowserBotError(
                f"{modern_text} Дополнительно legacy-поиск тоже не сработал: {legacy_text}. "
                f"URL: {url}. Фрагмент страницы: {snippet}"
            )
        if modern_text:
            raise BrowserBotError(f"{modern_text}. URL: {url}. Фрагмент страницы: {snippet}")
        raise BrowserBotError(f"{legacy_text}. URL: {url}. Фрагмент страницы: {snippet}")

def _open_more_menu(row, page, profile: Dict[str, Any]) -> None:
    log_event("browser", "open_more_menu_start")
    _dismiss_overlays(page, profile)
    last_error = None

    candidate_locators = [
        row.locator("[data-name='MoreButton'] button[class*='onlyIcon']").last,
        row.locator("[data-name='MoreButton'] button").last,
    ]

    for btn in candidate_locators:
        try:
            if btn.count() < 1:
                continue
            target = btn
            target.scroll_into_view_if_needed(timeout=1000)
            try:
                target.click(timeout=2500)
            except Exception:
                target.dispatch_event("click")
            try:
                page.wait_for_timeout(250)
            except Exception:
                pass
            if _complaint_menu_visible(page, profile):
                log_event("browser", "open_more_menu_success", method="modern")
                return
            if _already_complained_hint_visible(page):
                return
            _dismiss_overlays(page, profile)
        except Exception as exc:
            last_error = exc
            _dismiss_overlays(page, profile)
            continue

    try:
        _legacy_click_row_menu(row, page)
        try:
            page.wait_for_timeout(250)
        except Exception:
            pass
        if _complaint_menu_visible(page, profile) or _already_complained_hint_visible(page):
            log_event("browser", "open_more_menu_success", method="legacy")
            return
    except Exception as exc:
        last_error = exc

    raise BrowserBotError(f"Не удалось открыть меню отзыва. Подробности: {clean_text(last_error)}")

def _complaint_menu_visible(page, profile: Dict[str, Any]) -> bool:
    for label in profile.get("complaint_menu_texts") or ["Пожаловаться на отзыв", "Пожаловаться"]:
        try:
            loc = page.get_by_text(label, exact=False).first
            if loc.is_visible():
                return True
        except Exception:
            continue
    return False


def _already_complained_hint_visible(page) -> bool:
    hints = [
        "Вы уже отправляли жалобу",
        "дождитесь результатов проверки",
        "жалобу уже отправляли",
    ]
    for hint in hints:
        try:
            loc = page.get_by_text(hint, exact=False).first
            if loc.is_visible():
                return True
        except Exception:
            continue
    return False


def _open_complaint_menu(page, profile: Dict[str, Any]) -> str:
    log_event("browser", "complaint_menu_start")
    if _already_complained_hint_visible(page):
        return "already_complained"

    labels = profile.get("complaint_menu_texts") or ["Пожаловаться на отзыв", "Пожаловаться"]
    for label in labels:
        try:
            loc = page.get_by_text(label, exact=False).first
            loc.wait_for(timeout=2500)
            try:
                if loc.locator("xpath=ancestor-or-self::*[@disabled or @aria-disabled='true']").count() > 0:
                    raise BrowserBotError("Пункт «Пожаловаться на отзыв» недоступен для выбранного отзыва.")
            except Exception:
                pass
            loc.click(timeout=2500)
            log_event("browser", "complaint_menu_opened", label=label, method="direct")
            return "opened"
        except Exception:
            continue

    if _click_text_option(page, labels, timeout_ms=3500):
        log_event("browser", "complaint_menu_opened", label=clean_text(labels[0] if labels else "Пожаловаться"), method="legacy_text")
        return "opened"

    if _already_complained_hint_visible(page):
        return "already_complained"
    raise BrowserBotError("Не удалось выбрать пункт «Пожаловаться на отзыв».")

def _get_dialog(page):
    candidates = [
        page.locator("#Portal-feedback-complaint-modal"),
        page.locator("[id='Portal-feedback-complaint-modal']"),
        page.get_by_role("dialog"),
        page.locator("[role='dialog']"),
        page.locator("div[class*='modal']"),
        page.locator("div[class*='dialog']"),
    ]
    for loc in candidates:
        try:
            dlg = loc.last
            dlg.wait_for(timeout=3500)
            if dlg.is_visible():
                return dlg
        except Exception:
            continue
    return page

def _click_complaint_category(dialog, category: str) -> bool:
    category = clean_text(category)
    category_id = CATEGORY_ID_MAP.get(category)

    attempts = []
    if category_id:
        attempts.extend([
            dialog.locator(f"label[for='{category_id}']"),
            dialog.locator(f"input[id='{category_id}']"),
            dialog.locator(f"input[value='{category_id}']"),
        ])
    attempts.extend([
        dialog.get_by_label(category, exact=False),
        dialog.get_by_role("radio", name=category),
        dialog.get_by_text(category, exact=False),
    ])

    for loc in attempts:
        try:
            target = loc.first
            target.wait_for(timeout=2500)
            target.scroll_into_view_if_needed(timeout=1000)
            try:
                target.click(timeout=2000)
            except Exception:
                try:
                    target.check(force=True, timeout=2000)
                except Exception:
                    target.dispatch_event("click")
            if category_id:
                try:
                    inp = dialog.locator(f"input[id='{category_id}']").first
                    if inp.count() > 0:
                        page_is_checked = inp.is_checked()
                        if page_is_checked:
                            log_event("browser", "complaint_category_selected", category=category, method="id-check")
                            return True
                except Exception:
                    pass
            else:
                log_event("browser", "complaint_category_selected", category=category, method="text")
                return True
        except Exception:
            continue

    if category == "Другое" and category_id:
        try:
            dialog.locator(f"label[for='{category_id}'] span").last.click(timeout=2000)
            log_event("browser", "complaint_category_selected", category=category, method="fallback_other")
            return True
        except Exception:
            pass

    if _click_text_option(dialog, [category], timeout_ms=3500):
        log_event("browser", "complaint_category_selected", category=category, method="legacy_text")
        return True
    return False

def _normalize_reason_text(reason: str, category: str) -> str:
    reason = _collapse_spaces(reason)
    if not reason:
        reason = "Просим дополнительно проверить данный отзыв и исключить его из публикации."

    # WB form requires 50..1000 chars according to saved modal.
    if len(reason) < 50:
        extra_by_category = {
            "Другое": " Просим дополнительно проверить данный отзыв и исключить его из публикации.",
            "Нецензурная лексика": " Просим проверить формулировки отзыва и исключить его из публикации из-за некорректной лексики.",
            "Угрозы, оскорбления": " Просим проверить содержание отзыва и исключить его из публикации из-за оскорбительных формулировок.",
            "Отзыв не относится к товару": " Просим проверить содержание отзыва и исключить его из публикации, так как он относится не к самому товару.",
            "Отзыв оставили конкуренты": " Просим проверить публикацию на признаки недобросовестного конкурентного воздействия и исключить отзыв.",
            "Спам-реклама в тексте": " Просим проверить отзыв на признаки рекламного содержания и исключить его из публикации.",
            "Отзыв с политическим контекстом": " Просим проверить данный отзыв и исключить его из публикации из-за нерелевантного политического контекста.",
        }
        reason += extra_by_category.get(category, extra_by_category["Другое"])

    if len(reason) > 800:
        reason = reason[:799].rstrip() + "…"
    return reason


def _fill_reason(dialog, reason: str, category: str) -> bool:
    reason = _normalize_reason_text(reason, category)
    candidates = [
        dialog.locator("textarea#explanation"),
        dialog.locator("textarea"),
        dialog.get_by_role("textbox"),
        dialog.locator("[contenteditable='true']"),
    ]
    for locator in candidates:
        try:
            count = min(locator.count(), 5)
            for i in range(count):
                target = locator.nth(i)
                if not target.is_visible():
                    continue
                try:
                    target.scroll_into_view_if_needed(timeout=1000)
                except Exception:
                    pass
                try:
                    target.fill(reason)
                except Exception:
                    try:
                        target.click(timeout=1000)
                        target.press("Control+A")
                        target.type(reason, delay=8)
                    except Exception:
                        target.dispatch_event("click")
                        try:
                            target.type(reason, delay=8)
                        except Exception:
                            continue
                # verify value/text actually changed and meets min length
                value = ""
                try:
                    value = target.input_value(timeout=800)
                except Exception:
                    try:
                        value = target.inner_text(timeout=800)
                    except Exception:
                        value = ""
                value = _collapse_spaces(value)
                if len(value) >= 50:
                    log_event("browser", "complaint_reason_filled", category=category, length=len(value))
                    return True
        except Exception:
            continue
    return False


def _click_submit(dialog, profile: Dict[str, Any]) -> bool:
    texts = profile.get("submit_button_texts") or ["Отправить"]
    for label in texts:
        try:
            locs = [
                dialog.get_by_role("button", name=label),
                dialog.get_by_text(label, exact=False),
            ]
            for locator in locs:
                try:
                    target = locator.last
                    target.wait_for(timeout=3000)
                    try:
                        target.wait_for_element_state("visible", timeout=1000)
                    except Exception:
                        pass
                    for _ in range(8):
                        disabled = False
                        try:
                            disabled = not target.is_enabled()
                        except Exception:
                            disabled = False
                        if not disabled:
                            break
                        try:
                            dialog.page.wait_for_timeout(250)
                        except Exception:
                            pass
                    try:
                        target.scroll_into_view_if_needed(timeout=1000)
                    except Exception:
                        pass
                    target.click(timeout=3000)
                    log_event("browser", "complaint_submit_clicked", label=label, method="direct")
                    return True
                except Exception:
                    continue
        except Exception:
            continue

    if _click_text_option(dialog, texts, timeout_ms=4500):
        log_event("browser", "complaint_submit_clicked", label=clean_text(texts[0] if texts else "Отправить"), method="legacy_text")
        return True
    return False

def _submit_single(
    page,
    item: Dict[str, Any],
    dry_run: bool = False,
    progress_hook: Optional[Callable[..., None]] = None,
    *,
    run_id: str = "",
    profile_version: str = "",
) -> Dict[str, Any]:
    profile = _load_profile()
    profile_version = clean_text(profile_version) or _profile_version(profile)
    log_event("browser", "submit_single_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), review_id=clean_text(item.get("review_id")), category=clean_text(item.get("category")), dry_run=bool(dry_run), run_id=run_id, profile_version=profile_version)
    review_id = clean_text(item.get("review_id"))
    category = clean_text(item.get("category")) or "Другое"
    reason = clean_text(item.get("reason"))
    total_steps = 8

    def _progress(stage: str, message: str, *, item_step: int = 0, **extra: Any) -> None:
        if not callable(progress_hook):
            return
        payload = dict(extra)
        payload.setdefault("item_step", item_step)
        payload.setdefault("item_step_total", total_steps)
        progress_hook(stage=stage, message=message, **payload)

    urls = _build_candidate_urls(item, profile)
    _progress("item_prepare", f"Подготовлены адреса поиска WB: {len(urls)}", item_step=0, url_count=len(urls), review_id=review_id, category=category)
    if not urls:
        raise BrowserBotError("Не удалось построить адрес поиска отзыва для WB.")

    last_error: Optional[Exception] = None
    found_on_url: Optional[str] = None

    for url_index, url in enumerate(urls, start=1):
        page_kind = _page_kind_from_url(url)
        try:
            _progress("open_page", f"Открываю страницу WB {url_index}/{len(urls)}", item_step=1, url=url, review_id=review_id)
            _open_candidate_page(page, url, profile)

            _progress("find_review", "Ищу нужный отзыв на странице", item_step=2, url=url, review_id=review_id)
            container, matched_by = _find_review_container(page, item)
            found_on_url = url
            _progress("review_found", f"Нужный отзыв найден: {matched_by}", item_step=3, url=url, matched_by=matched_by, review_id=review_id)

            _progress("open_more_menu", "Открываю меню отзыва", item_step=4, review_id=review_id)
            _open_more_menu(container, page, profile)
            _progress("open_complaint_menu", "Открываю форму жалобы", item_step=5, review_id=review_id)
            menu_state = _open_complaint_menu(page, profile)
            if menu_state == "already_complained":
                _progress("already_complained", "Жалоба уже была отправлена ранее", item_step=8, review_id=review_id, status="already_complained")
                log_event("browser", "submit_single_already_complained", review_id=review_id, opened_url=url, matched_by=matched_by, run_id=run_id, profile_version=profile_version, page_kind=page_kind)
                return {
                    "status": "already_complained",
                    "opened_url": url,
                    "matched_by": matched_by,
                    "category": category,
                    "reason": reason,
                    "before_screenshot": _safe_screenshot(page, review_id, "already_complained"),
                    "profile_version": profile_version,
                    "page_kind": page_kind,
                    "run_id": run_id,
                }

            dialog = _get_dialog(page)
            _progress("select_category", f"Выбираю категорию: {category}", item_step=6, review_id=review_id, category=category)
            if not _click_complaint_category(dialog, category):
                raise BrowserBotError(f"Не удалось выбрать категорию жалобы: {category}")

            try:
                page.wait_for_timeout(350)
            except Exception:
                pass

            _progress("fill_reason", "Заполняю текст причины жалобы", item_step=7, review_id=review_id, category=category)
            if not _fill_reason(dialog, reason, category):
                raise BrowserBotError("Не удалось вставить текст причины в форму жалобы.")

            before_path = _safe_screenshot(page, review_id, "before_submit")

            if dry_run:
                _progress("dry_run", "Dry-run: проверка пройдена, отправка пропущена", item_step=8, review_id=review_id, status="dry_run")
                log_event("browser", "submit_single_dry_run", review_id=review_id, opened_url=url, matched_by=matched_by, category=category, run_id=run_id, profile_version=profile_version, page_kind=page_kind)
                return {
                    "status": "dry_run",
                    "opened_url": url,
                    "matched_by": matched_by,
                    "category": category,
                    "reason": _normalize_reason_text(reason, category),
                    "before_screenshot": before_path,
                    "profile_version": profile_version,
                    "page_kind": page_kind,
                    "run_id": run_id,
                }

            _progress("submit_click", "Нажимаю кнопку «Отправить»", item_step=8, review_id=review_id)
            if not _click_submit(dialog, profile):
                raise BrowserBotError("Не удалось нажать кнопку «Отправить».")

            success = False
            for hint in profile.get("success_hints") or []:
                try:
                    hint_loc = page.get_by_text(hint, exact=False).first
                    hint_loc.wait_for(timeout=2500)
                    if hint_loc.is_visible():
                        success = True
                        break
                except Exception:
                    continue

            after_path = _safe_screenshot(page, review_id, "after_submit")
            status_name = "submitted" if success else "submitted_click_only"
            _progress("item_done", f"Обработка отзыва завершена: {status_name}", item_step=8, review_id=review_id, status=status_name, matched_by=matched_by, url=url)
            log_event("browser", "submit_single_finish", review_id=review_id, opened_url=url, matched_by=matched_by, category=category, status=status_name, run_id=run_id, profile_version=profile_version, page_kind=page_kind)
            return {
                "status": status_name,
                "opened_url": url,
                "matched_by": matched_by,
                "category": category,
                "reason": _normalize_reason_text(reason, category),
                "before_screenshot": before_path,
                "after_screenshot": after_path,
                "profile_version": profile_version,
                "page_kind": page_kind,
                "run_id": run_id,
            }

        except Exception as exc:
            last_error = exc
            error_text = clean_text(exc)
            _progress("item_url_error", f"Ошибка на URL {url_index}/{len(urls)}: {error_text}", item_step=2, review_id=review_id, url=url, error=error_text)
            log_event("browser", "submit_single_error", level="error", review_id=review_id, url=url if "url" in locals() else "", error=str(exc), run_id=run_id, profile_version=profile_version, page_kind=page_kind)
            _safe_screenshot(page, review_id, "failed_attempt")
            if found_on_url is not None:
                break
            continue

    raise BrowserBotError(str(last_error) if last_error else "Не удалось обработать жалобу через браузер.")


def refresh_complaint_outcomes(max_items: int = 0) -> Dict[str, Any]:
    log_event("browser", "refresh_outcomes_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), max_items=max_items)
    _job_progress("Проверяю готовность Playwright для обновления статусов жалоб", stage="refresh_init", current=0, total=0, percent=0)
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise BrowserBotError(
            "Не установлен Playwright. Выполните: pip install playwright && playwright install"
        ) from exc

    if not has_saved_auth():
        raise BrowserBotError(
            f"Не найден файл сессии WB: {AUTH_STATE_FILE}. Сначала выполните python login_wb.py"
        )

    queue = reconcile_complaint_queue()
    candidate_indices = []
    for idx, item in enumerate(queue):
        status = clean_text(item.get("status"))
        if status in {"queued", "processing", "failed", "failed_stale", "skipped", "dry_run", "manual_review"}:
            continue
        candidate_indices.append(idx)

    if max_items and max_items > 0:
        candidate_indices = candidate_indices[:max_items]
    total_items = len(candidate_indices)
    _job_progress(
        f"Найдено жалоб для проверки статусов: {total_items}",
        stage="refresh_queue_ready",
        current=0,
        total=total_items,
        percent=0,
        checked=0,
        accepted=0,
        rejected=0,
        pending=0,
        items_total=total_items,
    )
    if not candidate_indices:
        _job_progress("Нет жалоб для проверки статусов", stage="refresh_empty", current=0, total=0, percent=100)
        return {"checked": 0, "accepted": 0, "rejected": 0, "pending": 0, "message": "Нет жалоб для проверки статусов."}

    checked = 0
    accepted = 0
    rejected = 0
    pending = 0

    with sync_playwright() as p:
        _job_progress("Открываю браузер WB для проверки статусов жалоб", stage="refresh_browser_launch", current=0, total=total_items, percent=0)
        browser = p.chromium.launch(
            headless=PLAYWRIGHT_HEADLESS,
            channel=PLAYWRIGHT_BROWSER_CHANNEL,
            slow_mo=PLAYWRIGHT_SLOW_MO_MS,
        )
        context = browser.new_context(storage_state=str(AUTH_STATE_FILE))
        _install_context_guards(context)
        page = context.new_page()
        _job_progress("Браузер WB открыт, начинаю проверку статусов", stage="refresh_browser_ready", current=0, total=total_items, percent=0)

        for position, idx in enumerate(candidate_indices, start=1):
            item = queue[idx]
            review_id = clean_text(item.get("review_id"))
            category = clean_text(item.get("category"))
            outcome = None
            try:
                profile = _load_profile()
                urls = _build_candidate_urls(item, profile)
                _job_progress(
                    f"Проверяю статус жалобы {position}/{total_items}",
                    stage="refresh_item_start",
                    current=position - 1,
                    total=total_items,
                    percent=round(((position - 1) / total_items) * 100.0, 1),
                    item_index=position,
                    item_total=total_items,
                    review_id=review_id,
                    category=category,
                    checked=checked,
                    accepted=accepted,
                    rejected=rejected,
                    pending=pending,
                )
                for url_index, url in enumerate(urls, start=1):
                    page_kind = _page_kind_from_url(url)
                    _job_progress(
                        f"Открываю WB-страницу {url_index}/{len(urls)} для проверки статуса",
                        stage="refresh_open_page",
                        current=position - 1,
                        total=total_items,
                        percent=round(((position - 1) / total_items) * 100.0, 1),
                        item_index=position,
                        item_total=total_items,
                        review_id=review_id,
                        category=category,
                        url=url,
                    )
                    _open_candidate_page(page, url, profile)
                    row, matched_by = _find_review_container(page, item)
                    outcome = _detect_row_outcome(row) or "pending"
                    _job_progress(
                        f"Статус жалобы определён: {outcome}",
                        stage="refresh_item_detected",
                        current=position,
                        total=total_items,
                        percent=round((position / total_items) * 100.0, 1),
                        item_index=position,
                        item_total=total_items,
                        review_id=review_id,
                        category=category,
                        status=outcome,
                        matched_by=matched_by,
                        url=url,
                    )
                    break
            except Exception as exc:
                error_text = clean_text(exc)
                log_event("browser", "refresh_outcomes_item_error", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), level="error", review_id=review_id, error=str(exc))
                _job_progress(
                    f"Ошибка при проверке статуса жалобы {review_id}: {error_text}",
                    stage="refresh_item_error",
                    current=position,
                    total=total_items,
                    percent=round((position / total_items) * 100.0, 1),
                    item_index=position,
                    item_total=total_items,
                    review_id=review_id,
                    category=category,
                    error=error_text,
                    checked=checked,
                    accepted=accepted,
                    rejected=rejected,
                    pending=pending,
                )
                outcome = None

            if outcome:
                queue[idx]["status"] = outcome
                queue[idx]["checked_at"] = utc_now_iso()
                append_result({
                    "review_id": review_id,
                    "status": outcome,
                    "category": clean_text(item.get("category")),
                    "reason": clean_text(item.get("reason")),
                    "processed_at": utc_now_iso(),
                    "source": "wb_refresh",
                    "review": item.get("review", {}) or {},
                })
                checked += 1
                if outcome == "accepted":
                    accepted += 1
                elif outcome == "rejected":
                    rejected += 1
                elif outcome == "pending":
                    pending += 1
                save_complaint_queue(queue)
                _job_progress(
                    f"Статусы обновлены: checked={checked}, accepted={accepted}, rejected={rejected}, pending={pending}",
                    stage="refresh_item_done",
                    current=position,
                    total=total_items,
                    percent=round((position / total_items) * 100.0, 1),
                    item_index=position,
                    item_total=total_items,
                    review_id=review_id,
                    category=category,
                    status=outcome,
                    checked=checked,
                    accepted=accepted,
                    rejected=rejected,
                    pending=pending,
                )

        _close_context(browser=browser, context=context)

    _job_progress(
        "Проверка статусов жалоб завершена",
        stage="refresh_done",
        current=total_items,
        total=total_items,
        percent=100,
        checked=checked,
        accepted=accepted,
        rejected=rejected,
        pending=pending,
    )
    log_event("browser", "refresh_outcomes_finish", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), checked=checked, accepted=accepted, rejected=rejected, pending=pending)
    return {
        "checked": checked,
        "accepted": accepted,
        "rejected": rejected,
        "pending": pending,
        "message": f"Проверено жалоб: {checked}, accepted: {accepted}, rejected: {rejected}, pending: {pending}.",
    }



def process_queue(max_items: int = 0, dry_run: bool = False, external_run_id: str = '', module_logger: Any = None) -> Dict[str, Any]:
    run_id = clean_text(external_run_id) or uuid.uuid4().hex[:12]
    profile = _load_profile()
    profile_version = _profile_version(profile)
    log_event("browser", "process_queue_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), max_items=max_items, dry_run=bool(dry_run), run_id=run_id, profile_version=profile_version)
    if module_logger is not None:
        module_logger.event('browser_process_start', stage='browser_start', max_items=max_items, dry_run=bool(dry_run), profile_version=profile_version)
    _job_progress("Проверяю готовность Playwright для обработки очереди жалоб", stage="process_init", current=0, total=0, percent=0, dry_run=bool(dry_run))
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise BrowserBotError(
            "Не установлен Playwright. Выполните: pip install playwright && playwright install"
        ) from exc

    if not has_saved_auth():
        raise BrowserBotError(
            f"Не найден файл сессии WB: {AUTH_STATE_FILE}. Сначала выполните python login_wb.py"
        )

    queue = reconcile_complaint_queue()
    pending_indices = [idx for idx, item in enumerate(queue) if clean_text(item.get("status")) == "queued"]
    if max_items and max_items > 0:
        pending_indices = pending_indices[:max_items]
    total_items = len(pending_indices)
    _job_progress(
        f"В очереди жалоб к отправке: {total_items}",
        stage="process_queue_ready",
        current=0,
        total=total_items,
        percent=0,
        items_done=0,
        items_total=total_items,
        success=0,
        failed=0,
        dry_run=bool(dry_run),
    )
    if not pending_indices:
        log_event("browser", "process_queue_empty", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""))
        _job_progress("Очередь жалоб пуста", stage="process_empty", current=0, total=0, percent=100)
        return {"processed": 0, "success": 0, "failed": 0, "message": "Очередь жалоб пуста."}

    processed = 0
    success = 0
    failed = 0
    parked = 0
    success_statuses = {"submitted", "submitted_click_only", "submitted_verified", "dry_run", "already_complained", "already_submitted", "success"}

    def _start_browser(playwright):
        browser = playwright.chromium.launch(
            headless=PLAYWRIGHT_HEADLESS,
            channel=PLAYWRIGHT_BROWSER_CHANNEL,
            slow_mo=PLAYWRIGHT_SLOW_MO_MS,
        )
        context = browser.new_context(storage_state=str(AUTH_STATE_FILE))
        _install_context_guards(context)
        page = context.new_page()
        return browser, context, page

    with sync_playwright() as p:
        _job_progress("Открываю браузер WB для обработки очереди жалоб", stage="browser_launch", current=0, total=total_items, percent=0)
        browser, context, page = _start_browser(p)
        _job_progress("Браузер WB открыт, начинаю обработку очереди", stage="browser_ready", current=0, total=total_items, percent=0)

        for position, idx in enumerate(pending_indices, start=1):
            item = queue[idx]
            review_id = clean_text(item.get("review_id"))
            category = clean_text(item.get("category")) or "Другое"
            log_event("browser", "process_queue_item_start", review_id=review_id, category=category, run_id=run_id, profile_version=profile_version)
            if module_logger is not None:
                module_logger.event('browser_item_start', stage='item_start', review_id=review_id, category=category, position=position, total=total_items, profile_version=profile_version)
            queue[idx]["status"] = "processing"
            queue[idx]["started_at"] = utc_now_iso()
            queue[idx]["finished_at"] = ""
            save_complaint_queue(queue)

            def _item_progress(*, stage: str, message: str, **extra: Any) -> None:
                step = extra.get("item_step")
                step_total = extra.get("item_step_total") or 0
                percent = round(((position - 1) / total_items) * 100.0, 1) if total_items else 0
                if isinstance(step, (int, float)) and isinstance(step_total, (int, float)) and step_total:
                    percent = round((((position - 1) + (float(step) / float(step_total))) / total_items) * 100.0, 1)
                payload = dict(extra)
                payload.setdefault("current", processed)
                payload.setdefault("total", total_items)
                payload.setdefault("percent", percent)
                payload.setdefault("items_done", processed)
                payload.setdefault("items_total", total_items)
                payload.setdefault("item_index", position)
                payload.setdefault("item_total", total_items)
                payload.setdefault("current_review_index", position)
                payload.setdefault("current_review_total", total_items)
                payload.setdefault("review_id", review_id)
                payload.setdefault("category", category)
                payload.setdefault("success", success)
                payload.setdefault("failed", failed)
                payload.setdefault("dry_run", bool(dry_run))
                payload.setdefault("run_id", run_id)
                payload.setdefault("profile_version", profile_version)
                current_value = payload.pop("current", None)
                total_value = payload.pop("total", None)
                percent_value = payload.pop("percent", None)
                _job_progress(
                    message,
                    stage=stage,
                    current=current_value,
                    total=total_value,
                    percent=percent_value,
                    **payload,
                )

            _item_progress(stage="item_start", message=f"Начинаю обработку жалобы {position}/{total_items}", item_step=0, item_step_total=8)
            breaker = _breaker_preflight(review_id, profile_version)
            if breaker:
                queue[idx]["status"] = "manual_review"
                queue[idx]["finished_at"] = utc_now_iso()
                queue[idx]["last_error"] = clean_text(breaker.get("last_error")) or "Повторяющаяся ошибка автоматизации"
                queue[idx]["last_error_code"] = clean_text(breaker.get("error_code")) or "manual_review"
                queue[idx]["last_profile_version"] = profile_version
                queue[idx]["last_page_kind"] = clean_text(breaker.get("last_page_kind"))
                queue[idx]["last_run_id"] = run_id
                queue[idx]["manual_review_required"] = True
                queue[idx]["forensics_path"] = clean_text(breaker.get("last_forensics_path"))
                append_result({
                    "review_id": review_id,
                    "status": "manual_review",
                    "category": clean_text(item.get("category")),
                    "reason": clean_text(item.get("reason")),
                    "confidence": 1.0,
                    "processed_at": utc_now_iso(),
                    "error": queue[idx]["last_error"],
                    "error_code": queue[idx]["last_error_code"],
                    "run_id": run_id,
                    "profile_version": profile_version,
                    "page_kind": queue[idx]["last_page_kind"],
                    "forensics_path": queue[idx]["forensics_path"],
                    "failures": int(breaker.get("failures") or 0),
                })
                processed += 1
                failed += 1
                parked += 1
                _job_progress(
                    f"Жалоба {position}/{total_items} переведена в ручную проверку после повторяющейся ошибки",
                    stage="item_manual_review",
                    current=processed,
                    total=total_items,
                    percent=round((processed / total_items) * 100.0, 1),
                    items_done=processed,
                    items_total=total_items,
                    item_index=position,
                    item_total=total_items,
                    review_id=review_id,
                    category=category,
                    status="manual_review",
                    error_code=queue[idx]["last_error_code"],
                    success=success,
                    failed=failed,
                    parked=parked,
                    run_id=run_id,
                    profile_version=profile_version,
                    dry_run=bool(dry_run),
                )
                save_complaint_queue(queue)
                continue
            attempt = 0
            while attempt < 2:
                try:
                    result = _submit_single(page, item, dry_run=dry_run, progress_hook=_item_progress, run_id=run_id, profile_version=profile_version)
                    queue[idx]["status"] = result.get("status") or "submitted_click_only"
                    queue[idx]["finished_at"] = utc_now_iso()
                    queue[idx]["last_error"] = ""
                    queue[idx]["last_error_code"] = ""
                    queue[idx]["last_profile_version"] = clean_text(result.get("profile_version") or profile_version)
                    queue[idx]["last_page_kind"] = clean_text(result.get("page_kind") or "")
                    queue[idx]["last_run_id"] = clean_text(result.get("run_id") or run_id)
                    queue[idx]["manual_review_required"] = False
                    queue[idx]["forensics_path"] = ""
                    _reset_failure_tracker(review_id, queue[idx]["last_profile_version"] or profile_version)
                    append_result({
                        "review_id": review_id,
                        "status": queue[idx]["status"],
                        "category": clean_text(item.get("category")),
                        "reason": clean_text(item.get("reason")),
                        "confidence": 1.0,
                        "processed_at": utc_now_iso(),
                        **result,
                    })
                    processed += 1
                    if queue[idx]["status"] in success_statuses:
                        success += 1
                    _job_progress(
                        f"Жалоба {position}/{total_items} завершена: {queue[idx]['status']}",
                        stage="item_done",
                        current=processed,
                        total=total_items,
                        percent=round((processed / total_items) * 100.0, 1),
                        items_done=processed,
                        items_total=total_items,
                        item_index=position,
                        item_total=total_items,
                        review_id=review_id,
                        category=category,
                        status=queue[idx]["status"],
                        success=success,
                        failed=failed,
                        parked=parked,
                        run_id=run_id,
                        profile_version=profile_version,
                        dry_run=bool(dry_run),
                    )
                    log_event("browser", "process_queue_item_result", review_id=review_id, status=queue[idx]["status"], run_id=run_id, profile_version=profile_version, page_kind=queue[idx].get("last_page_kind"))
                    if module_logger is not None:
                        module_logger.event('browser_item_result', stage='item_finish', review_id=review_id, category=category, status=queue[idx]['status'], page_kind=queue[idx].get('last_page_kind'), profile_version=profile_version)
                    break
                except Exception as exc:
                    error_text = clean_text(exc)
                    page_closed = "Target page, context or browser has been closed" in error_text or "has been closed" in error_text
                    if page_closed and attempt == 0:
                        _item_progress(stage="browser_restart", message="Страница или браузер закрылись. Перезапускаю браузер и пробую ещё раз", item_step=1, item_step_total=8, error=error_text)
                        try:
                            browser.close()
                        except Exception:
                            pass
                        browser, context, page = _start_browser(p)
                        attempt += 1
                        continue

                    error_code = _classify_browser_error(exc, page=page)
                    page_kind = _page_kind_from_url(clean_text(getattr(page, "url", "")))
                    artifacts = _capture_failure_artifacts(
                        page,
                        review_id=review_id,
                        run_id=run_id,
                        error_code=error_code,
                        profile_version=profile_version,
                        page_kind=page_kind,
                        note=error_text,
                    )
                    tracker = _register_failure(
                        review_id,
                        error_code,
                        profile_version,
                        page_kind=page_kind,
                        error_text=error_text,
                        forensics_path=artifacts.get("meta_path") or artifacts.get("html_path") or artifacts.get("screenshot_path") or "",
                        run_id=run_id,
                    )
                    queue[idx]["status"] = "manual_review" if int(tracker.get("failures") or 0) >= BROWSER_FAILURE_BREAKER_THRESHOLD else "failed"
                    queue[idx]["finished_at"] = utc_now_iso()
                    queue[idx]["last_error"] = error_text
                    queue[idx]["last_error_code"] = error_code
                    queue[idx]["last_profile_version"] = profile_version
                    queue[idx]["last_page_kind"] = page_kind
                    queue[idx]["last_run_id"] = run_id
                    queue[idx]["manual_review_required"] = queue[idx]["status"] == "manual_review"
                    queue[idx]["forensics_path"] = artifacts.get("meta_path") or artifacts.get("html_path") or artifacts.get("screenshot_path") or ""
                    append_result({
                        "review_id": review_id,
                        "status": queue[idx]["status"],
                        "category": clean_text(item.get("category")),
                        "reason": clean_text(item.get("reason")),
                        "confidence": 1.0,
                        "processed_at": utc_now_iso(),
                        "error": error_text,
                        "error_code": error_code,
                        "run_id": run_id,
                        "profile_version": profile_version,
                        "page_kind": page_kind,
                        "forensics_path": queue[idx]["forensics_path"],
                        "screenshot_path": artifacts.get("screenshot_path"),
                        "html_path": artifacts.get("html_path"),
                        "meta_path": artifacts.get("meta_path"),
                        "failures": int(tracker.get("failures") or 0),
                    })
                    processed += 1
                    failed += 1
                    if queue[idx]["status"] == "manual_review":
                        parked += 1
                    _job_progress(
                        f"Ошибка обработки жалобы {position}/{total_items}: {error_text}",
                        stage="item_error",
                        current=processed,
                        total=total_items,
                        percent=round((processed / total_items) * 100.0, 1),
                        items_done=processed,
                        items_total=total_items,
                        item_index=position,
                        item_total=total_items,
                        review_id=review_id,
                        category=category,
                        status=queue[idx]["status"],
                        error=error_text,
                        error_code=error_code,
                        forensics_path=queue[idx]["forensics_path"],
                        success=success,
                        failed=failed,
                        parked=parked,
                        run_id=run_id,
                        profile_version=profile_version,
                        dry_run=bool(dry_run),
                    )
                    log_event("browser", "process_queue_item_error", level="error", review_id=review_id, category=category, status=queue[idx]["status"], error=error_text, error_code=error_code, failures=int(tracker.get("failures") or 0), forensics_path=queue[idx]["forensics_path"], run_id=run_id, profile_version=profile_version, page_kind=page_kind)
                    if module_logger is not None:
                        module_logger.event('browser_item_error', level='error', stage='item_error', review_id=review_id, category=category, status=queue[idx]['status'], error=error_text, error_code=error_code, forensics_path=queue[idx]['forensics_path'], profile_version=profile_version, page_kind=page_kind)
                    break
                finally:
                    save_complaint_queue(queue)

        _close_context(browser=browser, context=context)

    queue = reconcile_complaint_queue()
    save_complaint_queue(queue)
    _job_progress(
        "Обработка очереди жалоб завершена",
        stage="process_done",
        current=processed,
        total=total_items,
        percent=100,
        items_done=processed,
        items_total=total_items,
        success=success,
        failed=failed,
        parked=parked,
        run_id=run_id,
        profile_version=profile_version,
        dry_run=bool(dry_run),
    )
    log_event("browser", "process_queue_finish", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), processed=processed, success=success, failed=failed, parked=parked, run_id=run_id, profile_version=profile_version, dry_run=bool(dry_run))
    if module_logger is not None:
        module_logger.event('browser_process_finish', stage='browser_finish', processed=processed, success=success, failed=failed, parked=parked, run_id=run_id, profile_version=profile_version, dry_run=bool(dry_run))
    return {
        "processed": processed,
        "success": success,
        "failed": failed,
        "parked": parked,
        "run_id": run_id,
        "profile_version": profile_version,
        "message": f"Обработано: {processed}, успешно: {success}, ошибок: {failed}, переведено в ручную проверку: {parked}.",
    }
