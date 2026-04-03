from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import common
import safe_files
from safe_logs import log_event

try:
    from browser_bot import (
        BrowserBotError,
        _assert_allowed_navigation_url,
        _body_debug_snippet,
        _close_context,
        _ensure_logged_in,
        _install_context_guards,
    )
except ImportError:
    from browser_bot import (
        BrowserBotError,
        _assert_allowed_navigation_url,
        _close_context,
        _ensure_logged_in,
        _install_context_guards,
    )

    def _body_debug_snippet(page, limit: int = 2000) -> str:
        try:
            body_text = page.locator("body").inner_text(timeout=1500)
            return common.clean_text(body_text)[: max(200, int(limit or 2000))]
        except Exception:
            return ""

PLAYWRIGHT_DEFAULT_TIMEOUT_MS = int(getattr(getattr(common, "config", object()), "PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 15000) or 15000)
PLAYWRIGHT_NAVIGATION_TIMEOUT_MS = int(getattr(getattr(common, "config", object()), "PLAYWRIGHT_NAVIGATION_TIMEOUT_MS", 45000) or 45000)


class AutomationBrowserError(RuntimeError):
    pass


class NetworkRecorder:
    def __init__(self, url_substrings: Optional[Sequence[str]] = None):
        self.url_substrings = [str(item).lower() for item in (url_substrings or []) if str(item).strip()]
        self.events: List[Dict[str, Any]] = []

    def handler(self, response) -> None:
        try:
            url = str(getattr(response, "url", "") or "")
            lowered = url.lower()
            if self.url_substrings and not any(marker in lowered for marker in self.url_substrings):
                return
            row: Dict[str, Any] = {
                "ts": common.utc_now_iso(),
                "url": url,
                "status": int(getattr(response, "status", 0) or 0),
            }
            headers = {}
            try:
                headers = dict(getattr(response, "headers", {}) or {})
            except Exception:
                headers = {}
            content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
            row["content_type"] = content_type
            if "json" in content_type.lower():
                try:
                    payload = response.json()
                    row["json"] = payload if isinstance(payload, (dict, list)) else str(payload)
                except Exception as exc:
                    row["json_error"] = common.clean_text(exc)
            else:
                try:
                    text = response.text()
                    row["text"] = common.clean_text(str(text))[:2000]
                except Exception:
                    pass
            self.events.append(row)
            self.events = self.events[-60:]
        except Exception:
            return


def _clean(value: Any) -> str:
    return common.clean_text(value)


@contextmanager
def open_authenticated_browser(
    tenant_id: str,
    *,
    headless: Optional[bool] = None,
    slow_mo_ms: Optional[int] = None,
    record_video_dir: Optional[Path] = None,
    record_video_size: Optional[Dict[str, int]] = None,
    context_options: Optional[Dict[str, Any]] = None,
    artifacts_holder: Optional[Dict[str, Any]] = None,
) -> Iterator[tuple[Any, Any, Any]]:
    tenant_id = _clean(tenant_id)
    auth_state = Path(str(common.AUTH_STATE_FILE))
    if not auth_state.exists():
        raise AutomationBrowserError(
            f"Не найден файл сессии WB для кабинета {tenant_id}: {auth_state}. Сначала выполните интерактивный вход."
        )
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - real dependency check
        raise AutomationBrowserError(
            "Не установлен Playwright. Выполните: pip install playwright && playwright install"
        ) from exc

    browser = None
    context = None
    page = None
    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch(
                headless=bool(common.PLAYWRIGHT_HEADLESS if headless is None else headless),
                channel=common.PLAYWRIGHT_BROWSER_CHANNEL,
                slow_mo=int(common.PLAYWRIGHT_SLOW_MO_MS if slow_mo_ms is None else slow_mo_ms),
            )
            ctx_options: Dict[str, Any] = {
                "storage_state": str(auth_state),
                "accept_downloads": True,
            }
            if record_video_dir is not None:
                Path(record_video_dir).mkdir(parents=True, exist_ok=True)
                ctx_options["record_video_dir"] = str(record_video_dir)
                if record_video_size:
                    ctx_options["record_video_size"] = dict(record_video_size)
            if context_options:
                ctx_options.update(dict(context_options))
            context = browser.new_context(**ctx_options)
            _install_context_guards(context)
            page = context.new_page()
            yield browser, context, page
        finally:
            _close_context(browser=browser, context=context)
            if artifacts_holder is not None:
                video_path = ""
                try:
                    if page is not None and getattr(page, "video", None) is not None:
                        video_path = str(page.video.path())
                except Exception:
                    video_path = ""
                if not video_path and record_video_dir is not None:
                    try:
                        candidates = sorted(Path(record_video_dir).glob('*.webm'), key=lambda p: p.stat().st_mtime, reverse=True)
                        if candidates:
                            video_path = str(candidates[0])
                    except Exception:
                        video_path = ""
                artifacts_holder["video_path"] = video_path


def safe_goto(page, url: str) -> None:
    _assert_allowed_navigation_url(url)
    page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAVIGATION_TIMEOUT_MS)
    try:
        page.wait_for_load_state("networkidle", timeout=min(12000, PLAYWRIGHT_NAVIGATION_TIMEOUT_MS))
    except Exception:
        pass
    _ensure_logged_in(page)


def capture_page_artifacts(page, destination_dir: Path, stem: str, *, note: str = "", include_html: bool = True) -> Dict[str, str]:
    destination_dir = Path(destination_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = _safe_filename(stem)
    screenshot_path = destination_dir / f"{safe_stem}.png"
    html_path = destination_dir / f"{safe_stem}.html"
    meta_path = destination_dir / f"{safe_stem}.json"
    payload: Dict[str, Any] = {
        "captured_at": common.utc_now_iso(),
        "note": _clean(note),
        "url": "",
        "title": "",
        "body_snippet": "",
    }
    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
    except Exception:
        pass
    try:
        payload["url"] = str(getattr(page, "url", "") or "")
    except Exception:
        pass
    try:
        payload["title"] = _clean(page.title())
    except Exception:
        pass
    try:
        payload["body_snippet"] = _body_debug_snippet(page, limit=1200)
    except Exception:
        pass
    if include_html:
        try:
            html = page.content()
            safe_files.write_text(html_path, html, encoding="utf-8")
        except Exception:
            pass
    safe_files.write_json(meta_path, payload, ensure_ascii=False, indent=2)
    return {
        "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
        "html_path": str(html_path) if html_path.exists() else "",
        "meta_path": str(meta_path),
    }


def wait_any(page_or_scope, selectors: Iterable[str], *, timeout_ms: int = PLAYWRIGHT_DEFAULT_TIMEOUT_MS):
    for selector in selectors:
        selector = str(selector or "").strip()
        if not selector:
            continue
        try:
            locator = page_or_scope.locator(selector).first
            locator.wait_for(timeout=timeout_ms)
            if locator.is_visible():
                return locator
        except Exception:
            continue
    return None


def click_first(
    page_or_scope,
    *,
    selectors: Iterable[str] = (),
    texts: Iterable[str] = (),
    role: str = "button",
    timeout_ms: int = PLAYWRIGHT_DEFAULT_TIMEOUT_MS,
) -> bool:
    locator = wait_any(page_or_scope, selectors, timeout_ms=timeout_ms)
    if locator is not None:
        try:
            locator.scroll_into_view_if_needed(timeout=timeout_ms)
        except Exception:
            pass
        try:
            locator.click(timeout=timeout_ms)
            return True
        except Exception:
            pass
    for text in texts:
        label = _clean(text)
        if not label:
            continue
        candidates = []
        if role:
            try:
                candidates.append(page_or_scope.get_by_role(role, name=label).first)
            except Exception:
                pass
        try:
            candidates.append(page_or_scope.get_by_text(label, exact=False).first)
        except Exception:
            pass
        for candidate in candidates:
            try:
                candidate.wait_for(timeout=timeout_ms)
                if not candidate.is_visible():
                    continue
                try:
                    candidate.scroll_into_view_if_needed(timeout=timeout_ms)
                except Exception:
                    pass
                candidate.click(timeout=timeout_ms)
                return True
            except Exception:
                continue
    return False


def fill_first(page_or_scope, selectors: Iterable[str], value: Any, *, timeout_ms: int = PLAYWRIGHT_DEFAULT_TIMEOUT_MS) -> bool:
    locator = wait_any(page_or_scope, selectors, timeout_ms=timeout_ms)
    if locator is None:
        return False
    try:
        locator.fill(str(value or ""), timeout=timeout_ms)
        return True
    except Exception:
        return False


def click_text_candidates(
    page_or_scope,
    texts: Iterable[str],
    *,
    roles: Sequence[str] = ("button", "menuitem", "radio", "link", "option"),
    timeout_ms: int = PLAYWRIGHT_DEFAULT_TIMEOUT_MS,
) -> str:
    for text in texts:
        label = _clean(text)
        if not label:
            continue
        candidates: List[tuple[str, Any]] = []
        for role in roles or ():
            role_name = _clean(role)
            if not role_name:
                continue
            try:
                candidates.append((f"role:{role_name}:{label}", page_or_scope.get_by_role(role_name, name=label).first))
            except Exception:
                continue
        try:
            candidates.append((f"label:{label}", page_or_scope.get_by_label(label).first))
        except Exception:
            pass
        try:
            candidates.append((f"text:{label}", page_or_scope.get_by_text(label, exact=False).first))
        except Exception:
            pass
        for candidate_name, candidate in candidates:
            try:
                candidate.wait_for(timeout=timeout_ms)
                if not candidate.is_visible():
                    continue
                try:
                    candidate.scroll_into_view_if_needed(timeout=timeout_ms)
                except Exception:
                    pass
                candidate.click(timeout=timeout_ms)
                return candidate_name
            except Exception:
                continue
    return ""


def extract_upload_ids(events: Iterable[Dict[str, Any]]) -> List[int]:
    found: List[int] = []
    seen: set[int] = set()

    def _visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                key_lower = str(key or "").lower()
                if key_lower in {"uploadid", "upload_id"}:
                    try:
                        upload_id = int(item)
                    except Exception:
                        upload_id = 0
                    if upload_id and upload_id not in seen:
                        seen.add(upload_id)
                        found.append(upload_id)
                else:
                    _visit(item)
        elif isinstance(value, list):
            for item in value:
                _visit(item)

    for row in events:
        _visit(row)
    return found


def wait_for_upload_id(events: List[Dict[str, Any]], *, timeout_seconds: float = 20.0, poll_interval: float = 0.5) -> Optional[int]:
    started = time.time()
    while time.time() - started < timeout_seconds:
        ids = extract_upload_ids(events)
        if ids:
            return ids[-1]
        time.sleep(poll_interval)
    return None


def ensure_run_tenant_dir(run_dir: Path, tenant_id: str, section: str) -> Path:
    target = Path(run_dir) / "tenants" / _safe_filename(tenant_id) / _safe_filename(section)
    target.mkdir(parents=True, exist_ok=True)
    return target


def log_browser_step(channel: str, action: str, *, tenant_id: str = "", level: str = "info", **data: Any) -> None:
    log_event(channel, action, tenant_id=_clean(tenant_id) or common.get_active_tenant_id(), level=level, **data)


def _safe_filename(value: Any) -> str:
    text = _clean(value)
    if not text:
        return "artifact"
    result = []
    for char in text:
        if char.isalnum() or char in {"-", "_", "."}:
            result.append(char)
        else:
            result.append("-")
    cleaned = "".join(result).strip("-._")
    return cleaned or "artifact"
