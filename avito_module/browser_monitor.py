from __future__ import annotations

import json
import mimetypes
import os
import time
from tempfile import NamedTemporaryFile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .audit import log_avito_event
from .compat import clean_text
from .config import AvitoModuleConfig
from .storage import AvitoStorage


@dataclass(slots=True)
class BrowserSyncResult:
    enabled: bool
    previews: List[Dict[str, Any]]
    notes: List[str]


class AvitoBrowserMonitor:
    """Optional Playwright-based observer.

    This is deliberately written as a fallback/observer, not the primary transport.
    It can recover chat previews when the official API is unavailable for a tenant,
    but it keeps selectors configurable because Avito UI is not stable enough to
    hardcode confidently without live validation.
    """

    def __init__(self, config: AvitoModuleConfig, storage: AvitoStorage) -> None:
        self.config = config
        self.storage = storage

    def available(self) -> bool:
        try:
            import playwright  # noqa: F401
            return True
        except Exception:
            return False

    def collect_chat_previews(self, max_items: int = 30) -> BrowserSyncResult:
        if not self.config.browser_fallback_enabled:
            return BrowserSyncResult(False, [], ["Browser fallback выключен в настройках модуля"])
        if not self.available():
            return BrowserSyncResult(False, [], ["Playwright не установлен в окружении"])
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as exc:
            return BrowserSyncResult(False, [], [f"Не удалось импортировать Playwright: {exc}"])

        selectors = self.config.browser_selector_profile
        notes: List[str] = []
        previews: List[Dict[str, Any]] = []
        state_path = self.storage.paths.browser_state_file
        if not state_path.exists():
            return BrowserSyncResult(False, [], [f"Не найден state-файл браузера: {state_path}"])

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                context = browser.new_context(storage_state=str(state_path))
                page = context.new_page()
                page.goto("https://www.avito.ru/profile/messenger", wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1500)
                items = page.locator(selectors.chat_item)
                count = min(items.count(), max_items)
                for idx in range(count):
                    item = items.nth(idx)
                    title = ""
                    last_message_text = ""
                    unread_count = 0
                    href = ""
                    try:
                        title = clean_text(item.inner_text(timeout=1000).splitlines()[0])
                    except Exception:
                        pass
                    try:
                        href = clean_text(item.get_attribute("href", timeout=1000))
                    except Exception:
                        pass
                    try:
                        unread_text = clean_text(item.locator(selectors.unread_badge).first.inner_text(timeout=500))
                        unread_count = int("".join(ch for ch in unread_text if ch.isdigit()) or 0)
                    except Exception:
                        unread_count = 0
                    try:
                        last_message_text = clean_text(item.inner_text(timeout=1000))
                    except Exception:
                        pass
                    chat_id = self._extract_chat_id(href, fallback=title or f"browser-{idx}")
                    previews.append(
                        {
                            "chat_id": chat_id,
                            "id": chat_id,
                            "title": title or chat_id,
                            "client_name": title,
                            "item_id": "",
                            "item_title": "",
                            "unread_count": unread_count,
                            "last_message_text": last_message_text,
                            "last_message_ts": "",
                            "raw": {"href": href, "source": "browser_fallback"},
                        }
                    )
                notes.append(f"Собрано превью чатов через браузер: {len(previews)}")
                log_avito_event(self.storage, channel="browser", stage="browser_preview_ok", message="Browser fallback собрал превью чатов", kind="avito_browser", count=len(previews))
                return BrowserSyncResult(True, previews, notes)
            except Exception as exc:
                log_avito_event(self.storage, channel="browser", stage="browser_preview_failed", message="Browser fallback завершился ошибкой", level="error", kind="avito_browser", error=str(exc))
                return BrowserSyncResult(False, [], [f"Ошибка browser fallback: {exc}"])
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

    def bootstrap_interactive_login(self, timeout_seconds: int = 0) -> Dict[str, Any]:
        """Launches a headed browser so the user can log into Avito manually.

        The method keeps the browser open for up to ``timeout_seconds`` and periodically
        tries to save storage state. This is intended for local/operator use and mirrors
        the "open separate login window" UX already used in the host WB module.
        """

        timeout_seconds = int(timeout_seconds or self.config.browser_bootstrap_timeout_seconds or 300)
        if not self.available():
            raise RuntimeError("Playwright не установлен в окружении")
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"Не удалось импортировать Playwright: {exc}") from exc

        state_path = self.storage.paths.browser_state_file
        state_path.parent.mkdir(parents=True, exist_ok=True)
        profile_hint_path = self.storage.paths.browser_profile_file
        profile_hint_path.parent.mkdir(parents=True, exist_ok=True)

        start_monotonic = time.monotonic()
        saved = False
        saved_at = ""
        notes: List[str] = []
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False, slow_mo=150)
            try:
                context = browser.new_context()
                page = context.new_page()
                target_url = "https://www.avito.ru/profile/messenger"
                page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                notes.append("Открыл Avito Messenger в отдельном окне браузера")
                log_avito_event(self.storage, channel="browser", stage="bootstrap_started", message="Открыт браузер Avito для ручного входа", kind="avito_browser", target_url=target_url, timeout_seconds=timeout_seconds)

                while (time.monotonic() - start_monotonic) <= max(30, timeout_seconds):
                    page.wait_for_timeout(1500)
                    current_url = clean_text(page.url)
                    looks_logged_in = "/profile" in current_url or "/messages" in current_url or "/profile/messenger" in current_url
                    if not looks_logged_in:
                        continue
                    try:
                        context.storage_state(path=str(state_path))
                        payload = {
                            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "url": current_url,
                            "browser": "chromium",
                            "source": "interactive_bootstrap",
                        }
                        profile_hint_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                        saved = True
                        saved_at = payload["saved_at"]
                        notes.append("Состояние браузера сохранено")
                        break
                    except Exception:
                        continue
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        if not saved:
            log_avito_event(self.storage, channel="browser", stage="bootstrap_timeout", message="Не удалось сохранить Avito state до истечения таймаута", level="warning", kind="avito_browser", timeout_seconds=timeout_seconds)
            raise RuntimeError("Не удалось сохранить Avito browser state до истечения таймаута. Повторите вход и попробуйте ещё раз.")
        log_avito_event(self.storage, channel="browser", stage="bootstrap_saved", message="Состояние браузера Avito сохранено", kind="avito_browser", state_path=str(state_path), saved_at=saved_at)
        return {"ok": True, "state_path": str(state_path), "saved_at": saved_at, "notes": notes}


    def _prepare_local_media_files(self, assets: List[Dict[str, Any]]) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        prepared: List[str] = []
        temp_files: List[str] = []
        skipped: List[Dict[str, Any]] = []
        max_assets = max(1, int(self.config.media_max_send_assets or 4))
        for asset in assets[:max_assets]:
            media_kind = clean_text(asset.get("media_kind") or "image")
            if self.config.media_send_images_only and media_kind != "image":
                skipped.append({"asset_id": asset.get("asset_id"), "reason": "non_image_media_not_allowed", "media_kind": media_kind})
                continue
            local_path = clean_text(asset.get("local_path"))
            if local_path and Path(local_path).exists():
                prepared.append(local_path)
                continue
            external_url = clean_text(asset.get("external_url"))
            if not external_url:
                skipped.append({"asset_id": asset.get("asset_id"), "reason": "no_local_or_external_path", "media_kind": media_kind})
                continue
            try:
                import requests
                response = requests.get(external_url, timeout=30)
                response.raise_for_status()
                ext = Path(external_url.split("?")[0]).suffix
                if not ext:
                    ext = mimetypes.guess_extension(clean_text(asset.get("mime_type")) or "") or ".bin"
                tmp_dir = self.storage.paths.media_dir / "_send_tmp"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                with NamedTemporaryFile(delete=False, suffix=ext, dir=str(tmp_dir)) as fh:
                    fh.write(response.content)
                    temp_name = fh.name
                temp_files.append(temp_name)
                prepared.append(temp_name)
            except Exception as exc:
                skipped.append({"asset_id": asset.get("asset_id"), "reason": f"download_failed:{exc}", "media_kind": media_kind, "external_url": external_url})
        return prepared, temp_files, skipped

    def send_message_with_media(
        self,
        chat_id: str,
        text: str,
        assets: List[Dict[str, Any]],
        *,
        headless: bool | None = None,
        timeout_seconds: int = 120,
    ) -> Dict[str, Any]:
        if not self.config.media_send_enabled:
            raise RuntimeError("Live media send выключен в настройках")
        if not self.config.browser_fallback_enabled:
            raise RuntimeError("Browser transport для медиа выключен в настройках")
        if not self.available():
            raise RuntimeError("Playwright не установлен в окружении")
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"Не удалось импортировать Playwright: {exc}") from exc

        state_path = self.storage.paths.browser_state_file
        if not state_path.exists():
            raise RuntimeError(f"Не найден state-файл браузера: {state_path}")

        file_paths, temp_files, skipped = self._prepare_local_media_files(list(assets or []))
        if not file_paths:
            raise RuntimeError("Нет доступных локальных фото для отправки")

        selectors = self.config.browser_selector_profile
        result: Dict[str, Any] = {
            "ok": False,
            "transport": "browser",
            "chat_id": clean_text(chat_id),
            "attached_files": list(file_paths),
            "skipped_assets": skipped,
            "message": {"id": f"browser-{int(time.time())}", "text": clean_text(text)},
        }
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.config.media_browser_send_headless if headless is None else headless, slow_mo=50)
            try:
                context = browser.new_context(storage_state=str(state_path))
                page = context.new_page()
                page.goto("https://www.avito.ru/profile/messenger", wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)

                direct_candidates = [
                    f'https://www.avito.ru/profile/messenger/{clean_text(chat_id)}',
                    f'https://www.avito.ru/profile/messages/{clean_text(chat_id)}',
                ]
                for target in direct_candidates:
                    try:
                        page.goto(target, wait_until='domcontentloaded', timeout=20000)
                        page.wait_for_timeout(1200)
                        if page.locator(selectors.send_box).count() > 0:
                            break
                    except Exception:
                        pass
                if page.locator(selectors.send_box).count() == 0:
                    candidate = page.locator(f'a[href*="{clean_text(chat_id)}"]').first
                    if candidate.count() == 0:
                        candidate = page.locator(selectors.chat_item).filter(has_text=clean_text(chat_id)).first
                    if candidate.count() == 0:
                        raise RuntimeError(f"Не удалось найти чат {chat_id} в браузерном messenger UI")
                    candidate.click(timeout=10000)
                    page.wait_for_timeout(1500)

                attachment_input = page.locator(selectors.attachment_input).first
                if attachment_input.count() == 0:
                    attach_button = page.locator(selectors.attachment_button).first
                    if attach_button.count() > 0:
                        attach_button.click(timeout=5000)
                        page.wait_for_timeout(700)
                        attachment_input = page.locator(selectors.attachment_input).first
                if attachment_input.count() == 0:
                    raise RuntimeError("Не найден file input для прикрепления фото в Avito UI")
                attachment_input.set_input_files(file_paths, timeout=max(15000, timeout_seconds * 1000 // 2))
                page.wait_for_timeout(1500)

                if clean_text(text):
                    send_box = page.locator(selectors.send_box).first
                    if send_box.count() == 0:
                        raise RuntimeError("Не найден input для текста сообщения")
                    try:
                        send_box.fill(str(text), timeout=5000)
                    except Exception:
                        send_box.click(timeout=5000)
                        page.keyboard.type(str(text))
                send_button = page.locator(selectors.send_button).first
                if send_button.count() == 0:
                    raise RuntimeError("Не найдена кнопка отправки сообщения")
                send_button.click(timeout=10000)
                page.wait_for_timeout(2500)
                result["ok"] = True
                log_avito_event(
                    self.storage,
                    channel="browser",
                    stage="browser_media_send_ok",
                    message="Фото отправлены в Avito через browser transport",
                    kind="avito_browser_media",
                    chat_id=clean_text(chat_id),
                    attached_files=file_paths,
                    skipped_assets=skipped,
                )
                return result
            except Exception as exc:
                log_avito_event(
                    self.storage,
                    channel="browser",
                    stage="browser_media_send_failed",
                    message="Не удалось отправить фото в Avito через browser transport",
                    level="error",
                    kind="avito_browser_media",
                    chat_id=clean_text(chat_id),
                    error=str(exc),
                    attached_files=file_paths,
                    skipped_assets=skipped,
                )
                raise
            finally:
                for temp_path in temp_files:
                    try:
                        os.unlink(temp_path)
                    except Exception:
                        pass
                try:
                    browser.close()
                except Exception:
                    pass

    @staticmethod
    def _extract_chat_id(href: str, fallback: str = "") -> str:
        href = clean_text(href)
        if "/messages/" in href:
            return href.rstrip("/").split("/")[-1]
        return clean_text(fallback) or "browser-chat"
