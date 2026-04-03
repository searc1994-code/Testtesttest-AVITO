from __future__ import annotations

import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import automation_browser
import automation_core
import background_jobs
import common
import promo_calendar
import safe_files
import tenant_manager
from safe_logs import log_event

DEFAULT_PROMO_PROFILE = {
    "calendar_url": f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar",
    "detail_url_template": f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar?action={{promotion_id}}",
    "detail_ready_selectors": [
        "[data-testid='auto-promo-wizard/step-0/action']",
        "[data-testid='auto-promo-wizard/step-0/submit-button-button-primary']",
        "[data-testid='auto-promo-wizard/step-1']",
    ],
    "configure_button_selectors": [
        "[data-testid='auto-promo-wizard/step-0/submit-button-button-primary']",
    ],
    "configure_button_texts": [
        "Настроить список товаров",
        "Исключить товары",
        "Изменить список товаров",
    ],
    "step1_ready_selectors": [
        "[data-testid='auto-promo-wizard/step-1']",
        "[data-testid='auto-promo-file-uploader/file-uploader-file-uploader-view-input']",
        "input[type='file'][accept*='xlsx']",
    ],
    "generate_button_selectors": [
        "[data-testid='auto-promo-wizard/step-1/generate-excel-file-button-button-interface']",
    ],
    "generate_button_texts": ["Сформировать файл"],
    "download_button_selectors": [
        "[data-testid='auto-promo-wizard/step-1/download-excel-file-button-button-interface']",
    ],
    "download_button_texts": ["Скачать файл"],
    "upload_input_selectors": [
        "[data-testid='auto-promo-file-uploader/file-uploader-file-uploader-view-input']",
        "input[type='file'][accept*='xlsx']",
        "input[type='file']",
    ],
    "upload_trigger_selectors": [
        "[data-testid='auto-promo-file-uploader']",
        "[data-testid='auto-promo-file-uploader/file-uploader-file-uploader-action-placeholder']",
    ],
    "submit_button_selectors": [
        "[data-testid='auto-promo-wizard/step-1/submit-button-button-primary']",
    ],
    "submit_button_texts": [
        "Исключить товары из акции",
        "Исключить товары",
        "Сохранить",
        "Применить",
    ],
    "confirm_checkbox_selectors": [
        "[data-testid='auto-promo-wizard/step-1/confirm-decision-banner-checkbox-checkbox-simple-input']",
        "input#confirm-decision-checkbox",
        "input[name='confirm-decision-checkbox']",
    ],
    "confirm_checkbox_label_selectors": [
        "label[for='confirm-decision-checkbox']",
        "[data-testid='auto-promo-wizard/step-1/confirm-decision-banner-checkbox-checkbox-with-label-label']",
        "[data-testid='auto-promo-wizard/step-1/confirm-decision-banner-checkbox-checkbox-with-label']",
        "[data-testid='auto-promo-wizard/step-1/confirm-decision-banner-checkbox-checkbox-simple']",
        "[data-testid='auto-promo-wizard/step-1/confirm-decision-banner-checkbox-checkbox-simple-icon']",
    ],
    "confirm_checkbox_texts": [
        "Подтверждаю своё решение",
        "Подтверждаю",
    ],
    "verify_skip_texts": [
        "Пропускаю",
        "Не участвую",
        "Товары исключены",
        "Список товаров сохранён",
        "Список товаров обновлён",
        "Изменения сохранены",
    ],
    "verify_bad_texts": [
        "Буду участвовать",
        "Участвую",
    ],
    "promo_card_selectors": [
        "[data-testid*='promotion']",
        "[class*='PromoCard']",
        "[class*='promotion-card']",
        "article",
        "li",
        "div",
    ],
    "step_transition_timeout_ms": 30000,
    "generate_wait_seconds": 90,
    "verify_wait_seconds": 45,
    "download_poll_seconds": 3,
    "network_markers": [
        "dp-promo-calendar",
        "promotion",
        "promo",
        "excel",
        "upload",
    ],
}


class PromotionExecutionError(RuntimeError):
    pass


BrowserPromoHandler = Callable[[str, Dict[str, Any], Path, Dict[str, Any]], Dict[str, Any]]


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _load_profile() -> Dict[str, Any]:
    profile = dict(DEFAULT_PROMO_PROFILE)
    try:
        custom = common.load_ui_profile() or {}
    except Exception:
        custom = {}
    if isinstance(custom, dict):
        promo_custom = custom.get("automation_promo") if isinstance(custom.get("automation_promo"), dict) else {}
        for key, value in promo_custom.items():
            if value:
                profile[key] = value
    return profile


def _text_markers(promo: Dict[str, Any]) -> List[str]:
    markers: List[str] = []
    promo_id = promo.get("id")
    if promo_id is not None:
        markers.append(str(promo_id))
    for key in ["name", "description"]:
        text = _clean(promo.get(key))
        if text:
            markers.append(text[:140])
            if ":" in text:
                markers.append(text.split(":", 1)[-1].strip()[:140])
    for key in ["startDateTime", "endDateTime"]:
        text = _clean(promo.get(key))
        if text:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                markers.append(dt.strftime("%d.%m.%Y"))
            except Exception:
                pass
    uniq: List[str] = []
    for marker in markers:
        if marker and marker not in uniq:
            uniq.append(marker)
    return uniq[:12]


def _body_text(page, *, limit: int = 12000) -> str:
    try:
        body = page.locator("body").inner_text(timeout=2500)
        return _clean(body)[: max(1000, int(limit or 12000))]
    except Exception:
        return _clean(automation_browser._body_debug_snippet(page, limit=limit))


def _contains_any(text: str, phrases: Iterable[str]) -> bool:
    lowered = _clean(text).lower()
    return any(_clean(item).lower() in lowered for item in phrases if _clean(item))


def _extract_int_before(text: str, phrase: str) -> Optional[int]:
    pattern = re.compile(rf"([\d\s\xa0]+)\s+{re.escape(phrase)}", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return None
    raw = re.sub(r"\D", "", match.group(1))
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _extract_int_after(text: str, phrase: str) -> Optional[int]:
    pattern = re.compile(rf"{re.escape(phrase)}\s*([\d\s\xa0]+)", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return None
    raw = re.sub(r"\D", "", match.group(1))
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _wait_for_visible(page, selectors: Sequence[str], *, timeout_ms: int, poll_ms: int = 500):
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        locator = automation_browser.wait_any(page, selectors, timeout_ms=min(poll_ms, timeout_ms))
        if locator is not None:
            return locator
        try:
            page.wait_for_timeout(poll_ms)
        except Exception:
            time.sleep(poll_ms / 1000.0)
    return None


def _find_attached_locator(page_or_scope, selectors: Sequence[str]):
    for selector in selectors:
        selector = _clean(selector)
        if not selector:
            continue
        try:
            locator = page_or_scope.locator(selector)
            if locator.count() > 0:
                return locator.first
        except Exception:
            continue
    return None


def _wait_input_attached(page, selectors: Sequence[str], *, timeout_ms: int, poll_ms: int = 500):
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        locator = _find_attached_locator(page, selectors)
        if locator is not None:
            return locator
        try:
            page.wait_for_timeout(poll_ms)
        except Exception:
            time.sleep(poll_ms / 1000.0)
    return None


def _find_button(page_or_scope, *, selectors: Sequence[str] = (), texts: Sequence[str] = (), timeout_ms: int = 4000):
    locator = automation_browser.wait_any(page_or_scope, selectors, timeout_ms=timeout_ms)
    if locator is not None:
        return locator
    for label in texts:
        text = _clean(label)
        if not text:
            continue
        candidates = []
        try:
            candidates.append(page_or_scope.get_by_role("button", name=text).first)
        except Exception:
            pass
        try:
            candidates.append(page_or_scope.get_by_text(text, exact=False).first)
        except Exception:
            pass
        for candidate in candidates:
            try:
                candidate.wait_for(timeout=timeout_ms)
                if candidate.is_visible():
                    return candidate
            except Exception:
                continue
    return None


def _wait_button_enabled(page_or_scope, *, selectors: Sequence[str] = (), texts: Sequence[str] = (), timeout_ms: int = 15000):
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        button = _find_button(page_or_scope, selectors=selectors, texts=texts, timeout_ms=1200)
        if button is not None:
            try:
                disabled = button.is_disabled()
            except Exception:
                disabled = False
            if not disabled:
                return button
        try:
            page_or_scope.page.wait_for_timeout(500)  # type: ignore[attr-defined]
        except Exception:
            time.sleep(0.5)
    return None


def _save_network_trace(tenant_dir: Path, stem: str, recorder: automation_browser.NetworkRecorder) -> str:
    path = tenant_dir / f"{automation_browser._safe_filename(stem)}_network.json"
    safe_files.write_json(path, recorder.events, ensure_ascii=False, indent=2)
    return str(path)


def _promo_workspace_dir() -> Path:
    root = Path(getattr(automation_core, "AUTOMATION_ROOT", Path(common.SHARED_DIR) / "automation"))
    target = root / "promo_workspace"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _resolve_local_exclusion_file(tenant_id: str) -> Optional[Path]:
    workspace = _promo_workspace_dir()
    candidates = [
        workspace / f"promo_exclusion__{tenant_id}.xlsx",
        workspace / "promo_exclusion_template.xlsx",
        workspace / "Товары для исключения 1.xlsx",
    ]
    for pattern in ["Товары для исключения*.xlsx", "*.xlsx"]:
        for path in sorted(workspace.glob(pattern)):
            if path not in candidates:
                candidates.append(path)
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def _wait_download(page, button_locator, save_to: Path, *, timeout_seconds: int, poll_seconds: float) -> Path:
    save_to.parent.mkdir(parents=True, exist_ok=True)
    last_error = ""
    deadline = time.time() + max(5, int(timeout_seconds))
    while time.time() < deadline:
        try:
            with page.expect_download(timeout=max(2000, int(poll_seconds * 1000)) + 5000) as download_info:
                try:
                    button_locator.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                button_locator.click(timeout=5000)
            download = download_info.value
            try:
                suggested = _clean(download.suggested_filename)
            except Exception:
                suggested = ""
            final_path = save_to if save_to.suffix else save_to / (suggested or "promo_exclusion.xlsx")
            if final_path.is_dir():
                final_path = final_path / (suggested or "promo_exclusion.xlsx")
            download.save_as(str(final_path))
            return final_path
        except Exception as exc:
            last_error = _clean(exc)
            try:
                page.wait_for_timeout(int(poll_seconds * 1000))
            except Exception:
                time.sleep(poll_seconds)
    raise PromotionExecutionError(last_error or "Не удалось дождаться скачивания файла для исключения товаров из акции")


def _open_promotion_detail(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    promo_id = int(promo.get("id") or 0)
    detail_template = _clean(profile.get("detail_url_template")) or f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar?action={{promotion_id}}"
    url = detail_template.format(promotion_id=promo_id)
    last_error = ""
    try:
        automation_browser.safe_goto(page, url)
        ready = _wait_for_visible(page, profile.get("detail_ready_selectors") or [], timeout_ms=max(15000, int(profile.get("step_transition_timeout_ms") or 30000)))
        if ready is None and not _contains_any(_body_text(page), [promo.get("name"), "Настроить список товаров", "Сформировать файл"]):
            raise PromotionExecutionError(f"Не открылась карточка акции {promo_id} по ссылке {url}")
        return {"opened_url": url, "open_mode": "detail_direct"}
    except Exception as exc:
        last_error = _clean(exc)
        automation_browser.capture_page_artifacts(page, tenant_dir, f"open_error_detail_{promo_id}", note=last_error)
    calendar_url = _clean(profile.get("calendar_url")) or f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar"
    try:
        automation_browser.safe_goto(page, calendar_url)
        ready = _wait_for_visible(page, profile.get("promo_card_selectors") or ["div"], timeout_ms=15000)
        if ready is None:
            raise PromotionExecutionError("Не открылась страница календаря акций")
        container, matched_by = _find_promotion_container(page, promo, profile)
        try:
            container.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        try:
            container.click(timeout=5000)
        except Exception:
            if not automation_browser.click_first(container, texts=[_clean(promo.get("name"))], timeout_ms=5000):
                raise
        detail_ready = _wait_for_visible(page, profile.get("detail_ready_selectors") or [], timeout_ms=max(12000, int(profile.get("step_transition_timeout_ms") or 30000)))
        if detail_ready is None and not _contains_any(_body_text(page), [promo.get("name"), "Настроить список товаров"]):
            raise PromotionExecutionError(f"После открытия карточки акции не загрузилась детальная панель: {matched_by}")
        return {"opened_url": calendar_url, "open_mode": "calendar_click", "matched_by": matched_by}
    except Exception as exc:
        last_error = _clean(exc)
        automation_browser.capture_page_artifacts(page, tenant_dir, f"open_error_calendar_{promo_id}", note=last_error)
        raise PromotionExecutionError(last_error or f"Не удалось открыть акцию {promo_id} в календаре WB")


def _find_promotion_container(page, promo: Dict[str, Any], profile: Dict[str, Any]):
    markers = _text_markers(promo)
    for marker in markers:
        if len(marker) < 3:
            continue
        try:
            locator = page.get_by_text(marker, exact=False).first
            locator.wait_for(timeout=2500)
            if not locator.is_visible():
                continue
            for xpath in [
                "xpath=ancestor::tr[1]",
                "xpath=ancestor::li[1]",
                "xpath=ancestor::article[1]",
            ] + [f"xpath=ancestor::div[{i}]" for i in range(1, 14)]:
                try:
                    container = locator.locator(xpath).first
                    if container.count() < 1 or not container.is_visible():
                        continue
                    text = _clean(container.inner_text())
                    if not text:
                        continue
                    if any(marker in text for marker in markers[:3]):
                        return container, f"text:{marker[:40]}"
                except Exception:
                    continue
        except Exception:
            continue
    for selector in profile.get("promo_card_selectors") or []:
        try:
            rows = page.locator(selector)
            count = min(rows.count(), 250)
            for index in range(count):
                row = rows.nth(index)
                try:
                    if not row.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    text = _clean(row.inner_text())
                except Exception:
                    continue
                if not text:
                    continue
                if all(marker not in text for marker in markers[:3]):
                    continue
                return row, f"fallback:{selector}"
        except Exception:
            continue
    raise PromotionExecutionError(f"Не удалось найти акцию на странице WB. Маркеры: {', '.join(markers[:4]) or 'нет'}")


def _open_exclusion_step(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    if _wait_for_visible(page, profile.get("step1_ready_selectors") or [], timeout_ms=2000) is not None:
        return {"wizard_step": "step1_already_open"}
    button = _wait_button_enabled(
        page,
        selectors=profile.get("configure_button_selectors") or [],
        texts=profile.get("configure_button_texts") or [],
        timeout_ms=max(10000, int(profile.get("step_transition_timeout_ms") or 30000)),
    )
    if button is None:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_missing_configure", note="Не найдена кнопка 'Настроить список товаров'")
        raise PromotionExecutionError("Не найдена кнопка 'Настроить список товаров'")
    try:
        button.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass
    button.click(timeout=5000)
    ready = _wait_for_visible(page, profile.get("step1_ready_selectors") or [], timeout_ms=max(10000, int(profile.get("step_transition_timeout_ms") or 30000)))
    if ready is None:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_step1_not_ready", note="Не открылась форма исключения товаров из акции")
        raise PromotionExecutionError("Не открылась форма исключения товаров из акции")
    return {"wizard_step": "step1_opened"}


def _generate_download_file(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {"source_mode": "generated"}
    generate_button = _wait_button_enabled(
        page,
        selectors=profile.get("generate_button_selectors") or [],
        texts=profile.get("generate_button_texts") or [],
        timeout_ms=10000,
    )
    if generate_button is None:
        raise PromotionExecutionError("Не найдена кнопка 'Сформировать файл'")
    try:
        generate_button.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass
    generate_button.click(timeout=5000)
    try:
        page.wait_for_timeout(1500)
    except Exception:
        time.sleep(1.5)
    download_button = _wait_button_enabled(
        page,
        selectors=profile.get("download_button_selectors") or [],
        texts=profile.get("download_button_texts") or [],
        timeout_ms=max(15000, int(profile.get("generate_wait_seconds") or 90) * 1000),
    )
    if download_button is None:
        raise PromotionExecutionError("Файл для исключения не стал доступен для скачивания")
    save_path = tenant_dir / f"promo_{int(promo.get('id') or 0)}__generated_exclusion.xlsx"
    download_path = _wait_download(
        page,
        download_button,
        save_path,
        timeout_seconds=int(profile.get("generate_wait_seconds") or 90),
        poll_seconds=float(profile.get("download_poll_seconds") or 3),
    )
    result["file_path"] = str(download_path)
    return result


def _pick_exclusion_file(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path, tenant_id: str) -> Dict[str, Any]:
    errors: List[str] = []
    try:
        return _generate_download_file(page, promo, profile, tenant_dir)
    except Exception as exc:
        errors.append(f"generated: {_clean(exc)}")
    template = _resolve_local_exclusion_file(tenant_id)
    if template is not None:
        return {
            "source_mode": "workspace_template",
            "file_path": str(template),
            "file_name": template.name,
        }
    raise PromotionExecutionError(
        "Не удалось подготовить файл для исключения товаров из акции. "
        f"Ошибки: {'; '.join(errors) or 'нет'}. "
        f"Либо исправьте шаг 'Сформировать файл', либо положите универсальный XLSX в {_promo_workspace_dir()}"
    )


def _try_filechooser_upload(page, profile: Dict[str, Any], file_path: Path) -> bool:
    for selector in profile.get("upload_trigger_selectors") or []:
        selector = _clean(selector)
        if not selector:
            continue
        try:
            candidate = page.locator(selector)
            if candidate.count() < 1:
                continue
            trigger = candidate.first
            with page.expect_file_chooser(timeout=4000) as chooser_info:
                try:
                    trigger.scroll_into_view_if_needed(timeout=2000)
                except Exception:
                    pass
                try:
                    trigger.click(timeout=3000, force=True)
                except Exception:
                    trigger.click(timeout=3000)
            chooser = chooser_info.value
            chooser.set_files(str(file_path))
            return True
        except Exception:
            continue
    return False


def _wait_upload_registered(page, profile: Dict[str, Any], file_path: Path, *, timeout_ms: int) -> Dict[str, Any]:
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    stem = _clean(file_path.stem).lower()
    file_name = _clean(file_path.name).lower()
    ready_selectors = [
        "[data-testid='auto-promo-file-uploader/file-card-file-card']",
        "[data-testid='auto-promo-file-uploader/file-card-file-name-text']",
        "[data-testid='auto-promo-file-uploader/file-card-desktop-button-delete-button-link']",
        "[data-testid='auto-promo-file-uploader/file-card-desktop-button-download-button-link']",
    ]
    while time.time() < deadline:
        body = _body_text(page, limit=5000).lower()
        if file_name and file_name in body:
            return {"registered": True, "reason": "file_name_visible"}
        if stem and stem in body:
            return {"registered": True, "reason": "file_stem_visible"}
        locator = _find_attached_locator(page, ready_selectors)
        if locator is not None:
            return {"registered": True, "reason": "file_card_visible"}
        try:
            page.wait_for_timeout(700)
        except Exception:
            time.sleep(0.7)
    return {"registered": False, "reason": "timeout"}


def _set_input_file(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path, file_path: Path) -> str:
    if not file_path.exists():
        raise PromotionExecutionError(f"Не найден файл для загрузки в акцию: {file_path}")

    input_locator = _wait_input_attached(page, profile.get("upload_input_selectors") or [], timeout_ms=10000)
    uploaded = False
    if input_locator is not None:
        try:
            input_locator.set_input_files(str(file_path), timeout=10000)
            uploaded = True
        except Exception:
            uploaded = False
    if not uploaded:
        uploaded = _try_filechooser_upload(page, profile, file_path)

    if not uploaded:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_missing_upload_input", note="Не найден рабочий input/filechooser для загрузки Excel-файла")
        raise PromotionExecutionError("Не найден рабочий input/filechooser для загрузки Excel-файла")

    try:
        page.wait_for_timeout(1200)
    except Exception:
        time.sleep(1.2)

    wait_result = _wait_upload_registered(
        page,
        profile,
        file_path,
        timeout_ms=max(12000, int(profile.get("step_transition_timeout_ms") or 30000)),
    )
    if not wait_result.get("registered"):
        automation_browser.capture_page_artifacts(
            page,
            tenant_dir,
            f"promo_{promo.get('id')}_upload_not_applied",
            note=f"Файл выбран, но WB не подтвердил загрузку: {_clean(wait_result.get('reason')) or 'timeout'}",
        )
        raise PromotionExecutionError("WB не подтвердил загрузку Excel-файла: кнопка подтверждения не активировалась")
    return str(file_path)




def _is_submit_enabled(page, profile: Dict[str, Any]) -> bool:
    button = _find_button(page, selectors=profile.get("submit_button_selectors") or [], texts=profile.get("submit_button_texts") or [], timeout_ms=1200)
    if button is None:
        return False
    try:
        return not button.is_disabled()
    except Exception:
        return True


def _checkbox_checked_dom(page, profile: Dict[str, Any]) -> bool:
    selectors = [item for item in (profile.get("confirm_checkbox_selectors") or []) if _clean(item)]
    script = """
(args) => {
  const selectors = Array.isArray(args?.selectors) ? args.selectors : [];
  for (const selector of selectors) {
    try {
      const input = document.querySelector(selector);
      if (input) return !!input.checked;
    } catch (e) {}
  }
  return false;
}
"""
    try:
        return bool(page.evaluate(script, {"selectors": selectors}))
    except Exception:
        return False


def _wait_confirmation_applied(page, checkbox, profile: Dict[str, Any], timeout_ms: int = 4000) -> bool:
    deadline = time.time() + max(0.5, timeout_ms / 1000.0)
    while time.time() < deadline:
        try:
            if checkbox.is_checked():
                return True
        except Exception:
            pass
        if _checkbox_checked_dom(page, profile):
            return True
        try:
            page.wait_for_timeout(250)
        except Exception:
            time.sleep(0.25)
    return False


def _click_locator_center(page, locator, *, attempts: int = 2) -> bool:
    for _ in range(max(1, attempts)):
        try:
            locator.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass
        try:
            box = locator.bounding_box()
        except Exception:
            box = None
        if box:
            try:
                page.mouse.move(box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0)
                page.mouse.down()
                page.mouse.up()
                return True
            except Exception:
                pass
            try:
                page.mouse.click(box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0)
                return True
            except Exception:
                pass
    return False


def _force_confirm_checkbox_via_dom(page, profile: Dict[str, Any]) -> bool:
    selectors = [item for item in (profile.get("confirm_checkbox_selectors") or []) if _clean(item)]
    label_selectors = [item for item in (profile.get("confirm_checkbox_label_selectors") or []) if _clean(item)]
    script = """
(args) => {
  const selectors = Array.isArray(args?.selectors) ? args.selectors : [];
  const labelSelectors = Array.isArray(args?.labelSelectors) ? args.labelSelectors : [];
  const find = (items) => {
    for (const selector of items || []) {
      try {
        const node = document.querySelector(selector);
        if (node) return node;
      } catch (e) {}
    }
    return null;
  };
  const input = find(selectors);
  if (!input) return {ok:false, reason:'input_not_found'};
  const dispatchPointerSequence = (node) => {
    if (!node) return false;
    try { if (node.scrollIntoView) node.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
    const types = ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click'];
    for (const type of types) {
      try {
        const event = type.startsWith('pointer')
          ? new PointerEvent(type, {bubbles:true, cancelable:true, composed:true, pointerType:'mouse', isPrimary:true, buttons:1})
          : new MouseEvent(type, {bubbles:true, cancelable:true, composed:true, buttons:1});
        node.dispatchEvent(event);
      } catch (e) {
        try { node.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, composed:true})); } catch (e2) {}
      }
    }
    return true;
  };
  const nativeCheckedSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'checked')?.set;
  const setChecked = (value) => {
    try {
      if (nativeCheckedSetter) nativeCheckedSetter.call(input, value);
      else input.checked = value;
      if (value) input.setAttribute('checked', 'checked');
      else input.removeAttribute('checked');
      input.dispatchEvent(new Event('input', {bubbles:true, composed:true}));
      input.dispatchEvent(new Event('change', {bubbles:true, composed:true}));
      return true;
    } catch (e) {
      return false;
    }
  };
  const wrappers = [];
  try { if (input.closest('[data-testid]')) wrappers.push(input.closest('[data-testid]')); } catch (e) {}
  try { if (input.parentElement) wrappers.push(input.parentElement); } catch (e) {}
  try { if (input.parentElement && input.parentElement.parentElement) wrappers.push(input.parentElement.parentElement); } catch (e) {}
  try { if (input.parentElement && input.parentElement.parentElement && input.parentElement.parentElement.parentElement) wrappers.push(input.parentElement.parentElement.parentElement); } catch (e) {}
  const label = input.id ? document.querySelector(`label[for="${input.id}"]`) : null;
  const altLabel = find(labelSelectors);
  const clickTargets = [label, altLabel, ...wrappers, input].filter(Boolean);
  for (const node of clickTargets) {
    try { node.click?.(); } catch (e) {}
    if (input.checked) return {ok:true, mode:'dom_click'};
    dispatchPointerSequence(node);
    if (input.checked) return {ok:true, mode:'dom_pointer'};
  }
  if (setChecked(true)) {
    dispatchPointerSequence(input);
    if (input.checked) return {ok:true, mode:'native_setter'};
  }
  return {ok: !!input.checked, mode:'failed'};
}
"""
    try:
        result = page.evaluate(script, {"selectors": selectors, "labelSelectors": label_selectors})
    except Exception:
        return False
    if isinstance(result, dict):
        return bool(result.get("ok"))
    return bool(result)


def _ensure_confirmation_checked(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    checkbox = _wait_input_attached(page, profile.get("confirm_checkbox_selectors") or [], timeout_ms=10000)
    if checkbox is None:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_confirm_checkbox_missing", note="Не найден чекбокс подтверждения перед исключением товаров")
        raise PromotionExecutionError("Не найден чекбокс подтверждения перед исключением товаров")

    if _wait_confirmation_applied(page, checkbox, profile, timeout_ms=1000):
        return {"confirmed": True, "mode": "already_checked"}

    errors = []

    def _confirmed(mode: str, timeout_ms: int = 2500):
        if _wait_confirmation_applied(page, checkbox, profile, timeout_ms=timeout_ms):
            return {"confirmed": True, "mode": mode}
        return None

    try:
        control = page.get_by_label("Подтверждаю своё решение", exact=False).first
        control.wait_for(timeout=2500)
        control.check(timeout=4000, force=True)
        result = _confirmed("get_by_label_check")
        if result:
            return result
    except Exception as exc:
        errors.append(_clean(exc))

    try:
        checkbox.set_checked(True, timeout=5000, force=True)
        result = _confirmed("set_checked")
        if result:
            return result
    except Exception as exc:
        errors.append(_clean(exc))

    try:
        checkbox.click(timeout=3000)
        result = _confirmed("checkbox_click")
        if result:
            return result
    except Exception as exc:
        errors.append(_clean(exc))

    try:
        checkbox.click(timeout=3000, force=True)
        result = _confirmed("checkbox_click_force")
        if result:
            return result
    except Exception as exc:
        errors.append(_clean(exc))

    try:
        if _click_locator_center(page, checkbox, attempts=2):
            result = _confirmed("checkbox_mouse_center")
            if result:
                return result
    except Exception as exc:
        errors.append(_clean(exc))

    for selector in profile.get("confirm_checkbox_label_selectors") or []:
        selector = _clean(selector)
        if not selector:
            continue
        try:
            label = page.locator(selector)
            if label.count() < 1:
                continue
            target = label.first
            try:
                target.scroll_into_view_if_needed(timeout=2000)
            except Exception:
                pass
            try:
                target.click(timeout=3000)
            except Exception as exc:
                errors.append(_clean(exc))
            result = _confirmed(f"label:{selector}")
            if result:
                return result
            try:
                target.click(timeout=3000, force=True)
            except Exception as exc:
                errors.append(_clean(exc))
            result = _confirmed(f"label_force:{selector}")
            if result:
                return result
            if _click_locator_center(page, target, attempts=2):
                result = _confirmed(f"label_mouse:{selector}")
                if result:
                    return result
        except Exception as exc:
            errors.append(_clean(exc))

    for text_label in profile.get("confirm_checkbox_texts") or []:
        label_text = _clean(text_label)
        if not label_text:
            continue
        try:
            target = page.get_by_text(label_text, exact=False).first
            target.wait_for(timeout=2000)
            try:
                target.click(timeout=3000)
            except Exception as exc:
                errors.append(_clean(exc))
            result = _confirmed(f"text:{label_text}")
            if result:
                return result
            try:
                target.click(timeout=3000, force=True)
            except Exception as exc:
                errors.append(_clean(exc))
            result = _confirmed(f"text_force:{label_text}")
            if result:
                return result
            if _click_locator_center(page, target, attempts=2):
                result = _confirmed(f"text_mouse:{label_text}")
                if result:
                    return result
        except Exception as exc:
            errors.append(_clean(exc))

    try:
        checkbox.press("Space", timeout=3000)
        result = _confirmed("checkbox_space")
        if result:
            return result
    except Exception as exc:
        errors.append(_clean(exc))

    try:
        if _force_confirm_checkbox_via_dom(page, profile):
            result = _confirmed("dom_force", timeout_ms=3000)
            if result:
                return result
    except Exception as exc:
        errors.append(_clean(exc))

    automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_confirm_checkbox_failed", note="; ".join(item for item in errors if item) or "checkbox_not_checked")
    raise PromotionExecutionError("Не удалось отметить подтверждение перед исключением товаров из акции")


def _submit_exclusion(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    confirm_meta = _ensure_confirmation_checked(page, promo, profile, tenant_dir)
    try:
        page.wait_for_timeout(500)
    except Exception:
        time.sleep(0.5)
    submit_button = _wait_button_enabled(
        page,
        selectors=profile.get("submit_button_selectors") or [],
        texts=profile.get("submit_button_texts") or [],
        timeout_ms=25000,
    )
    if submit_button is None:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_submit_not_ready", note="Кнопка 'Исключить товары из акции' осталась недоступной")
        raise PromotionExecutionError("Кнопка 'Исключить товары из акции' осталась недоступной после загрузки файла")
    recorder = automation_browser.NetworkRecorder(profile.get("network_markers") or [])
    try:
        page.on("response", recorder.handler)
    except Exception:
        recorder = automation_browser.NetworkRecorder([])
    try:
        try:
            submit_button.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        submit_button.click(timeout=5000)
        try:
            page.wait_for_load_state("networkidle", timeout=max(12000, int(profile.get("verify_wait_seconds") or 45) * 1000))
        except Exception:
            pass
        try:
            page.wait_for_timeout(2000)
        except Exception:
            time.sleep(2)
    finally:
        try:
            page.remove_listener("response", recorder.handler)
        except Exception:
            pass
    network_path = _save_network_trace(tenant_dir, f"promo_{promo.get('id')}_submit", recorder)
    return {"network_path": network_path, "network_events": recorder.events[-20:], **confirm_meta}


def _browser_verify_current_page(page, promo: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    body = _body_text(page)
    participants = _extract_int_before(body, "будут участвовать")
    excluded = _extract_int_before(body, "исключено")
    if excluded is None:
        excluded = _extract_int_after(body, "не участвует")
    participating_now = _extract_int_after(body, "участвует")
    has_skip = _contains_any(body, profile.get("verify_skip_texts") or [])
    has_bad = _contains_any(body, profile.get("verify_bad_texts") or [])
    has_update_note = "количество товаров в акции обновится" in body.lower()
    success = False
    reason = ""
    partial = False
    if participants == 0 and (excluded is None or excluded >= 0):
        success = True
        reason = "zero_participants"
    elif participating_now == 0 and excluded is not None and excluded >= 0:
        success = True
        reason = "calendar_counts_zero_participating"
    elif has_skip and not has_bad:
        success = True
        reason = "skip_text"
    elif has_update_note and participating_now == 0:
        success = True
        reason = "queued_update_zero_participating"
    elif excluded is not None and excluded > 0:
        partial = True
        reason = "excluded_positive_partial"
    return {
        "success": success,
        "reason": reason,
        "participants": participants,
        "excluded": excluded,
        "participating_now": participating_now,
        "has_skip": has_skip,
        "has_bad": has_bad,
        "has_update_note": has_update_note,
        "partial": partial,
        "body_excerpt": body[:1200],
    }


def _browser_verify_detail_reload(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    try:
        detail_url = (_clean(profile.get("detail_url_template")) or f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar?action={{promotion_id}}")
        detail_url = detail_url.format(promotion_id=int(promo.get("id") or 0))
        automation_browser.safe_goto(page, detail_url)
        current = _browser_verify_current_page(page, promo, profile)
        if current.get("success"):
            return {"mode": "detail_reload", **current}
        return {"mode": "detail_reload", **current, "reason": _clean(current.get("reason")) or "detail_reload_not_confirmed"}
    except Exception as exc:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_detail_verify_error", note=_clean(exc))
        return {
            "success": False,
            "reason": _clean(exc) or "detail_reload_failed",
            "mode": "detail_reload",
        }


def _browser_verify_calendar_card(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    try:
        automation_browser.safe_goto(page, _clean(profile.get("calendar_url")) or f"{common.WB_SELLER_BASE_URL}/dp-promo-calendar")
        container, matched_by = _find_promotion_container(page, promo, profile)
        text = _clean(container.inner_text())
        lowered = text.lower()
        participants = _extract_int_after(text, "Участвует")
        excluded = _extract_int_after(text, "Не участвует")
        success = (("пропускаю" in lowered or "не участвую" in lowered) and "буду участвовать" not in lowered) or (participants == 0 and excluded is not None)
        reason = "calendar_badge" if (("пропускаю" in lowered or "не участвую" in lowered) and "буду участвовать" not in lowered) else ("calendar_counts" if success else "calendar_badge_not_found")
        return {
            "success": success,
            "reason": reason,
            "matched_by": matched_by,
            "participants": participants,
            "excluded": excluded,
            "card_excerpt": text[:1200],
        }
    except Exception as exc:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo.get('id')}_calendar_verify_error", note=_clean(exc))
        return {
            "success": False,
            "reason": _clean(exc) or "calendar_verify_failed",
        }


def _verify_exclusion(page, promo: Dict[str, Any], profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    deadline = time.time() + max(10, int(profile.get("verify_wait_seconds") or 45))
    last_current: Dict[str, Any] = {}
    while time.time() < deadline:
        current = _browser_verify_current_page(page, promo, profile)
        last_current = current
        if current.get("success"):
            return {"mode": "current_page", **current}
        try:
            page.wait_for_timeout(2000)
        except Exception:
            time.sleep(2)
    detail_result = _browser_verify_detail_reload(page, promo, profile, tenant_dir)
    if detail_result.get("success"):
        return detail_result
    calendar_result = _browser_verify_calendar_card(page, promo, profile, tenant_dir)
    if calendar_result.get("success"):
        return {"mode": "calendar", **calendar_result}
    return {
        "mode": "unknown",
        **calendar_result,
        "detail_result": detail_result,
        "last_current": last_current,
    }


def _execute_single_browser_promotion(page, promo: Dict[str, Any], tenant_dir: Path, profile: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
    promo_id = int(promo.get("id") or 0)
    background_jobs.progress("promo_item_open_detail", f"Открываю карточку акции {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=10)
    open_meta = _open_promotion_detail(page, promo, profile, tenant_dir)
    before = automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo_id}_before", note="До открытия настройки списка товаров")

    background_jobs.progress("promo_item_open_configure", f"Открываю настройку списка товаров для акции {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=25)
    wizard_meta = _open_exclusion_step(page, promo, profile, tenant_dir)

    background_jobs.progress("promo_item_prepare_file", f"Готовлю Excel-файл для исключения товаров из акции {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=40)
    file_meta = _pick_exclusion_file(page, promo, profile, tenant_dir, tenant_id)
    file_path = Path(str(file_meta.get("file_path") or "")).expanduser()
    if not file_path.exists():
        raise PromotionExecutionError(f"Не найден Excel-файл для исключения товаров из акции: {file_path}")

    background_jobs.progress("promo_item_upload_file", f"Загружаю Excel-файл в акцию {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=58, file_name=file_path.name)
    uploaded_path = _set_input_file(page, promo, profile, tenant_dir, file_path)

    background_jobs.progress("promo_item_submit", f"Подтверждаю исключение товаров из акции {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=72)
    submit_meta = _submit_exclusion(page, promo, profile, tenant_dir)

    background_jobs.progress("promo_item_verify_browser", f"Проверяю результат исключения товаров из акции {promo_id}", tenant_id=tenant_id, promotion_id=promo_id, percent=88)
    verify_meta = _verify_exclusion(page, promo, profile, tenant_dir)
    after = automation_browser.capture_page_artifacts(page, tenant_dir, f"promo_{promo_id}_after", note="После попытки исключения товаров из акции")
    if not verify_meta.get("success"):
        raise PromotionExecutionError(
            f"Не удалось подтвердить через ЛК, что акция {promo_id} переведена в режим пропуска. Причина: {_clean(verify_meta.get('reason')) or 'нет данных'}"
        )
    return {
        **open_meta,
        **wizard_meta,
        **file_meta,
        **submit_meta,
        "uploaded_file_path": uploaded_path,
        "promotion_id": promo_id,
        "promotion_name": _clean(promo.get("name")),
        "verify": verify_meta,
        "before": before,
        "after": after,
    }


def _default_browser_handler(tenant_id: str, tenant_scan: Dict[str, Any], run_dir: Path, settings: Dict[str, Any]) -> Dict[str, Any]:
    tenant_dir = automation_browser.ensure_run_tenant_dir(run_dir, tenant_id, "promo_execute")
    profile = _load_profile()
    rows = tenant_scan.get("actionable") if isinstance(tenant_scan.get("actionable"), list) else []
    max_retries = max(1, int((settings.get("promo") or {}).get("max_retries") or 3))
    attempts: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    browser_verify_success_total = 0

    with automation_browser.open_authenticated_browser(tenant_id) as (_, context, bootstrap_page):
        try:
            bootstrap_page.close()
        except Exception:
            pass
        for index, promo in enumerate(rows, start=1):
            promo_id = int(promo.get("id") or 0)
            background_jobs.progress(
                "promo_execute_item",
                f"Обрабатываю акцию {index}/{len(rows)} для кабинета {tenant_id}",
                tenant_id=tenant_id,
                current=index,
                total=len(rows),
                percent=round((index - 1) / max(1, len(rows)) * 100.0, 1),
                promotion_id=promo_id,
                promotion_name=_clean(promo.get("name")),
            )
            last_error = ""
            for attempt in range(1, max_retries + 1):
                page = context.new_page()
                try:
                    result = _execute_single_browser_promotion(page, promo, tenant_dir, profile, tenant_id)
                    result["attempt"] = attempt
                    attempts.append(result)
                    if bool(((result.get("verify") or {}).get("success"))):
                        browser_verify_success_total += 1
                    break
                except Exception as exc:
                    last_error = _clean(exc)
                    automation_browser.capture_page_artifacts(
                        page,
                        tenant_dir,
                        f"promo_{promo_id}_error_attempt_{attempt}",
                        note=last_error,
                    )
                    automation_browser.log_browser_step(
                        "automation",
                        "promo_execute_attempt_error",
                        tenant_id=tenant_id,
                        level="error",
                        promotion_id=promo_id,
                        attempt=attempt,
                        error=last_error,
                    )
                    if attempt >= max_retries:
                        failures.append(
                            {
                                "promotion_id": promo_id,
                                "promotion_name": _clean(promo.get("name")),
                                "error": last_error,
                            }
                        )
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
            if last_error and (not attempts or attempts[-1].get("promotion_id") != promo_id):
                background_jobs.progress(
                    "promo_execute_item_error",
                    f"Не удалось убрать акцию {promo_id}: {last_error}",
                    tenant_id=tenant_id,
                    promotion_id=promo_id,
                    error=last_error,
                )
    return {
        "tenant_id": tenant_id,
        "attempts": attempts,
        "failures": failures,
        "processed": len(rows),
        "browser_success": len(attempts),
        "browser_failed": len(failures),
        "browser_verified_total": browser_verify_success_total,
    }


def execute_future_promotions(
    tenant_ids: Optional[Iterable[str]] = None,
    run_source: str = "manual",
    *,
    scan_summary: Optional[Dict[str, Any]] = None,
    browser_handler: Optional[BrowserPromoHandler] = None,
) -> Dict[str, Any]:
    settings = automation_core.load_settings()
    selected = [
        _clean(tenant_id)
        for tenant_id in (tenant_ids or automation_core.list_enabled_tenant_ids(settings, feature="promo"))
        if _clean(tenant_id)
    ]
    if not selected:
        raise PromotionExecutionError("Нет кабинетов, включённых для ночной обработке календаря акций.")

    background_jobs.progress("promo_exec_init", "Готовлю ночной контур удаления будущих акций", percent=0, tenants=len(selected), source=run_source)
    run_dir = automation_core.create_run_dir("promo_execute")
    if scan_summary is None:
        background_jobs.progress("promo_exec_scan", "Сканирую будущие акции перед удалением", percent=3)
        scan_summary = promo_calendar.scan_future_promotions(tenant_ids=selected, run_source=f"{run_source}:pre-scan")

    handler = browser_handler or _default_browser_handler
    rows_by_tenant = {str(item.get("tenant_id")): item for item in (scan_summary.get("rows") or []) if isinstance(item, dict)}
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    verify_rows: List[Dict[str, Any]] = []

    for index, tenant_id in enumerate(selected, start=1):
        tenant_scan = rows_by_tenant.get(tenant_id) or {"tenant_id": tenant_id, "actionable": [], "actionable_total": 0, "new_actionable_ids": []}
        background_jobs.progress(
            "promo_exec_tenant_start",
            f"Удаляю будущие акции для кабинета {tenant_id}",
            tenant_id=tenant_id,
            current=index,
            total=len(selected),
            percent=5 + round((index - 1) / max(1, len(selected)) * 70.0, 1),
            actionable_total=int(tenant_scan.get("actionable_total") or len(tenant_scan.get("actionable") or [])),
        )
        if not tenant_scan.get("actionable"):
            results.append(
                {
                    "tenant_id": tenant_id,
                    "processed": 0,
                    "browser_success": 0,
                    "browser_failed": 0,
                    "attempts": [],
                    "failures": [],
                    "message": "Будущих акций к снятию не найдено.",
                    "browser_verified_total": 0,
                }
            )
            continue
        tenant = tenant_manager.get_tenant(tenant_id)
        paths = tenant_manager.ensure_tenant_dirs(tenant_id)
        tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
        try:
            result = handler(tenant_id, tenant_scan, run_dir, settings)
            results.append(result)
        except Exception as exc:
            error_text = _clean(exc)
            failures.append({"tenant_id": tenant_id, "error": error_text})
            results.append(
                {
                    "tenant_id": tenant_id,
                    "processed": int(tenant_scan.get("actionable_total") or len(tenant_scan.get("actionable") or [])),
                    "browser_success": 0,
                    "browser_failed": int(tenant_scan.get("actionable_total") or len(tenant_scan.get("actionable") or [])),
                    "attempts": [],
                    "failures": [{"error": error_text}],
                    "message": error_text,
                    "browser_verified_total": 0,
                }
            )
            background_jobs.progress(
                "promo_exec_tenant_error",
                f"Ошибка удаления будущих акций для кабинета {tenant_id}: {error_text}",
                tenant_id=tenant_id,
                error=error_text,
            )
        finally:
            common.reset_tenant_context(tokens)

    if bool((settings.get("promo") or {}).get("verify_after_action", True)):
        background_jobs.progress("promo_exec_verify", "Повторно проверяю будущие акции после удаления", percent=86)
        verify_summary = promo_calendar.scan_future_promotions(tenant_ids=selected, run_source=f"{run_source}:verify")
        verify_rows = verify_summary.get("rows") or []
        verify_map = {str(item.get("tenant_id")): item for item in verify_rows if isinstance(item, dict)}
        for row in results:
            tenant_verify = verify_map.get(_clean(row.get("tenant_id"))) or {}
            api_remaining = int(tenant_verify.get("actionable_total") or 0)
            browser_verified = int(row.get("browser_verified_total") or 0)
            row["verify_actionable_total"] = api_remaining
            row["verify_new_actionable_ids"] = tenant_verify.get("new_actionable_ids") or []
            row["verified_ok"] = bool(browser_verified > 0 or api_remaining == 0)
            row["effective_remaining_actionable_total"] = 0 if browser_verified > 0 else api_remaining

    summary = {
        "run_source": _clean(run_source) or "manual",
        "run_dir": str(run_dir),
        "selected_tenants": selected,
        "scan_summary": scan_summary,
        "rows": results,
        "verify_rows": verify_rows,
        "failures": failures,
        "browser_success_total": sum(int(item.get("browser_success") or 0) for item in results),
        "browser_failed_total": sum(int(item.get("browser_failed") or 0) for item in results),
        "browser_verified_total": sum(int(item.get("browser_verified_total") or 0) for item in results),
        "remaining_actionable_total": sum(int(item.get("effective_remaining_actionable_total", item.get("verify_actionable_total") or 0)) for item in results),
    }
    safe_files.write_json(Path(run_dir) / "summary.json", summary, ensure_ascii=False, indent=2)
    archive_path = Path(shutil.make_archive(str(run_dir), "zip", root_dir=run_dir))
    summary["archive_path"] = str(archive_path)
    report_path = automation_core.write_report(
        "promo_execute",
        status="completed" if not failures else "partial",
        title="Удаление будущих акций через браузерный ЛК",
        payload=summary,
    )
    summary["report_path"] = str(report_path)
    safe_files.write_json(Path(run_dir) / "summary.json", summary, ensure_ascii=False, indent=2)
    background_jobs.progress(
        "promo_exec_done",
        "Ночной контур удаления будущих акций завершён",
        percent=100,
        browser_success_total=summary["browser_success_total"],
        browser_failed_total=summary["browser_failed_total"],
        browser_verified_total=summary["browser_verified_total"],
        remaining_actionable_total=summary["remaining_actionable_total"],
    )
    log_event(
        "automation",
        "promo_execute_done",
        tenant_id="_system",
        browser_success_total=summary["browser_success_total"],
        browser_failed_total=summary["browser_failed_total"],
        browser_verified_total=summary["browser_verified_total"],
        remaining_actionable_total=summary["remaining_actionable_total"],
    )
    return {
        **summary,
        "message": (
            f"Кабинетов обработано: {len(selected)}. "
            f"Успешных браузерных действий: {summary['browser_success_total']}. "
            f"Подтверждено через ЛК: {summary['browser_verified_total']}. "
            f"Ошибок: {summary['browser_failed_total']}. "
            f"После повторной проверки акций осталось: {summary['remaining_actionable_total']}."
        ),
    }
