from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import automation_browser
import automation_core
import background_jobs
import common
import price_pipeline
import safe_files
import tenant_manager
import wb_price_api
from safe_logs import log_event

DEFAULT_PRICE_PROFILE = {
    "upload_urls": [
        f"{common.WB_SELLER_BASE_URL}/discount-and-prices",
    ],
    "page_ready_selectors": [
        "[data-testid='xlsx-action-open-test-id-button-interface']",
        "[data-testid='nm-search-test-id-simple-input']",
    ],
    "prices_page_texts": [
        "Цены и скидки",
        "Обновить через Excel",
        "Артикул продавца или WB",
    ],
    "dashboard_markers": [
        "Быстрый доступ к справочным материалам",
        "Баланс",
        "Задачи по магазину",
        "Ответить на отзывы",
        "Доверьте продвижение профессионалам",
        "Попробуйте «Джем» бесплатно 24 часа",
        "Новое в статистике по подменным артикулам",
    ],
    "recover_page_link_selectors": [
        "[data-testid='menu.discounts-prices-front-chips-component']",
        "a[href='https://seller.wildberries.ru/discount-and-prices']",
        "a[href='/discount-and-prices']",
    ],
    "overlay_close_selectors": [
        "[data-testid*='close'][type='button']",
        "button[aria-label='Закрыть']",
    ],
    "overlay_close_texts": [
        "Закрыть",
        "Понятно",
        "Не сейчас",
        "Пропустить",
        "Потом",
        "Позже",
    ],
    "open_upload_button_selectors": [
        "[data-testid='xlsx-action-open-test-id-button-interface']",
    ],
    "open_upload_button_texts": [
        "Обновить через Excel",
    ],
    "open_upload_dropdown_list_selectors": [
        "[data-testid='xlsx-action-options-test-id-dropdown-list']",
    ],
    "open_upload_dropdown_option_selectors": [
        "[data-testid='xlsx-action-options-test-id-dropdown-option']",
        "button[class*='Dropdown-option']",
    ],
    "open_upload_dropdown_option_texts": [
        "Цены и скидки",
    ],
    "modal_ready_selectors": [
        "[data-testid='xlsx-action-file-test-id-file-uploader-view-input']",
        "[data-testid='xlsx-action-action-test-id-button-primary']",
        "[data-testid='xlsx-action-step-two-test-id']",
        "[data-testid='xlsx-action-info-block-test-id']",
    ],
    "modal_texts": [
        "Обновить цены и скидки через Excel",
        "Загрузите заполненный шаблон",
        "Формат файла — XLSX",
    ],
    "file_input_selectors": [
        "[data-testid='xlsx-action-file-test-id-file-uploader-view-input']",
        "input#excel-upload",
        "input[type='file'][accept*='xlsx']",
        "input[type='file']",
    ],
    "upload_trigger_selectors": [
        "[data-testid='xlsx-action-file-test-id-upload-button']",
        "[data-testid='xlsx-action-file-test-id-file-uploader-content']",
        "[data-testid='xlsx-action-file-test-id-file-uploader-action-placeholder']",
    ],
    "file_card_selectors": [
        "[data-testid='xlsx-action-file-test-id-file-name-text']",
        "[data-testid='xlsx-action-file-test-id-buttons']",
        "[data-testid='xlsx-action-file-test-id-file-size-text']",
    ],
    "submit_button_selectors": [
        "[data-testid='xlsx-action-action-test-id-button-primary']",
    ],
    "submit_button_texts": [
        "Обновить стоимость",
        "Обновить цены",
        "Обновить",
    ],
    "error_texts": [
        "Указано неверное значение скидки",
        "Указано неверное значение цены",
        "Неверное значение скидки",
        "Неверное значение цены",
        "Некорректное значение скидки",
        "Некорректное значение цены",
        "Ошибка загрузки",
        "Ошибка обработки файла",
        "Файл не прошёл проверку",
    ],
    "warning_block_selectors": [
        "[data-testid='check-changes-warning-block-test-id']",
    ],
    "warning_checkbox_selectors": [
        "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-input']",
        "input#agree-changes",
        "input[name='agree-changes']",
    ],
    "warning_checkbox_label_selectors": [
        "label[for='agree-changes']",
        "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label-label']",
        "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label']",
        "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple']",
        "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-icon']",
    ],
    "warning_checkbox_texts": [
        "Всё верно — это не ошибка",
        "Все верно — это не ошибка",
        "Всё верно",
        "Это не ошибка",
    ],
    "success_texts": [
        "Файл загружен",
        "Файл принят",
        "Загрузка создана",
        "Задача создана",
        "Успешно",
        "Изменения приняты",
        "Товаров на обновление",
    ],
    "network_markers": [
        "discount-and-prices",
        "discounts-prices",
        "discounts-prices-api",
        "upload",
        "buffer/tasks",
        "history/tasks",
    ],
    "post_submit_modal_close_timeout_ms": 90000,
    "post_submit_success_wait_ms": 60000,
}


class PriceUploadError(RuntimeError):
    pass


BrowserPriceHandler = Callable[[str, Path, Dict[str, Any], Path], Dict[str, Any]]


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _load_profile() -> Dict[str, Any]:
    profile = dict(DEFAULT_PRICE_PROFILE)
    try:
        custom = common.load_ui_profile() or {}
    except Exception:
        custom = {}
    if isinstance(custom, dict):
        prices_custom = custom.get("automation_prices") if isinstance(custom.get("automation_prices"), dict) else {}
        for key, value in prices_custom.items():
            if value:
                profile[key] = value
    return profile


def _find_latest_output_file(tenant_id: str) -> Optional[Path]:
    tenant_id = _clean(tenant_id)
    if not tenant_id:
        return None
    candidates = sorted(
        [path for path in automation_core.PRICE_OUTPUT_DIR.glob(f"*{tenant_id}*") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _build_or_collect_files(
    tenant_ids: List[str],
    run_source: str,
    rebuild: bool,
    prebuilt_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if prebuilt_summary is not None:
        return prebuilt_summary
    if rebuild:
        return price_pipeline.build_price_files(tenant_ids=tenant_ids, run_source=f"{run_source}:build")
    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for tenant_id in tenant_ids:
        path = _find_latest_output_file(tenant_id)
        if not path:
            failures.append({"tenant_id": tenant_id, "error": "Не найден ранее подготовленный файл цен"})
            continue
        rows.append({"tenant_id": tenant_id, "output_path": str(path), "updated": None, "matched": None, "warnings": []})
    return {
        "run_source": run_source,
        "results": rows,
        "failures": failures,
        "prepared": len(rows),
        "failed": len(failures),
    }


def _body_text(page, *, limit: int = 12000) -> str:
    try:
        body = page.locator("body").inner_text(timeout=2500)
        return _clean(body)[: max(1000, int(limit or 12000))]
    except Exception:
        return _clean(automation_browser._body_debug_snippet(page, limit=limit))


def _contains_any(text: str, phrases: Iterable[str]) -> bool:
    lowered = _clean(text).lower()
    return any(_clean(item).lower() in lowered for item in phrases if _clean(item))


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


def _wait_attached(page, selectors: Sequence[str], *, timeout_ms: int, poll_ms: int = 500):
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


def _button_enabled(locator) -> bool:
    try:
        payload = locator.evaluate(
            """(el) => {
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                const ariaDisabled = (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                const disabled = !!el.disabled || ariaDisabled;
                return {
                    disabled,
                    pointerEvents: style.pointerEvents,
                    display: style.display,
                    visibility: style.visibility,
                    width: rect.width,
                    height: rect.height,
                };
            }"""
        )
    except Exception:
        try:
            return not locator.is_disabled()
        except Exception:
            return False
    if not isinstance(payload, dict):
        return False
    if payload.get("disabled"):
        return False
    if payload.get("pointerEvents") == "none":
        return False
    if payload.get("display") == "none" or payload.get("visibility") == "hidden":
        return False
    if float(payload.get("width") or 0) <= 0 or float(payload.get("height") or 0) <= 0:
        return False
    try:
        locator.click(trial=True, timeout=1200)
    except Exception:
        pass
    return True


def _wait_button_enabled(page_or_scope, *, selectors: Sequence[str] = (), texts: Sequence[str] = (), timeout_ms: int = 15000):
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        button = _find_button(page_or_scope, selectors=selectors, texts=texts, timeout_ms=1200)
        if button is not None and _button_enabled(button):
            return button
        try:
            page_or_scope.page.wait_for_timeout(500)  # type: ignore[attr-defined]
        except Exception:
            time.sleep(0.5)
    return None


def _extract_modal_errors(page, profile: Dict[str, Any]) -> List[str]:
    body = _body_text(page, limit=8000)
    found: List[str] = []
    for text in profile.get("error_texts") or []:
        marker = _clean(text)
        if marker and marker.lower() in body.lower() and marker not in found:
            found.append(marker)
    return found


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
                page.mouse.click(box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0)
                return True
            except Exception:
                pass
    return False


def _body_indicates_prices_page(body: str, profile: Dict[str, Any]) -> bool:
    return _contains_any(body, profile.get("prices_page_texts") or ["Цены и скидки", "Обновить через Excel"])


def _body_indicates_dashboard_redirect(body: str, profile: Dict[str, Any]) -> bool:
    return _contains_any(body, profile.get("dashboard_markers") or [])


def _wait_modal_ready(page, profile: Dict[str, Any], *, timeout_ms: int = 12000):
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        locator = automation_browser.wait_any(page, profile.get("modal_ready_selectors") or [], timeout_ms=1200)
        if locator is not None:
            return locator
        body = _body_text(page, limit=5000)
        if _contains_any(body, profile.get("modal_texts") or []):
            return True
        try:
            page.wait_for_timeout(400)
        except Exception:
            time.sleep(0.4)
    return None


def _dropdown_visible(page, profile: Dict[str, Any], *, timeout_ms: int = 600) -> bool:
    return automation_browser.wait_any(page, profile.get("open_upload_dropdown_list_selectors") or [], timeout_ms=timeout_ms) is not None


def _dom_click_upload_dropdown_option(page, profile: Dict[str, Any]) -> Optional[str]:
    list_selectors = [item for item in (profile.get("open_upload_dropdown_list_selectors") or []) if _clean(item)]
    option_selectors = [item for item in (profile.get("open_upload_dropdown_option_selectors") or []) if _clean(item)]
    option_texts = [item for item in (profile.get("open_upload_dropdown_option_texts") or []) if _clean(item)]
    script = """
(args) => {
  const listSelectors = Array.isArray(args?.listSelectors) ? args.listSelectors : [];
  const optionSelectors = Array.isArray(args?.optionSelectors) ? args.optionSelectors : [];
  const optionTexts = (Array.isArray(args?.optionTexts) ? args.optionTexts : []).map((item) => String(item || '').trim()).filter(Boolean);
  const isVisible = (node) => {
    if (!node) return false;
    const style = window.getComputedStyle(node);
    const rect = node.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
  };
  const clickNode = (node) => {
    try { node.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
    try { node.focus?.(); } catch (e) {}
    const rect = node.getBoundingClientRect();
    const payload = { bubbles: true, cancelable: true, composed: true, clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2, buttons: 1 };
    const fire = (type, Ctor, extra = {}) => {
      try {
        node.dispatchEvent(new Ctor(type, Object.assign({}, payload, extra)));
      } catch (e) {}
    };
    fire('pointerdown', window.PointerEvent || MouseEvent, { pointerType: 'mouse', isPrimary: true });
    fire('mousedown', MouseEvent);
    fire('pointerup', window.PointerEvent || MouseEvent, { pointerType: 'mouse', isPrimary: true, buttons: 0 });
    fire('mouseup', MouseEvent, { buttons: 0 });
    try { node.click?.(); } catch (e) {}
    fire('click', MouseEvent, { buttons: 0 });
  };
  const collect = (root) => {
    const items = [];
    if (optionSelectors.length) {
      for (const selector of optionSelectors) {
        try { items.push(...root.querySelectorAll(selector)); } catch (e) {}
      }
    }
    if (!items.length) {
      try { items.push(...root.querySelectorAll('button,[role="button"],[role="menuitem"],li')); } catch (e) {}
    }
    return items.filter(isVisible);
  };
  const roots = [];
  if (listSelectors.length) {
    for (const selector of listSelectors) {
      try { roots.push(...document.querySelectorAll(selector)); } catch (e) {}
    }
  } else {
    roots.push(document.body);
  }
  for (const root of roots) {
    if (!isVisible(root)) continue;
    const options = collect(root);
    for (const wanted of optionTexts) {
      const wantedLower = wanted.toLowerCase();
      const target = options.find((node) => (node.innerText || node.textContent || '').trim().toLowerCase().includes(wantedLower));
      if (!target) continue;
      clickNode(target);
      return `dom_text:${(target.innerText || target.textContent || '').trim()}`;
    }
    const fallback = options.find((node) => !!(node.innerText || node.textContent || '').trim());
    if (fallback) {
      clickNode(fallback);
      return `dom_first:${(fallback.innerText || fallback.textContent || '').trim()}`;
    }
  }
  return '';
}
"""
    try:
        result = _clean(page.evaluate(script, {"listSelectors": list_selectors, "optionSelectors": option_selectors, "optionTexts": option_texts}))
    except Exception:
        result = ""
    return result or None


def _click_upload_dropdown_option(page, profile: Dict[str, Any], *, timeout_ms: int = 2500) -> Optional[str]:
    dropdown = automation_browser.wait_any(page, profile.get("open_upload_dropdown_list_selectors") or [], timeout_ms=timeout_ms)
    if dropdown is None:
        return None

    option_texts = [item for item in (profile.get("open_upload_dropdown_option_texts") or []) if _clean(item)]
    option_selectors = [item for item in (profile.get("open_upload_dropdown_option_selectors") or []) if _clean(item)]
    seen_names: set[str] = set()
    candidates: List[tuple[str, Any]] = []

    def _add_candidate(name: str, locator) -> None:
        if not name or name in seen_names or locator is None:
            return
        try:
            if locator.count() < 1:
                return
        except Exception:
            return
        seen_names.add(name)
        candidates.append((name, locator.first))

    for label in option_texts:
        clean_label = _clean(label)
        if not clean_label:
            continue
        for selector in option_selectors:
            try:
                _add_candidate(f"selector_text:{selector}:{clean_label}", dropdown.locator(selector).filter(has_text=clean_label))
            except Exception:
                continue
        try:
            _add_candidate(f"role_button:{clean_label}", dropdown.get_by_role("button", name=clean_label))
        except Exception:
            pass
        try:
            _add_candidate(f"text:{clean_label}", dropdown.get_by_text(clean_label, exact=False))
        except Exception:
            pass

    for selector in option_selectors:
        try:
            _add_candidate(f"selector_first:{selector}", dropdown.locator(selector))
        except Exception:
            continue
    try:
        _add_candidate("role_button:first", dropdown.get_by_role("button"))
    except Exception:
        pass

    for candidate_name, target in candidates:
        try:
            target.wait_for(timeout=min(timeout_ms, 1200))
            if not target.is_visible():
                continue
        except Exception:
            continue
        for action_name, callback in (
            ("click", lambda t=target: t.click(timeout=2200)),
            ("click_force", lambda t=target: t.click(timeout=2200, force=True)),
            ("mouse_center", lambda t=target: _click_locator_center(page, t, attempts=1)),
            ("mouse_left", lambda t=target: _click_locator_relative(page, t, x_ratio=0.22, y_ratio=0.5, attempts=1)),
            ("enter", lambda t=target: t.press("Enter", timeout=1200)),
            ("space", lambda t=target: t.press("Space", timeout=1200)),
        ):
            try:
                callback()
            except Exception:
                continue
            try:
                page.wait_for_timeout(200)
            except Exception:
                time.sleep(0.2)
            if _wait_modal_ready(page, profile, timeout_ms=900) is not None:
                return f"{candidate_name}.{action_name}"
            if not _dropdown_visible(page, profile, timeout_ms=250):
                return f"{candidate_name}.{action_name}"

    dom_mode = _dom_click_upload_dropdown_option(page, profile)
    if dom_mode:
        try:
            page.wait_for_timeout(200)
        except Exception:
            time.sleep(0.2)
        return dom_mode
    return None


def _dom_click_by_selector(page, selector: str) -> bool:
    selector = _clean(selector)
    if not selector:
        return False
    script = """
(selector) => {
  const el = document.querySelector(selector);
  if (!el) return false;
  try { el.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
  try { el.focus?.(); } catch (e) {}
  const dispatch = (node, type, Ctor, extra = {}) => {
    try {
      node.dispatchEvent(new Ctor(type, Object.assign({bubbles:true, cancelable:true, composed:true}, extra)));
      return true;
    } catch (e) {
      return false;
    }
  };
  dispatch(el, 'pointerdown', window.PointerEvent || MouseEvent, {pointerType:'mouse', buttons:1});
  dispatch(el, 'mousedown', MouseEvent, {buttons:1});
  dispatch(el, 'pointerup', window.PointerEvent || MouseEvent, {pointerType:'mouse'});
  dispatch(el, 'mouseup', MouseEvent, {});
  try { el.click?.(); } catch (e) {}
  dispatch(el, 'click', MouseEvent, {buttons:1});
  return true;
}
"""
    try:
        return bool(page.evaluate(script, selector))
    except Exception:
        return False


def _dom_pointer_by_selector(page, selector: str) -> bool:
    selector = _clean(selector)
    if not selector:
        return False
    script = """
(selector) => {
  const el = document.querySelector(selector);
  if (!el) return false;
  try { el.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
  try { el.focus?.(); } catch (e) {}
  const rect = el.getBoundingClientRect();
  const payload = { bubbles: true, cancelable: true, composed: true, clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2, buttons: 1, pointerType: 'mouse', isPrimary: true };
  const fire = (type, Ctor, extra = {}) => {
    try {
      el.dispatchEvent(new Ctor(type, Object.assign({}, payload, extra)));
      return true;
    } catch (e) {
      return false;
    }
  };
  fire('pointerover', window.PointerEvent || MouseEvent);
  fire('pointerenter', window.PointerEvent || MouseEvent);
  fire('mouseover', MouseEvent);
  fire('mouseenter', MouseEvent);
  fire('pointermove', window.PointerEvent || MouseEvent);
  fire('mousemove', MouseEvent);
  fire('pointerdown', window.PointerEvent || MouseEvent);
  fire('mousedown', MouseEvent);
  fire('pointerup', window.PointerEvent || MouseEvent, {buttons: 0});
  fire('mouseup', MouseEvent, {buttons: 0});
  fire('click', MouseEvent, {buttons: 0});
  return true;
}
"""
    try:
        return bool(page.evaluate(script, selector))
    except Exception:
        return False


def _click_locator_relative(page, locator, *, x_ratio: float = 0.5, y_ratio: float = 0.5, attempts: int = 2) -> bool:
    for _ in range(max(1, attempts)):
        try:
            locator.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass
        try:
            box = locator.bounding_box()
        except Exception:
            box = None
        if not box:
            continue
        try:
            x = box["x"] + max(1.0, box["width"] * x_ratio)
            y = box["y"] + max(1.0, box["height"] * y_ratio)
            page.mouse.move(x, y)
            page.mouse.down()
            page.mouse.up()
            return True
        except Exception:
            try:
                page.mouse.click(x, y)
                return True
            except Exception:
                continue
    return False


def _dismiss_prices_overlays(page, profile: Dict[str, Any]) -> List[str]:
    closed: List[str] = []
    for selector in profile.get("overlay_close_selectors") or []:
        selector = _clean(selector)
        if not selector:
            continue
        try:
            locator = page.locator(selector)
            count = min(locator.count(), 3)
            for index in range(count):
                target = locator.nth(index)
                try:
                    if not target.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    target.click(timeout=1200)
                    closed.append(f"selector:{selector}")
                    try:
                        page.wait_for_timeout(250)
                    except Exception:
                        time.sleep(0.25)
                    break
                except Exception:
                    if _click_locator_center(page, target, attempts=1):
                        closed.append(f"selector_mouse:{selector}")
                        break
        except Exception:
            continue
    for label in profile.get("overlay_close_texts") or []:
        text_label = _clean(label)
        if not text_label:
            continue
        try:
            target = page.get_by_text(text_label, exact=False).first
            target.wait_for(timeout=800)
            if not target.is_visible():
                continue
            try:
                target.click(timeout=1200)
                closed.append(f"text:{text_label}")
                try:
                    page.wait_for_timeout(250)
                except Exception:
                    time.sleep(0.25)
            except Exception:
                if _click_locator_center(page, target, attempts=1):
                    closed.append(f"text_mouse:{text_label}")
        except Exception:
            continue
    return closed


def _recover_prices_page(page, profile: Dict[str, Any], tenant_dir: Path, *, reason: str = "") -> bool:
    body = _body_text(page, limit=6000)
    if not _body_indicates_dashboard_redirect(body, profile) and not str(getattr(page, "url", "") or "").rstrip("/").endswith("seller.wildberries.ru"):
        return False
    recovered = automation_browser.click_first(
        page,
        selectors=profile.get("recover_page_link_selectors") or [],
        texts=["Цены и скидки", "Управление ценами"],
        role="link",
        timeout_ms=6000,
    )
    if recovered:
        try:
            page.wait_for_load_state("domcontentloaded", timeout=12000)
        except Exception:
            pass
        try:
            page.wait_for_timeout(1200)
        except Exception:
            time.sleep(1.2)
        body = _body_text(page, limit=6000)
        if _body_indicates_prices_page(body, profile):
            return True
    try:
        automation_browser.safe_goto(page, (profile.get("upload_urls") or [f"{common.WB_SELLER_BASE_URL}/discount-and-prices"])[0])
    except Exception as exc:
        automation_browser.capture_page_artifacts(page, tenant_dir, "price_recover_failed", note=f"{reason}: {_clean(exc)}")
        return False
    body = _body_text(page, limit=6000)
    return _body_indicates_prices_page(body, profile)


def _open_upload_modal(page, profile: Dict[str, Any], tenant_dir: Path) -> bool:
    errors: List[str] = []
    selectors = [item for item in (profile.get("open_upload_button_selectors") or []) if _clean(item)]
    texts = [item for item in (profile.get("open_upload_button_texts") or []) if _clean(item)]

    def _text_locator():
        for label in texts:
            try:
                locator = page.get_by_text(_clean(label), exact=False).first
                if locator.count() > 0:
                    return locator
            except Exception:
                continue
        return None

    def _svg_locator():
        for selector in selectors:
            try:
                node = page.locator(f"{selector} svg").first
                if node.count() > 0:
                    return node
            except Exception:
                continue
        return None

    def _button_locator():
        locator = _wait_button_enabled(page, selectors=selectors, texts=texts, timeout_ms=2500)
        if locator is not None:
            return locator
        return _find_attached_locator(page, selectors)

    direct_methods = [
        ("dom.button.click", lambda: any(_dom_click_by_selector(page, selector) for selector in selectors)),
        ("dom.button.pointer", lambda: any(_dom_pointer_by_selector(page, selector) for selector in selectors)),
    ]

    for round_index in range(1, 5):
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        _dismiss_prices_overlays(page, profile)

        button = _button_locator()
        text_locator = _text_locator()
        svg_locator = _svg_locator()

        attempts = []
        if button is not None:
            attempts.extend([
                ("button.click", lambda b=button: b.click(timeout=3500)),
                ("button.click.force", lambda b=button: b.click(timeout=3500, force=True)),
                ("button.mouse.center", lambda b=button: _click_locator_center(page, b, attempts=2)),
                ("button.mouse.left", lambda b=button: _click_locator_relative(page, b, x_ratio=0.18, y_ratio=0.5, attempts=2)),
                ("button.mouse.right", lambda b=button: _click_locator_relative(page, b, x_ratio=0.82, y_ratio=0.5, attempts=2)),
                ("button.enter", lambda b=button: b.press("Enter", timeout=2500)),
                ("button.space", lambda b=button: b.press("Space", timeout=2500)),
            ])
        if text_locator is not None:
            attempts.extend([
                ("text.click", lambda t=text_locator: t.click(timeout=2500)),
                ("text.click.force", lambda t=text_locator: t.click(timeout=2500, force=True)),
                ("text.mouse.center", lambda t=text_locator: _click_locator_center(page, t, attempts=2)),
            ])
        if svg_locator is not None:
            attempts.extend([
                ("svg.click", lambda s=svg_locator: s.click(timeout=2500, force=True)),
                ("svg.mouse.center", lambda s=svg_locator: _click_locator_center(page, s, attempts=2)),
            ])
        attempts.extend(direct_methods)

        for name, callback in attempts:
            try:
                background_jobs.progress(
                    "prices_upload_open_modal_attempt",
                    f"Пробую открыть 'Обновить через Excel': {name} (круг {round_index})",
                    percent=12,
                    method=name,
                    round=round_index,
                )
            except Exception:
                pass
            try:
                callback()
                modal = _wait_modal_ready(page, profile, timeout_ms=1500)
                dropdown_mode = None
                if modal is None:
                    dropdown_mode = _click_upload_dropdown_option(page, profile, timeout_ms=1600)
                    if dropdown_mode:
                        modal = _wait_modal_ready(page, profile, timeout_ms=4500)
                    else:
                        modal = _wait_modal_ready(page, profile, timeout_ms=3200)
                if modal is not None:
                    success_method = name if not dropdown_mode else f"{name} -> {dropdown_mode}"
                    try:
                        background_jobs.progress(
                            "prices_upload_open_modal_success",
                            f"Окно 'Обновить через Excel' открыто: {success_method} (круг {round_index})",
                            percent=18,
                            method=success_method,
                            round=round_index,
                        )
                    except Exception:
                        pass
                    return True
            except Exception as exc:
                errors.append(f"{name}@{round_index}: {_clean(exc)}")
                continue

        try:
            page.wait_for_timeout(800)
        except Exception:
            time.sleep(0.8)

    automation_browser.capture_page_artifacts(page, tenant_dir, "price_modal_not_opened", note='; '.join(errors) or 'modal_not_opened')
    return False


def _open_upload_page(page, file_path: Path, profile: Dict[str, Any], tenant_dir: Path) -> str:
    url = (profile.get("upload_urls") or [f"{common.WB_SELLER_BASE_URL}/discount-and-prices"])[0]
    last_error = ""
    try:
        automation_browser.safe_goto(page, url)
    except Exception as exc:
        last_error = _clean(exc)
        automation_browser.capture_page_artifacts(page, tenant_dir, "price_open_error_1", note=last_error)
        raise PriceUploadError(last_error or f"Не удалось открыть раздел загрузки цен для файла {file_path.name}")

    body = _body_text(page, limit=6000)
    if not _body_indicates_prices_page(body, profile):
        if _body_indicates_dashboard_redirect(body, profile):
            recovered = _recover_prices_page(page, profile, tenant_dir, reason="dashboard_redirect")
            body = _body_text(page, limit=6000)
            if not recovered and not _body_indicates_prices_page(body, profile):
                automation_browser.capture_page_artifacts(page, tenant_dir, "price_open_error_2", note="После открытия цены и скидки произошёл переход на главную страницу WB")
                raise PriceUploadError("После открытия раздела 'Цены и скидки' WB вернул на главную страницу")
        else:
            automation_browser.capture_page_artifacts(page, tenant_dir, "price_open_error_2", note=f"Не открылась страница загрузки цен: {url}")
            raise PriceUploadError(f"Не открылась страница загрузки цен: {url}")

    ready = automation_browser.wait_any(page, profile.get("page_ready_selectors") or [], timeout_ms=15000)
    if ready is None and not _body_indicates_prices_page(_body_text(page, limit=6000), profile):
        automation_browser.capture_page_artifacts(page, tenant_dir, "price_open_error_3", note="Не появились основные элементы страницы 'Цены и скидки'")
        raise PriceUploadError("Не появились основные элементы страницы 'Цены и скидки'")

    opened = False
    for attempt in range(1, 4):
        _dismiss_prices_overlays(page, profile)
        if _wait_modal_ready(page, profile, timeout_ms=1000) is not None:
            opened = True
            break
        if _open_upload_modal(page, profile, tenant_dir):
            opened = True
            break
        body = _body_text(page, limit=6000)
        if _body_indicates_dashboard_redirect(body, profile):
            _recover_prices_page(page, profile, tenant_dir, reason=f"attempt_{attempt}_redirect")
        else:
            try:
                page.wait_for_timeout(800)
            except Exception:
                time.sleep(0.8)

    if not opened:
        automation_browser.capture_page_artifacts(page, tenant_dir, "price_open_error_4", note="Не удалось открыть окно 'Обновить через Excel'")
        raise PriceUploadError("Не открылось окно 'Обновить цены и скидки через Excel'")
    return url


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
            with page.expect_file_chooser(timeout=5000) as chooser_info:
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


def _wait_file_registered(page, profile: Dict[str, Any], file_path: Path, *, timeout_ms: int = 10000) -> Dict[str, Any]:
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    stem = _clean(file_path.stem).lower()
    file_name = _clean(file_path.name).lower()
    while time.time() < deadline:
        body = _body_text(page, limit=6000).lower()
        if file_name and file_name in body:
            return {"registered": True, "reason": "file_name_visible"}
        if stem and stem in body:
            return {"registered": True, "reason": "file_stem_visible"}
        locator = _find_attached_locator(page, profile.get("file_card_selectors") or [])
        if locator is not None:
            return {"registered": True, "reason": "file_card_visible"}
        try:
            page.wait_for_timeout(700)
        except Exception:
            time.sleep(0.7)
    return {"registered": False, "reason": "timeout"}


def _set_input_file(page, file_path: Path, profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    if not file_path.exists():
        raise PriceUploadError(f"Не найден файл цен для загрузки: {file_path}")
    input_locator = _wait_attached(page, profile.get("file_input_selectors") or [], timeout_ms=12000)
    uploaded = False
    if input_locator is not None:
        try:
            input_locator.set_input_files(str(file_path), timeout=12000)
            uploaded = True
        except Exception:
            uploaded = False
    if not uploaded:
        uploaded = _try_filechooser_upload(page, profile, file_path)
    if not uploaded:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"price_upload_missing_input_{file_path.stem}", note="Не найден рабочий input/filechooser для загрузки файла цен")
        raise PriceUploadError("Не найден рабочий input/filechooser для загрузки файла цен")
    try:
        page.wait_for_timeout(1200)
    except Exception:
        time.sleep(1.2)
    wait_result = _wait_file_registered(page, profile, file_path, timeout_ms=12000)
    errors = _extract_modal_errors(page, profile)
    if not wait_result.get("registered"):
        automation_browser.capture_page_artifacts(page, tenant_dir, f"price_upload_not_applied_{file_path.stem}", note="WB не подтвердил загрузку Excel-файла")
        raise PriceUploadError("WB не подтвердил загрузку Excel-файла")
    if errors:
        raise PriceUploadError("; ".join(errors))
    return {"uploaded": True, "reason": wait_result.get("reason") or "file_card_visible"}


def _checkbox_checked_dom(page, profile: Dict[str, Any]) -> bool:
    selectors = [item for item in (profile.get("warning_checkbox_selectors") or []) if _clean(item)]
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


def _force_warning_checkbox_via_dom(page, profile: Dict[str, Any]) -> bool:
    selectors = [item for item in (profile.get("warning_checkbox_selectors") or []) if _clean(item)]
    label_selectors = [item for item in (profile.get("warning_checkbox_label_selectors") or []) if _clean(item)]
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
  if (!input) return false;
  const label = input.id ? document.querySelector(`label[for="${input.id}"]`) : null;
  const altLabel = find(labelSelectors);
  const nativeCheckedSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'checked')?.set;
  const clickTargets = [label, altLabel, input.parentElement, input.parentElement?.parentElement, input].filter(Boolean);
  for (const node of clickTargets) {
    try { node.click?.(); } catch (e) {}
    if (input.checked) return true;
  }
  try {
    if (nativeCheckedSetter) nativeCheckedSetter.call(input, true);
    else input.checked = true;
    input.setAttribute('checked', 'checked');
    input.dispatchEvent(new Event('input', {bubbles:true, composed:true}));
    input.dispatchEvent(new Event('change', {bubbles:true, composed:true}));
    input.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, composed:true}));
  } catch (e) {}
  return !!input.checked;
}
"""
    try:
        return bool(page.evaluate(script, {"selectors": selectors, "labelSelectors": label_selectors}))
    except Exception:
        return False


def _ensure_warning_confirmed(page, profile: Dict[str, Any], tenant_dir: Path, file_path: Path) -> Dict[str, Any]:
    warning_block = _find_attached_locator(page, profile.get("warning_block_selectors") or [])
    if warning_block is None and not _contains_any(_body_text(page, limit=5000), ["не ошибка", "попадут в карантин", "резко снизили цену"]):
        return {"warning_required": False, "warning_confirmed": False}

    checkbox = _wait_attached(page, profile.get("warning_checkbox_selectors") or [], timeout_ms=4000)
    if checkbox is None:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"price_warning_checkbox_missing_{file_path.stem}", note="Не найден чекбокс подтверждения резкого изменения цены")
        raise PriceUploadError("Не найден чекбокс подтверждения резкого изменения цены")

    if _checkbox_checked_dom(page, profile):
        return {"warning_required": True, "warning_confirmed": True, "warning_mode": "already_checked"}

    attempts: List[str] = []

    def _done(mode: str) -> Optional[Dict[str, Any]]:
        if _checkbox_checked_dom(page, profile):
            return {"warning_required": True, "warning_confirmed": True, "warning_mode": mode}
        return None

    for action in (
        ("check", lambda: checkbox.check(timeout=4000, force=True)),
        ("set_checked", lambda: checkbox.set_checked(True, timeout=4000, force=True)),
        ("click", lambda: checkbox.click(timeout=3000)),
        ("click_force", lambda: checkbox.click(timeout=3000, force=True)),
    ):
        name, callback = action
        try:
            callback()
            result = _done(name)
            if result:
                return result
        except Exception as exc:
            attempts.append(_clean(exc))

    try:
        checkbox.press("Space", timeout=3000)
        result = _done("space")
        if result:
            return result
    except Exception as exc:
        attempts.append(_clean(exc))

    for selector in profile.get("warning_checkbox_label_selectors") or []:
        selector = _clean(selector)
        if not selector:
            continue
        try:
            locator = page.locator(selector)
            if locator.count() < 1:
                continue
            target = locator.first
            try:
                target.click(timeout=3000)
            except Exception:
                pass
            result = _done(f"label:{selector}")
            if result:
                return result
            try:
                target.click(timeout=3000, force=True)
            except Exception:
                pass
            result = _done(f"label_force:{selector}")
            if result:
                return result
            if _click_locator_center(page, target, attempts=2):
                result = _done(f"label_mouse:{selector}")
                if result:
                    return result
        except Exception as exc:
            attempts.append(_clean(exc))

    for text_value in profile.get("warning_checkbox_texts") or []:
        label_text = _clean(text_value)
        if not label_text:
            continue
        try:
            target = page.get_by_text(label_text, exact=False).first
            target.wait_for(timeout=2000)
            try:
                target.click(timeout=3000)
            except Exception:
                pass
            result = _done(f"text:{label_text}")
            if result:
                return result
            if _click_locator_center(page, target, attempts=2):
                result = _done(f"text_mouse:{label_text}")
                if result:
                    return result
        except Exception as exc:
            attempts.append(_clean(exc))

    try:
        if _force_warning_checkbox_via_dom(page, profile):
            result = _done("dom_force")
            if result:
                return result
    except Exception as exc:
        attempts.append(_clean(exc))

    automation_browser.capture_page_artifacts(page, tenant_dir, f"price_warning_checkbox_failed_{file_path.stem}", note="; ".join(item for item in attempts if item) or "checkbox_not_checked")
    raise PriceUploadError("Не удалось подтвердить предупреждение о резком изменении цены")


def _extract_upload_id_from_page(page) -> Optional[int]:
    body = _body_text(page, limit=4000)
    if not body:
        return None
    for marker in ["uploadID", "uploadId", "ID загрузки", "Upload ID"]:
        position = body.find(marker)
        if position < 0:
            continue
        chunk = body[position : position + 160]
        digits = "".join(ch if ch.isdigit() else " " for ch in chunk).split()
        for token in digits:
            try:
                value = int(token)
            except Exception:
                continue
            if value > 0:
                return value
    return None


def _submit_button_now(page, profile: Dict[str, Any]):
    return _find_button(page, selectors=profile.get("submit_button_selectors") or [], texts=profile.get("submit_button_texts") or [], timeout_ms=1500)


def _submit_price_update(page, file_path: Path, profile: Dict[str, Any], tenant_dir: Path) -> Dict[str, Any]:
    errors = _extract_modal_errors(page, profile)
    if errors:
        automation_browser.capture_page_artifacts(page, tenant_dir, f"price_upload_validation_error_{file_path.stem}", note='; '.join(errors))
        raise PriceUploadError("; ".join(errors))

    warning_meta = _ensure_warning_confirmed(page, profile, tenant_dir, file_path)

    submit_button = _wait_button_enabled(
        page,
        selectors=profile.get("submit_button_selectors") or [],
        texts=profile.get("submit_button_texts") or [],
        timeout_ms=15000,
    )
    if submit_button is None:
        errors = _extract_modal_errors(page, profile)
        automation_browser.capture_page_artifacts(page, tenant_dir, f"price_submit_not_ready_{file_path.stem}", note='; '.join(errors) or "submit_button_not_ready")
        if errors:
            raise PriceUploadError("; ".join(errors))
        raise PriceUploadError("Кнопка 'Обновить стоимость' не стала доступной после загрузки файла")

    for mode, callback in (
        ("click", lambda: submit_button.click(timeout=5000)),
        ("click_force", lambda: submit_button.click(timeout=5000, force=True)),
    ):
        try:
            callback()
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            try:
                page.wait_for_timeout(1800)
            except Exception:
                time.sleep(1.8)
            return {**warning_meta, "submit_mode": mode}
        except Exception:
            continue
    if _click_locator_center(page, submit_button, attempts=2):
        try:
            page.wait_for_timeout(1800)
        except Exception:
            time.sleep(1.8)
        return {**warning_meta, "submit_mode": "mouse_center"}

    automation_browser.capture_page_artifacts(page, tenant_dir, f"price_submit_click_failed_{file_path.stem}", note="Не удалось нажать кнопку 'Обновить стоимость'")
    raise PriceUploadError("Не удалось нажать кнопку 'Обновить стоимость'")


def _body_contains(page, texts: Iterable[str]) -> bool:
    body = _body_text(page, limit=6000)
    return _contains_any(body, texts)


def _modal_still_open(page, profile: Dict[str, Any]) -> bool:
    try:
        if automation_browser.wait_any(page, profile.get("modal_ready_selectors") or [], timeout_ms=700) is not None:
            return True
    except Exception:
        pass
    for text in profile.get("modal_texts") or []:
        label = _clean(text)
        if not label:
            continue
        try:
            locator = page.get_by_text(label, exact=False)
            if locator.count() > 0 and locator.first.is_visible(timeout=400):
                return True
        except Exception:
            continue
    return False


def _prices_page_ready_now(page, profile: Dict[str, Any]) -> bool:
    try:
        if automation_browser.wait_any(page, profile.get("page_ready_selectors") or [], timeout_ms=700) is not None:
            return True
    except Exception:
        pass
    try:
        body = _body_text(page, limit=5000)
    except Exception:
        body = ""
    return _body_indicates_prices_page(body, profile)


def _wait_modal_close_and_prices_page(page, profile: Dict[str, Any], *, timeout_ms: int = 90000, poll_ms: int = 500) -> Dict[str, Any]:
    started = time.time()
    last_body = ""
    while time.time() - started < max(1.0, timeout_ms / 1000.0):
        try:
            last_body = _body_text(page, limit=5000)
        except Exception:
            last_body = ""
        modal_open = _modal_still_open(page, profile)
        page_ready = _prices_page_ready_now(page, profile) or _body_indicates_prices_page(last_body, profile)
        upload_id = _extract_upload_id_from_page(page)
        success_hint = _contains_any(last_body, profile.get("success_texts") or [])
        if (not modal_open and page_ready) or upload_id is not None or success_hint:
            return {
                "ok": True,
                "modal_open": modal_open,
                "page_ready": page_ready,
                "upload_id": upload_id,
                "success_hint": success_hint,
                "elapsed_ms": round((time.time() - started) * 1000),
                "body_excerpt": last_body[:2000],
            }
        try:
            page.wait_for_timeout(poll_ms)
        except Exception:
            time.sleep(poll_ms / 1000.0)
    return {
        "ok": False,
        "modal_open": _modal_still_open(page, profile),
        "page_ready": _prices_page_ready_now(page, profile),
        "upload_id": _extract_upload_id_from_page(page),
        "success_hint": _contains_any(last_body, profile.get("success_texts") or []),
        "elapsed_ms": round((time.time() - started) * 1000),
        "body_excerpt": last_body[:2000],
    }


def _default_browser_handler(tenant_id: str, file_path: Path, settings: Dict[str, Any], run_dir: Path) -> Dict[str, Any]:
    tenant_dir = automation_browser.ensure_run_tenant_dir(run_dir, tenant_id, "prices_upload")
    profile = _load_profile()
    recorder: Optional[automation_browser.NetworkRecorder] = None
    browser_cfg = settings.get("prices") or {}
    wait_after_close_ms = max(0, int(browser_cfg.get("post_submit_success_wait_ms") or profile.get("post_submit_success_wait_ms") or 60000))
    wait_modal_close_ms = max(10000, int(browser_cfg.get("post_submit_modal_close_timeout_ms") or profile.get("post_submit_modal_close_timeout_ms") or 90000))

    with automation_browser.open_authenticated_browser(tenant_id) as (_, _, page):
        recorder = automation_browser.NetworkRecorder(profile.get("network_markers") or [])
        try:
            page.on("response", recorder.handler)
        except Exception:
            pass

        background_jobs.progress("prices_upload_open_page", f"Открываю раздел цен и скидок для кабинета {tenant_id}", tenant_id=tenant_id, percent=8, file_name=file_path.name)
        opened_url = _open_upload_page(page, file_path, profile, tenant_dir)
        before = automation_browser.capture_page_artifacts(page, tenant_dir, f"price_upload_{tenant_id}_before", note=f"До загрузки файла {file_path.name}")

        background_jobs.progress("prices_upload_attach_file", f"Загружаю Excel-файл цен для кабинета {tenant_id}", tenant_id=tenant_id, percent=25, file_name=file_path.name)
        upload_meta = _set_input_file(page, file_path, profile, tenant_dir)

        background_jobs.progress("prices_upload_submit", f"Подтверждаю обновление цен для кабинета {tenant_id}", tenant_id=tenant_id, percent=42, file_name=file_path.name)
        submit_meta = _submit_price_update(page, file_path, profile, tenant_dir)

        finalize_meta = _wait_modal_close_and_prices_page(page, profile, timeout_ms=wait_modal_close_ms)
        upload_id = finalize_meta.get("upload_id") or (automation_browser.wait_for_upload_id(recorder.events, timeout_seconds=3.0, poll_interval=0.5) if recorder else None)
        if upload_id is None:
            upload_id = _extract_upload_id_from_page(page)

        if finalize_meta.get("ok"):
            background_jobs.progress(
                "prices_upload_page_ready",
                f"Форма WB закрылась, завершаю браузерный этап для кабинета {tenant_id}",
                tenant_id=tenant_id,
                percent=80,
                file_name=file_path.name,
                upload_id=upload_id,
            )
            try:
                page.wait_for_timeout(wait_after_close_ms)
            except Exception:
                time.sleep(wait_after_close_ms / 1000.0)
            errors_after: List[str] = []
            success_hint = True
            after = automation_browser.capture_page_artifacts(
                page,
                tenant_dir,
                f"price_upload_{tenant_id}_after",
                note=f"Модальное окно закрылось, страница цен активна. Завершение через {wait_after_close_ms} мс",
            )
            try:
                page.context.close()
            except Exception:
                pass
        else:
            try:
                page.wait_for_timeout(1500)
            except Exception:
                time.sleep(1.5)
            errors_after = _extract_modal_errors(page, profile)
            success_hint = _body_contains(page, profile.get("success_texts") or []) and not errors_after
            after = automation_browser.capture_page_artifacts(page, tenant_dir, f"price_upload_{tenant_id}_after", note=f"После загрузки файла {file_path.name}")

    if errors_after:
        raise PriceUploadError("; ".join(errors_after))

    return {
        "tenant_id": tenant_id,
        "file_path": str(file_path),
        "opened_url": opened_url,
        "submitted": True,
        "success_hint": success_hint,
        "upload_id": upload_id,
        "skip_verification": True if finalize_meta.get("ok") else False,
        "finalize_meta": finalize_meta,
        "network_events": recorder.events if recorder is not None else [],
        "before": before,
        "after": after,
        **upload_meta,
        **submit_meta,
    }


def _verify_uploaded_file(tenant_id: str, file_path: Path, settings: Dict[str, Any], upload_id: Optional[int]) -> Dict[str, Any]:
    tenant = tenant_manager.get_tenant(tenant_id) or {}
    api_key = _clean(tenant.get("wb_api_key"))
    if not api_key:
        raise PriceUploadError(f"У кабинета {tenant_id} нет WB API key для проверки цен.")
    verification: Dict[str, Any] = {}
    if upload_id:
        verification["upload_snapshot"] = wb_price_api.poll_upload_until_processed(api_key, int(upload_id))
    verification["current_prices"] = wb_price_api.verify_prices_against_file(api_key, file_path, settings)
    return verification


def run_price_upload_cycle(
    tenant_ids: Optional[Iterable[str]] = None,
    run_source: str = "manual",
    *,
    rebuild: bool = True,
    build_summary: Optional[Dict[str, Any]] = None,
    browser_handler: Optional[BrowserPriceHandler] = None,
) -> Dict[str, Any]:
    settings = automation_core.load_settings()
    selected = [
        _clean(tenant_id)
        for tenant_id in (tenant_ids or automation_core.list_enabled_tenant_ids(settings, feature="prices"))
        if _clean(tenant_id)
    ]
    if not selected:
        raise PriceUploadError("Нет кабинетов, включённых для ночной загрузки цен.")

    background_jobs.progress("prices_upload_init", "Готовлю ночную загрузку цен через браузерный ЛК", percent=0, tenants=len(selected), source=run_source)
    prepared_summary = _build_or_collect_files(selected, run_source, rebuild, prebuilt_summary=build_summary)
    run_dir = automation_core.create_run_dir("prices_upload")
    safe_files.write_json(Path(run_dir) / "build_summary.json", prepared_summary, ensure_ascii=False, indent=2)

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    handler = browser_handler or _default_browser_handler
    rows_by_tenant = {str(item.get("tenant_id")): item for item in (prepared_summary.get("results") or []) if isinstance(item, dict)}

    for index, tenant_id in enumerate(selected, start=1):
        row = rows_by_tenant.get(tenant_id)
        file_path = Path(str(row.get("output_path") or "")) if isinstance(row, dict) and _clean(row.get("output_path")) else None
        if not file_path or not file_path.exists():
            failures.append({"tenant_id": tenant_id, "error": "Не найден подготовленный файл цен для загрузки"})
            continue

        background_jobs.progress(
            "prices_upload_tenant_start",
            f"Загружаю файл цен для кабинета {tenant_id}",
            tenant_id=tenant_id,
            current=index,
            total=len(selected),
            percent=5 + round((index - 1) / max(1, len(selected)) * 70.0, 1),
            file_name=file_path.name,
        )
        tenant = tenant_manager.get_tenant(tenant_id)
        paths = tenant_manager.ensure_tenant_dirs(tenant_id)
        tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
        try:
            browser_result = handler(tenant_id, file_path, settings, Path(run_dir))
            verification = {}
            if bool((settings.get("prices") or {}).get("verify_via_api", True)) and not bool(browser_result.get("skip_verification")):
                background_jobs.progress(
                    "prices_upload_verify",
                    f"Проверяю итоговую загрузку цен через WB API для кабинета {tenant_id}",
                    tenant_id=tenant_id,
                    percent=75 + round(index / max(1, len(selected)) * 20.0, 1),
                    upload_id=browser_result.get("upload_id"),
                )
                verification = _verify_uploaded_file(tenant_id, file_path, settings, browser_result.get("upload_id"))
            result = {
                "tenant_id": tenant_id,
                "file_path": str(file_path),
                "browser": browser_result,
                "verification": verification,
            }
            results.append(result)
            current_prices = verification.get("current_prices") if isinstance(verification, dict) else {}
            background_jobs.progress(
                "prices_upload_tenant_done",
                f"Загрузка цен по кабинету {tenant_id} завершена",
                tenant_id=tenant_id,
                percent=75 + round(index / max(1, len(selected)) * 20.0, 1),
                upload_id=browser_result.get("upload_id"),
                mismatched=(current_prices or {}).get("mismatched"),
                quarantine_count=(current_prices or {}).get("quarantine_count"),
            )
        except Exception as exc:
            error_text = _clean(exc)
            failures.append({"tenant_id": tenant_id, "error": error_text})
            background_jobs.progress(
                "prices_upload_tenant_error",
                f"Ошибка загрузки цен для кабинета {tenant_id}: {error_text}",
                tenant_id=tenant_id,
                error=error_text,
            )
            log_event("automation", "prices_upload_tenant_error", tenant_id=tenant_id, level="error", error=error_text)
        finally:
            common.reset_tenant_context(tokens)

    summary = {
        "run_source": _clean(run_source) or "manual",
        "run_dir": str(run_dir),
        "build_summary": prepared_summary,
        "selected_tenants": selected,
        "results": results,
        "failures": failures,
        "uploaded": len(results),
        "failed": len(failures),
        "upload_ids": [item.get("browser", {}).get("upload_id") for item in results if item.get("browser", {}).get("upload_id")],
        "mismatched_total": sum(int((((item.get("verification") or {}).get("current_prices") or {}).get("mismatched") or 0)) for item in results),
        "quarantine_total": sum(int((((item.get("verification") or {}).get("current_prices") or {}).get("quarantine_count") or 0)) for item in results),
    }
    safe_files.write_json(Path(run_dir) / "summary.json", summary, ensure_ascii=False, indent=2)
    archive_path = Path(shutil.make_archive(str(run_dir), "zip", root_dir=run_dir))
    summary["archive_path"] = str(archive_path)
    report_path = automation_core.write_report(
        "prices_upload",
        status="completed" if not failures else "partial",
        title="Ночная загрузка цен через браузерный ЛК и проверка по API",
        payload=summary,
    )
    summary["report_path"] = str(report_path)
    safe_files.write_json(Path(run_dir) / "summary.json", summary, ensure_ascii=False, indent=2)
    background_jobs.progress(
        "prices_upload_done",
        "Ночная загрузка цен завершена",
        percent=100,
        uploaded=summary["uploaded"],
        failed=summary["failed"],
        mismatched_total=summary["mismatched_total"],
        quarantine_total=summary["quarantine_total"],
    )
    return {
        **summary,
        "message": (
            f"Файлов загружено: {summary['uploaded']}. "
            f"Ошибок: {summary['failed']}. "
            f"Несовпадений после API-проверки: {summary['mismatched_total']}. "
            f"Карантин: {summary['quarantine_total']}."
        ),
    }
