from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

import common
import wb_ui_actions


CHECKBOX_CONTAINER_SELECTOR = "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label']"
CHECKBOX_INPUT_SELECTOR = "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-input']"
CHECKBOX_LABEL_SELECTOR = "label[for='agree-changes']"
CHECKBOX_WRAPPER_SELECTOR = "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label'] [class*='checkboxWrapper']"
CHECKBOX_SIMPLE_SELECTOR = "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple']"
CHECKBOX_ICON_SELECTOR = "[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-icon']"
WARNING_BLOCK_SELECTOR = "[data-testid='check-changes-warning-block-test-id']"
SUBMIT_BUTTON_SELECTOR = "[data-testid='xlsx-action-action-test-id-button-primary']"



def _clean(value: Any) -> str:
    return common.clean_text(value)



def _list(values: Iterable[Any]) -> List[str]:
    return [_clean(item) for item in (values or []) if _clean(item)]



def _sleep(page, ms: int) -> None:
    delay = max(0, int(ms or 0))
    if delay <= 0:
        return
    try:
        page.wait_for_timeout(delay)
    except Exception:
        time.sleep(delay / 1000.0)



def _first_visible_locator(locator, *, limit: int = 8):
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



def _find_visible_locator(page_or_scope, selectors: Sequence[str], *, timeout_ms: int = 0, poll_ms: int = 180):
    deadline = time.time() + max(0.05, timeout_ms / 1000.0) if timeout_ms else None
    while True:
        for selector in selectors:
            selector = _clean(selector)
            if not selector:
                continue
            try:
                locator = page_or_scope.locator(selector)
                visible = _first_visible_locator(locator)
                if visible is not None:
                    return visible
            except Exception:
                continue
        if deadline is None or time.time() >= deadline:
            return None
        _sleep(page_or_scope, poll_ms)  # type: ignore[arg-type]



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



def _read_locator_text(locator) -> str:
    if locator is None:
        return ""
    for method in ("inner_text", "text_content"):
        try:
            value = getattr(locator, method)(timeout=1200) if method == "inner_text" else getattr(locator, method)()
            return _clean(value)
        except Exception:
            continue
    return ""



def _locator_enabled(locator) -> bool:
    if locator is None:
        return False
    try:
        if locator.is_disabled():
            return False
    except Exception:
        pass
    script = """(el) => {
      if (!el) return false;
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      const ariaDisabled = String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
      const cls = String(el.className || '').toLowerCase();
      if (el.disabled || ariaDisabled) return false;
      if (cls.includes('disabled')) return false;
      if (style.display === 'none' || style.visibility === 'hidden' || style.pointerEvents === 'none') return false;
      return rect.width > 0 && rect.height > 0;
    }"""
    try:
        return bool(locator.evaluate(script))
    except Exception:
        return True



def _locator_is_checked(locator) -> bool:
    if locator is None:
        return False
    try:
        if hasattr(locator, "is_checked") and locator.is_checked(timeout=500):
            return True
    except Exception:
        pass
    script = """(el) => {
      if (!el) return false;
      const read = (node) => {
        if (!node) return false;
        if (typeof node.checked === 'boolean' && node.checked) return true;
        const aria = String(node.getAttribute?.('aria-checked') || '').toLowerCase();
        if (aria === 'true') return true;
        const nested = node.querySelector?.("input[type='checkbox']");
        return !!(nested && nested.checked);
      };
      return read(el);
    }"""
    try:
        return bool(locator.evaluate(script))
    except Exception:
        return False



def _locator_box(locator) -> Optional[Dict[str, float]]:
    if locator is None:
        return None
    try:
        box = locator.bounding_box()
    except Exception:
        box = None
    if not box:
        return None
    try:
        return {
            "x": float(box.get("x", 0.0)),
            "y": float(box.get("y", 0.0)),
            "width": float(box.get("width", 0.0)),
            "height": float(box.get("height", 0.0)),
        }
    except Exception:
        return None



def _mouse_click_locator(page, locator, *, x_ratio: float = 0.5, y_ratio: float = 0.5, attempts: int = 1) -> bool:
    for _ in range(max(1, int(attempts or 1))):
        try:
            locator.scroll_into_view_if_needed(timeout=1500)
        except Exception:
            pass
        box = _locator_box(locator)
        if not box or box["width"] <= 0 or box["height"] <= 0:
            continue
        x = box["x"] + max(2.0, min(box["width"] - 2.0, box["width"] * float(x_ratio)))
        y = box["y"] + max(2.0, min(box["height"] - 2.0, box["height"] * float(y_ratio)))
        try:
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



def _js_click_locator(locator) -> bool:
    if locator is None:
        return False
    script = """(el) => {
      if (!el) return false;
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.pointerEvents !== 'none' && rect.width > 0 && rect.height > 0;
      };
      const fire = (node, type, Ctor, extra = {}) => {
        try {
          node.dispatchEvent(new Ctor(type, Object.assign({bubbles:true, cancelable:true, composed:true}, extra)));
          return true;
        } catch (e) {
          return false;
        }
      };
      const chain = [
        el,
        el.closest?.('label'),
        el.closest?.('[role="checkbox"]'),
        el.closest?.('button'),
        el.parentElement,
        el.parentElement?.parentElement,
      ].filter(Boolean);
      for (const node of chain) {
        if (!isVisible(node)) continue;
        try { node.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
        try { node.focus?.(); } catch (e) {}
        const rect = node.getBoundingClientRect();
        const payload = {clientX: rect.left + Math.max(3, rect.width / 2), clientY: rect.top + Math.max(3, rect.height / 2), buttons: 1, pointerType: 'mouse', isPrimary: true};
        fire(node, 'pointerover', window.PointerEvent || MouseEvent, payload);
        fire(node, 'mouseover', MouseEvent, payload);
        fire(node, 'pointermove', window.PointerEvent || MouseEvent, payload);
        fire(node, 'mousemove', MouseEvent, payload);
        fire(node, 'pointerdown', window.PointerEvent || MouseEvent, payload);
        fire(node, 'mousedown', MouseEvent, payload);
        fire(node, 'pointerup', window.PointerEvent || MouseEvent, Object.assign({}, payload, {buttons: 0}));
        fire(node, 'mouseup', MouseEvent, Object.assign({}, payload, {buttons: 0}));
        try { node.click?.(); } catch (e) {}
        fire(node, 'click', MouseEvent, Object.assign({}, payload, {buttons: 0}));
        return true;
      }
      return false;
    }"""
    try:
        return bool(locator.evaluate(script))
    except Exception:
        return False



def _js_force_checkbox(locator) -> bool:
    if locator is None:
        return False
    script = """(el) => {
      if (!el) return false;
      const root = el.closest?.("[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label']") || el.parentElement || document;
      const resolveInput = (node) => {
        if (!node) return null;
        if (node.matches?.("input[type='checkbox']")) return node;
        const labelFor = node.getAttribute?.('for');
        if (labelFor) {
          const linked = document.getElementById(labelFor);
          if (linked) return linked;
        }
        return node.querySelector?.("input[type='checkbox']") || root.querySelector?.("input#agree-changes, input[name='agree-changes'], [data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-input']");
      };
      const input = resolveInput(el) || resolveInput(root);
      if (!input) return false;
      try { input.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
      try { input.focus?.(); } catch (e) {}
      if (!input.checked) {
        try { input.click?.(); } catch (e) {}
      }
      if (!input.checked) {
        try {
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'checked')?.set;
          if (setter) setter.call(input, true); else input.checked = true;
          input.setAttribute('checked', 'checked');
        } catch (e) {}
      }
      try { input.dispatchEvent(new Event('input', {bubbles:true, composed:true})); } catch (e) {}
      try { input.dispatchEvent(new Event('change', {bubbles:true, composed:true})); } catch (e) {}
      try { input.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, composed:true, buttons: 1})); } catch (e) {}
      return !!input.checked;
    }"""
    try:
        return bool(locator.evaluate(script))
    except Exception:
        return False



def _base_payload(profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rootSelectors": _list(
            [
                "[data-testid='dp-update-modal-test-id-drawer-overlay']",
                "#Portal-drawer [class*='excel-modal']",
                "#Portal-drawer",
            ]
            + list(profile.get("modal_root_selectors") or [])
        ),
        "warningBlockSelectors": _list([WARNING_BLOCK_SELECTOR] + list(profile.get("warning_block_selectors") or [])),
        "warningTexts": _list(profile.get("warning_texts") or []),
        "checkboxSelectors": _list(
            [
                CHECKBOX_INPUT_SELECTOR,
                "input#agree-changes",
                "input[name='agree-changes']",
                CHECKBOX_CONTAINER_SELECTOR,
                CHECKBOX_WRAPPER_SELECTOR,
                CHECKBOX_SIMPLE_SELECTOR,
                CHECKBOX_ICON_SELECTOR,
            ]
            + list(profile.get("warning_checkbox_selectors") or [])
        ),
        "labelSelectors": _list(
            [
                CHECKBOX_LABEL_SELECTOR,
                "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label-label']",
                CHECKBOX_CONTAINER_SELECTOR,
                CHECKBOX_WRAPPER_SELECTOR,
                CHECKBOX_SIMPLE_SELECTOR,
                CHECKBOX_ICON_SELECTOR,
            ]
            + list(profile.get("warning_checkbox_label_selectors") or [])
        ),
        "submitSelectors": _list([SUBMIT_BUTTON_SELECTOR] + list(profile.get("submit_button_selectors") or [])),
        "submitTexts": _list(profile.get("submit_button_texts") or []),
    }



def _modal_root_locator(page, profile: Dict[str, Any]):
    selectors = _base_payload(profile)["rootSelectors"]
    best = None
    best_area = -1.0
    for selector in selectors:
        visible = _find_visible_locator(page, [selector], timeout_ms=0)
        if visible is None:
            continue
        try:
            text = _read_locator_text(visible).lower()
        except Exception:
            text = ""
        score = 0.0
        if "обновить цены и скидки через excel" in text:
            score += 5_000_000.0
        if "загрузите заполненный шаблон" in text or "формат файла" in text:
            score += 2_000_000.0
        box = _locator_box(visible)
        if box:
            score += box["width"] * box["height"]
        if selector == "[data-testid='dp-update-modal-test-id-drawer-overlay']":
            score += 200_000.0
        if score > best_area:
            best = visible
            best_area = score
    return best



def _submit_locator(root, profile: Dict[str, Any]):
    selectors = _base_payload(profile)["submitSelectors"]
    locator = _find_visible_locator(root, selectors, timeout_ms=0)
    if locator is not None:
        return locator
    try:
        buttons = root.locator("button, [role='button']")
        count = min(buttons.count(), 10)
    except Exception:
        count = 0
        buttons = None
    submit_texts = [item.lower() for item in _list(profile.get("submit_button_texts") or [])] or ["обновить стоимость", "обновить цены", "обновить"]
    for index in range(count):
        try:
            item = buttons.nth(index)
        except Exception:
            continue
        text = _read_locator_text(item).lower()
        if any(label in text for label in submit_texts):
            return item
    return None



def _checkbox_targets(page, profile: Dict[str, Any]) -> Dict[str, Any]:
    root = _modal_root_locator(page, profile)
    warning_block = None
    container = None
    input_locator = None
    label_locator = None
    wrapper_locator = None
    simple_locator = None
    icon_locator = None
    submit_button = None

    if root is not None:
        warning_block = _find_visible_locator(root, [WARNING_BLOCK_SELECTOR] + _list(profile.get("warning_block_selectors") or []), timeout_ms=0)
        if warning_block is None:
            try:
                candidates = root.locator("div, section, article")
                count = min(candidates.count(), 20)
            except Exception:
                count = 0
                candidates = None
            warning_terms = ["карантин", "не ошибка", "резко снизили цену"]
            for index in range(count):
                try:
                    item = candidates.nth(index)
                except Exception:
                    continue
                text = _read_locator_text(item).lower()
                if any(term in text for term in warning_terms):
                    warning_block = item
                    break

    scope = warning_block or root or page
    if scope is not None:
        container = _find_visible_locator(scope, [CHECKBOX_CONTAINER_SELECTOR], timeout_ms=0)
        search_scope = container or warning_block or root or scope
        input_locator = _find_attached_locator(
            search_scope,
            [
                CHECKBOX_INPUT_SELECTOR,
                "input#agree-changes",
                "input[name='agree-changes']",
            ] + _list(profile.get("warning_checkbox_selectors") or []),
        )
        label_locator = _find_visible_locator(
            search_scope,
            [
                CHECKBOX_LABEL_SELECTOR,
                "[data-testid='check-changes-warning-checkbox-test-id-checkbox-with-label-label']",
                CHECKBOX_CONTAINER_SELECTOR,
            ] + _list(profile.get("warning_checkbox_label_selectors") or []),
            timeout_ms=0,
        )
        wrapper_locator = _find_visible_locator(
            search_scope,
            [
                CHECKBOX_WRAPPER_SELECTOR,
                CHECKBOX_SIMPLE_SELECTOR,
            ],
            timeout_ms=0,
        )
        simple_locator = _find_visible_locator(search_scope, [CHECKBOX_SIMPLE_SELECTOR], timeout_ms=0)
        icon_locator = _find_visible_locator(search_scope, [CHECKBOX_ICON_SELECTOR], timeout_ms=0)
        submit_button = _submit_locator(root or search_scope, profile)

    return {
        "root": root,
        "warning_block": warning_block,
        "container": container,
        "input": input_locator,
        "label": label_locator,
        "wrapper": wrapper_locator,
        "simple": simple_locator,
        "icon": icon_locator,
        "submit": submit_button,
    }



def identify_checkbox(page, profile: Dict[str, Any], *, text_limit: int = 2000) -> Dict[str, Any]:
    targets = _checkbox_targets(page, profile)
    root = targets.get("root")
    warning_block = targets.get("warning_block")
    container = targets.get("container")
    input_locator = targets.get("input")
    submit_button = targets.get("submit")

    modal_text = _read_locator_text(root)[: max(400, int(text_limit or 2000))] if root is not None else ""
    warning_text = _read_locator_text(warning_block)[:600] if warning_block is not None else ""
    submit_text = _read_locator_text(submit_button)

    return {
        "modal_open": root is not None,
        "warning_present": warning_block is not None or any(term in modal_text.lower() for term in ["карантин", "не ошибка", "резко снизили цену"]),
        "checkbox_present": any(targets.get(key) is not None for key in ("input", "label", "wrapper", "container", "icon")),
        "checkbox_checked": _locator_is_checked(input_locator) or _locator_is_checked(container) or _locator_is_checked(targets.get("wrapper")) or _locator_enabled(submit_button),
        "submit_enabled": _locator_enabled(submit_button),
        "submit_text": submit_text,
        "modal_text": modal_text,
        "warning_text": warning_text,
        "has_input": input_locator is not None,
        "has_label": targets.get("label") is not None,
        "has_wrapper": targets.get("wrapper") is not None,
        "has_container": container is not None,
        "has_icon": targets.get("icon") is not None,
    }



def modal_state(page, profile: Dict[str, Any], *, text_limit: int = 2000) -> Dict[str, Any]:
    return identify_checkbox(page, profile, text_limit=text_limit)



def warning_present(page, profile: Dict[str, Any]) -> bool:
    state = identify_checkbox(page, profile, text_limit=1600)
    return bool(state.get("modal_open") and state.get("warning_present"))



def checkbox_checked(page, profile: Dict[str, Any]) -> bool:
    state = identify_checkbox(page, profile, text_limit=1200)
    return bool(state.get("checkbox_checked"))



def submit_enabled(page, profile: Dict[str, Any]) -> bool:
    state = identify_checkbox(page, profile, text_limit=1200)
    return bool(state.get("submit_enabled"))



def scroll_modal_bottom(page, profile: Dict[str, Any], *, repeats: int = 3) -> bool:
    payload = _base_payload(profile)
    script = r"""
(args) => {
  const selectors = Array.isArray(args?.rootSelectors) ? args.rootSelectors : [];
  const normalize = (value) => String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  const isVisible = (node) => {
    if (!node || typeof node.getBoundingClientRect !== 'function') return false;
    const rect = node.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return false;
    const style = window.getComputedStyle(node);
    return style.display !== 'none' && style.visibility !== 'hidden';
  };
  let root = null;
  for (const selector of selectors) {
    try {
      const nodes = [...document.querySelectorAll(selector)].filter(isVisible);
      for (const node of nodes) {
        const text = normalize(node.innerText || node.textContent || '');
        if (text.includes('обновить цены и скидки через excel') || text.includes('загрузите заполненный шаблон') || text.includes('формат файла')) {
          root = node;
          break;
        }
      }
      if (root) break;
      if (nodes.length && !root) root = nodes[0];
    } catch (e) {}
  }
  if (!root) {
    const drawer = document.querySelector('#Portal-drawer');
    if (drawer && isVisible(drawer)) root = drawer;
  }
  if (!root) return false;
  const nodes = [root, ...root.querySelectorAll('*')].filter(isVisible);
  let moved = false;
  for (const node of nodes) {
    try {
      const style = window.getComputedStyle(node);
      const overflowY = String(style.overflowY || '');
      const scrollable = (overflowY.includes('auto') || overflowY.includes('scroll') || String(node.className || '').includes('ScrollBar')) && node.scrollHeight > node.clientHeight + 8;
      if (!scrollable) continue;
      node.scrollTop = node.scrollHeight;
      moved = true;
    } catch (e) {}
  }
  try { root.scrollIntoView({block: 'center', inline: 'center'}); } catch (e) {}
  return moved;
}
"""
    moved = False
    for _ in range(max(1, int(repeats or 1))):
        try:
            moved = bool(page.evaluate(script, payload)) or moved
        except Exception:
            pass
        _sleep(page, 180)
    return moved



def _wait_ready_after_action(page, profile: Dict[str, Any], *, timeout_ms: int = 1800) -> bool:
    deadline = time.time() + max(0.4, int(timeout_ms or 0) / 1000.0)
    while time.time() < deadline:
        state = identify_checkbox(page, profile, text_limit=1200)
        if state.get("checkbox_checked") or state.get("submit_enabled"):
            return True
        _sleep(page, 160)
    return False



def _dom_click_warning(page, profile: Dict[str, Any], *, x_ratios: Optional[List[float]] = None) -> Dict[str, Any]:
    payload = _base_payload(profile)
    payload["xRatios"] = list(x_ratios or [0.07, 0.10, 0.13, 0.16, 0.19, 0.22])
    script = r"""
(args) => {
  const rootSelectors = Array.isArray(args?.rootSelectors) ? args.rootSelectors : [];
  const warningBlockSelectors = Array.isArray(args?.warningBlockSelectors) ? args.warningBlockSelectors : [];
  const xRatios = Array.isArray(args?.xRatios) ? args.xRatios : [0.07, 0.1, 0.13, 0.16];
  const normalize = (value) => String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  const isVisible = (node) => {
    if (!node || typeof node.getBoundingClientRect !== 'function') return false;
    const rect = node.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return false;
    const style = window.getComputedStyle(node);
    return style.display !== 'none' && style.visibility !== 'hidden' && style.pointerEvents !== 'none';
  };
  const pickRoot = () => {
    for (const selector of rootSelectors) {
      try {
        const nodes = [...document.querySelectorAll(selector)].filter(isVisible);
        for (const node of nodes) {
          const text = normalize(node.innerText || node.textContent || '');
          if (text.includes('обновить цены и скидки через excel') || text.includes('загрузите заполненный шаблон')) return node;
        }
        if (nodes.length) return nodes[0];
      } catch (e) {}
    }
    const drawer = document.querySelector('#Portal-drawer');
    if (drawer && isVisible(drawer)) return drawer;
    return null;
  };
  const root = pickRoot();
  if (!root) return {ok: false, step: 'no_root'};
  const fire = (node, type, Ctor, extra = {}) => {
    try { node.dispatchEvent(new Ctor(type, Object.assign({bubbles: true, cancelable: true, composed: true}, extra))); } catch (e) {}
  };
  const clickNode = (node) => {
    if (!node || !isVisible(node)) return false;
    try { node.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
    try { node.focus?.(); } catch (e) {}
    const rect = node.getBoundingClientRect();
    const x = rect.left + Math.max(2, Math.min(rect.width - 2, rect.width * 0.5));
    const y = rect.top + Math.max(2, Math.min(rect.height - 2, rect.height * 0.5));
    const payload = {clientX: x, clientY: y, buttons: 1, pointerType: 'mouse', isPrimary: true};
    fire(node, 'pointerover', window.PointerEvent || MouseEvent, payload);
    fire(node, 'mouseover', MouseEvent, payload);
    fire(node, 'pointermove', window.PointerEvent || MouseEvent, payload);
    fire(node, 'mousemove', MouseEvent, payload);
    fire(node, 'pointerdown', window.PointerEvent || MouseEvent, payload);
    fire(node, 'mousedown', MouseEvent, payload);
    fire(node, 'pointerup', window.PointerEvent || MouseEvent, Object.assign({}, payload, {buttons: 0}));
    fire(node, 'mouseup', MouseEvent, Object.assign({}, payload, {buttons: 0}));
    try { node.click?.(); } catch (e) {}
    fire(node, 'click', MouseEvent, Object.assign({}, payload, {buttons: 0}));
    return true;
  };
  const checked = () => {
    const explicit = root.querySelector("input#agree-changes, input[name='agree-changes'], [data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-input']");
    if (explicit && explicit.checked) return true;
    const generic = root.querySelector("input[type='checkbox']:checked, [role='checkbox'][aria-checked='true']");
    return !!generic;
  };
  const submitEnabled = () => {
    const buttons = [
      ...root.querySelectorAll("[data-testid='xlsx-action-action-test-id-button-primary']"),
      ...root.querySelectorAll('button, [role="button"]'),
    ].filter(isVisible);
    return buttons.some((node) => {
      const text = normalize(node.innerText || node.textContent || '');
      const ariaDisabled = String(node.getAttribute?.('aria-disabled') || '').toLowerCase() === 'true';
      return text.includes('обновить стоимость') && !node.disabled && !ariaDisabled;
    });
  };

  let blocks = [];
  for (const selector of warningBlockSelectors) {
    try {
      const items = [...root.querySelectorAll(selector)].filter(isVisible);
      if (items.length) {
        blocks = items;
        break;
      }
    } catch (e) {}
  }
  if (!blocks.length) {
    blocks = [...root.querySelectorAll('div, section, article')].filter((node) => {
      if (!isVisible(node)) return false;
      const text = normalize(node.innerText || node.textContent || '');
      return text.includes('не ошибка') || text.includes('карантин') || text.includes('резко снизили цену');
    }).slice(0, 4);
  }

  const actions = [];
  for (const block of blocks) {
    const rect = block.getBoundingClientRect();
    const y = rect.top + Math.min(rect.height - 4, Math.max(20, rect.height * 0.72));
    for (const ratio of xRatios) {
      const x = rect.left + Math.max(10, Math.min(rect.width - 6, rect.width * ratio));
      const hit = document.elementFromPoint(x, y) || block;
      const target = (hit.closest && (hit.closest('label, [role="checkbox"], button, div, span') || hit)) || hit;
      if (clickNode(target)) actions.push(`scan:${ratio}`);
      if (checked() || submitEnabled()) return {ok: true, step: 'scan', actions};
    }
  }
  return {ok: checked() || submitEnabled(), step: 'done', actions};
}
"""
    try:
        result = page.evaluate(script, payload)
    except Exception as exc:
        return {"ok": False, "step": "exception", "error": _clean(exc), "actions": []}
    return result if isinstance(result, dict) else {"ok": bool(result), "step": "unknown", "actions": []}



def solve_checkbox(page, profile: Dict[str, Any], *, pause_before_scan_ms: int = 0) -> Dict[str, Any]:
    actions: List[str] = []
    if pause_before_scan_ms > 0:
        _sleep(page, pause_before_scan_ms)
    scroll_modal_bottom(page, profile, repeats=3)

    state = identify_checkbox(page, profile, text_limit=1500)
    if not state.get("modal_open"):
        return {"ok": False, "reason": "modal_closed", "actions": actions, "state": state}
    if not state.get("warning_present") and not state.get("checkbox_present"):
        return {"ok": False, "reason": "warning_not_present", "actions": actions, "state": state}
    if state.get("checkbox_checked") or state.get("submit_enabled"):
        return {"ok": True, "reason": "already_ready", "actions": actions, "state": state}

    targets = _checkbox_targets(page, profile)
    input_locator = targets.get("input")
    label_locator = targets.get("label")
    wrapper_locator = targets.get("wrapper")
    simple_locator = targets.get("simple")
    icon_locator = targets.get("icon")
    container_locator = targets.get("container") or targets.get("warning_block")

    def _done(reason: str) -> Optional[Dict[str, Any]]:
        if _wait_ready_after_action(page, profile, timeout_ms=1800):
            return {
                "ok": True,
                "reason": reason,
                "actions": list(actions),
                "state": identify_checkbox(page, profile, text_limit=1200),
            }
        return None

    # 1. Real input first: this is the safest semantic target.
    if input_locator is not None:
        for mode in ("check", "set_checked", "click_force", "click", "space", "js_click", "js_force"):
            try:
                if mode == "check" and hasattr(input_locator, "check"):
                    input_locator.check(timeout=2500, force=True)
                elif mode == "set_checked" and hasattr(input_locator, "set_checked"):
                    input_locator.set_checked(True, timeout=2500, force=True)
                elif mode == "click_force":
                    input_locator.click(timeout=2500, force=True)
                elif mode == "click":
                    input_locator.click(timeout=2500)
                elif mode == "space":
                    input_locator.press("Space", timeout=2500)
                elif mode == "js_click":
                    _js_click_locator(input_locator)
                elif mode == "js_force":
                    _js_force_checkbox(input_locator)
                actions.append(f"input:{mode}")
                result = _done(f"input:{mode}")
                if result:
                    return result
            except Exception:
                continue

    # 2. Bound label is the next safest target because it is explicitly linked by for='agree-changes'.
    if label_locator is not None:
        for mode in ("click", "click_force", "mouse_center", "mouse_left", "js_click", "js_force"):
            try:
                if mode == "click":
                    label_locator.click(timeout=2500)
                elif mode == "click_force":
                    label_locator.click(timeout=2500, force=True)
                elif mode == "mouse_center":
                    _mouse_click_locator(page, label_locator, x_ratio=0.50, y_ratio=0.50, attempts=1)
                elif mode == "mouse_left":
                    _mouse_click_locator(page, label_locator, x_ratio=0.08, y_ratio=0.50, attempts=1)
                elif mode == "js_click":
                    _js_click_locator(label_locator)
                elif mode == "js_force":
                    _js_force_checkbox(label_locator)
                actions.append(f"label:{mode}")
                result = _done(f"label:{mode}")
                if result:
                    return result
            except Exception:
                continue

    # 3. Wrapper / simple checkbox block from the HTML structure.
    for target_name, locator in (("wrapper", wrapper_locator), ("simple", simple_locator), ("icon", icon_locator), ("container", container_locator)):
        if locator is None:
            continue
        for mode, x_ratio in (
            ("click", None),
            ("click_force", None),
            ("mouse_center", 0.50),
            ("mouse_left", 0.10),
            ("mouse_right", 0.82),
            ("js_click", None),
            ("js_force", None),
        ):
            try:
                if mode == "click":
                    locator.click(timeout=2500)
                elif mode == "click_force":
                    locator.click(timeout=2500, force=True)
                elif mode == "mouse_center":
                    _mouse_click_locator(page, locator, x_ratio=float(x_ratio or 0.5), y_ratio=0.5, attempts=1)
                elif mode == "mouse_left":
                    _mouse_click_locator(page, locator, x_ratio=float(x_ratio or 0.1), y_ratio=0.55, attempts=1)
                elif mode == "mouse_right":
                    _mouse_click_locator(page, locator, x_ratio=float(x_ratio or 0.82), y_ratio=0.55, attempts=1)
                elif mode == "js_click":
                    _js_click_locator(locator)
                elif mode == "js_force":
                    _js_force_checkbox(locator)
                actions.append(f"{target_name}:{mode}")
                result = _done(f"{target_name}:{mode}")
                if result:
                    return result
            except Exception:
                continue

    # 4. Smart scored click within warning block/modal scope.
    smart = wb_ui_actions.smart_click(
        page,
        scope_selectors=_list([WARNING_BLOCK_SELECTOR, CHECKBOX_CONTAINER_SELECTOR] + list(profile.get("warning_block_selectors") or []) + list(profile.get("modal_root_selectors") or [])),
        selectors=_list([
            CHECKBOX_INPUT_SELECTOR,
            CHECKBOX_LABEL_SELECTOR,
            CHECKBOX_WRAPPER_SELECTOR,
            CHECKBOX_SIMPLE_SELECTOR,
            CHECKBOX_ICON_SELECTOR,
            CHECKBOX_CONTAINER_SELECTOR,
        ]),
        texts=["Всё верно — это не ошибка", "Все верно — это не ошибка", "Всё верно", "Это не ошибка"],
        testid_fragments=["check-changes-warning", "checkbox-simple", "checkbox-with-label", "agree-changes"],
        class_fragments=["checkboxWrapper", "checkboxWithLabel", "checkbox__", "label__", "content__"],
        max_candidates=16,
    )
    if smart.get("ok"):
        clicked = smart.get("clicked") if isinstance(smart.get("clicked"), dict) else {}
        actions.append("smart:" + _clean(clicked.get("source") or clicked.get("dataTestId") or clicked.get("text") or "ok"))
        result = _done("smart_click")
        if result:
            result["smart"] = smart
            return result

    # 5. Final DOM scan inside the warning block after extra scroll.
    scroll_modal_bottom(page, profile, repeats=2)
    dom = _dom_click_warning(page, profile)
    dom_actions = dom.get("actions") if isinstance(dom, dict) and isinstance(dom.get("actions"), list) else []
    actions.extend([_clean(item) for item in dom_actions if _clean(item)])
    if dom.get("ok"):
        result = _done("dom_warning")
        if result:
            result["dom"] = dom
            return result

    final_state = identify_checkbox(page, profile, text_limit=1500)
    return {
        "ok": bool(final_state.get("checkbox_checked") or final_state.get("submit_enabled")),
        "reason": "final_state",
        "actions": actions,
        "state": final_state,
        "smart": smart,
        "dom": dom,
    }
