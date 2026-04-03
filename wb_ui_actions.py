from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence

import common


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _list(values: Iterable[Any]) -> List[str]:
    return [_clean(item) for item in (values or []) if _clean(item)]


def smart_click(
    page,
    *,
    scope_selectors: Sequence[str] = (),
    selectors: Sequence[str] = (),
    texts: Sequence[str] = (),
    testid_fragments: Sequence[str] = (),
    class_fragments: Sequence[str] = (),
    max_candidates: int = 12,
) -> Dict[str, Any]:
    payload = {
        "scopeSelectors": _list(scope_selectors),
        "selectors": _list(selectors),
        "texts": [item.lower() for item in _list(texts)],
        "testidFragments": [item.lower() for item in _list(testid_fragments)],
        "classFragments": [item.lower() for item in _list(class_fragments)],
        "maxCandidates": max(1, int(max_candidates or 12)),
    }
    script = """
(args) => {
  const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
  const lower = (value) => normalize(value).toLowerCase();
  const scopeSelectors = Array.isArray(args?.scopeSelectors) ? args.scopeSelectors : [];
  const selectors = Array.isArray(args?.selectors) ? args.selectors : [];
  const texts = Array.isArray(args?.texts) ? args.texts : [];
  const testidFragments = Array.isArray(args?.testidFragments) ? args.testidFragments : [];
  const classFragments = Array.isArray(args?.classFragments) ? args.classFragments : [];
  const maxCandidates = Math.max(1, Number(args?.maxCandidates) || 12);
  const isVisible = (el) => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && style.pointerEvents !== 'none' && rect.width > 0 && rect.height > 0;
  };
  const root = (() => {
    for (const selector of scopeSelectors) {
      try {
        const node = document.querySelector(selector);
        if (node) return node;
      } catch (e) {}
    }
    return document;
  })();
  const fireChain = (node) => {
    if (!node || !isVisible(node)) return false;
    try { node.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
    try { node.focus?.(); } catch (e) {}
    const rect = node.getBoundingClientRect();
    const x = rect.left + Math.max(3, rect.width / 2);
    const y = rect.top + Math.max(3, rect.height / 2);
    const payload = {clientX: x, clientY: y, bubbles: true, cancelable: true, composed: true, buttons: 1, pointerType: 'mouse', isPrimary: true};
    const send = (type, Ctor, extra = {}) => {
      try { node.dispatchEvent(new Ctor(type, Object.assign({}, payload, extra))); } catch (e) {}
    };
    send('pointerover', window.PointerEvent || MouseEvent);
    send('mouseover', MouseEvent);
    send('pointermove', window.PointerEvent || MouseEvent);
    send('mousemove', MouseEvent);
    send('pointerdown', window.PointerEvent || MouseEvent);
    send('mousedown', MouseEvent);
    send('pointerup', window.PointerEvent || MouseEvent, {buttons: 0});
    send('mouseup', MouseEvent, {buttons: 0});
    try { node.click?.(); } catch (e) {}
    send('click', MouseEvent, {buttons: 0});
    return true;
  };
  const candidates = [];
  const add = (node, source) => {
    if (!node || !node.tagName) return;
    const tag = lower(node.tagName);
    if (['html', 'body', 'head'].includes(tag)) return;
    if (candidates.some((row) => row.node === node)) return;
    const text = normalize(node.innerText || node.textContent || '');
    const dataTestId = normalize(node.getAttribute?.('data-testid'));
    const classes = normalize(node.className);
    const role = normalize(node.getAttribute?.('role'));
    const rect = node.getBoundingClientRect();
    let score = 0;
    if (source.startsWith('selector:')) score += 600;
    if (['button', 'input', 'label', 'a'].includes(tag)) score += 140;
    if (role === 'button' || role === 'checkbox') score += 120;
    if (isVisible(node)) score += 100;
    if (texts.some((sample) => sample && lower(text).includes(sample))) score += 260;
    if (testidFragments.some((sample) => sample && lower(dataTestId).includes(sample))) score += 220;
    if (classFragments.some((sample) => sample && lower(classes).includes(sample))) score += 120;
    if (tag === 'input' && lower(node.getAttribute?.('type')) === 'checkbox') score += 220;
    if (lower(text).includes('не ошибка')) score += 200;
    candidates.push({
      node,
      score,
      info: {
        source,
        tag: normalize(node.tagName),
        role,
        type: normalize(node.getAttribute?.('type')),
        text: text.slice(0, 240),
        dataTestId,
        classes,
        bbox: {x: rect.x, y: rect.y, width: rect.width, height: rect.height},
      },
    });
  };
  for (const selector of selectors) {
    try {
      Array.from(root.querySelectorAll(selector)).slice(0, maxCandidates).forEach((node) => add(node, `selector:${selector}`));
    } catch (e) {}
  }
  if (!candidates.length && texts.length) {
    try {
      Array.from(root.querySelectorAll("button, [role], input, label, a, [data-testid]")).slice(0, maxCandidates * 8).forEach((node) => {
        const text = lower(node.innerText || node.textContent || '');
        if (texts.some((sample) => sample && text.includes(sample))) add(node, 'text_fallback');
      });
    } catch (e) {}
  }
  candidates.sort((a, b) => b.score - a.score);
  for (const row of candidates.slice(0, maxCandidates)) {
    if (fireChain(row.node)) {
      return {ok: true, clicked: row.info, candidates: candidates.slice(0, maxCandidates).map((item) => item.info)};
    }
  }
  return {ok: false, clicked: null, candidates: candidates.slice(0, maxCandidates).map((item) => item.info)};
}
"""
    try:
        result = page.evaluate(script, payload)
        return result if isinstance(result, dict) else {"ok": bool(result)}
    except Exception as exc:
        return {"ok": False, "error": _clean(exc)}
