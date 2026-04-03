from __future__ import annotations

import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import common
import safe_files
from safe_logs import sanitize

MAX_TEXT_LEN = 1200
INTERACTIVE_FALLBACK_SELECTOR = (
    "button, [role='button'], [role='menuitem'], [role='option'], [role='checkbox'], "
    "[role='radio'], [role='link'], input, label, a, [data-testid], [data-name]"
)


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _utc_now() -> str:
    return common.utc_now_iso()


# NOTE: keep filenames deterministic and Windows-safe.
def _safe_name(value: Any) -> str:
    text = _clean(value)
    if not text:
        return "artifact"
    result: List[str] = []
    for char in text:
        if char.isalnum() or char in {"-", "_", "."}:
            result.append(char)
        else:
            result.append("-")
    cleaned = "".join(result).strip("-._")
    return cleaned or "artifact"


def _truncate(value: Any, limit: int = MAX_TEXT_LEN) -> str:
    text = _clean(value)
    lim = max(80, int(limit or MAX_TEXT_LEN))
    if len(text) > lim:
        return text[:lim] + "…"
    return text


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, str):
            return _truncate(value, 2000)
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in list(value)]
    return _truncate(repr(value), 2000)


def _derive_run_id(base_dir: Path, tenant_id: str, scenario: str) -> str:
    parts = list(Path(base_dir).parts)
    lowered = [part.lower() for part in parts]
    try:
        runs_idx = lowered.index("runs")
    except ValueError:
        runs_idx = -1
    run_bits: List[str] = []
    if runs_idx >= 0:
        run_bits.extend(parts[runs_idx + 1 : runs_idx + 3])
    else:
        run_bits.extend(parts[-3:])
    if tenant_id:
        run_bits.append(tenant_id)
    if scenario:
        run_bits.append(scenario)
    safe_bits = [_safe_name(item) for item in run_bits if _clean(item)]
    return "__".join(safe_bits) or _safe_name(uuid.uuid4().hex)


def _console_severity(console_type: str) -> str:
    value = _clean(console_type).lower()
    if value in {"error", "assert"}:
        return "error"
    if value in {"warning", "warn"}:
        return "warning"
    if value in {"debug", "trace"}:
        return "debug"
    return "info"


def _compute_actionability_status(details: Mapping[str, Any]) -> str:
    if not details or not details.get("found"):
        return "missing"
    selected = details.get("selected") if isinstance(details.get("selected"), dict) else details
    if not isinstance(selected, Mapping):
        return "unknown"
    visible = bool(selected.get("visible") or selected.get("is_visible"))
    enabled = bool(selected.get("enabled", True))
    in_viewport = bool(selected.get("in_viewport", False))
    center_hits_self = bool(selected.get("center_hits_self", False))
    pointer_events = _clean(selected.get("pointer_events")).lower()
    display = _clean(selected.get("display")).lower()
    visibility = _clean(selected.get("visibility")).lower()
    if display == "none" or visibility == "hidden":
        return "present_but_hidden"
    if not visible:
        return "present_but_hidden"
    if not enabled:
        return "disabled"
    if pointer_events == "none":
        return "blocked_by_pointer_events"
    if not in_viewport:
        return "out_of_viewport"
    if not center_hits_self:
        return "overlapped"
    return "clickable"


class UIForensicsError(RuntimeError):
    pass


class UIForensicsSession:
    def __init__(
        self,
        base_dir: Path,
        *,
        tenant_id: str = "",
        scenario: str = "",
        file_name: str = "",
        job: str = "",
        session_name: str = "",
    ) -> None:
        self.base_dir = Path(base_dir)
        self.tenant_id = _clean(tenant_id)
        self.scenario = _clean(scenario) or _clean(job) or "ui"
        self.file_name = _clean(file_name)
        self.session_name = _clean(session_name) or self.file_name or self.scenario
        self.run_id = _derive_run_id(self.base_dir, self.tenant_id, self.scenario)
        self.root_dir = self.base_dir / "forensics"
        self.screenshots_dir = self.root_dir / "screenshots"
        self.dom_dir = self.root_dir / "dom"
        self.probes_dir = self.root_dir / "probes"
        self.watch_dir = self.root_dir / "watch"
        self.trace_dir = self.root_dir / "trace"
        self.video_dir = self.root_dir / "video"
        self.events_path = self.root_dir / "events.jsonl"
        self.console_path = self.root_dir / "console.jsonl"
        self.network_path = self.root_dir / "network.jsonl"
        self.summary_path = self.root_dir / "forensic_summary.json"
        self.manifest_path = self.root_dir / "manifest.json"
        self._event_seq = 0
        self._step_index = 0
        self._probe_index = 0
        self._aux_seq = 0
        self._network_seq = 0
        self._trace_started = False
        self._trace_stopped = False
        self._trace_path = ""
        self._video_path = ""
        self._last_state = ""
        self._network_markers: List[str] = []
        self._context: Dict[str, Any] = {
            "phase": "",
            "step_id": "",
            "attempt": None,
            "branch_id": "",
            "correlation_id": "",
        }
        for directory in [
            self.root_dir,
            self.screenshots_dir,
            self.dom_dir,
            self.probes_dir,
            self.watch_dir,
            self.trace_dir,
            self.video_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        self.summary: Dict[str, Any] = {
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "scenario": self.scenario,
            "session_name": self.session_name,
            "file_name": self.file_name,
            "base_dir": str(self.base_dir),
            "root_dir": str(self.root_dir),
            "events_count": 0,
            "captures_count": 0,
            "probes_count": 0,
            "watch_ticks": 0,
            "console_count": 0,
            "network_count": 0,
            "last_event": "",
            "last_state": "",
            "status": "running",
            "branch_id": "",
            "current_phase": "",
            "artifacts": {
                "events": str(self.events_path),
                "console": str(self.console_path),
                "network": str(self.network_path),
                "screenshots_dir": str(self.screenshots_dir),
                "dom_dir": str(self.dom_dir),
                "probes_dir": str(self.probes_dir),
                "watch_dir": str(self.watch_dir),
                "trace_dir": str(self.trace_dir),
                "video_dir": str(self.video_dir),
            },
            "steps": [],
            "probes": [],
            "watchers": [],
        }
        safe_files.write_json(
            self.manifest_path,
            {
                "created_at": _utc_now(),
                "run_id": self.run_id,
                "tenant_id": self.tenant_id,
                "scenario": self.scenario,
                "session_name": self.session_name,
                "paths": self.summary["artifacts"],
            },
            ensure_ascii=False,
            indent=2,
        )
        self._write_summary()
        self.record_event("forensics_started", note="Инициализирован forensic bundle")

    # ----------------------------
    # context / schema helpers
    # ----------------------------
    def set_flow_context(
        self,
        *,
        phase: Optional[str] = None,
        step_id: Optional[str] = None,
        attempt: Optional[int] = None,
        branch_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if phase is not None:
            self._context["phase"] = _clean(phase)
            self.summary["current_phase"] = self._context["phase"]
        if step_id is not None:
            self._context["step_id"] = _clean(step_id)
        if attempt is not None:
            try:
                self._context["attempt"] = int(attempt)
            except Exception:
                self._context["attempt"] = None
        if branch_id is not None:
            self._context["branch_id"] = _clean(branch_id)
            if self._context["branch_id"]:
                self.summary["branch_id"] = self._context["branch_id"]
        if correlation_id is not None:
            self._context["correlation_id"] = _clean(correlation_id)
        self._write_summary()
        return dict(self._context)

    def clear_flow_context(self) -> None:
        self._context.update({"phase": "", "step_id": "", "attempt": None, "branch_id": self.summary.get("branch_id") or "", "correlation_id": ""})
        self.summary["current_phase"] = ""
        self._write_summary()

    def _with_context(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["run_id"] = self.run_id
        row["tenant_id"] = self.tenant_id
        row["scenario"] = self.scenario
        row["session_name"] = self.session_name
        if self._context.get("phase"):
            row["phase"] = self._context["phase"]
        if self._context.get("step_id"):
            row["step_id"] = self._context["step_id"]
        if self._context.get("attempt") is not None:
            row["attempt"] = self._context["attempt"]
        if self._context.get("branch_id"):
            row["branch_id"] = self._context["branch_id"]
        if self._context.get("correlation_id"):
            row["correlation_id"] = self._context["correlation_id"]
        return row

    def _next_event_seq(self) -> int:
        self._event_seq += 1
        return self._event_seq

    def _next_aux_seq(self) -> int:
        self._aux_seq += 1
        return self._aux_seq

    def _write_summary(self) -> None:
        self.summary["updated_at"] = _utc_now()
        safe_files.write_json(self.summary_path, self.summary, ensure_ascii=False, indent=2)

    # ----------------------------
    # main structured events
    # ----------------------------
    def record_event(
        self,
        event: str,
        *,
        state_name: str = "",
        confidence: Optional[float] = None,
        level: str = "info",
        step_id: str = "",
        phase: str = "",
        attempt: Optional[int] = None,
        branch_id: str = "",
        **data: Any,
    ) -> Dict[str, Any]:
        if phase or step_id or attempt is not None or branch_id:
            self.set_flow_context(
                phase=phase if phase else None,
                step_id=step_id if step_id else None,
                attempt=attempt,
                branch_id=branch_id if branch_id else None,
            )
        row: Dict[str, Any] = {
            "ts": _utc_now(),
            "seq": self._next_event_seq(),
            "event": _clean(event) or "event",
            "level": _clean(level) or "info",
        }
        self._with_context(row)
        if state_name:
            row["state_name"] = _clean(state_name)
        if confidence is not None:
            try:
                row["confidence"] = max(0.0, min(1.0, float(confidence)))
            except Exception:
                row["confidence"] = 0.0
        for key, value in data.items():
            row[str(key)] = _json_safe(value)
        safe_files.append_jsonl(self.events_path, row, ensure_ascii=False)
        self.summary["events_count"] = int(self.summary.get("events_count") or 0) + 1
        self.summary["last_event"] = row["event"]
        if state_name:
            self.summary["last_state"] = _clean(state_name)
            self._last_state = _clean(state_name)
        self._write_summary()
        return row

    def event(self, event: str, *, level: str = "info", **data: Any) -> Dict[str, Any]:
        return self.record_event(event, level=level, **data)

    def mark_state(
        self,
        state_name: str,
        *,
        confidence: float = 1.0,
        evidence: Optional[Sequence[Any]] = None,
        step_id: str = "",
        phase: str = "",
        branch_id: str = "",
        **data: Any,
    ) -> Dict[str, Any]:
        return self.record_event(
            "ui_state",
            state_name=state_name,
            confidence=confidence,
            evidence=[_truncate(item, limit=400) for item in (evidence or []) if _clean(item)],
            step_id=step_id,
            phase=phase,
            branch_id=branch_id,
            **data,
        )

    # ----------------------------
    # browser side channels
    # ----------------------------
    def attach_page(self, page, *, network_markers: Sequence[str] = ()) -> None:
        self._network_markers = [str(item).lower() for item in (network_markers or []) if _clean(item)]
        try:
            page.on("console", self._on_console)
        except Exception:
            pass
        try:
            page.on("pageerror", self._on_pageerror)
        except Exception:
            pass
        try:
            page.on("requestfailed", self._on_requestfailed)
        except Exception:
            pass
        try:
            page.on("response", self._on_response)
        except Exception:
            pass
        self.record_event("page_handlers_attached", network_markers=self._network_markers)

    def _append_aux_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        safe_files.append_jsonl(path, payload, ensure_ascii=False)

    def _aux_base(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "ts": _utc_now(),
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "scenario": self.scenario,
            "session_name": self.session_name,
            "seq": self._next_aux_seq(),
        }
        if self._context.get("phase"):
            row["phase"] = self._context["phase"]
        if self._context.get("step_id"):
            row["step_id"] = self._context["step_id"]
        if self._context.get("attempt") is not None:
            row["attempt"] = self._context["attempt"]
        if self._context.get("branch_id"):
            row["branch_id"] = self._context["branch_id"]
        return row

    def _on_console(self, message) -> None:
        try:
            console_type = _clean(getattr(message, "type", lambda: "console")() if callable(getattr(message, "type", None)) else getattr(message, "type", "console"))
            payload = self._aux_base()
            payload.update(
                {
                    "channel": "console",
                    "type": console_type,
                    "severity": _console_severity(console_type),
                    "text": _clean(getattr(message, "text", lambda: "")() if callable(getattr(message, "text", None)) else getattr(message, "text", "")),
                }
            )
            try:
                payload["location"] = sanitize(getattr(message, "location", lambda: {})() if callable(getattr(message, "location", None)) else getattr(message, "location", {}))
            except Exception:
                payload["location"] = {}
            self._append_aux_jsonl(self.console_path, payload)
            self.summary["console_count"] = int(self.summary.get("console_count") or 0) + 1
            self._write_summary()
        except Exception:
            return

    def _on_pageerror(self, error) -> None:
        try:
            payload = self._aux_base()
            payload.update({"channel": "pageerror", "severity": "error", "text": _clean(error)})
            self._append_aux_jsonl(self.console_path, payload)
            self.summary["console_count"] = int(self.summary.get("console_count") or 0) + 1
            self._write_summary()
        except Exception:
            return

    def _on_requestfailed(self, request) -> None:
        try:
            payload = self._aux_base()
            payload.update(
                {
                    "channel": "network",
                    "kind": "requestfailed",
                    "request_seq": self._network_seq + 1,
                    "url": _clean(request.url),
                    "method": _clean(getattr(request, "method", "")),
                    "resource_type": _clean(getattr(request, "resource_type", "")),
                    "failure": sanitize(request.failure) if getattr(request, "failure", None) else {},
                }
            )
            self._network_seq += 1
            self._append_aux_jsonl(self.network_path, payload)
            self.summary["network_count"] = int(self.summary.get("network_count") or 0) + 1
            self._write_summary()
        except Exception:
            return

    def _should_log_response(self, url: str, status: int, resource_type: str) -> bool:
        url_lower = _clean(url).lower()
        if status >= 400:
            return True
        if resource_type in {"xhr", "fetch"} and any(marker in url_lower for marker in self._network_markers):
            return True
        if any(marker in url_lower for marker in self._network_markers):
            return True
        return False

    def _on_response(self, response) -> None:
        try:
            url = _clean(response.url)
            status = int(getattr(response, "status", 0) or 0)
            request = getattr(response, "request", None)
            resource_type = _clean(getattr(request, "resource_type", ""))
            if not self._should_log_response(url, status, resource_type):
                return
            payload = self._aux_base()
            payload.update(
                {
                    "channel": "network",
                    "kind": "response",
                    "request_seq": self._network_seq + 1,
                    "url": url,
                    "status": status,
                    "ok": bool(getattr(response, "ok", False)),
                    "method": _clean(getattr(request, "method", "")),
                    "resource_type": resource_type,
                }
            )
            try:
                headers = dict(getattr(response, "headers", {}) or {})
                payload["content_type"] = _clean(headers.get("content-type") or headers.get("Content-Type") or "")
            except Exception:
                payload["content_type"] = ""
            try:
                text = response.text()
                payload["body_excerpt"] = _truncate(text, 2000)
            except Exception:
                pass
            self._network_seq += 1
            self._append_aux_jsonl(self.network_path, payload)
            self.summary["network_count"] = int(self.summary.get("network_count") or 0) + 1
            self._write_summary()
        except Exception:
            return

    # ----------------------------
    # trace / video
    # ----------------------------
    def start_trace(self, context, *, name: str = "playwright_trace") -> None:
        if self._trace_started:
            return
        try:
            context.tracing.start(screenshots=True, snapshots=True, sources=False)
            self._trace_started = True
            self._trace_stopped = False
            self.record_event("trace_started", trace_name=name)
        except Exception as exc:
            self.record_event("trace_start_failed", level="warning", error=_clean(exc), trace_name=name)

    def stop_trace(self, context, *, name: str = "playwright_trace") -> str:
        if not self._trace_started or self._trace_stopped:
            return self._trace_path
        target = self.trace_dir / f"{_safe_name(name)}.zip"
        try:
            context.tracing.stop(path=str(target))
            self._trace_path = str(target)
            self.record_event("trace_stopped", trace_path=str(target))
        except Exception as exc:
            self.record_event("trace_stop_failed", level="warning", error=_clean(exc), trace_name=name)
        finally:
            self._trace_stopped = True
            self._trace_started = False
        return self._trace_path

    def import_video_from_holder(self, holder: Dict[str, Any], *, name: str = "session") -> str:
        source = _clean((holder or {}).get("video_path"))
        if not source:
            return ""
        source_path = Path(source)
        if not source_path.exists():
            return ""
        target = self.video_dir / f"{_safe_name(name)}{source_path.suffix or '.webm'}"
        try:
            if source_path.parent == self.video_dir:
                if source_path.resolve() != target.resolve():
                    if target.exists():
                        target.unlink()
                    source_path.replace(target)
                else:
                    target = source_path
            elif source_path.resolve() != target.resolve():
                shutil.copy2(source_path, target)
            else:
                target = source_path
            self._video_path = str(target)
            self.record_event("video_ready", video_path=str(target))
            return str(target)
        except Exception as exc:
            self.record_event("video_prepare_failed", level="warning", source=source, error=_clean(exc))
            return ""

    # ----------------------------
    # DOM / screenshots
    # ----------------------------
    def _capture_container_html(self, page, selector: str) -> str:
        selector = _clean(selector)
        if not selector:
            return ""
        try:
            locator = page.locator(selector).first
            locator.wait_for(timeout=1200)
            return str(locator.evaluate("el => el.outerHTML") or "")
        except Exception:
            return ""

    def capture_step(
        self,
        page,
        step_name: str,
        *,
        note: str = "",
        include_page_html: bool = True,
        full_page: bool = True,
        container_selectors: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        self._step_index += 1
        stem = f"{self._step_index:03d}_{_safe_name(step_name)}"
        screenshot_path = self.screenshots_dir / f"{stem}.png"
        page_html_path = self.dom_dir / f"{stem}__page.html"
        meta_path = self.dom_dir / f"{stem}__meta.json"
        containers: Dict[str, str] = {}
        if page is not None:
            try:
                page.screenshot(path=str(screenshot_path), full_page=bool(full_page))
            except Exception:
                pass
        url = ""
        title = ""
        if page is not None:
            try:
                url = _clean(getattr(page, "url", ""))
            except Exception:
                pass
            try:
                title = _clean(page.title())
            except Exception:
                pass
            if include_page_html:
                try:
                    safe_files.write_text(page_html_path, page.content(), encoding="utf-8")
                except Exception:
                    pass
            if container_selectors:
                for alias, selector in container_selectors.items():
                    alias_name = _safe_name(alias)
                    html = self._capture_container_html(page, selector)
                    if not html:
                        continue
                    container_path = self.dom_dir / f"{stem}__{alias_name}.html"
                    safe_files.write_text(container_path, html, encoding="utf-8")
                    containers[alias_name] = str(container_path)
        safe_files.write_json(
            meta_path,
            {
                "captured_at": _utc_now(),
                "run_id": self.run_id,
                "step_name": _clean(step_name),
                "note": _clean(note),
                "url": url,
                "title": title,
                "phase": self._context.get("phase") or "",
                "step_id": self._context.get("step_id") or "",
                "attempt": self._context.get("attempt"),
                "branch_id": self._context.get("branch_id") or "",
                "containers": containers,
            },
            ensure_ascii=False,
            indent=2,
        )
        artifact_row = {
            "step_name": _clean(step_name),
            "note": _clean(note),
            "phase": self._context.get("phase") or "",
            "step_id": self._context.get("step_id") or "",
            "attempt": self._context.get("attempt"),
            "branch_id": self._context.get("branch_id") or "",
            "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
            "page_html_path": str(page_html_path) if page_html_path.exists() else "",
            "meta_path": str(meta_path),
            "containers": containers,
        }
        self.summary["captures_count"] = int(self.summary.get("captures_count") or 0) + 1
        steps = self.summary.get("steps") if isinstance(self.summary.get("steps"), list) else []
        steps.append(artifact_row)
        self.summary["steps"] = steps[-200:]
        self.record_event("step_capture", step_name=step_name, note=note, artifacts=artifact_row)
        return artifact_row

    def save_dom_snapshot(self, page, name: str, *, selector: str, alias: str = "container") -> Optional[Path]:
        html = self._capture_container_html(page, selector)
        if not html:
            self.record_event("dom_snapshot_missing", selector=selector, alias=alias, name=name)
            return None
        target = self.dom_dir / f"{_safe_name(name)}__{_safe_name(alias)}.html"
        safe_files.write_text(target, html, encoding="utf-8")
        self.record_event("dom_snapshot_saved", name=name, selector=selector, alias=alias, path=str(target))
        return target

    # ----------------------------
    # probes / actionability
    # ----------------------------
    def probe_locator(self, name: str, locator, *, note: str = "", selector_hint: str = "") -> Dict[str, Any]:
        probe: Dict[str, Any] = {
            "captured_at": _utc_now(),
            "run_id": self.run_id,
            "name": _clean(name),
            "note": _clean(note),
            "selector_hint": _clean(selector_hint),
            "found": locator is not None,
        }
        if locator is not None:
            script = """(el) => {
              const clean = (v) => String(v || '').replace(/\\s+/g, ' ').trim();
              const rect = el?.getBoundingClientRect ? el.getBoundingClientRect() : null;
              const style = el ? window.getComputedStyle(el) : null;
              const centerX = rect ? rect.left + (rect.width / 2) : 0;
              const centerY = rect ? rect.top + (rect.height / 2) : 0;
              let hit = null;
              try { hit = document.elementFromPoint(centerX, centerY); } catch (e) {}
              const viewport = { width: window.innerWidth, height: window.innerHeight };
              return {
                tag: clean(el?.tagName),
                id: clean(el?.id),
                classes: clean(el?.className),
                role: clean(el?.getAttribute?.('role')),
                type: clean(el?.getAttribute?.('type')),
                name: clean(el?.getAttribute?.('name')),
                aria_disabled: clean(el?.getAttribute?.('aria-disabled')),
                aria_checked: clean(el?.getAttribute?.('aria-checked')),
                disabled_attr: !!el?.disabled,
                checked: !!(typeof el?.checked === 'boolean' ? el.checked : el?.querySelector?.("input[type='checkbox']")?.checked),
                visible: !!(style && style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect && rect.width > 0 && rect.height > 0),
                enabled: !(el?.disabled || clean(el?.getAttribute?.('aria-disabled')).toLowerCase() === 'true'),
                pointer_events: clean(style?.pointerEvents),
                display: clean(style?.display),
                visibility: clean(style?.visibility),
                opacity: clean(style?.opacity),
                text_excerpt: clean(el?.innerText || el?.textContent || '').slice(0, 1200),
                bbox: rect ? {x: rect.x, y: rect.y, width: rect.width, height: rect.height} : null,
                in_viewport: !!(rect && rect.bottom >= 0 && rect.right >= 0 && rect.top <= viewport.height && rect.left <= viewport.width),
                center_hit_tag: clean(hit?.tagName),
                center_hit_id: clean(hit?.id),
                center_hit_classes: clean(hit?.className),
                center_hits_self: !!(hit && (hit === el || el.contains?.(hit) || hit.contains?.(el))),
              };
            }"""
            try:
                details = locator.evaluate(script)
                if isinstance(details, dict):
                    probe.update(sanitize(details))
            except Exception as exc:
                probe["probe_error"] = _clean(exc)
            try:
                probe["is_visible"] = bool(locator.is_visible())
            except Exception:
                pass
            try:
                if hasattr(locator, "is_checked"):
                    probe["is_checked"] = bool(locator.is_checked(timeout=500))
            except Exception:
                pass
            try:
                probe["locator_text"] = _truncate(locator.inner_text(timeout=1200), 1200)
            except Exception:
                pass
        probe["actionability_status"] = _compute_actionability_status(probe)
        probe_path = self.probes_dir / f"{self._probe_index + 1:03d}_{_safe_name(name)}.json"
        safe_files.write_json(probe_path, probe, ensure_ascii=False, indent=2)
        self._probe_index += 1
        self.summary["probes_count"] = int(self.summary.get("probes_count") or 0) + 1
        probes = self.summary.get("probes") if isinstance(self.summary.get("probes"), list) else []
        probes.append({"name": _clean(name), "probe_path": str(probe_path)})
        self.summary["probes"] = probes[-200:]
        self.record_event(
            "element_probe",
            name=name,
            note=note,
            selector_hint=selector_hint,
            found=probe.get("found"),
            probe_path=str(probe_path),
            checked=probe.get("checked") or probe.get("is_checked"),
            enabled=probe.get("enabled"),
            actionability_status=probe.get("actionability_status"),
            center_hits_self=probe.get("center_hits_self"),
        )
        return probe

    def probe_element(
        self,
        page,
        element_name: str,
        *,
        selectors: Optional[Sequence[str]] = None,
        texts: Optional[Sequence[str]] = None,
        scope_selectors: Optional[Sequence[str]] = None,
        roles: Optional[Sequence[str]] = None,
        allow_text_fallback: bool = True,
    ) -> Dict[str, Any]:
        selectors_list = [str(item) for item in (selectors or []) if _clean(item)]
        texts_list = [str(item) for item in (texts or []) if _clean(item)]
        scope_list = [str(item) for item in (scope_selectors or []) if _clean(item)]
        roles_list = [str(item) for item in (roles or []) if _clean(item)]
        script = """
(args) => {
  const selectors = Array.isArray(args?.selectors) ? args.selectors : [];
  const texts = Array.isArray(args?.texts) ? args.texts.map((x) => String(x || '').toLowerCase()) : [];
  const scopeSelectors = Array.isArray(args?.scopeSelectors) ? args.scopeSelectors : [];
  const roles = Array.isArray(args?.roles) ? args.roles : [];
  const allowTextFallback = !!args?.allowTextFallback;
  const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
  const lower = (value) => normalize(value).toLowerCase();
  const isVisible = (el) => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
  };
  const inViewport = (rect) => !!(rect && rect.bottom >= 0 && rect.right >= 0 && rect.top <= window.innerHeight && rect.left <= window.innerWidth);
  const scope = (() => {
    for (const selector of scopeSelectors) {
      try {
        const node = document.querySelector(selector);
        if (node) return node;
      } catch (e) {}
    }
    return document;
  })();
  const candidates = [];
  const interactiveTag = (tag) => ['button', 'input', 'label', 'a', 'select', 'textarea'].includes((tag || '').toLowerCase());
  const computeStatus = (info) => {
    if (!info.visible) return 'present_but_hidden';
    if (!info.enabled) return 'disabled';
    if ((info.pointer_events || '').toLowerCase() === 'none') return 'blocked_by_pointer_events';
    if (!info.in_viewport) return 'out_of_viewport';
    if (!info.center_hits_self) return 'overlapped';
    return 'clickable';
  };
  const addCandidate = (el, reasonType, reasonValue) => {
    if (!el || !el.tagName) return;
    const tagName = normalize(el.tagName).toLowerCase();
    if (['html', 'body', 'head'].includes(tagName)) return;
    if (candidates.some((item) => item.el === el)) return;
    const text = normalize(el.innerText || el.textContent || '');
    const rect = el.getBoundingClientRect ? el.getBoundingClientRect() : null;
    const style = el.nodeType === 1 ? window.getComputedStyle(el) : null;
    const centerX = rect ? rect.left + (rect.width / 2) : 0;
    const centerY = rect ? rect.top + (rect.height / 2) : 0;
    let hit = null;
    try { hit = document.elementFromPoint(centerX, centerY); } catch (e) {}
    const info = {
      tag: normalize(el.tagName),
      id: normalize(el.id),
      classes: normalize(el.className),
      role: normalize(el.getAttribute?.('role')),
      type: normalize(el.getAttribute?.('type')),
      name: normalize(el.getAttribute?.('name')),
      aria_disabled: normalize(el.getAttribute?.('aria-disabled')),
      aria_checked: normalize(el.getAttribute?.('aria-checked')),
      disabled_attr: !!el.disabled,
      checked: !!(typeof el.checked === 'boolean' ? el.checked : el.querySelector?.("input[type='checkbox']")?.checked),
      visible: isVisible(el),
      enabled: !(el.disabled || normalize(el.getAttribute?.('aria-disabled')).toLowerCase() === 'true'),
      pointer_events: normalize(style?.pointerEvents),
      display: normalize(style?.display),
      visibility: normalize(style?.visibility),
      opacity: normalize(style?.opacity),
      text_excerpt: text.slice(0, 1200),
      bbox: rect ? {x: rect.x, y: rect.y, width: rect.width, height: rect.height} : null,
      in_viewport: inViewport(rect),
      center_hit_tag: normalize(hit?.tagName),
      center_hit_id: normalize(hit?.id),
      center_hit_classes: normalize(hit?.className),
      center_hits_self: !!(hit && (hit === el || el.contains?.(hit) || hit.contains?.(el))),
    };
    info.actionability_status = computeStatus(info);
    candidates.push({
      el,
      reason_type: reasonType,
      reason_value: reasonValue,
      info,
    });
  };
  for (const selector of selectors) {
    try {
      const nodes = Array.from(scope.querySelectorAll(selector));
      nodes.slice(0, 8).forEach((node) => addCandidate(node, 'selector', selector));
    } catch (e) {}
  }
  for (const role of roles) {
    const query = role === 'button'
      ? "button, [role='button']"
      : `[role='${role.replace(/'/g, "\\'")}']`;
    try {
      const nodes = Array.from(scope.querySelectorAll(query));
      nodes.slice(0, 8).forEach((node) => addCandidate(node, 'role', role));
    } catch (e) {}
  }
  if (allowTextFallback && texts.length) {
    try {
      const nodes = Array.from(scope.querySelectorAll(args?.textFallbackSelector || "button, [role], input, label, a, [data-testid], [data-name]"));
      for (const node of nodes) {
        const text = lower(node.innerText || node.textContent || '');
        if (!text) continue;
        if (texts.some((sample) => sample && text.includes(sample))) {
          addCandidate(node, 'text', texts.find((sample) => sample && text.includes(sample)) || 'text');
        }
      }
    } catch (e) {}
  }
  const score = (item) => {
    const info = item.info || {};
    let value = 0;
    if (item.reason_type === 'selector') value += 800;
    else if (item.reason_type === 'role') value += 500;
    else if (item.reason_type === 'text') value += 250;
    const tag = (info.tag || '').toLowerCase();
    if (interactiveTag(tag)) value += 120;
    if ((info.role || '').toLowerCase() === 'button') value += 80;
    if ((info.type || '').toLowerCase() === 'checkbox') value += 80;
    if (info.visible) value += 60;
    if (info.enabled) value += 50;
    if (info.in_viewport) value += 30;
    if (info.center_hits_self) value += 30;
    if ((info.actionability_status || '') === 'clickable') value += 70;
    if ((info.tag || '').toLowerCase() === 'div' && !(info.role || '') && !(info.id || '') && !(info.classes || '').includes('checkbox')) value -= 25;
    return value;
  };
  candidates.sort((a, b) => score(b) - score(a));
  return {
    found: candidates.length > 0,
    total_candidates: candidates.length,
    selected_reason_type: candidates[0]?.reason_type || '',
    selected_reason_value: candidates[0]?.reason_value || '',
    selected: candidates[0]?.info || null,
    candidates: candidates.slice(0, 5).map((item) => ({reason_type: item.reason_type, reason_value: item.reason_value, ...item.info})),
  };
}
"""
        try:
            result = page.evaluate(
                script,
                {
                    "selectors": selectors_list,
                    "texts": [item.lower() for item in texts_list],
                    "scopeSelectors": scope_list,
                    "roles": roles_list,
                    "allowTextFallback": bool(allow_text_fallback),
                    "textFallbackSelector": INTERACTIVE_FALLBACK_SELECTOR,
                },
            )
        except Exception as exc:
            result = {"found": False, "error": _clean(exc), "selected": None, "candidates": []}
        if isinstance(result, dict):
            selected = result.get("selected") if isinstance(result.get("selected"), dict) else None
            if isinstance(selected, dict):
                selected["actionability_status"] = _compute_actionability_status({"found": result.get("found"), **selected})
            for candidate in result.get("candidates") or []:
                if isinstance(candidate, dict):
                    candidate["actionability_status"] = _compute_actionability_status({"found": True, **candidate})
            result["actionability_status"] = _compute_actionability_status({"found": result.get("found"), **(selected or {})})
        else:
            result = {"found": False, "selected": None, "candidates": [], "actionability_status": "missing"}
        probe_path = self.probes_dir / f"{self._probe_index + 1:03d}_{_safe_name(element_name)}.json"
        safe_files.write_json(probe_path, result, ensure_ascii=False, indent=2)
        self._probe_index += 1
        self.summary["probes_count"] = int(self.summary.get("probes_count") or 0) + 1
        probes = self.summary.get("probes") if isinstance(self.summary.get("probes"), list) else []
        probes.append({"name": _clean(element_name), "probe_path": str(probe_path)})
        self.summary["probes"] = probes[-200:]
        self.record_event(
            "element_probe",
            name=element_name,
            selectors=selectors_list,
            texts=texts_list,
            roles=roles_list,
            scope_selectors=scope_list,
            probe_path=str(probe_path),
            actionability_status=(result or {}).get("actionability_status"),
            selected_reason_type=(result or {}).get("selected_reason_type"),
            selected_reason_value=(result or {}).get("selected_reason_value"),
        )
        return result if isinstance(result, dict) else {"found": False, "actionability_status": "missing"}

    def probe_from_selectors(self, page, name: str, selectors: Iterable[str], *, note: str = "") -> Dict[str, Any]:
        selected = ""
        locator = None
        for selector in selectors:
            sel = _clean(selector)
            if not sel:
                continue
            try:
                candidate = page.locator(sel)
                count = min(candidate.count(), 4)
            except Exception:
                continue
            for idx in range(count):
                try:
                    item = candidate.nth(idx)
                    selected = sel
                    locator = item
                    try:
                        if item.is_visible():
                            return self.probe_locator(name, item, note=note, selector_hint=selected)
                    except Exception:
                        return self.probe_locator(name, item, note=note, selector_hint=selected)
                except Exception:
                    continue
        return self.probe_locator(name, locator, note=note or "locator_not_found", selector_hint=selected)

    def capture_locator_html(self, locator, stem: str, *, note: str = "") -> str:
        target = self.dom_dir / f"{_safe_name(stem)}.html"
        meta_path = self.dom_dir / f"{_safe_name(stem)}.json"
        html = ""
        text = ""
        try:
            html = locator.evaluate("(el) => el ? el.outerHTML : ''") or ""
        except Exception:
            html = ""
        if html:
            safe_files.write_text(target, html, encoding="utf-8")
        try:
            text = _clean(locator.inner_text(timeout=1200))
        except Exception:
            text = ""
        safe_files.write_json(meta_path, {"captured_at": _utc_now(), "run_id": self.run_id, "note": _clean(note), "text_excerpt": text[:1000]}, ensure_ascii=False, indent=2)
        self.record_event("locator_capture", stem=stem, note=note, html_path=str(target) if target.exists() else "", meta_path=str(meta_path))
        return str(target) if target.exists() else ""

    # ----------------------------
    # post-submit watcher
    # ----------------------------
    def watch_post_submit(
        self,
        page,
        watch_name: str,
        snapshot_getter: Callable[[], Dict[str, Any]],
        *,
        duration_seconds: float = 24.0,
        interval_seconds: float = 2.0,
        capture_on_change: bool = True,
        container_selectors: Optional[Mapping[str, str]] = None,
        timing_points_seconds: Sequence[float] = (0.0, 2.0, 5.0, 10.0),
        phase: str = "",
        step_id: str = "",
    ) -> List[Dict[str, Any]]:
        old_context = dict(self._context)
        if phase or step_id:
            self.set_flow_context(phase=phase if phase else None, step_id=step_id if step_id else None)
        timeline: List[Dict[str, Any]] = []
        last_state = ""
        deadline = time.time() + max(2.0, float(duration_seconds or 24.0))
        tick = 0
        captured_points: set[int] = set()
        timing_points_ms = [int(max(0.0, float(item)) * 1000.0) for item in timing_points_seconds]
        while True:
            tick += 1
            elapsed_ms = int((time.time() - (deadline - max(2.0, float(duration_seconds or 24.0)))) * 1000)
            try:
                snapshot = snapshot_getter() or {}
            except Exception as exc:
                snapshot = {"state_name": "watcher_error", "error": _clean(exc), "terminal": False}
            if not isinstance(snapshot, dict):
                snapshot = {"value": _json_safe(snapshot), "state_name": "unknown", "terminal": False}
            snapshot = sanitize(_json_safe(snapshot))
            state_name = _clean(snapshot.get("state_name") or "unknown")
            state_confidence = snapshot.get("confidence")
            branch_id = _clean(snapshot.get("branch_id") or "")
            if branch_id:
                self.set_flow_context(branch_id=branch_id)
            row = {
                "tick": tick,
                "elapsed_ms": elapsed_ms,
                **snapshot,
            }
            timeline.append(row)
            self.summary["watch_ticks"] = int(self.summary.get("watch_ticks") or 0) + 1
            self.record_event(
                "watch_tick",
                state_name=state_name,
                confidence=float(state_confidence) if isinstance(state_confidence, (int, float)) else None,
                watch_name=watch_name,
                tick=tick,
                elapsed_ms=elapsed_ms,
                snapshot=row,
            )
            changed = bool(state_name and state_name != last_state)
            should_capture_timing = False
            for point in timing_points_ms:
                if elapsed_ms >= point and point not in captured_points:
                    captured_points.add(point)
                    should_capture_timing = True
            if page is not None and ((changed and capture_on_change) or should_capture_timing or tick == 1):
                suffix = f"t{max(0, int(round(elapsed_ms / 1000.0)))}"
                self.capture_step(
                    page,
                    f"{watch_name}_{state_name}_{suffix}",
                    note=f"watch tick {tick}; elapsed_ms={elapsed_ms}",
                    include_page_html=True,
                    full_page=True,
                    container_selectors=container_selectors,
                )
            last_state = state_name or last_state
            if bool(snapshot.get("terminal")):
                break
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            wait_seconds = min(max(0.2, float(interval_seconds or 2.0)), remaining)
            try:
                if page is not None:
                    page.wait_for_timeout(int(wait_seconds * 1000))
                else:
                    time.sleep(wait_seconds)
            except Exception:
                time.sleep(wait_seconds)
        timeline_path = self.watch_dir / f"{_safe_name(watch_name)}.json"
        safe_files.write_json(timeline_path, timeline, ensure_ascii=False, indent=2)
        watchers = self.summary.get("watchers") if isinstance(self.summary.get("watchers"), list) else []
        watchers.append({"watch_name": _clean(watch_name), "timeline_path": str(timeline_path), "ticks": len(timeline)})
        self.summary["watchers"] = watchers[-50:]
        self.record_event("watch_completed", watch_name=watch_name, ticks=len(timeline), timeline_path=str(timeline_path))
        self._context = old_context
        self.summary["current_phase"] = self._context.get("phase") or ""
        self._write_summary()
        return timeline

    def write_summary(self, payload: Dict[str, Any]) -> str:
        self.summary["extra"] = sanitize(payload)
        self._write_summary()
        return str(self.summary_path)

    def finalize(self, status: str, **data: Any) -> None:
        self.summary["status"] = _clean(status) or "completed"
        self.summary["trace_path"] = self._trace_path
        self.summary["video_path"] = self._video_path
        self.summary["final"] = sanitize(data)
        self.record_event("forensics_finished", status=self.summary["status"], **data)
        self._write_summary()


class UIForensics(UIForensicsSession):
    pass


# Backward-safe alias for an earlier typo.
UIForeignicsError = UIForensicsError
