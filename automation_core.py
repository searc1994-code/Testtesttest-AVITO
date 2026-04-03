from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import common
import config
import safe_files
import tenant_manager

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

DEFAULT_TIMEZONE = str(getattr(config, "AUTOMATION_DEFAULT_TIMEZONE", "Europe/Moscow") or "Europe/Moscow")
DEFAULT_PROMO_TIME = str(getattr(config, "AUTOMATION_DEFAULT_PROMO_TIME", "23:00") or "23:00")
DEFAULT_PRICE_TIME = str(getattr(config, "AUTOMATION_DEFAULT_PRICE_TIME", "01:00") or "01:00")
DEFAULT_MASTER_FILENAME = str(getattr(config, "AUTOMATION_DEFAULT_MASTER_FILENAME", "master_prices.xlsm") or "master_prices.xlsm")
DEFAULT_TEMPLATE_PATTERN = str(getattr(config, "AUTOMATION_DEFAULT_TEMPLATE_PATTERN", "price_template__{tenant_id}.xlsx") or "price_template__{tenant_id}.xlsx")
DEFAULT_OUTPUT_PATTERN = str(getattr(config, "AUTOMATION_DEFAULT_OUTPUT_PATTERN", "{date}__{tenant_id}__prices{ext}") or "{date}__{tenant_id}__prices{ext}")

AUTOMATION_ROOT = Path(common.SHARED_DIR) / "automation"
SETTINGS_FILE = AUTOMATION_ROOT / "settings.json"
STATE_FILE = AUTOMATION_ROOT / "state.json"
REPORTS_DIR = AUTOMATION_ROOT / "reports"
RUNS_DIR = AUTOMATION_ROOT / "runs"
PRICE_WORKSPACE_DIR = AUTOMATION_ROOT / "price_workspace"
PRICE_OUTPUT_DIR = AUTOMATION_ROOT / "price_output"
PROMO_SNAPSHOTS_DIR = AUTOMATION_ROOT / "promo_snapshots"


def ensure_dirs() -> Dict[str, Path]:
    for directory in [AUTOMATION_ROOT, REPORTS_DIR, RUNS_DIR, PRICE_WORKSPACE_DIR, PRICE_OUTPUT_DIR, PROMO_SNAPSHOTS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
    if not SETTINGS_FILE.exists():
        safe_files.write_json(SETTINGS_FILE, normalize_settings(default_settings()), ensure_ascii=False, indent=2)
    if not STATE_FILE.exists():
        safe_files.write_json(STATE_FILE, default_state(), ensure_ascii=False, indent=2)
    return {
        "root": AUTOMATION_ROOT,
        "settings_file": SETTINGS_FILE,
        "state_file": STATE_FILE,
        "reports_dir": REPORTS_DIR,
        "runs_dir": RUNS_DIR,
        "price_workspace_dir": PRICE_WORKSPACE_DIR,
        "price_output_dir": PRICE_OUTPUT_DIR,
        "promo_snapshots_dir": PROMO_SNAPSHOTS_DIR,
    }


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _now_iso() -> str:
    return common.utc_now_iso()


def timezone_obj(name: str = ""):
    tz_name = _clean(name) or DEFAULT_TIMEZONE
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass
    return timezone(timedelta(hours=3))


def now_local(settings: Optional[Dict[str, Any]] = None) -> datetime:
    tz = timezone_obj((settings or {}).get("timezone") if isinstance(settings, dict) else "")
    return datetime.now(tz)


def _time_or_default(value: Any, fallback: str) -> str:
    text = _clean(value) or fallback
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return f"{hour:02d}:{minute:02d}"
    except Exception:
        return fallback


def default_tenant_entry(tenant_id: str, position: int = 0, template_pattern: str = DEFAULT_TEMPLATE_PATTERN) -> Dict[str, Any]:
    safe_id = _clean(tenant_id)
    return {
        "enabled": True,
        "promo_enabled": True,
        "price_enabled": True,
        "template_filename": template_pattern.format(tenant_id=safe_id),
        "notes": "",
        "sort_order": position + 1,
    }


def default_settings() -> Dict[str, Any]:
    tenants = tenant_manager.load_tenants()
    template_pattern = DEFAULT_TEMPLATE_PATTERN
    return {
        "version": 1,
        "timezone": DEFAULT_TIMEZONE,
        "schedule_enabled": True,
        "archive_runs": True,
        "updated_at": _now_iso(),
        "promo": {
            "enabled": False,
            "mode": "manual",
            "schedule_time": DEFAULT_PROMO_TIME,
            "window_days": 7,
            "future_only": True,
            "all_promotions": True,
            "include_auto": True,
            "include_regular": True,
            "max_retries": 3,
            "strategy": "api_then_browser",
            "verify_after_action": True,
        },
        "prices": {
            "enabled": False,
            "mode": "manual",
            "schedule_time": DEFAULT_PRICE_TIME,
            "master_filename": DEFAULT_MASTER_FILENAME,
            "master_sheet_name": "",
            "template_sheet_name": "",
            "template_pattern": template_pattern,
            "output_pattern": DEFAULT_OUTPUT_PATTERN,
            "warn_change_pct": 30.0,
            "row_start": 2,
            "master_article_col": "D",
            "master_price_col": "S",
            "master_discount_col": "U",
            "template_article_col": "C",
            "template_price_col": "J",
            "template_discount_col": "L",
            "recalc_mode": "auto",
            "verify_via_api": True,
            "upload_via_browser": False,
        },
        "tenants": {
            _clean(tenant.get("id")): default_tenant_entry(_clean(tenant.get("id")), position=index, template_pattern=template_pattern)
            for index, tenant in enumerate(tenants)
            if _clean(tenant.get("id"))
        },
    }


def default_state() -> Dict[str, Any]:
    return {
        "scheduler": {
            "started_at": "",
            "last_tick_at": "",
            "status": "idle",
            "last_error": "",
        },
        "plans": {},
    }


def _deep_merge(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def normalize_settings(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = default_settings()
    data = raw if isinstance(raw, dict) else {}
    merged = _deep_merge(base, data)
    merged["timezone"] = _clean(merged.get("timezone")) or DEFAULT_TIMEZONE
    merged["schedule_enabled"] = bool(merged.get("schedule_enabled", True))
    merged["archive_runs"] = bool(merged.get("archive_runs", True))
    merged["updated_at"] = _now_iso()

    promo = merged.get("promo") if isinstance(merged.get("promo"), dict) else {}
    promo["enabled"] = bool(promo.get("enabled", False))
    promo["mode"] = _clean(promo.get("mode")) or "manual"
    promo["schedule_time"] = _time_or_default(promo.get("schedule_time"), DEFAULT_PROMO_TIME)
    promo["window_days"] = max(1, min(30, int(promo.get("window_days") or 7)))
    promo["future_only"] = bool(promo.get("future_only", True))
    promo["all_promotions"] = bool(promo.get("all_promotions", True))
    promo["include_auto"] = bool(promo.get("include_auto", True))
    promo["include_regular"] = bool(promo.get("include_regular", True))
    promo["max_retries"] = max(1, min(10, int(promo.get("max_retries") or 3)))
    promo["strategy"] = _clean(promo.get("strategy")) or "api_then_browser"
    promo["verify_after_action"] = bool(promo.get("verify_after_action", True))
    merged["promo"] = promo

    prices = merged.get("prices") if isinstance(merged.get("prices"), dict) else {}
    prices["enabled"] = bool(prices.get("enabled", False))
    prices["mode"] = _clean(prices.get("mode")) or "manual"
    prices["schedule_time"] = _time_or_default(prices.get("schedule_time"), DEFAULT_PRICE_TIME)
    prices["master_filename"] = _clean(prices.get("master_filename")) or DEFAULT_MASTER_FILENAME
    prices["master_sheet_name"] = _clean(prices.get("master_sheet_name"))
    prices["template_sheet_name"] = _clean(prices.get("template_sheet_name"))
    prices["template_pattern"] = _clean(prices.get("template_pattern")) or DEFAULT_TEMPLATE_PATTERN
    prices["output_pattern"] = _clean(prices.get("output_pattern")) or DEFAULT_OUTPUT_PATTERN
    prices["warn_change_pct"] = max(0.0, min(500.0, float(prices.get("warn_change_pct") or 30.0)))
    prices["row_start"] = max(1, min(1000, int(prices.get("row_start") or 2)))
    for key, fallback in [
        ("master_article_col", "D"),
        ("master_price_col", "S"),
        ("master_discount_col", "U"),
        ("template_article_col", "C"),
        ("template_price_col", "J"),
        ("template_discount_col", "L"),
    ]:
        prices[key] = (_clean(prices.get(key)) or fallback).upper()
    prices["recalc_mode"] = _clean(prices.get("recalc_mode")) or "auto"
    prices["verify_via_api"] = bool(prices.get("verify_via_api", True))
    prices["upload_via_browser"] = bool(prices.get("upload_via_browser", False))
    merged["prices"] = prices

    tenants_section = merged.get("tenants") if isinstance(merged.get("tenants"), dict) else {}
    normalized_tenants: Dict[str, Any] = {}
    for index, tenant in enumerate(tenant_manager.load_tenants()):
        tenant_id = _clean(tenant.get("id"))
        if not tenant_id:
            continue
        tenant_defaults = default_tenant_entry(tenant_id, position=index, template_pattern=prices["template_pattern"])
        current = tenants_section.get(tenant_id) if isinstance(tenants_section.get(tenant_id), dict) else {}
        tenant_row = _deep_merge(tenant_defaults, current)
        tenant_row["enabled"] = bool(tenant_row.get("enabled", True))
        tenant_row["promo_enabled"] = bool(tenant_row.get("promo_enabled", True))
        tenant_row["price_enabled"] = bool(tenant_row.get("price_enabled", True))
        tenant_row["template_filename"] = _clean(tenant_row.get("template_filename")) or tenant_defaults["template_filename"]
        tenant_row["notes"] = _clean(tenant_row.get("notes"))
        tenant_row["sort_order"] = max(1, int(tenant_row.get("sort_order") or index + 1))
        normalized_tenants[tenant_id] = tenant_row
    merged["tenants"] = normalized_tenants
    return merged


def load_settings() -> Dict[str, Any]:
    ensure_dirs()
    return normalize_settings(safe_files.read_json(SETTINGS_FILE, {}))


def save_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    ensure_dirs()
    normalized = normalize_settings(data)
    safe_files.write_json(SETTINGS_FILE, normalized, ensure_ascii=False, indent=2)
    return normalized


def load_state() -> Dict[str, Any]:
    ensure_dirs()
    data = safe_files.read_json(STATE_FILE, default_state())
    return data if isinstance(data, dict) else default_state()


def save_state(data: Dict[str, Any]) -> Dict[str, Any]:
    ensure_dirs()
    safe_files.write_json(STATE_FILE, data, ensure_ascii=False, indent=2)
    return data


def update_scheduler_state(**updates: Any) -> Dict[str, Any]:
    state = load_state()
    scheduler = state.get("scheduler") if isinstance(state.get("scheduler"), dict) else {}
    for key, value in updates.items():
        scheduler[str(key)] = value
    state["scheduler"] = scheduler
    return save_state(state)


def _plan_bucket(state: Dict[str, Any], plan_key: str) -> Dict[str, Any]:
    plans = state.get("plans") if isinstance(state.get("plans"), dict) else {}
    bucket = plans.get(plan_key) if isinstance(plans.get(plan_key), dict) else {}
    plans[plan_key] = bucket
    state["plans"] = plans
    return bucket


def is_plan_due(plan_key: str, schedule_time: str, *, enabled: bool, mode: str, now: Optional[datetime] = None, state: Optional[Dict[str, Any]] = None, schedule_enabled: bool = True) -> bool:
    if not schedule_enabled or not enabled:
        return False
    if _clean(mode) != "auto":
        return False
    current = now or now_local(load_settings())
    current_time = current.strftime("%H:%M")
    if current_time < _time_or_default(schedule_time, "00:00"):
        return False
    state_payload = state if isinstance(state, dict) else load_state()
    bucket = _plan_bucket(state_payload, plan_key)
    last_run_date = _clean(bucket.get("last_run_date"))
    if last_run_date == current.strftime("%Y-%m-%d"):
        return False
    return True


def mark_plan_started(plan_key: str, *, now: Optional[datetime] = None, job_id: str = "", source: str = "") -> Dict[str, Any]:
    state = load_state()
    current = now or now_local(load_settings())
    bucket = _plan_bucket(state, plan_key)
    bucket["last_run_date"] = current.strftime("%Y-%m-%d")
    bucket["last_started_at"] = current.astimezone(timezone.utc).isoformat()
    bucket["last_job_id"] = _clean(job_id)
    bucket["last_source"] = _clean(source) or "scheduler"
    save_state(state)
    return bucket


def mark_plan_finished(plan_key: str, *, status: str, report_path: str = "", now: Optional[datetime] = None) -> Dict[str, Any]:
    state = load_state()
    current = now or now_local(load_settings())
    bucket = _plan_bucket(state, plan_key)
    bucket["last_finished_at"] = current.astimezone(timezone.utc).isoformat()
    bucket["last_status"] = _clean(status) or "completed"
    bucket["last_report_path"] = _clean(report_path)
    save_state(state)
    return bucket


def expected_template_filename(tenant_id: str, settings: Optional[Dict[str, Any]] = None) -> str:
    cfg = settings or load_settings()
    tenant_cfg = (cfg.get("tenants") or {}).get(_clean(tenant_id)) or {}
    filename = _clean(tenant_cfg.get("template_filename"))
    if filename:
        return filename
    pattern = _clean((cfg.get("prices") or {}).get("template_pattern")) or DEFAULT_TEMPLATE_PATTERN
    return pattern.format(tenant_id=_clean(tenant_id))


def resolve_master_path(settings: Optional[Dict[str, Any]] = None) -> Path:
    cfg = settings or load_settings()
    filename = _clean((cfg.get("prices") or {}).get("master_filename")) or DEFAULT_MASTER_FILENAME
    return PRICE_WORKSPACE_DIR / filename


def resolve_template_path(tenant_id: str, settings: Optional[Dict[str, Any]] = None) -> Path:
    return PRICE_WORKSPACE_DIR / expected_template_filename(tenant_id, settings=settings)


def build_workspace_manifest(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = settings or load_settings()
    ensure_dirs()
    files: List[Dict[str, Any]] = []
    for path in sorted(PRICE_WORKSPACE_DIR.glob("*")):
        if path.is_file():
            stat = path.stat()
            files.append({
                "name": path.name,
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            })
    tenants_rows: List[Dict[str, Any]] = []
    tenants_sorted = list_enabled_tenant_ids(cfg) + [
        tenant_id for tenant_id in [_clean(item.get("id")) for item in tenant_manager.load_tenants() if _clean(item.get("id"))]
        if tenant_id not in list_enabled_tenant_ids(cfg)
    ]
    tenant_map = {_clean(item.get("id")): item for item in tenant_manager.load_tenants() if _clean(item.get("id"))}
    for tenant_id in tenants_sorted:
        tenant = tenant_map.get(tenant_id) or {}
        tenant_id = _clean(tenant.get("id"))
        if not tenant_id:
            continue
        template_path = resolve_template_path(tenant_id, cfg)
        tenant_cfg = (cfg.get("tenants") or {}).get(tenant_id) or {}
        tenants_rows.append({
            "tenant_id": tenant_id,
            "tenant_name": _clean(tenant.get("name")) or tenant_id,
            "template_filename": template_path.name,
            "template_exists": template_path.exists(),
            "promo_enabled": bool(tenant_cfg.get("promo_enabled", True)),
            "price_enabled": bool(tenant_cfg.get("price_enabled", True)),
            "notes": _clean(tenant_cfg.get("notes")),
        })
    master_path = resolve_master_path(cfg)
    return {
        "workspace_dir": str(PRICE_WORKSPACE_DIR),
        "master_filename": master_path.name,
        "master_exists": master_path.exists(),
        "files": files,
        "tenants": tenants_rows,
        "rename_hint": "Положите в папку один master-файл и по одному шаблону на кабинет. Рекомендуемые имена указаны в таблице ниже.",
    }


def next_runs(settings: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    cfg = settings or load_settings()
    current = now_local(cfg)
    result: Dict[str, str] = {}
    for key, section_key in [("promo", "promo"), ("prices", "prices")]:
        section = cfg.get(section_key) or {}
        schedule_time = _time_or_default(section.get("schedule_time"), DEFAULT_PROMO_TIME if key == "promo" else DEFAULT_PRICE_TIME)
        hour, minute = [int(part) for part in schedule_time.split(":", 1)]
        planned = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if planned <= current:
            planned = planned + timedelta(days=1)
        result[key] = planned.isoformat(timespec="minutes")
    return result


def _safe_name(value: Any) -> str:
    text = _clean(value)
    if not text:
        return "run"
    out = []
    for char in text:
        if char.isalnum() or char in {"-", "_", "."}:
            out.append(char)
        else:
            out.append("-")
    name = "".join(out).strip("-_.")
    return name or "run"


def create_run_dir(kind: str, now: Optional[datetime] = None) -> Path:
    ensure_dirs()
    current = now or now_local(load_settings())
    run_dir = RUNS_DIR / current.strftime("%Y%m%d") / f"{current.strftime('%H%M%S')}_{_safe_name(kind)}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_report(kind: str, *, status: str, title: str, payload: Dict[str, Any], tenant_id: str = "", run_dir: Optional[Path] = None) -> Path:
    ensure_dirs()
    current = now_local(load_settings())
    target_dir = run_dir if run_dir is not None else REPORTS_DIR / current.strftime("%Y%m%d")
    target_dir.mkdir(parents=True, exist_ok=True)
    report_path = target_dir / f"{current.strftime('%H%M%S')}_{_safe_name(kind)}.json"
    body = {
        "kind": _clean(kind),
        "status": _clean(status) or "completed",
        "title": _clean(title),
        "tenant_id": _clean(tenant_id),
        "created_at": _now_iso(),
        "payload": payload,
    }
    safe_files.write_json(report_path, body, ensure_ascii=False, indent=2)
    return report_path


def list_recent_reports(limit: int = 20) -> List[Dict[str, Any]]:
    ensure_dirs()
    rows: List[Dict[str, Any]] = []
    for file_path in REPORTS_DIR.rglob("*.json"):
        try:
            payload = safe_files.read_json(file_path, {})
            if isinstance(payload, dict):
                body = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                rows.append({
                    "path": str(file_path),
                    "name": file_path.name,
                    "created_at": _clean(payload.get("created_at")) or datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(timespec="seconds"),
                    "kind": _clean(payload.get("kind")),
                    "status": _clean(payload.get("status")),
                    "title": _clean(payload.get("title")),
                    "tenant_id": _clean(payload.get("tenant_id")),
                    "run_dir": _clean(body.get("run_dir")),
                    "archive_path": _clean(body.get("archive_path")),
                    "report_path": str(file_path),
                    "prepared": int(body.get("prepared") or 0),
                    "uploaded": int(body.get("uploaded") or 0),
                    "failed": int(body.get("failed") or 0),
                    "actionable_total": int(body.get("actionable_total") or 0),
                    "new_total": int(body.get("new_total") or 0),
                    "mismatched_total": int(body.get("mismatched_total") or 0),
                    "quarantine_total": int(body.get("quarantine_total") or 0),
                    "browser_success_total": int(body.get("browser_success_total") or 0),
                    "browser_failed_total": int(body.get("browser_failed_total") or 0),
                    "remaining_actionable_total": int(body.get("remaining_actionable_total") or 0),
                })
        except Exception:
            continue
    rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
    return rows[: max(1, int(limit or 20))]


def archive_file(source: Path, destination_dir: Path) -> Optional[Path]:
    source = Path(source)
    if not source.exists() or not source.is_file():
        return None
    destination_dir.mkdir(parents=True, exist_ok=True)
    target = destination_dir / source.name
    shutil.copy2(source, target)
    return target


def list_enabled_tenant_ids(settings: Optional[Dict[str, Any]] = None, feature: str = "") -> List[str]:
    cfg = settings or load_settings()
    rows: List[tuple[int, str]] = []
    for tenant in tenant_manager.load_tenants():
        tenant_id = _clean(tenant.get("id"))
        if not tenant_id:
            continue
        tenant_cfg = (cfg.get("tenants") or {}).get(tenant_id) or {}
        if not bool(tenant_cfg.get("enabled", True)):
            continue
        if feature == "promo" and not bool(tenant_cfg.get("promo_enabled", True)):
            continue
        if feature == "prices" and not bool(tenant_cfg.get("price_enabled", True)):
            continue
        rows.append((int(tenant_cfg.get("sort_order") or 0), tenant_id))
    rows.sort(key=lambda item: (item[0], item[1]))
    return [tenant_id for _, tenant_id in rows]
