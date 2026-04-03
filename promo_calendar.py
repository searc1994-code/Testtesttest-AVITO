from __future__ import annotations

import time
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

import automation_core
import background_jobs
import common
import safe_files
import tenant_manager
from safe_logs import log_event

PROMOTIONS_LIST_URL = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions"
PROMOTIONS_DETAILS_URL = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions/details"
PROMO_REQUEST_DELAY_SECONDS = 0.65
PROMO_BATCH_SIZE = 100


class PromotionScanError(RuntimeError):
    pass


def _clean(value: Any) -> str:
    return common.clean_text(value)


def _headers(api_key: str) -> Dict[str, str]:
    token = _clean(api_key)
    if not token:
        raise PromotionScanError("Для кабинета не указан WB API key.")
    return {"Authorization": token}


def _request_json(url: str, headers: Dict[str, str], params: Any, *, retries: int = 3) -> Dict[str, Any]:
    last_error = ""
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=max(10, int(getattr(common, "REQUEST_TIMEOUT", 20) or 20)))
            if response.status_code == 429:
                time.sleep(PROMO_REQUEST_DELAY_SECONDS * attempt)
                last_error = f"HTTP 429: {response.text[:300]}"
                continue
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("error"):
                raise PromotionScanError(_clean(payload.get("errorText") or "WB API вернул ошибку"))
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            last_error = _clean(exc)
            if attempt >= retries:
                break
            time.sleep(PROMO_REQUEST_DELAY_SECONDS * attempt)
    raise PromotionScanError(last_error or "Не удалось выполнить запрос к Promotions Calendar")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(text: Any) -> Optional[datetime]:
    raw = _clean(text)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _list_promotions(api_key: str, start_dt: datetime, end_dt: datetime, *, all_promo: bool = True, retries: int = 3) -> List[Dict[str, Any]]:
    headers = _headers(api_key)
    rows: List[Dict[str, Any]] = []
    offset = 0
    limit = 1000
    while True:
        payload = _request_json(
            PROMOTIONS_LIST_URL,
            headers,
            {
                "startDateTime": _iso_z(start_dt),
                "endDateTime": _iso_z(end_dt),
                "allPromo": bool(all_promo),
                "limit": limit,
                "offset": offset,
            },
            retries=retries,
        )
        page = (((payload or {}).get("data") or {}).get("promotions") or [])
        if not isinstance(page, list):
            page = []
        rows.extend(item for item in page if isinstance(item, dict))
        if len(page) < limit:
            break
        offset += limit
        time.sleep(PROMO_REQUEST_DELAY_SECONDS)
    return rows


def _details_promotions(api_key: str, promotion_ids: List[int], *, retries: int = 3) -> List[Dict[str, Any]]:
    if not promotion_ids:
        return []
    headers = _headers(api_key)
    rows: List[Dict[str, Any]] = []
    for start in range(0, len(promotion_ids), PROMO_BATCH_SIZE):
        batch = promotion_ids[start:start + PROMO_BATCH_SIZE]
        params = [("promotionIDs", int(pid)) for pid in batch]
        payload = _request_json(PROMOTIONS_DETAILS_URL, headers, params, retries=retries)
        page = (((payload or {}).get("data") or {}).get("promotions") or [])
        if not isinstance(page, list):
            page = []
        rows.extend(item for item in page if isinstance(item, dict))
        if start + PROMO_BATCH_SIZE < len(promotion_ids):
            time.sleep(PROMO_REQUEST_DELAY_SECONDS)
    return rows


def _text_has_any(text: str, phrases: Iterable[str]) -> bool:
    haystack = _clean(text).lower()
    return any(_clean(item).lower() in haystack for item in phrases if _clean(item))


PROMO_POSITIVE_TEXT_MARKERS = [
    "автоакци",
    "автоматическ",
    "будут участвовать",
    "товары участвуют",
    "ваши товары попали",
    "ваши товары будут участвовать",
    "участие в акции",
    "акция для ваших товаров",
]

PROMO_NEGATIVE_TEXT_MARKERS = [
    "не подключается автоматически",
    "нужно добавить вручную",
    "добавить вручную",
    "подключается вручную",
    "товары нужно добавить вручную",
]


def _actionable_promotion(promo: Dict[str, Any], settings: Dict[str, Any], current_time: datetime) -> tuple[bool, str]:
    promo_settings = settings.get("promo") or {}
    promo_type = _clean(promo.get("type")).lower() or "regular"
    if promo_type == "auto" and not bool(promo_settings.get("include_auto", True)):
        return False, "auto_disabled"
    if promo_type != "auto" and not bool(promo_settings.get("include_regular", True)):
        return False, "regular_disabled"
    start_dt = _parse_dt(promo.get("startDateTime"))
    if bool(promo_settings.get("future_only", True)) and start_dt and start_dt <= current_time:
        return False, "already_started"

    in_action_total = int(promo.get("inPromoActionTotal") or 0)
    in_action_leftovers = int(promo.get("inPromoActionLeftovers") or 0)
    not_in_action_total = int(promo.get("notInPromoActionTotal") or 0)
    participation_percentage = float(promo.get("participationPercentage") or 0)
    exception_products = int(promo.get("exceptionProductsCount") or 0)
    if any(value > 0 for value in [in_action_total, in_action_leftovers, participation_percentage, exception_products]):
        return True, "participation_metrics"

    combined_text = " ".join([
        _clean(promo.get("name")),
        _clean(promo.get("description")),
    ])
    has_negative_text = _text_has_any(combined_text, PROMO_NEGATIVE_TEXT_MARKERS)
    has_positive_text = _text_has_any(combined_text, PROMO_POSITIVE_TEXT_MARKERS)

    if promo_type == "auto":
        if has_negative_text and not has_positive_text:
            return False, "future_auto_manual_only"
        if not_in_action_total > 0:
            return True, "future_auto_not_in_metrics"
        return True, "future_auto"

    if has_positive_text and not has_negative_text:
        return True, "future_regular_text_signal"

    return False, "no_participation_signal"


def _snapshot_path(tenant_id: str, stamp: str) -> Path:
    path = automation_core.PROMO_SNAPSHOTS_DIR / tenant_id
    path.mkdir(parents=True, exist_ok=True)
    return path / f"promo_snapshot_{stamp}.json"


def _previous_snapshot(tenant_id: str) -> Dict[str, Any]:
    directory = automation_core.PROMO_SNAPSHOTS_DIR / tenant_id
    if not directory.exists():
        return {}
    snapshots = sorted(directory.glob("promo_snapshot_*.json"))
    if not snapshots:
        return {}
    return safe_files.read_json(snapshots[-1], {})


def scan_tenant_promotions(tenant_id: str, settings: Dict[str, Any], run_dir: Path) -> Dict[str, Any]:
    tenant = tenant_manager.get_tenant(tenant_id)
    if not tenant:
        raise PromotionScanError(f"Кабинет не найден: {tenant_id}")
    current_local = automation_core.now_local(settings)
    days_ahead = int((settings.get("promo") or {}).get("window_days") or 7)
    start_dt = current_local.astimezone(timezone.utc)
    end_dt = (current_local + timedelta(days=days_ahead)).astimezone(timezone.utc)
    retries = int((settings.get("promo") or {}).get("max_retries") or 3)
    api_key = _clean(tenant.get("wb_api_key"))
    log_event("automation", "promo_scan_tenant_start", tenant_id=tenant_id, start=str(start_dt), end=str(end_dt), days_ahead=days_ahead)

    list_rows = _list_promotions(api_key, start_dt, end_dt, all_promo=bool((settings.get("promo") or {}).get("all_promotions", True)), retries=retries)
    promotion_ids = [int(item.get("id")) for item in list_rows if item.get("id") is not None]
    details_rows = _details_promotions(api_key, promotion_ids, retries=retries)
    details_map = {int(item.get("id")): item for item in details_rows if item.get("id") is not None}

    actionable: List[Dict[str, Any]] = []
    combined_rows: List[Dict[str, Any]] = []
    for row in list_rows:
        try:
            promo_id = int(row.get("id"))
        except Exception:
            continue
        detail = details_map.get(promo_id, row)
        combined = {
            "id": promo_id,
            "name": _clean(detail.get("name") or row.get("name")),
            "type": _clean(detail.get("type") or row.get("type")) or "regular",
            "startDateTime": _clean(detail.get("startDateTime") or row.get("startDateTime")),
            "endDateTime": _clean(detail.get("endDateTime") or row.get("endDateTime")),
            "inPromoActionLeftovers": int(detail.get("inPromoActionLeftovers") or 0),
            "inPromoActionTotal": int(detail.get("inPromoActionTotal") or 0),
            "notInPromoActionLeftovers": int(detail.get("notInPromoActionLeftovers") or 0),
            "notInPromoActionTotal": int(detail.get("notInPromoActionTotal") or 0),
            "participationPercentage": float(detail.get("participationPercentage") or 0),
            "exceptionProductsCount": int(detail.get("exceptionProductsCount") or 0),
            "description": _clean(detail.get("description")),
        }
        combined_rows.append(combined)
        is_actionable, action_classifier = _actionable_promotion(combined, settings, start_dt)
        if is_actionable:
            action_row = dict(combined)
            action_row["action_method"] = "browser_required"
            action_row["action_classifier"] = action_classifier
            action_row["action_reason"] = "Публичный API календаря акций не даёт штатного remove-контура для исключения участия; нужна браузерная ветка или ручная проверка."
            actionable.append(action_row)

    previous = _previous_snapshot(tenant_id)
    prev_ids = {int(item.get("id")) for item in ((previous.get("actionable") or []) if isinstance(previous, dict) else []) if item.get("id") is not None}
    new_ids = [item["id"] for item in actionable if int(item.get("id") or 0) not in prev_ids]

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    snapshot = {
        "tenant_id": tenant_id,
        "tenant_name": _clean(tenant.get("name")) or tenant_id,
        "scanned_at": common.utc_now_iso(),
        "window_days": days_ahead,
        "promotions_total": len(combined_rows),
        "actionable_total": len(actionable),
        "new_actionable_ids": new_ids,
        "all": combined_rows,
        "actionable": actionable,
    }
    snapshot_path = _snapshot_path(tenant_id, stamp)
    safe_files.write_json(snapshot_path, snapshot, ensure_ascii=False, indent=2)
    archive_copy = run_dir / f"promo_snapshot__{tenant_id}.json"
    safe_files.write_json(archive_copy, snapshot, ensure_ascii=False, indent=2)
    log_event("automation", "promo_scan_tenant_done", tenant_id=tenant_id, promotions_total=len(combined_rows), actionable_total=len(actionable), new_total=len(new_ids))
    return {
        "tenant_id": tenant_id,
        "tenant_name": snapshot["tenant_name"],
        "promotions_total": len(combined_rows),
        "actionable_total": len(actionable),
        "new_actionable_ids": new_ids,
        "snapshot_path": str(snapshot_path),
        "actionable": actionable,
    }


def scan_future_promotions(tenant_ids: Optional[Iterable[str]] = None, run_source: str = "manual") -> Dict[str, Any]:
    settings = automation_core.load_settings()
    automation_core.ensure_dirs()
    selected = [
        _clean(tenant_id)
        for tenant_id in (tenant_ids or automation_core.list_enabled_tenant_ids(settings, feature="promo"))
        if _clean(tenant_id)
    ]
    if not selected:
        raise PromotionScanError("Нет кабинетов, включённых для контроля календаря акций.")

    run_dir = automation_core.create_run_dir("promo_scan")
    background_jobs.progress("promo_scan_start", "Начинаю сканирование будущих акций", percent=0, tenants=len(selected), source=run_source)

    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for index, tenant_id in enumerate(selected, start=1):
        percent = int(index / max(1, len(selected)) * 90)
        background_jobs.progress("promo_scan_tenant", f"Проверяю кабинет {tenant_id}", percent=percent, tenant_id=tenant_id, current=index, total=len(selected))
        try:
            rows.append(scan_tenant_promotions(tenant_id, settings, run_dir))
        except Exception as exc:
            error_text = _clean(exc)
            failures.append({"tenant_id": tenant_id, "error": error_text})
            log_event("automation", "promo_scan_tenant_error", tenant_id=tenant_id, level="error", error=error_text)
            background_jobs.progress("promo_scan_error", f"Ошибка сканирования по кабинету {tenant_id}: {error_text}", percent=percent, tenant_id=tenant_id, error=error_text)
            continue

    summary = {
        "run_source": _clean(run_source) or "manual",
        "run_dir": str(run_dir),
        "selected_tenants": selected,
        "rows": rows,
        "failures": failures,
        "actionable_total": sum(int(item.get("actionable_total") or 0) for item in rows),
        "new_total": sum(len(item.get("new_actionable_ids") or []) for item in rows),
    }
    safe_files.write_json(run_dir / "summary.json", summary, ensure_ascii=False, indent=2)
    archive_path = Path(shutil.make_archive(str(run_dir), "zip", root_dir=run_dir))
    summary["archive_path"] = str(archive_path)
    report_path = automation_core.write_report(
        "promo_scan",
        status="completed" if not failures else "partial",
        title="Сканирование будущих акций на ближайшие дни",
        payload=summary,
    )
    summary["report_path"] = str(report_path)
    safe_files.write_json(run_dir / "summary.json", summary, ensure_ascii=False, indent=2)
    background_jobs.progress("promo_scan_done", "Сканирование будущих акций завершено", percent=100, actionable_total=summary["actionable_total"], failures=len(failures))
    return {
        **summary,
        "report_path": str(report_path),
        "message": f"Проверено кабинетов: {len(selected)}. Акций с будущим участием: {summary['actionable_total']}. Ошибок: {len(failures)}.",
    }
