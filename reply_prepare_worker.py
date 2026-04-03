import argparse
import traceback
from pathlib import Path
from typing import Any, Dict, List

import tenant_manager
import common
import app as appmod
from safe_logs import safe_log_event, safe_log_exception


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def run_worker(tenant_id: str) -> int:
    tenant_manager.apply_tenant_context(tenant_id, module_globals=appmod.__dict__)
    job = appmod._load_reply_prepare_job(tenant_id)
    reviews = job.get("reviews") or []
    if not isinstance(reviews, list):
        reviews = []
    force = bool(job.get("force"))
    missing_ids = job.get("missing_ids") or []
    total = len(reviews)
    appmod._clear_reply_prepare_stop_flag(tenant_id)
    meta = appmod._load_reply_prepare_meta(tenant_id)
    meta.update({
        "tenant_id": tenant_id,
        "status": "running",
        "phase": "prepare",
        "started_at": meta.get("started_at") or common.utc_now_iso(),
        "last_status_message": "Идёт генерация AI-черновиков",
        "last_heartbeat_at": common.utc_now_iso(),
        "last_progress_at": common.utc_now_iso(),
        "processed_count": 0,
        "generated_count": 0,
        "failed_count": 0,
        "current_review_id": "",
        "total_selected": _safe_int(meta.get("total_selected"), total + len(missing_ids)) or (total + len(missing_ids)),
    })
    appmod._save_reply_prepare_meta(meta, tenant_id)
    safe_log_event("replies", tenant_id, "info", "Worker генерации черновиков запущен", total=total, missing=len(missing_ids), force=force)

    generated = 0
    failed = 0
    processed = 0
    for idx, review in enumerate(reviews, 1):
        if appmod._reply_prepare_stop_requested(tenant_id):
            meta.update({
                "status": "stopped",
                "phase": "stopped",
                "finished_at": common.utc_now_iso(),
                "stop_requested": True,
                "stopped_by_user": True,
                "last_status_message": "Генерация черновиков остановлена пользователем",
                "processed_count": processed,
                "generated_count": generated,
                "failed_count": failed + len(missing_ids),
                "last_heartbeat_at": common.utc_now_iso(),
            })
            appmod._save_reply_prepare_meta(meta, tenant_id)
            safe_log_event("replies", tenant_id, "warning", "Генерация черновиков остановлена", processed=processed, generated=generated, failed=failed)
            return 0
        review_id = common.clean_text(review.get("id"))
        try:
            appmod.generate_reply_for_review(review, force=force)
            generated += 1
        except Exception as exc:
            failed += 1
            meta["last_error"] = common.clean_text(str(exc))
            safe_log_exception("replies", tenant_id, exc, message="Ошибка генерации черновика", review_id=review_id)
        processed = idx
        if idx == 1 or idx % 5 == 0 or idx == total:
            meta.update({
                "status": "running",
                "phase": "prepare",
                "processed_count": processed,
                "generated_count": generated,
                "failed_count": failed,
                "current_review_id": review_id,
                "last_progress_at": common.utc_now_iso(),
                "last_heartbeat_at": common.utc_now_iso(),
                "last_batch_count": 5,
                "last_status_message": f"Обработано {processed} из {total}",
            })
            appmod._save_reply_prepare_meta(meta, tenant_id)
            if idx % 25 == 0 or idx == total:
                safe_log_event("replies", tenant_id, "info", "Промежуточный прогресс генерации", processed=processed, generated=generated, failed=failed)

    failed_total = failed + len(missing_ids)
    if missing_ids:
        safe_log_event("replies", tenant_id, "warning", "Часть отзывов не найдена для генерации", missing_count=len(missing_ids))
    meta.update({
        "status": "completed",
        "phase": "done",
        "finished_at": common.utc_now_iso(),
        "processed_count": processed,
        "generated_count": generated,
        "failed_count": failed_total,
        "missing_count": len(missing_ids),
        "last_progress_at": common.utc_now_iso(),
        "last_heartbeat_at": common.utc_now_iso(),
        "last_status_message": f"Генерация завершена. Успешно: {generated}, ошибок: {failed_total}",
    })
    appmod._save_reply_prepare_meta(meta, tenant_id)
    safe_log_event("replies", tenant_id, "info", "Генерация черновиков завершена", processed=processed, generated=generated, failed=failed_total)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    args = parser.parse_args()
    tenant_id = common.clean_text(args.tenant)
    try:
        return run_worker(tenant_id)
    except Exception as exc:
        try:
            tenant_manager.apply_tenant_context(tenant_id, module_globals=appmod.__dict__)
            meta = appmod._load_reply_prepare_meta(tenant_id)
            meta.update({
                "status": "error",
                "phase": "terminated",
                "finished_at": common.utc_now_iso(),
                "last_error": common.clean_text(str(exc)),
                "last_status_message": "Воркер генерации черновиков завершился с ошибкой",
                "last_heartbeat_at": common.utc_now_iso(),
            })
            appmod._save_reply_prepare_meta(meta, tenant_id)
        except Exception:
            pass
        safe_log_exception("replies", tenant_id, exc, message="Критическая ошибка worker генерации черновиков")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
