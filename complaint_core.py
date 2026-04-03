import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import common
import background_jobs
import safe_files
from safe_logs import log_event

# --- adapters / fallbacks to existing project ---
clean_text = common.clean_text
build_review_text = common.build_review_text
normalize_review = common.normalize_review
review_signature = common.review_signature
read_json = common.read_json
write_json = common.write_json
fetch_pending_reviews = common.fetch_pending_reviews
utc_now_iso = common.utc_now_iso
call_ai = common.call_ai

PRIVATE_DIR = common.PRIVATE_DIR
COMPLAINT_DRAFTS_FILE = common.COMPLAINT_DRAFTS_FILE
COMPLAINT_QUEUE_FILE = common.COMPLAINT_QUEUE_FILE
COMPLAINT_RESULTS_FILE = common.COMPLAINT_RESULTS_FILE
OPENAI_COMPLAINT_MODEL = getattr(common, "OPENAI_COMPLAINT_MODEL", getattr(common, "OPENAI_MODEL", "gpt-4o-mini"))

LOW_RATING_CACHE_FILE = common.LOW_RATING_CACHE_FILE
SNAPSHOT_TTL_SECONDS = int(getattr(common, "COMPLAINTS_SNAPSHOT_TTL", 120))
WB_FEEDBACKS_MIN_INTERVAL_SEC = float(getattr(common, "WB_FEEDBACKS_MIN_INTERVAL_SEC", 0.40))
WB_FEEDBACKS_FETCH_RETRIES = int(getattr(common, "WB_FEEDBACKS_FETCH_RETRIES", 4))
_last_feedback_request_ts = 0.0

COMPLAINT_CATEGORIES = [
    "Отзыв оставили конкуренты",
    "Отзыв не относится к товару",
    "Спам-реклама в тексте",
    "Нецензурная лексика",
    "Отзыв с политическим контекстом",
    "Угрозы, оскорбления",
    "Другое",
]

# Сильные сигналы. Если они есть — категорию фиксируем жёстко.
PROFANITY_PATTERNS = [
    r"\bхер(?:ово|ня|овый|овый|овеньк)?\b",
    r"\bговн(?:о|о?вый|ища?)\b",
    r"\bдерьм(?:о|овый)?\b",
    r"\bмуда(?:к|ки|чье)\b",
    r"\bидиот(?:ы|ка)?\b",
    r"\bдебил(?:ы|ка)?\b",
    r"\bурод(?:ы|ина)?\b",
    r"\bмраз(?:ь|и)\b",
    r"\bтвар(?:ь|и)\b",
    r"\bсук(?:а|и)\b",
    r"\bбля(?:дь|ха|ха-муха)?\b",
    r"\bпизд(?:ец|ёж|а)\b",
    r"\bохрен(?:еть|ел|ела|енно)?\b",
]

THREAT_PATTERNS = [
    r"сломаю", r"разнесу", r"засужу", r"найду вас", r"пожалеете", r"уничтожу",
    r"угрож", r"порчу вам", r"доберусь",
]

SPAM_PATTERNS = [
    r"https?://", r"t\.me/", r"wa\.me/", r"@\w+", r"telegram", r"телеграм",
    r"whatsapp", r"ватсап", r"viber", r"инст", r"instagram", r"наш магазин",
    r"купите у нас", r"берите у нас", r"перейдите", r"ссылка", r"пишите в директ",
]

POLITICAL_PATTERNS = [
    r"политик", r"президент", r"выбор", r"санкц", r"госдум", r"министр", r"парти", r"оппозици",
]

UNRELATED_PATTERNS = [
    r"курьер", r"доставка", r"пункт выдачи", r"пвз", r"вайлдберриз", r"wb ", r"поддержк",
    r"продавец не ответил", r"деньги не вернули", r"логистик",
]

COMPETITOR_PATTERNS = [
    r"у другого продавца", r"у конкурента", r"лучше купить .*у", r"берите у .*магазин", r"наш магазин",
]


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    _ensure_parent(path)
    safe_files.append_jsonl(path, row, ensure_ascii=False)


def _read_results(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    # поддерживаем и jsonl, и старый json-list
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            data = json.loads(text)
            return data if isinstance(data, list) else []
        except Exception:
            return []
    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def load_complaint_drafts() -> Dict[str, Dict[str, Any]]:
    data = read_json(COMPLAINT_DRAFTS_FILE, {})
    return data if isinstance(data, dict) else {}


def save_complaint_drafts(data: Dict[str, Dict[str, Any]]) -> None:
    write_json(COMPLAINT_DRAFTS_FILE, data)



def load_complaint_queue() -> List[Dict[str, Any]]:
    data = read_json(COMPLAINT_QUEUE_FILE, [])
    return data if isinstance(data, list) else []



def save_complaint_queue(data: List[Dict[str, Any]]) -> None:
    write_json(COMPLAINT_QUEUE_FILE, data)



def append_result(row: Dict[str, Any]) -> None:
    _append_jsonl(COMPLAINT_RESULTS_FILE, row)
    log_event("complaints", "result_appended", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), review_id=clean_text(row.get("review_id")), status=clean_text(row.get("status")), category=clean_text(row.get("category")), level="error" if clean_text(row.get("status"))=="failed" else "info")



def load_recent_results(limit: int = 200) -> List[Dict[str, Any]]:
    rows = _read_results(COMPLAINT_RESULTS_FILE)
    return rows[-limit:]



def _text_block(review: Dict[str, Any]) -> str:
    return "\n".join(
        [
            f"Текст: {clean_text(review.get('text'))}",
            f"Плюсы: {clean_text(review.get('pros'))}",
            f"Минусы: {clean_text(review.get('cons'))}",
        ]
    ).strip()



def _find_matches(patterns: List[str], text: str) -> List[str]:
    found: List[str] = []
    lower = text.lower()
    for pattern in patterns:
        try:
            m = re.search(pattern, lower, flags=re.IGNORECASE)
        except re.error:
            m = None
        if m:
            token = m.group(0).strip()
            if token and token not in found:
                found.append(token)
    return found[:8]



def _detect_signals(review: Dict[str, Any]) -> Dict[str, Any]:
    combined = " ".join(
        [
            clean_text(review.get("text")),
            clean_text(review.get("pros")),
            clean_text(review.get("cons")),
            clean_text(review.get("productDetails", {}).get("productName")),
        ]
    ).lower()

    profanity = _find_matches(PROFANITY_PATTERNS, combined)
    threats = _find_matches(THREAT_PATTERNS, combined)
    spam = _find_matches(SPAM_PATTERNS, combined)
    politics = _find_matches(POLITICAL_PATTERNS, combined)
    unrelated = _find_matches(UNRELATED_PATTERNS, combined)
    competitor = _find_matches(COMPETITOR_PATTERNS, combined)

    forced_category = None
    if profanity:
        forced_category = "Нецензурная лексика"
    elif threats:
        forced_category = "Угрозы, оскорбления"
    elif spam:
        forced_category = "Спам-реклама в тексте"
    elif politics:
        forced_category = "Отзыв с политическим контекстом"
    elif competitor:
        forced_category = "Отзыв оставили конкуренты"

    return {
        "forced_category": forced_category,
        "signals": {
            "profanity": profanity,
            "threats": threats,
            "spam": spam,
            "politics": politics,
            "unrelated": unrelated,
            "competitor": competitor,
        },
    }



def normalize_category(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return "Другое"
    if text in COMPLAINT_CATEGORIES:
        return text
    low = text.lower()
    if "конкур" in low:
        return "Отзыв оставили конкуренты"
    if "не относится" in low or "не о товаре" in low:
        return "Отзыв не относится к товару"
    if "спам" in low or "реклам" in low:
        return "Спам-реклама в тексте"
    if "неценз" in low or "мат" in low or "лексик" in low:
        return "Нецензурная лексика"
    if "полит" in low:
        return "Отзыв с политическим контекстом"
    if "угроз" in low or "оскорб" in low:
        return "Угрозы, оскорбления"
    return "Другое"



def _fallback_reason(category: str, review: Dict[str, Any], signals: Dict[str, List[str]]) -> str:
    if category == "Нецензурная лексика":
        return "Текст отзыва содержит грубую и нецензурную лексику, что нарушает правила публикации отзывов. Просим исключить данный отзыв из публикации."
    if category == "Угрозы, оскорбления":
        return "Отзыв содержит оскорбительные и агрессивные формулировки в адрес продавца или товара. Просим исключить его из публикации."
    if category == "Спам-реклама в тексте":
        return "В тексте есть признаки рекламы или упоминания сторонних каналов связи, что нарушает правила публикации отзывов. Просим исключить данный отзыв."
    if category == "Отзыв с политическим контекстом":
        return "Содержание отзыва содержит политический контекст и не относится к оценке качества товара. Просим исключить данный отзыв из публикации."
    if category == "Отзыв оставили конкуренты":
        return "Отзыв носит характер недобросовестной дискредитации товара и не содержит корректного описания опыта использования. Просим дополнительно проверить публикацию и исключить отзыв."
    if category == "Отзыв не относится к товару":
        return "Содержание отзыва не относится к характеристикам и использованию товара, а описывает сторонние обстоятельства. Просим исключить данный отзыв из публикации."
    return "Просим дополнительно проверить данный отзыв и исключить его из публикации, поскольку формулировки носят дискредитирующий характер и не содержат корректного описания использования товара."



def _extract_json_object(text: str) -> Dict[str, Any]:
    text = clean_text(text)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        parsed = json.loads(text[start : end + 1])
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}



def _ai_prepare_category_and_reason(review: Dict[str, Any], forced_category: Optional[str], signals: Dict[str, List[str]]) -> Dict[str, Any]:
    product = review.get("productDetails", {}) or {}
    review_text = build_review_text(review)

    prompt = f"""
Ты помогаешь подготовить черновик жалобы на отзыв для маркетплейса.

Важно:
- Пользователь УЖЕ выбрал этот отзыв вручную и хочет отправить жалобу.
- Нельзя писать, что оснований недостаточно.
- Нельзя писать, что жалоба не нужна.
- Нельзя писать фразы вроде: «обычная негативная обратная связь», «недостаточно признаков», «оснований мало».
- Нужно ОБЯЗАТЕЛЬНО выбрать одну категорию и написать короткий деловой текст жалобы.
- Текст причины должен быть не длиннее 800 символов.
- Причина должна объяснять, почему отзыв нужно исключить из публикации.

Категории:
1. Отзыв оставили конкуренты
2. Отзыв не относится к товару
3. Спам-реклама в тексте
4. Нецензурная лексика
5. Отзыв с политическим контекстом
6. Угрозы, оскорбления
7. Другое

Данные по отзыву:
- Товар: {clean_text(product.get('productName'))}
- Артикул продавца: {clean_text(product.get('supplierArticle'))}
- nmID: {product.get('nmId', '')}
- Оценка: {int(review.get('productValuation', 0) or 0)}
- Дата: {clean_text(review.get('createdDate'))}
- Пользователь: {clean_text(review.get('userName'))}

Отзыв:
{review_text}

Сильные сигналы:
- profanity: {signals.get('profanity') or []}
- threats: {signals.get('threats') or []}
- spam: {signals.get('spam') or []}
- politics: {signals.get('politics') or []}
- unrelated: {signals.get('unrelated') or []}
- competitor: {signals.get('competitor') or []}

{"Категория уже определена жёстко: " + forced_category if forced_category else "Категорию выбери сам из списка выше."}

Верни только JSON:
{{
  "category": "одна категория из списка",
  "reason": "короткий деловой текст жалобы до 800 символов",
  "signals": ["необязательно"]
}}
""".strip()

    raw = call_ai(
        [
            {
                "role": "system",
                "content": (
                    "Ты внимательный помощник по модерации отзывов. "
                    "Нужно всегда подготовить категорию и жалобу. "
                    "Нельзя отказывать, нельзя спорить с пользователем, нельзя писать про недостаток оснований."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        model=OPENAI_COMPLAINT_MODEL,
        temperature=0.15,
    )
    payload = _extract_json_object(raw)
    return {"raw": raw, "payload": payload}



def analyze_complaint_review(review: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    log_event("complaints", "draft_prepare_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), review_id=clean_text((review or {}).get("id")), force=bool(force), job_id=background_jobs.current_job_id(), run_id=background_jobs.current_job_id())
    review = normalize_review(review)
    review_id = clean_text(review.get("id"))
    signature = review_signature(review)

    drafts = load_complaint_drafts()
    cached = drafts.get(review_id)
    if cached and cached.get("signature") == signature and not force:
        log_event("complaints", "draft_prepare_cached", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), review_id=review_id, job_id=background_jobs.current_job_id(), run_id=background_jobs.current_job_id())
        return cached

    detected = _detect_signals(review)
    forced_category = detected.get("forced_category")
    signals = detected.get("signals") or {}

    ai_result: Dict[str, Any] = {"raw": "", "payload": {}}
    try:
        ai_result = _ai_prepare_category_and_reason(review, forced_category, signals)
    except Exception as exc:
        ai_result = {"raw": "", "payload": {}}
        ai_error = common.classify_ai_error(exc)
        log_event(
            "complaints",
            "draft_prepare_fallback",
            tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""),
            review_id=review_id,
            level="warning",
            error_type=ai_error.get("type") or "ai_unavailable",
            error=ai_error.get("message") or str(exc),
            job_id=background_jobs.current_job_id(),
            run_id=background_jobs.current_job_id(),
        )

    payload = ai_result.get("payload") or {}
    category = normalize_category(payload.get("category"))
    if forced_category:
        category = forced_category
    if category not in COMPLAINT_CATEGORIES:
        category = "Другое"

    reason = clean_text(payload.get("reason"))
    if not reason:
        reason = _fallback_reason(category, review, signals)
    if len(reason) > 800:
        reason = reason[:799].rstrip() + "…"

    merged_signals: List[str] = []
    for bucket in signals.values():
        for item in bucket:
            if item and item not in merged_signals:
                merged_signals.append(item)
    for item in payload.get("signals", []) if isinstance(payload.get("signals"), list) else []:
        item = clean_text(item)
        if item and item not in merged_signals:
            merged_signals.append(item)

    product = review.get("productDetails", {}) or {}
    search_value = clean_text(product.get("nmId") or "") or clean_text(product.get("supplierArticle") or "") or clean_text(review_id)

    entry = {
        "review_id": review_id,
        "signature": signature,
        "prepared_at": utc_now_iso(),
        "supported": True,
        "category": category,
        "reason": reason,
        "confidence": 1.0,
        "signals": merged_signals[:10],
        "source": "ai_with_heuristics" if ai_result.get("payload") else "heuristic_only",
        "raw_model_text": clean_text(ai_result.get("raw"))[:3000],
        "search_value": search_value,
        "review": {
            "product_name": clean_text(product.get("productName")),
            "supplier_article": clean_text(product.get("supplierArticle")),
            "brand_name": clean_text(product.get("brandName")),
            "nm_id": int(product.get("nmId", 0) or 0),
            "stars": int(review.get("productValuation", 0) or 0),
            "user_name": clean_text(review.get("userName")),
            "created_date": clean_text(review.get("createdDate")),
            "subject_name": clean_text(review.get("subjectName")),
            "text": clean_text(review.get("text")),
            "pros": clean_text(review.get("pros")),
            "cons": clean_text(review.get("cons")),
            "review_text": build_review_text(review),
        },
    }
    drafts[review_id] = entry
    save_complaint_drafts(drafts)
    log_event("complaints", "draft_prepare_finish", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), review_id=review_id, category=category, source=entry.get("source"), signals=merged_signals[:5], job_id=background_jobs.current_job_id(), run_id=background_jobs.current_job_id())
    return entry



def can_enqueue(entry: Dict[str, Any]) -> Tuple[bool, str]:
    if not entry:
        return False, "Нет подготовленного черновика жалобы."
    category = normalize_category(entry.get("category"))
    if category not in COMPLAINT_CATEGORIES:
        return False, "Не выбрана корректная категория жалобы."
    reason = clean_text(entry.get("reason"))
    if len(reason) < 15:
        return False, "Текст жалобы слишком короткий."
    if len(reason) > 800:
        return False, "Текст жалобы длиннее 800 символов."
    search_value = clean_text(entry.get("search_value")) or clean_text((entry.get("review") or {}).get("nm_id"))
    if not search_value:
        return False, "Не найден search_value для перехода к отзыву (nmID / артикул)."
    return True, "OK"



def queue_complaint_entries(entries: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
    log_event("complaints", "queue_add_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), requested=len(entries))
    """
    Ставит жалобы в очередь.

    Важно: если по отзыву уже был статус failed / failed_stale / skipped,
    пользователь должен иметь возможность повторно поставить его в очередь.
    Раньше новая queued-задача тут же затиралась старым failed-результатом,
    и /complaints/process видел пустую очередь.
    """
    queue = reconcile_complaint_queue()
    result_index = get_result_index()
    existing_active_keys = {
        (clean_text(x.get("review_id")), clean_text(x.get("signature")))
        for x in queue
        if clean_text(x.get("status")) in {"queued", "processing"}
    }

    # failed-элементы, которые можно оживить повторной постановкой
    retriable_by_key: Dict[Tuple[str, str], int] = {}
    retriable_by_review: Dict[str, int] = {}
    for idx, item in enumerate(queue):
        status = clean_text(item.get("status"))
        if status not in TERMINAL_FAILURE_STATUSES:
            continue
        rid = clean_text(item.get("review_id"))
        sig = clean_text(item.get("signature"))
        retriable_by_key[(rid, sig)] = idx
        retriable_by_review[rid] = idx

    added = 0
    notes: List[str] = []

    for entry in entries:
        ok, msg = can_enqueue(entry)
        if not ok:
            notes.append(f"{entry.get('review_id')}: {msg}")
            continue

        review_id = clean_text(entry.get("review_id"))
        signature = clean_text(entry.get("signature"))
        key = (review_id, signature)
        last_result = result_index.get(review_id)
        if clean_text((last_result or {}).get("status")) in TERMINAL_SUCCESS_STATUSES:
            notes.append(f"{review_id}: по этому отзыву уже есть успешный результат, повторно в очередь не добавлен")
            continue
        if key in existing_active_keys:
            notes.append(f"{review_id}: уже есть в очереди")
            continue

        review = entry.get("review", {}) or {}
        nm_id = clean_text(review.get("nm_id"))
        supplier_article = clean_text(review.get("supplier_article"))
        search_value = clean_text(entry.get("search_value")) or nm_id or supplier_article or review_id

        payload = {
            "review_id": review_id,
            "signature": signature,
            "status": "queued",
            "queued_at": utc_now_iso(),
            "started_at": "",
            "finished_at": "",
            "last_error": "",
            "category": normalize_category(entry.get("category")),
            "reason": clean_text(entry.get("reason")),
            "confidence": 1.0,
            "signals": entry.get("signals", []),
            "search_value": search_value,
            "review": review,
        }

        revive_idx = retriable_by_key.get(key)
        if revive_idx is None:
            revive_idx = retriable_by_review.get(review_id)

        if revive_idx is not None and 0 <= revive_idx < len(queue):
            queue[revive_idx].update(payload)
            notes.append(f"{review_id}: повторно поставлено в очередь после failed")
        else:
            queue.append(payload)
        existing_active_keys.add(key)
        added += 1

    save_complaint_queue(queue)
    log_event("complaints", "queue_add_finish", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), added=added, notes_count=len(notes), queued_total=len(queue))
    return added, notes



def _parse_iso_dt(value: Any):
    from datetime import datetime, timezone
    text = clean_text(value)
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _choose_better_result(existing: Optional[Dict[str, Any]], candidate: Dict[str, Any]) -> Dict[str, Any]:
    if not existing:
        return candidate
    e_status = clean_text(existing.get("status"))
    c_status = clean_text(candidate.get("status"))
    e_pr = RESULT_STATUS_PRIORITY.get(e_status, 0)
    c_pr = RESULT_STATUS_PRIORITY.get(c_status, 0)
    if c_pr > e_pr:
        return candidate
    if c_pr < e_pr:
        return existing
    e_dt = _parse_iso_dt(existing.get("processed_at") or existing.get("finished_at"))
    c_dt = _parse_iso_dt(candidate.get("processed_at") or candidate.get("finished_at"))
    if c_dt and (not e_dt or c_dt >= e_dt):
        return candidate
    return existing


def reconcile_complaint_queue() -> List[Dict[str, Any]]:
    log_event("complaints", "queue_reconcile_start", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""))
    from datetime import datetime, timezone, timedelta
    queue = load_complaint_queue()
    results = _read_results(COMPLAINT_RESULTS_FILE)
    best_results: Dict[str, Dict[str, Any]] = {}
    for row in results:
        rid = clean_text(row.get("review_id"))
        if not rid:
            continue
        best_results[rid] = _choose_better_result(best_results.get(rid), row)

    now = datetime.now(timezone.utc)
    changed = False
    for item in queue:
        rid = clean_text(item.get("review_id"))
        current_status = clean_text(item.get("status"))
        best = best_results.get(rid)
        if best:
            best_status = clean_text(best.get("status"))

            # Успешные результаты безопасно протягиваем в очередь автоматически.
            if best_status in TERMINAL_SUCCESS_STATUSES:
                if current_status != best_status:
                    item["status"] = best_status
                    item["finished_at"] = clean_text(best.get("processed_at") or best.get("finished_at") or utc_now_iso())
                    item["last_error"] = ""
                    changed = True
                continue

            # Старый failed-результат НЕ должен затирать новую queued-задачу,
            # если пользователь повторно отправил жалобу в очередь.
            # Обновляем failure-статус только когда сама queue-запись тоже уже
            # находится в failure-состоянии.
            if best_status in TERMINAL_FAILURE_STATUSES and current_status in TERMINAL_FAILURE_STATUSES:
                if current_status != best_status:
                    item["status"] = best_status
                    item["finished_at"] = clean_text(best.get("processed_at") or best.get("finished_at") or utc_now_iso())
                    item["last_error"] = clean_text(best.get("error"))
                    changed = True
                elif not clean_text(item.get("last_error")) and clean_text(best.get("error")):
                    item["last_error"] = clean_text(best.get("error"))
                    changed = True
                continue

        if current_status == "processing":
            started = _parse_iso_dt(item.get("started_at"))
            if started and now - started > timedelta(minutes=STALE_PROCESSING_MINUTES):
                item["status"] = "failed_stale"
                item["finished_at"] = utc_now_iso()
                if not clean_text(item.get("last_error")):
                    item["last_error"] = "Задача зависла в processing и была автоматически помечена как failed_stale."
                changed = True

    if changed:
        save_complaint_queue(queue)
    log_event("complaints", "queue_reconcile_finish", tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), total=len(queue))
    return queue


def get_queue_index() -> Dict[str, Dict[str, Any]]:
    queue = reconcile_complaint_queue()
    return {clean_text(x.get("review_id")): x for x in queue}


def get_result_index() -> Dict[str, Dict[str, Any]]:
    result_index: Dict[str, Dict[str, Any]] = {}
    for row in _read_results(COMPLAINT_RESULTS_FILE):
        rid = clean_text(row.get("review_id"))
        if not rid:
            continue
        result_index[rid] = _choose_better_result(result_index.get(rid), row)
    return result_index


def _rate_limit_retry_seconds(exc: Exception, attempt: int) -> float:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", {}) or {}
    retry_values = [
        headers.get("X-Ratelimit-Retry"),
        headers.get("Retry-After"),
        headers.get("retry-after"),
    ]
    for value in retry_values:
        if value is None:
            continue
        try:
            seconds = float(str(value).strip())
            if seconds > 0:
                return seconds
        except Exception:
            pass
    return min(2.0 * (attempt + 1), 8.0)


def _wb_feedbacks_sleep_if_needed() -> None:
    global _last_feedback_request_ts
    elapsed = time.monotonic() - _last_feedback_request_ts
    if elapsed < WB_FEEDBACKS_MIN_INTERVAL_SEC:
        time.sleep(WB_FEEDBACKS_MIN_INTERVAL_SEC - elapsed)


def _fetch_pending_reviews_safe(skip: int, take: int) -> Tuple[List[Dict[str, Any]], int, int]:
    global _last_feedback_request_ts
    last_exc: Optional[Exception] = None
    for attempt in range(WB_FEEDBACKS_FETCH_RETRIES + 1):
        _wb_feedbacks_sleep_if_needed()
        try:
            result = fetch_pending_reviews(skip=skip, take=take)
            _last_feedback_request_ts = time.monotonic()
            return result
        except Exception as exc:
            _last_feedback_request_ts = time.monotonic()
            last_exc = exc
            if "429" not in str(exc):
                raise
            time.sleep(_rate_limit_retry_seconds(exc, attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось загрузить отзывы из WB")


def _snapshot_base_row(review: Dict[str, Any]) -> Dict[str, Any]:
    product = review.get("productDetails", {}) or {}
    return {
        "id": clean_text(review.get("id")),
        "product_name": clean_text(product.get("productName")),
        "supplier_article": clean_text(product.get("supplierArticle")),
        "brand_name": clean_text(product.get("brandName")),
        "nm_id": int(product.get("nmId", 0) or 0),
        "stars": int(review.get("productValuation", 0) or 0),
        "stars_view": "⭐" * int(review.get("productValuation", 0) or 0),
        "review_text": build_review_text(review),
        "text": clean_text(review.get("text")),
        "pros": clean_text(review.get("pros")),
        "cons": clean_text(review.get("cons")),
        "user_name": clean_text(review.get("userName")),
        "created_date": clean_text(review.get("createdDate")),
        "subject_name": clean_text(review.get("subjectName")),
    }


def _load_low_rating_snapshot() -> Dict[str, Any]:
    data = read_json(LOW_RATING_CACHE_FILE, {})
    return data if isinstance(data, dict) else {}


def _save_low_rating_snapshot(data: Dict[str, Any]) -> None:
    write_json(LOW_RATING_CACHE_FILE, data)


def _snapshot_is_fresh(snapshot: Dict[str, Any]) -> bool:
    fetched_at_unix = float(snapshot.get("fetched_at_unix", 0) or 0)
    if fetched_at_unix <= 0:
        return False
    return (time.time() - fetched_at_unix) <= SNAPSHOT_TTL_SECONDS


def _scan_low_rating_reviews(max_raw_batches: int, raw_batch_size: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    raw_skip = 0
    count_unanswered = 0
    count_archive = 0

    for _ in range(max_raw_batches):
        feedbacks, count_unanswered, count_archive = _fetch_pending_reviews_safe(skip=raw_skip, take=raw_batch_size)
        if not feedbacks:
            break

        for review in feedbacks:
            if int(review.get("productValuation", 0) or 0) > 3:
                continue
            rows.append(_snapshot_base_row(review))

        raw_skip += raw_batch_size
        if len(feedbacks) < raw_batch_size:
            break

    snapshot = {
        "fetched_at_unix": time.time(),
        "fetched_at": utc_now_iso(),
        "ttl_seconds": SNAPSHOT_TTL_SECONDS,
        "raw_scanned": raw_skip,
        "count_unanswered": count_unanswered,
        "count_archive": count_archive,
        "rows": rows,
    }
    _save_low_rating_snapshot(snapshot)
    snapshot["cache_state"] = "fresh"
    return snapshot


def _get_low_rating_snapshot(force_refresh: bool, max_raw_batches: int, raw_batch_size: int) -> Dict[str, Any]:
    snapshot = _load_low_rating_snapshot()
    has_rows = isinstance(snapshot.get("rows"), list) and bool(snapshot.get("rows"))

    if snapshot and has_rows and _snapshot_is_fresh(snapshot) and not force_refresh:
        snapshot = dict(snapshot)
        snapshot["cache_state"] = "fresh"
        snapshot["served_from_cache"] = True
        return snapshot

    try:
        fresh = _scan_low_rating_reviews(max_raw_batches=max_raw_batches, raw_batch_size=raw_batch_size)
        fresh["served_from_cache"] = False
        return fresh
    except Exception as exc:
        if snapshot and has_rows:
            snapshot = dict(snapshot)
            snapshot["cache_state"] = "stale"
            snapshot["served_from_cache"] = True
            snapshot["cache_error"] = str(exc)
            return snapshot
        raise


def _build_cache_info(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    fetched_at_unix = float(snapshot.get("fetched_at_unix", 0) or 0)
    age_seconds = max(0, int(time.time() - fetched_at_unix)) if fetched_at_unix else None
    ttl_seconds = int(snapshot.get("ttl_seconds", SNAPSHOT_TTL_SECONDS) or SNAPSHOT_TTL_SECONDS)
    return {
        "fetched_at": clean_text(snapshot.get("fetched_at")),
        "age_seconds": age_seconds,
        "ttl_seconds": ttl_seconds,
        "state": clean_text(snapshot.get("cache_state")) or "fresh",
        "served_from_cache": bool(snapshot.get("served_from_cache")),
        "error": clean_text(snapshot.get("cache_error")),
    }


SUBMITTED_STATUSES = {"submitted", "submitted_click_only", "success", "submitted_verified", "already_complained", "already_submitted"}
TERMINAL_SUCCESS_STATUSES = SUBMITTED_STATUSES | {"dry_run"}
TERMINAL_FAILURE_STATUSES = {"failed", "failed_stale", "skipped"}
RESULT_STATUS_PRIORITY = {
    "submitted": 100,
    "submitted_verified": 99,
    "submitted_click_only": 98,
    "success": 97,
    "already_complained": 96,
    "already_submitted": 95,
    "dry_run": 60,
    "failed": 20,
    "failed_stale": 19,
    "skipped": 10,
}
STALE_PROCESSING_MINUTES = 20


def _parse_created_date(value: Any):
    from datetime import datetime

    text = clean_text(value)
    if not text:
        return datetime.min
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def _review_matches_text(row: Dict[str, Any], query: str) -> bool:
    query = clean_text(query).lower()
    if not query:
        return True
    haystack = " ".join(
        [
            clean_text(row.get("id")),
            clean_text(row.get("product_name")),
            clean_text(row.get("supplier_article")),
            clean_text(row.get("brand_name")),
            clean_text(row.get("user_name")),
            clean_text(row.get("subject_name")),
            clean_text(row.get("review_text")),
            clean_text(row.get("text")),
            clean_text(row.get("pros")),
            clean_text(row.get("cons")),
            str(row.get("nm_id") or ""),
        ]
    ).lower()
    return query in haystack


def _row_is_submitted(row: Dict[str, Any]) -> bool:
    queue_status = clean_text(row.get("queue_status"))
    result_status = clean_text(row.get("result_status"))
    return queue_status in TERMINAL_SUCCESS_STATUSES or result_status in TERMINAL_SUCCESS_STATUSES


def _row_matches_filters(
    row: Dict[str, Any],
    stars_filter: str = "all",
    draft_filter: str = "all",
    queue_filter: str = "all",
    search_query: str = "",
    hide_submitted: bool = True,
) -> bool:
    stars = int(row.get("stars", 0) or 0)
    if stars_filter in {"1", "2", "3"} and stars != int(stars_filter):
        return False

    has_draft = bool(row.get("draft"))
    if draft_filter == "with_draft" and not has_draft:
        return False
    if draft_filter == "without_draft" and has_draft:
        return False

    queue_status = clean_text(row.get("queue_status"))
    result_status = clean_text(row.get("result_status"))
    if queue_filter == "ready" and has_draft is False:
        return False
    if queue_filter == "queued" and queue_status != "queued":
        return False
    if queue_filter == "processing" and queue_status != "processing":
        return False
    if queue_filter == "failed" and result_status != "failed" and queue_status != "failed":
        return False
    if queue_filter == "submitted" and not _row_is_submitted(row):
        return False
    if queue_filter == "not_queued" and queue_status in {"queued", "processing"}:
        return False

    if hide_submitted and _row_is_submitted(row):
        return False

    if not _review_matches_text(row, search_query):
        return False

    return True


def _sort_rows(rows: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
    sort_by = clean_text(sort_by) or "newest"
    if sort_by == "oldest":
        return sorted(rows, key=lambda r: _parse_created_date(r.get("created_date")))
    if sort_by == "stars_low":
        return sorted(rows, key=lambda r: (int(r.get("stars", 0) or 0), _parse_created_date(r.get("created_date"))))
    if sort_by == "stars_high":
        return sorted(rows, key=lambda r: (-int(r.get("stars", 0) or 0), _parse_created_date(r.get("created_date"))), reverse=False)
    return sorted(rows, key=lambda r: _parse_created_date(r.get("created_date")), reverse=True)


def fetch_low_rating_reviews(
    page: int = 1,
    page_size: int = 100,
    max_raw_batches: int = 30,
    raw_batch_size: int = 100,
    sort_by: str = "newest",
    stars_filter: str = "all",
    draft_filter: str = "all",
    queue_filter: str = "all",
    search_query: str = "",
    hide_submitted: bool = True,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    page = max(page, 1)
    drafts = load_complaint_drafts()
    queue_index = get_queue_index()
    result_index = get_result_index()

    snapshot = _get_low_rating_snapshot(
        force_refresh=force_refresh,
        max_raw_batches=max_raw_batches,
        raw_batch_size=raw_batch_size,
    )
    base_rows = snapshot.get("rows", []) if isinstance(snapshot.get("rows"), list) else []

    all_candidate_rows: List[Dict[str, Any]] = []
    for base in base_rows:
        review_id = clean_text(base.get("id"))
        draft = drafts.get(review_id)
        queue_row = queue_index.get(review_id)
        result_row = result_index.get(review_id)
        row = dict(base)
        row.update(
            {
                "draft": draft,
                "queue_status": clean_text(queue_row.get("status")) if queue_row else "",
                "result_status": clean_text(result_row.get("status")) if result_row else "",
                "result_error": clean_text(result_row.get("error")) if result_row else "",
            }
        )
        all_candidate_rows.append(row)

    submitted_hidden = 0
    filtered_rows: List[Dict[str, Any]] = []
    for row in all_candidate_rows:
        if hide_submitted and _row_is_submitted(row):
            submitted_hidden += 1
        if _row_matches_filters(
            row,
            stars_filter=stars_filter,
            draft_filter=draft_filter,
            queue_filter=queue_filter,
            search_query=search_query,
            hide_submitted=hide_submitted,
        ):
            filtered_rows.append(row)

    filtered_rows = _sort_rows(filtered_rows, sort_by)

    total_filtered = len(filtered_rows)
    page_count = max(1, (total_filtered + page_size - 1) // page_size) if total_filtered else 1
    if page > page_count:
        page = page_count
    start = (page - 1) * page_size
    end = start + page_size
    page_reviews = filtered_rows[start:end]

    return {
        "rows": page_reviews,
        "page": page,
        "page_size": page_size,
        "page_count": page_count,
        "has_prev": page > 1,
        "has_next": page < page_count,
        "total_filtered": total_filtered,
        "submitted_hidden": submitted_hidden,
        "raw_scanned": int(snapshot.get("raw_scanned", 0) or 0),
        "low_total_scanned": len(all_candidate_rows),
        "count_unanswered": int(snapshot.get("count_unanswered", 0) or 0),
        "count_archive": int(snapshot.get("count_archive", 0) or 0),
        "queue_total": len(load_complaint_queue()),
        "draft_total": len(load_complaint_drafts()),
        "auth_required": True,
        "cache_info": _build_cache_info(snapshot),
        "filters": {
            "sort": sort_by or "newest",
            "stars": stars_filter or "all",
            "draft": draft_filter or "all",
            "queue": queue_filter or "all",
            "q": search_query or "",
            "hide_submitted": bool(hide_submitted),
        },
    }


def complaint_dashboard_stats() -> Dict[str, Any]:
    drafts = load_complaint_drafts()
    queue = reconcile_complaint_queue()
    results = load_recent_results(limit=500)

    category_counter = Counter(
        normalize_category(v.get("category"))
        for v in drafts.values()
        if normalize_category(v.get("category"))
    )
    status_counter = Counter(clean_text(v.get("status")) for v in queue if clean_text(v.get("status")))
    result_counter = Counter(clean_text(v.get("status")) for v in results if clean_text(v.get("status")))

    return {
        "drafts_supported": len(drafts),
        "drafts_unsupported": 0,
        "top_categories": category_counter.most_common(7),
        "queue_statuses": status_counter.most_common(),
        "result_statuses": result_counter.most_common(),
        "recent_results": list(reversed(results[-20:])),
    }


TERMINAL_FAILURE_STATUSES = {"failed", "failed_stale", "skipped"}


def normalize_complaint_outcome(status: Any) -> str:
    value = clean_text(status).lower()
    if value in {"accepted", "approved", "complaint_accepted", "accepted_by_wb"}:
        return "accepted"
    if value in {"rejected", "declined", "complaint_rejected", "rejected_by_wb", "already_rejected"}:
        return "rejected"
    if value in {"submitted", "submitted_click_only", "success", "submitted_verified", "already_complained", "already_submitted", "dry_run", "pending"}:
        return "pending"
    if value in TERMINAL_FAILURE_STATUSES:
        return "failed"
    return "other"


def build_complaint_effectiveness(limit: int = 5000) -> Dict[str, Any]:
    rows = load_recent_results(limit=limit)
    total_sent = 0
    accepted = 0
    rejected = 0
    pending = 0
    category_stats: Dict[str, Dict[str, Any]] = {}
    product_stats: Dict[str, Dict[str, Any]] = {}
    recent: List[Dict[str, Any]] = []

    # берём последнюю запись по review_id, чтобы не удваивать статистику
    latest_by_review: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        review_id = clean_text(row.get("review_id"))
        key = review_id or clean_text(row.get("processed_at")) or str(len(latest_by_review))
        latest_by_review[key] = row

    for row in latest_by_review.values():
        outcome = normalize_complaint_outcome(row.get("outcome") or row.get("status") or row.get("result_status"))
        if outcome not in {"accepted", "rejected", "pending"}:
            continue
        total_sent += 1
        if outcome == "accepted":
            accepted += 1
        elif outcome == "rejected":
            rejected += 1
        elif outcome == "pending":
            pending += 1

        category = normalize_category(row.get("category"))
        cat = category_stats.setdefault(category, {"category": category, "total": 0, "accepted": 0, "rejected": 0, "pending": 0})
        cat["total"] += 1
        cat[outcome] += 1

        review = row.get("review", {}) or {}
        product_name = clean_text(review.get("product_name") or row.get("product_name") or row.get("product"))
        nm_id = clean_text(review.get("nm_id") or row.get("nm_id"))
        product_key = f"{nm_id}|{product_name}"
        prod = product_stats.setdefault(product_key, {"product_name": product_name or "—", "nm_id": nm_id or "—", "total": 0, "accepted": 0, "rejected": 0, "pending": 0})
        prod["total"] += 1
        prod[outcome] += 1

        recent.append({
            "review_id": clean_text(row.get("review_id")),
            "category": category,
            "outcome": outcome,
            "processed_at": clean_text(row.get("processed_at") or row.get("finished_at")),
            "product_name": product_name or "—",
            "nm_id": nm_id or "—",
            "reason": clean_text(row.get("reason")),
        })

    category_rows = list(category_stats.values())
    for item in category_rows:
        item["accept_rate"] = round((item["accepted"] / item["total"] * 100.0), 2) if item["total"] else 0.0
    category_rows.sort(key=lambda x: (-x["accept_rate"], -x["accepted"], -x["total"], x["category"]))

    product_rows = list(product_stats.values())
    for item in product_rows:
        item["accept_rate"] = round((item["accepted"] / item["total"] * 100.0), 2) if item["total"] else 0.0
    product_rows.sort(key=lambda x: (-x["accepted"], -x["accept_rate"], -x["total"], x["product_name"]))

    recent.sort(key=lambda x: clean_text(x.get("processed_at")), reverse=True)

    return {
        "total_sent": total_sent,
        "accepted": accepted,
        "rejected": rejected,
        "pending": pending,
        "category_rows": category_rows[:20],
        "product_rows": product_rows[:25],
        "recent": recent[:50],
    }
