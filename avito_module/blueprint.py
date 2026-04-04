from __future__ import annotations

import hashlib
import hmac
import ipaddress
import os
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, Flask, Response, abort, current_app, flash, g, jsonify, redirect, render_template, request, send_file, session, url_for

from .audit import AvitoAuditLogger, log_avito_event
from .compat import background_jobs_available, clean_text, list_latest_jobs, log_event, resolve_tenant, submit_background_job
from .config import AvitoModuleConfig
from .service import (
    AvitoService,
    run_backfill_job,
    run_browser_bootstrap_job,
    run_generate_drafts_job,
    run_replay_dlq_job,
    run_send_drafts_job,
    run_sync_job,
)
from .storage import AvitoStorage


avito_bp = Blueprint("avito_module", __name__, template_folder="templates")

MAX_KNOWLEDGE_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_MEDIA_UPLOAD_BYTES = 20 * 1024 * 1024
ALLOWED_MEDIA_MIME_PREFIXES = ("image/", "video/")
ALLOWED_MEDIA_MIME_EXACT = {"application/pdf"}

AVITO_UI_OPTION_LABELS = {
    "auto_mode": {
        "disabled": "Автоответы выключены",
        "draft_only": "Только готовить черновики",
        "simple_only": "Автоответы только на простые вопросы",
        "all": "Максимальная автоматизация",
    },
    "knowledge_mode": {
        "disabled": "Не использовать базу знаний",
        "assist": "Использовать как подсказку",
        "require_for_auto": "Автоответ только при найденной опоре",
    },
    "knowledge_answer_style": {
        "grounded": "Строго по фактам из базы",
        "concise": "Коротко и по делу",
        "sales": "Более продающий стиль",
    },
    "media_send_transport": {
        "disabled": "Не отправлять вложения автоматически",
        "browser_first": "Сначала браузер, затем fallback",
        "browser_only": "Только браузер",
        "api_first": "Сначала API, затем fallback",
        "api_only": "Только API",
    },
    "chat_status": {
        "all": "Все статусы",
        "new": "Новый",
        "in_progress": "В работе",
        "waiting_customer": "Ждём клиента",
        "closed": "Закрыт",
        "escalation": "Эскалация",
        "open": "Новый / активный",
        "lead": "Тёплый лид",
        "waiting": "Ждём клиента",
    },
    "priority": {
        "low": "Низкий",
        "normal": "Обычный",
        "high": "Высокий",
    },
    "draft_state": {
        "draft": "Черновик",
        "review": "На проверке",
        "hold": "Отложен",
        "ready": "Готов к отправке",
        "sending": "Отправляется",
        "error": "Ошибка",
        "rejected": "Отклонён",
        "sent": "Отправлен",
        "partial_sent_text_only": "Отправлен только текст",
    },
    "run_status": {
        "queued": "В очереди",
        "running": "Выполняется",
        "completed": "Завершён",
        "warning": "Завершён с предупреждениями",
        "abandoned": "Прерван",
        "error": "Ошибка",
    },
    "route": {
        "auto": "Автоматический ответ",
        "manual": "Нужен человек",
        "escalate": "Эскалация менеджеру",
        "blocked": "Заблокировано правилами",
    },
    "decision_level": {
        "draft": "Черновик",
        "recommendation": "Рекомендация",
        "auto_send": "Автоотправка",
    },
    "operator_bucket": {
        "team": "Вся команда",
        "mine": "Моя очередь",
        "unassigned": "Неназначенные",
        "overdue": "Просроченные по SLA",
        "human": "Нужен человек",
        "escalation": "Эскалация",
        "waiting": "Ждём клиента",
        "with_media": "С медиа",
        "bargain": "С торгом",
    },
    "operator_action": {
        "claim_me": "Назначить на меня",
        "assign": "Назначить оператора",
        "release": "Снять назначение",
        "start": "Взять в работу",
        "waiting": "Ждём клиента",
        "escalate": "Эскалация",
        "close": "Закрыть",
        "reopen": "Переоткрыть",
        "priority_high": "Высокий приоритет",
        "priority_normal": "Обычный приоритет",
        "priority_low": "Низкий приоритет",
    },
    "media_kind": {
        "image": "Изображение",
        "video": "Видео",
        "document": "Документ",
        "other": "Другое",
    },
    "dlq_status": {
        "open": "Открытые",
        "resolved": "Повторно обработанные",
        "discarded": "Игнорированные",
        "all": "Все",
    },
    "knowledge_kind": {
        "faq": "FAQ / частый вопрос",
        "listing_card": "Карточка объявления",
        "policy": "Правило / политика",
        "shipping": "Доставка / оплата",
        "condition": "Состояние / дефекты",
        "size": "Размеры / параметры",
        "script": "Скрипт продаж",
        "qa": "Готовые ответы менеджера",
    },
    "log_channel": {
        "all": "Все каналы",
        "sync": "Синхронизация",
        "ai": "ИИ и черновики",
        "send": "Отправка",
        "browser": "Браузерный резерв",
        "webhook": "Webhook",
        "ui": "Интерфейс",
        "ops": "Служебные операции",
        "decision": "Лог решений",
        "knowledge": "База знаний",
        "media": "Медиа",
        "health": "Здоровье и алерты",
    },
    "health_alert_status": {
        "open": "Активный",
        "acknowledged": "Подтверждён",
        "resolved": "Закрыт",
        "all": "Все",
    },
    "health_component_status": {
        "ok": "Норма",
        "warning": "Предупреждение",
        "error": "Ошибка",
        "info": "Информация",
    },
}

AVITO_FIELD_HINTS = {
    "client_id": "Идентификатор приложения Avito API. Возьми его в кабинете разработчика Авито и вставь без пробелов.",
    "client_secret": "Секрет приложения Avito API. Не публикуй его в репозитории и не пересылай другим людям.",
    "user_id": "ID аккаунта или пользователя Авито, от имени которого читаются и отправляются сообщения.",
    "ai_api_key": "Ключ AI-провайдера для генерации черновиков. Нужен только если используешь ИИ-ответы.",
    "ai_base_url": "Необязательный URL совместимого AI API. Оставь пустым для стандартного провайдера.",
    "webhook_secret": "Секрет для защиты webhook. Используй длинную случайную строку и не публикуй её открыто.",
    "browser_bootstrap_timeout_seconds": "Сколько секунд держать окно входа открытым, чтобы успеть вручную авторизоваться и сохранить browser state.",
    "auto_mode": "Выбирает уровень автоматизации: от простых черновиков до автоотправки.",
    "ai_model": "Название модели, которой будет генерироваться ответ.",
    "auto_send_confidence_threshold": "Минимальная уверенность ИИ для автоотправки. Чем выше значение, тем осторожнее модуль.",
    "max_context_messages": "Сколько последних сообщений из чата передавать модели как контекст.",
    "system_prompt": "Главная инструкция для ИИ: стиль, ограничения, запреты и требования к тону ответа.",
    "knowledge_text": "Короткий общий контекст по магазину, доставке и правилам. Используется вместе с базой знаний.",
    "knowledge_mode": "Определяет, как база знаний участвует в генерации ответа.",
    "knowledge_max_hits": "Максимум фрагментов базы знаний, которые можно подмешать в один ответ.",
    "knowledge_min_score": "Минимальная релевантность knowledge-фрагмента. Чем выше значение, тем строже отбор.",
    "knowledge_chunk_chars": "Размер чанка в символах при разбиении длинных документов базы знаний.",
    "knowledge_chunk_overlap_chars": "Сколько символов перекрытия оставлять между соседними чанками.",
    "knowledge_answer_style": "Стиль ответа при использовании базы знаний.",
    "knowledge_enabled": "Полностью включает или выключает поиск по базе знаний.",
    "similar_dialogs_enabled": "Разрешает искать похожие прошлые диалоги и использовать их как примеры для ответа.",
    "similar_dialogs_max_hits": "Сколько похожих диалогов максимум учитывать.",
    "similar_dialogs_min_score": "Минимальный порог похожести, при котором старый диалог считается полезным.",
    "hitl_enabled": "Включает очередь ручной проверки ответов человеком.",
    "hitl_auto_ready_threshold": "Если уверенность выше этого значения, черновик можно сразу помечать как готовый.",
    "hitl_queue_default_assignee": "Кому по умолчанию назначать спорные диалоги на ручную проверку.",
    "media_registry_enabled": "Включает реестр фото, видео и документов, привязанных к товарам и сценариям.",
    "media_auto_suggest_enabled": "Разрешает модулю автоматически подбирать подходящие медиа к диалогу.",
    "media_max_suggestions": "Сколько материалов максимум показывать в подсказках.",
    "media_send_enabled": "Разрешает реальную отправку выбранных вложений вместе с сообщением.",
    "media_send_transport": "Каким путём отправлять вложения: через браузер, через API или с резервным переходом между ними.",
    "media_max_send_assets": "Максимальное число файлов в одном ответе.",
    "media_send_images_only": "Если включено, автоматически отправляются только изображения — без видео и документов.",
    "media_browser_send_headless": "Разрешает отправку медиа через браузер без видимого окна.",
    "media_api_upload_endpoint": "Необязательный endpoint загрузки вложений, если на твоём контуре подтверждён прямой API для медиа.",
    "media_api_send_endpoint": "Необязательный endpoint отправки сообщения с вложениями через API.",
    "media_allow_external_fetch": "Разрешить серверу скачивать внешние файлы по URL. По умолчанию лучше держать выключенным из соображений безопасности.",
    "media_allowed_external_hosts": "Список разрешённых доменов для внешних ссылок, через запятую. Если список заполнен, скачивание разрешено только с этих доменов.",
    "media_upload_max_bytes": "Максимальный размер локально загружаемого медиа-файла в байтах.",
    "media_download_max_bytes": "Максимальный размер внешнего файла, который сервер разрешит скачать по URL.",
    "knowledge_upload_max_bytes": "Максимальный размер файла для импорта базы знаний в байтах.",
    "media_allowed_mime_types": "Разрешённые MIME-типы медиа, через запятую. Всё остальное будет отклонено.",
    "knowledge_allowed_mime_types": "Разрешённые MIME-типы для файлов базы знаний, через запятую.",
    "polling_interval_seconds": "Как часто резервный опрос обращается к Авито, если вебхук недоступен или нужен как запасной канал.",
    "sync_page_limit": "Сколько чатов или сообщений брать за один запрос к API при синке.",
    "sync_max_pages": "Сколько страниц подряд читать за один запуск синка.",
    "webhook_query_param": "Имя query-параметра для режима совместимости. По умолчанию безопаснее использовать только заголовки и подпись, без секрета в ссылке.",
    "webhook_signature_header": "Имя заголовка с подписью вебхука.",
    "webhook_timestamp_header": "Имя заголовка с временем отправки вебхука.",
    "webhook_nonce_header": "Имя заголовка с одноразовым nonce для защиты от повторов.",
    "webhook_event_id_header": "Имя заголовка с уникальным идентификатором webhook-события.",
    "webhook_allowed_skew_seconds": "Допустимая разница во времени между отправителем и сервером при проверке webhook.",
    "webhook_nonce_ttl_seconds": "Сколько секунд хранить nonce, чтобы защититься от повторной доставки того же события.",
    "browser_fallback_enabled": "Если API недоступен, модуль сможет попытаться дочитать или дослать данные через браузерный контур.",
    "polling_fallback_enabled": "Если webhook не приходит, модуль продолжит получать новые сообщения через периодический polling.",
    "webhook_first_enabled": "Использовать webhook как основной источник новых событий.",
    "webhook_auto_generate_draft": "Сразу генерировать черновик ответа при поступлении webhook-события.",
    "webhook_require_signature": "Если подпись пришла в заголовке, модуль будет считать её обязательной.",
    "api_retry_budget": "Сколько повторных попыток делать при временных ошибках API.",
    "api_backoff_base_seconds": "Базовая пауза для экспоненциального увеличения задержки между повторами.",
    "api_backoff_max_seconds": "Максимальная пауза между повторными запросами.",
    "api_max_requests_per_minute": "Лимит запросов в минуту для одного кабинета.",
    "api_min_request_interval_ms": "Минимальный интервал между запросами в миллисекундах.",
    "api_circuit_breaker_threshold": "Сколько подряд серьёзных ошибок допускается до открытия предохранителя ошибок.",
    "api_circuit_breaker_cooldown_seconds": "Через сколько секунд после открытия предохранителя ошибок модуль попробует восстановиться.",
    "rbac_view_users": "Пользователи, которым разрешён просмотр Avito-вкладки. Указывай логины через запятую.",
    "rbac_reply_users": "Пользователи, которые могут вручную отвечать в чатах Авито.",
    "rbac_bulk_send_users": "Пользователи, которые могут массово отправлять готовые ответы.",
    "rbac_ai_rules_users": "Пользователи, которым разрешено менять AI-правила, базу знаний и очередь ревью.",
    "rbac_connect_users": "Пользователи, которые могут менять подключение, webhook и browser state.",
    "rbac_secret_users": "Пользователи, которым разрешено видеть секреты в интерфейсе без маскировки.",
    "rbac_admin_users": "Полные администраторы Avito-модуля. Имеют доступ ко всем действиям.",
    "queue_states": "Какие состояния черновиков показывать в очереди: на проверке, отложено, ошибка и другие.",
    "status_filter": "Фильтр списка чатов по рабочему статусу.",
    "unanswered_only": "Показывать только диалоги, где ещё нет исходящего ответа после последнего входящего сообщения.",
    "with_bargain_only": "Показывать диалоги с признаками торга или запроса скидки.",
    "with_media_only": "Показывать только диалоги, где для ответа уже выбраны медиа-материалы.",
    "needs_human_only": "Показывать только диалоги, которые правила отправляют человеку.",
    "overdue_only": "Показывать только диалоги, где просрочен первый ответ по SLA.",
    "assigned_to_filter": "Показывать только чаты, назначенные конкретному оператору.",
    "operator_bucket": "Выбирает рабочую очередь: моя, неназначенные, просроченные, эскалация и другие сценарии.",
    "operator_action": "Что сделать с выбранными диалогами: назначить, взять в работу, перевести в ожидание, закрыть и т.д.",
    "operator_note": "Короткий комментарий к массовому действию. Он добавится в заметку чата.",
    "operator_limit": "Сколько диалогов показать на странице операторской очереди.",
    "kb_search": "Поиск по названию, тексту, товару и тегам документов базы знаний.",
    "kb_kind": "Фильтр по типу документа базы знаний.",
    "media_search": "Поиск по названию, подписи, товару и тегам медиа.",
    "media_kind": "Фильтр по типу материала.",
    "import_file": "Файл, из которого будут импортированы документы базы знаний или медиа-материалы.",
    "media_allow_external_fetch": "Разрешать серверу скачивать внешний файл по URL. По умолчанию лучше держать выключенным, чтобы не открыть SSRF-риски.",
    "media_allowed_external_hosts": "Список разрешённых внешних доменов для серверной загрузки файлов. Указывай через запятую, например cdn.example.com, img.example.ru.",
    "media_upload_max_bytes": "Максимальный размер файла, который можно загрузить в реестр медиа через интерфейс, в байтах.",
    "media_download_max_bytes": "Максимальный размер файла, который сервер может скачать по внешней ссылке, в байтах.",
    "knowledge_upload_max_bytes": "Максимальный размер файла импорта базы знаний, в байтах.",
    "media_allowed_mime_types": "Разрешённые MIME-типы для загружаемых медиа. Указывай через запятую, например image/jpeg, image/png, video/mp4.",
    "knowledge_allowed_mime_types": "Разрешённые MIME-типы для файлов импорта базы знаний. Указывай через запятую, например text/plain, text/markdown, application/pdf.",
    "health_alerts_enabled": "Включает расчёт сигналов здоровья и журнал алертов для Avito-модуля.",
    "health_webhook_silent_hours": "Через сколько часов без webhook-событий считать канал подозрительно тихим.",
    "health_alert_webhook_verify_fail_pct": "Порог процента отклонённых webhook-событий за сутки, после которого появится тревога.",
    "health_alert_token_refresh_events_60m": "Порог числа обновлений токена за час. Если превышен — возможны проблемы с авторизацией API.",
    "health_alert_circuit_open_events_60m": "Сколько срабатываний circuit breaker за час считать деградацией API.",
    "health_alert_browser_fallback_share_pct": "Если резерв через браузер используется слишком часто, модуль поднимет предупреждение.",
    "health_alert_send_errors_24h": "Порог ошибок отправки за последние 24 часа, после которого нужно разбираться с доставкой сообщений.",
    "health_alert_overdue_queue_count": "Минимум просроченных по SLA диалогов, после которого операторская очередь считается проблемной.",
    "health_alert_poll_lag_seconds": "Через сколько секунд без poll-sync резервный опрос считается отстающим.",
    "health_alert_dlq_open_count": "Сколько открытых DLQ-событий можно терпеть до появления предупреждения.",
    "health_alert_knowledge_hit_rate_min_pct": "Минимальный допустимый процент попаданий базы знаний в ответы. Если меньше — KB работает слабо.",
}


def _ui_option_label(group: str, value: Any) -> str:
    value = clean_text(value)
    return AVITO_UI_OPTION_LABELS.get(clean_text(group), {}).get(value, value or "—")


def _field_hint(name: str) -> str:
    return AVITO_FIELD_HINTS.get(clean_text(name), "")


def _safe_context_part(service: AvitoService, name: str, default: Any, callback):
    try:
        return callback()
    except Exception as exc:
        log_avito_event(
            service.storage,
            channel="ops",
            stage="avito_ui_context_error",
            message=f"Не удалось собрать блок контекста: {name}",
            kind="avito_ops",
            level="error",
            section=name,
            error=str(exc),
        )
        return default


@avito_bp.app_context_processor
def _inject_template_globals() -> dict[str, object]:
    try:
        view_functions = set((current_app.view_functions or {}).keys())
    except Exception:
        view_functions = set()
    return {
        "current_app": current_app,
        "host_view_functions": view_functions,
        "host_has_view": lambda name: name in view_functions,
        "avito_option_label": _ui_option_label,
        "avito_field_hint": _field_hint,
    }


def _get_base_dir() -> Optional[Path]:
    cfg = current_app.extensions.get("avito_module", {}) if current_app else {}
    base_dir = cfg.get("base_dir")
    return Path(str(base_dir)) if base_dir else None



def _safe_int(value: Any, default: int = 0, *, minimum: int = 0, maximum: int = 10_000) -> int:
    try:
        parsed = int(value or default)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))






def _safe_float(value: Any, default: float = 0.0, *, minimum: float = 0.0, maximum: float = 1_000_000.0) -> float:
    try:
        parsed = float(value if value not in (None, "") else default)
    except Exception:
        parsed = float(default)
    return max(minimum, min(maximum, parsed))



def _csv_list(value: Any) -> list[str]:
    raw = str(value or "")
    return [clean_text(part) for part in raw.replace(";", ",").split(",") if clean_text(part)]



def _safe_fs_name(value: str) -> str:
    safe = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._-]+", "_", clean_text(value) or "file")
    return safe[:120] or "file"


def _is_safe_external_url(value: str) -> bool:
    value = clean_text(value)
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    scheme = clean_text(parsed.scheme).lower()
    host = clean_text(parsed.hostname).lower()
    if scheme not in {"http", "https"} or not host:
        return False
    if host in {"localhost", "127.0.0.1", "::1"} or host.endswith('.local'):
        return False
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            return False
    except ValueError:
        pass
    try:
        cfg = _load_config_only()
        allowlist = [clean_text(item).lower() for item in (cfg.media_allowed_external_hosts or []) if clean_text(item)]
        if allowlist and not any(host == domain or host.endswith('.' + domain) for domain in allowlist):
            return False
    except Exception:
        pass
    return True



def _read_uploaded_text(*, max_bytes: int = 2 * 1024 * 1024, allowed_mime_types: Optional[list[str]] = None) -> str:
    payload, _filename = _read_uploaded_bytes("upload_file", max_bytes=max_bytes, allowed_mime_types=allowed_mime_types)
    if not payload:
        return ""
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return payload.decode(encoding)
        except Exception:
            continue
    return payload.decode("utf-8", errors="ignore")


def _read_uploaded_bytes(
    field_name: str = "upload_file",
    *,
    max_bytes: Optional[int] = None,
    allowed_mime_types: Optional[list[str]] = None,
) -> tuple[bytes, str]:
    upload = request.files.get(field_name)
    if not upload or not clean_text(getattr(upload, "filename", "")):
        return b"", ""
    if max_bytes is not None:
        try:
            declared = int(getattr(upload, "content_length", 0) or 0)
        except Exception:
            declared = 0
        if declared and declared > max(1, int(max_bytes)):
            raise ValueError(f"Размер файла превышает лимит {int(max_bytes)} байт")
    mime_type = clean_text(getattr(upload, "mimetype", "")).lower()
    allowed = {clean_text(item).lower() for item in (allowed_mime_types or []) if clean_text(item)}
    if allowed and mime_type and mime_type not in allowed:
        raise ValueError(f"Тип файла не разрешён: {mime_type}")
    payload = upload.read() or b""
    if max_bytes is not None and len(payload) > max(1, int(max_bytes)):
        raise ValueError(f"Размер файла превышает лимит {int(max_bytes)} байт")
    return payload, clean_text(getattr(upload, "filename", ""))


def _save_uploaded_media(storage: AvitoStorage, config: AvitoModuleConfig) -> tuple[str, str, str]:
    upload = request.files.get("media_file")
    if not upload or not clean_text(getattr(upload, "filename", "")):
        return "", "", ""
    original = clean_text(upload.filename)
    payload, _ = _read_uploaded_bytes(
        "media_file",
        max_bytes=max(1, int(config.media_upload_max_bytes or 8 * 1024 * 1024)),
        allowed_mime_types=list(config.media_allowed_mime_types or []),
    )
    mime_type = clean_text(getattr(upload, "mimetype", "")).lower()
    file_name = f"{int(time.time())}_{_safe_fs_name(original)}"
    target = storage.paths.media_dir / file_name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return str(target), file_name, mime_type


def _active_tenant_id() -> str:
    for candidate in (
        request.values.get("tenant_id"),
        getattr(g, "active_tenant_id", None),
        session.get("tenant_id"),
        current_app.config.get("AVITO_MODULE_DEFAULT_TENANT_ID"),
        "default",
    ):
        value = clean_text(candidate)
        if value:
            return value
    return "default"



def _current_user() -> str:
    return clean_text(session.get("auth_user") or getattr(g, "current_user", ""))



def _load_config_only(tenant_id: Optional[str] = None) -> AvitoModuleConfig:
    tenant_id = clean_text(tenant_id) or _active_tenant_id()
    cfg_override = current_app.config.get("AVITO_MODULE_CONFIG_OVERRIDES", {}) if current_app else {}
    tenant = resolve_tenant(tenant_id)
    return AvitoModuleConfig.from_sources(
        tenant_id,
        tenant=tenant,
        settings_override=cfg_override.get(tenant_id) if isinstance(cfg_override, dict) else None,
        base_dir=_get_base_dir(),
    )



def _service(tenant_id: Optional[str] = None) -> AvitoService:
    tenant_id = clean_text(tenant_id) or _active_tenant_id()
    cfg = _load_config_only(tenant_id)
    storage = AvitoStorage(tenant_id, base_dir=_get_base_dir())
    return AvitoService(tenant_id, config=cfg, storage=storage)



def _rbac_denied(permission: str, tenant_id: str) -> Response:
    username = _current_user()
    log_event(
        "avito_security",
        "rbac_denied",
        tenant_id=tenant_id,
        level="warning",
        permission=permission,
        user=username,
        path=request.path,
        method=request.method,
    )
    if request.path.startswith("/avito/api") or request.accept_mimetypes.best == "application/json":
        return jsonify({"ok": False, "error": "forbidden", "permission": permission}), 403
    flash(f"Недостаточно прав для действия: {permission}.", "error")
    return redirect(url_for("avito_module.avito_index", tenant_id=tenant_id))



def _check_permission(config: AvitoModuleConfig, permission: str) -> Optional[Response]:
    username = _current_user()
    if config.can_user(username, permission):
        return None
    return _rbac_denied(permission, config.tenant_id)


@avito_bp.before_request
def _avito_view_guard() -> Optional[Response]:
    if request.endpoint == "avito_module.avito_webhook":
        return None
    config = _load_config_only()
    g.avito_config_acl = config
    return _check_permission(config, "view")



def _browser_state_meta(service: AvitoService) -> Dict[str, Any]:
    state_path = service.storage.paths.browser_state_file
    state_meta_path = service.storage.paths.browser_profile_file
    profile_meta: Dict[str, Any] = {}
    if state_meta_path.exists():
        try:
            import json

            payload = json.loads(state_meta_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                profile_meta = payload
        except Exception:
            profile_meta = {}
    return {
        "exists": state_path.exists(),
        "path": str(state_path),
        "saved_at": clean_text(profile_meta.get("saved_at")) or (state_path and state_path.exists() and str(int(state_path.stat().st_mtime)) or ""),
        "meta": profile_meta,
    }



def _webhook_public_url(service: AvitoService) -> str:
    return url_for("avito_module.avito_webhook", tenant_id=service.tenant_id, _external=True)



def _masked(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if len(value) <= 6:
        return "••••••"
    return value[:3] + "••••••" + value[-2:]



def _config_edit_payload(service: AvitoService) -> Dict[str, Any]:
    config = service.config
    can_view_secrets = config.can_user(_current_user(), "secret_view") or config.can_user(_current_user(), "connect")
    payload = config.to_storage_dict(include_secrets=can_view_secrets)
    if not can_view_secrets:
        payload.update(
            {
                "client_id": _masked(config.client_id),
                "client_secret": _masked(config.client_secret),
                "user_id": _masked(config.user_id),
                "ai_api_key": _masked(config.ai_api_key),
                "webhook_secret": _masked(config.webhook_secret),
            }
        )
    payload["can_view_secrets"] = can_view_secrets
    return payload



def _permission_snapshot(config: AvitoModuleConfig) -> Dict[str, bool]:
    username = _current_user()
    return {
        "view": config.can_user(username, "view"),
        "reply": config.can_user(username, "reply"),
        "bulk_send": config.can_user(username, "bulk_send"),
        "ai_rules": config.can_user(username, "ai_rules"),
        "connect": config.can_user(username, "connect"),
        "secret_view": config.can_user(username, "secret_view"),
        "admin": config.can_user(username, "admin"),
    }



def _common_context(service: AvitoService) -> Dict[str, Any]:
    tenant = _safe_context_part(service, "tenant", {}, lambda: resolve_tenant(service.tenant_id))
    latest_jobs = _safe_context_part(service, "jobs", [], lambda: list_latest_jobs(service.tenant_id, limit=8))
    recent_runs = _safe_context_part(service, "recent_runs", [], lambda: service.storage.list_recent_runs(limit=12))
    latest_run = recent_runs[0] if recent_runs else {}
    metrics = _safe_context_part(service, "metrics", {}, lambda: service.metrics_snapshot())
    health = _safe_context_part(service, "health", {"alerts": [], "alert_counts": {"error": 0, "warning": 0}}, lambda: service.health_snapshot())
    recent_dlq = _safe_context_part(service, "recent_dlq", [], lambda: service.storage.list_dead_letters(limit=6))
    recent_webhooks = _safe_context_part(service, "recent_webhooks", [], lambda: service.storage.list_webhook_events(limit=8))
    last_sync = _safe_context_part(service, "last_sync", {}, lambda: service.storage.load_sync_state("last_sync", {}))
    last_backfill = _safe_context_part(service, "last_backfill", {}, lambda: service.storage.load_sync_state("last_backfill", {}))
    browser_state = _safe_context_part(service, "browser_state", {"exists": False, "path": ""}, lambda: _browser_state_meta(service))
    webhook_url = _safe_context_part(service, "webhook_url", "", lambda: _webhook_public_url(service))
    return {
        "page_title": "Входящие Avito",
        "active_tenant_id": service.tenant_id,
        "active_tenant": tenant,
        "avito_config": service.config.to_public_dict(),
        "avito_config_edit": _config_edit_payload(service),
        "avito_permissions": _permission_snapshot(service.config),
        "avito_last_sync": last_sync,
        "avito_last_backfill": last_backfill,
        "avito_browser_state": browser_state,
        "avito_jobs": latest_jobs,
        "avito_recent_runs": recent_runs,
        "avito_latest_run": latest_run,
        "avito_logs_root": str(service.storage.paths.avito_logs_dir),
        "avito_webhook_url": webhook_url,
        "avito_metrics": metrics,
        "avito_health": health,
        "avito_media_send_enabled": bool(service.config.media_send_enabled),
        "avito_recent_dlq": recent_dlq,
        "avito_recent_webhooks": recent_webhooks,
        "avito_current_user": _current_user(),
    }



def _submit_tenant_job(kind: str, label: str, target, *, unique_key: str = "", **kwargs: Any) -> Tuple[bool, Dict[str, Any]]:
    tenant_id = _active_tenant_id()
    if background_jobs_available():
        job, created = submit_background_job(
            kind=kind,
            tenant_id=tenant_id,
            label=label,
            target=target,
            kwargs=kwargs,
            unique_key=unique_key or kind,
        )
        job_id = clean_text(job.get("job_id"))
        if created:
            flash(f"{label}: задача поставлена в фон. Job ID: {job_id}.", "success")
        else:
            flash(f"{label}: похожая задача уже выполняется. Job ID: {job_id}.", "success")
        return True, job
    result = target(tenant_id, **kwargs)
    return False, result if isinstance(result, dict) else {"result": result}



def _flash_inline_result(label: str, payload: Dict[str, Any]) -> None:
    message = clean_text(payload.get("message")) or label
    category = "error" if int(payload.get("failed") or 0) > 0 else "success"
    run_id = clean_text(payload.get("run_id"))
    if run_id:
        message = f"{message} Run ID: {run_id}."
    flash(message, category)
    notes = payload.get("notes") if isinstance(payload.get("notes"), list) else []
    if notes:
        flash(" ; ".join(clean_text(x) for x in notes[:3] if clean_text(x)), category)


def _ui_safe_error_message(prefix: str, exc: Exception, *, run_id: str = "") -> str:
    message = prefix.rstrip('.')
    if run_id:
        return f"{message}. Код запуска: {run_id}. Подробности смотри в логах Avito."
    return f"{message}. Подробности смотри в логах Avito."



def _signature_candidates(raw_body: bytes, config: AvitoModuleConfig, *, timestamp: str = "", nonce: str = "") -> Dict[str, str]:
    candidates: Dict[str, str] = {}
    secret = clean_text(config.webhook_secret)
    if secret:
        candidates["hmac_webhook_secret"] = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if timestamp:
            candidates["hmac_webhook_secret_ts"] = hmac.new(secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + raw_body, hashlib.sha256).hexdigest()
        if timestamp and nonce:
            candidates["hmac_webhook_secret_ts_nonce"] = hmac.new(secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + nonce.encode("utf-8") + b"." + raw_body, hashlib.sha256).hexdigest()
    client_secret = clean_text(config.client_secret)
    if client_secret:
        candidates["hmac_client_secret"] = hmac.new(client_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if timestamp:
            candidates["hmac_client_secret_ts"] = hmac.new(client_secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + raw_body, hashlib.sha256).hexdigest()
    if bool(config.webhook_allow_legacy_plain_sha256):
        candidates["sha256_legacy"] = hashlib.sha256(raw_body).hexdigest()
    return candidates


def _parse_webhook_timestamp(value: str) -> Optional[float]:
    value = clean_text(value)
    if not value:
        return None
    try:
        if value.isdigit():
            raw = int(value)
            if raw > 10_000_000_000:
                return raw / 1000.0
            return float(raw)
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc).timestamp()
    except Exception:
        return None



def _verify_webhook_request(service: AvitoService) -> Tuple[bool, Dict[str, Any]]:
    config = service.config
    raw_body = request.get_data(cache=True) or b""
    provided_signature = clean_text(
        request.headers.get(config.webhook_signature_header)
        or request.headers.get(config.webhook_signature_header.upper())
        or request.headers.get(config.webhook_signature_header.title())
    )
    timestamp = clean_text(request.headers.get(config.webhook_timestamp_header) or request.headers.get("X-Webhook-Timestamp"))
    nonce = clean_text(request.headers.get(config.webhook_nonce_header) or request.headers.get("X-Webhook-Nonce"))
    header_event_id = clean_text(request.headers.get(config.webhook_event_id_header) or request.headers.get("X-Webhook-Id"))
    dedupe_key = clean_text(request.headers.get("Idempotency-Key") or header_event_id or provided_signature or hashlib.sha256(raw_body).hexdigest())
    secret = clean_text(config.webhook_secret)
    candidates = _signature_candidates(raw_body, config, timestamp=timestamp, nonce=nonce)
    verified_by = ""
    if provided_signature:
        for mode, candidate in candidates.items():
            if candidate and hmac.compare_digest(candidate, provided_signature):
                verified_by = f"signature:{mode}"
                break
        if not verified_by:
            return False, {"reason": "signature_mismatch", "event_id": header_event_id or dedupe_key, "dedupe_key": dedupe_key, "timestamp": timestamp, "nonce": nonce, "signature": provided_signature}
    elif config.webhook_require_signature:
        return False, {"reason": "signature_missing", "event_id": header_event_id or dedupe_key, "dedupe_key": dedupe_key, "timestamp": timestamp, "nonce": nonce}

    parsed_ts = _parse_webhook_timestamp(timestamp)
    if timestamp and parsed_ts is None:
        return False, {"reason": "timestamp_invalid", "event_id": header_event_id or dedupe_key, "dedupe_key": dedupe_key, "timestamp": timestamp, "nonce": nonce, "signature": provided_signature}
    if parsed_ts is not None:
        skew = abs(time.time() - parsed_ts)
        if skew > max(10, int(config.webhook_allowed_skew_seconds or 900)):
            return False, {"reason": "timestamp_out_of_window", "event_id": header_event_id or dedupe_key, "dedupe_key": dedupe_key, "timestamp": timestamp, "nonce": nonce, "signature": provided_signature, "skew_seconds": round(skew, 2)}
    if nonce and not service.storage.remember_nonce(nonce, ttl_seconds=max(60, int(config.webhook_nonce_ttl_seconds or 900))):
        return False, {"reason": "nonce_replayed", "event_id": header_event_id or dedupe_key, "dedupe_key": dedupe_key, "timestamp": timestamp, "nonce": nonce, "signature": provided_signature}
    return True, {
        "verified_by": verified_by,
        "event_id": header_event_id or dedupe_key,
        "dedupe_key": dedupe_key,
        "timestamp": timestamp,
        "nonce": nonce,
        "signature": provided_signature,
    }


@avito_bp.route("/avito")
def avito_index() -> str:
    service = _service()
    try:
        status = clean_text(request.args.get("status") or "all")
        unanswered_only = request.args.get("unanswered") in {"1", "true", "yes"}
        assigned_to = clean_text(request.args.get("assigned_to") or "")
        overdue_only = request.args.get("overdue") in {"1", "true", "yes"}
        needs_human_only = request.args.get("needs_human") in {"1", "true", "yes"}
        with_media_only = request.args.get("with_media") in {"1", "true", "yes"}
        with_bargain_only = request.args.get("with_bargain") in {"1", "true", "yes"}
        chats = service.storage.list_chats(
            status=status,
            only_unanswered=unanswered_only,
            assigned_to=assigned_to,
            overdue_only=overdue_only,
            needs_human_only=needs_human_only,
            with_media_only=with_media_only,
            with_bargain_only=with_bargain_only,
            limit=100,
            offset=0,
        )
        rows = []
        for chat in chats:
            chat["draft"] = service.storage.get_draft(chat["chat_id"])
            chat["flags"] = service.storage.chat_flags(chat["chat_id"])
            rows.append(chat)
        return render_template(
            "avito/index.html",
            chats=rows,
            status=status,
            unanswered_only=unanswered_only,
            assigned_to_filter=assigned_to,
            overdue_only=overdue_only,
            needs_human_only=needs_human_only,
            with_media_only=with_media_only,
            with_bargain_only=with_bargain_only,
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/queue")
def avito_queue() -> str:
    service = _service()
    try:
        states = _csv_list(request.args.get("states") or "review,hold,error")
        queue_items = service.review_queue_snapshot(states=states, limit=200)
        return render_template(
            "avito/queue.html",
            queue_items=queue_items,
            queue_states=states,
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/health")
def avito_health() -> str:
    service = _service()
    try:
        dashboard = service.health_dashboard_snapshot(persist_alerts=True)
        return render_template("avito/health.html", health_dashboard=dashboard, **_common_context(service))
    finally:
        service.close()


@avito_bp.route("/avito/health/ack/<int:alert_id>", methods=["POST"])
def avito_health_ack(alert_id: int) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "admin")
        if denied:
            return denied
        actor = _current_user()
        ok = service.storage.acknowledge_health_alert(alert_id, actor=actor)
        if ok:
            log_avito_event(service.storage, channel="health", stage="avito_health_alert_ack", message=f"Подтверждён сигнал здоровья #{alert_id}", kind="avito_health", alert_id=int(alert_id), actor=actor)
            flash("Сигнал здоровья помечен как подтверждённый.", "success")
        else:
            flash("Подходящий активный сигнал не найден или он уже закрыт.", "error")
        return redirect(url_for("avito_module.avito_health", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/operator")
def avito_operator() -> str:
    service = _service()
    try:
        denied = _check_permission(service.config, "view")
        if denied:
            return denied
        current_user = _current_user()
        bucket = clean_text(request.args.get("bucket") or ("mine" if current_user else "team"))
        status = clean_text(request.args.get("status") or "all")
        assignee = clean_text(request.args.get("assigned_to") or "")
        only_unanswered = request.args.get("unanswered") in {"1", "true", "yes"}
        limit = _safe_int(request.args.get("limit"), 120, minimum=10, maximum=500)
        dashboard = service.operator_dashboard_snapshot(actor=current_user)
        rows = service.operator_queue_snapshot(
            bucket=bucket,
            actor=current_user,
            assignee=assignee,
            status=status,
            limit=limit,
            only_unanswered=only_unanswered,
        )
        return render_template(
            "avito/operator.html",
            operator_dashboard=dashboard,
            operator_rows=rows,
            operator_bucket=bucket,
            operator_status_filter=status,
            operator_assignee_filter=assignee,
            operator_unanswered=only_unanswered,
            operator_limit=limit,
            operator_assignees=dashboard.get("assignees") or [],
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/operator/action", methods=["POST"])
def avito_operator_action() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        current_user = _current_user()
        action = clean_text(request.form.get("action"))
        assignee = clean_text(request.form.get("assignee"))
        note = clean_text(request.form.get("note"))
        chat_ids = request.form.getlist("chat_ids")
        result = service.apply_operator_action(chat_ids, action=action, actor=current_user, assignee=assignee, note=note)
        updated = int(result.get("updated") or 0)
        if updated > 0:
            flash(f"Операторское действие выполнено: {result.get('action_label')}. Обновлено диалогов: {updated}.", "success")
        else:
            flash("Подходящие диалоги для обновления не найдены.", "error")
        redirect_args = {"tenant_id": service.tenant_id}
        for source_key, target_key in (("bucket", "bucket"), ("status", "status"), ("assigned_to", "assigned_to"), ("limit", "limit")):
            value = clean_text(request.form.get(source_key))
            if value:
                redirect_args[target_key] = value
        if request.form.get("unanswered") in {"1", "true", "yes", "on"}:
            redirect_args["unanswered"] = 1
        return redirect(url_for("avito_module.avito_operator", **redirect_args))
    except ValueError as exc:
        flash(str(exc), "error")
        redirect_args = {"tenant_id": service.tenant_id}
        for source_key, target_key in (("bucket", "bucket"), ("status", "status"), ("assigned_to", "assigned_to"), ("limit", "limit")):
            value = clean_text(request.form.get(source_key))
            if value:
                redirect_args[target_key] = value
        if request.form.get("unanswered") in {"1", "true", "yes", "on"}:
            redirect_args["unanswered"] = 1
        return redirect(url_for("avito_module.avito_operator", **redirect_args))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/operator-action", methods=["POST"])
def avito_chat_operator_action(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        action = clean_text(request.form.get("action"))
        assignee = clean_text(request.form.get("assignee"))
        note = clean_text(request.form.get("note"))
        result = service.apply_operator_action([chat_id], action=action, actor=_current_user(), assignee=assignee, note=note)
        updated = int(result.get("updated") or 0)
        if updated > 0:
            flash(f"Операторское действие выполнено: {result.get('action_label')}.", "success")
        else:
            flash("Чат не удалось обновить указанным действием.", "error")
        next_url = clean_text(request.form.get("next"))
        if next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>")
def avito_chat(chat_id: str) -> str:
    service = _service()
    try:
        chat = service.storage.get_chat(chat_id)
        if not chat:
            flash("Чат не найден. Сначала выполните синхронизацию.", "error")
            return redirect(url_for("avito_module.avito_index", tenant_id=service.tenant_id))
        messages = service.storage.get_messages(chat_id, limit=250)
        draft = service.storage.get_draft(chat_id)
        advisory = service.chat_context_snapshot(chat_id)
        selected_media = service.storage.list_draft_media_assets(chat_id)
        return render_template(
            "avito/chat.html",
            chat=chat,
            messages=messages,
            draft=draft,
            knowledge_hits=advisory.get("knowledge_hits") or [],
            media_suggestions=advisory.get("media_suggestions") or [],
            similar_dialogs=advisory.get("similar_dialogs") or [],
            selected_media=selected_media,
            send_history=advisory.get("send_history") or [],
            chat_flags=advisory.get("flags") or {},
            decision_level=advisory.get("decision_level") or clean_text((draft or {}).get("meta", {}).get("decision_level") if isinstance((draft or {}).get("meta"), dict) else ""),
            scenario=advisory.get("scenario") or clean_text((draft or {}).get("meta", {}).get("scenario") if isinstance((draft or {}).get("meta"), dict) else ""),
            blocked_by=advisory.get("blocked_by") or clean_text((draft or {}).get("meta", {}).get("blocked_by") if isinstance((draft or {}).get("meta"), dict) else ""),
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/settings", methods=["GET"])
def avito_settings() -> str:
    service = _service()
    try:
        return render_template("avito/settings.html", config_edit=_config_edit_payload(service), **_common_context(service))
    finally:
        service.close()


@avito_bp.route("/avito/settings/ai", methods=["POST"])
def avito_settings_ai() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        service.config.auto_mode = clean_text(request.form.get("auto_mode") or service.config.auto_mode)
        service.config.ai_model = clean_text(request.form.get("ai_model") or service.config.ai_model)
        service.config.system_prompt = str(request.form.get("system_prompt") or service.config.system_prompt)
        service.config.knowledge_text = str(request.form.get("knowledge_text") or "")
        service.config.auto_send_confidence_threshold = _safe_float(request.form.get("auto_send_confidence_threshold"), service.config.auto_send_confidence_threshold, minimum=0.0, maximum=1.0)
        service.config.max_context_messages = _safe_int(request.form.get("max_context_messages"), service.config.max_context_messages, minimum=1, maximum=100)
        service.config.knowledge_enabled = request.form.get("knowledge_enabled") == "on"
        service.config.knowledge_mode = clean_text(request.form.get("knowledge_mode") or service.config.knowledge_mode)
        service.config.knowledge_max_hits = _safe_int(request.form.get("knowledge_max_hits"), service.config.knowledge_max_hits, minimum=1, maximum=20)
        service.config.knowledge_min_score = _safe_float(request.form.get("knowledge_min_score"), service.config.knowledge_min_score, minimum=0.0, maximum=5.0)
        service.config.knowledge_chunk_chars = _safe_int(request.form.get("knowledge_chunk_chars"), service.config.knowledge_chunk_chars, minimum=200, maximum=5000)
        service.config.knowledge_chunk_overlap_chars = _safe_int(request.form.get("knowledge_chunk_overlap_chars"), service.config.knowledge_chunk_overlap_chars, minimum=0, maximum=1500)
        service.config.knowledge_answer_style = clean_text(request.form.get("knowledge_answer_style") or service.config.knowledge_answer_style)
        service.config.similar_dialogs_enabled = request.form.get("similar_dialogs_enabled") == "on"
        service.config.similar_dialogs_max_hits = _safe_int(request.form.get("similar_dialogs_max_hits"), service.config.similar_dialogs_max_hits, minimum=1, maximum=12)
        service.config.similar_dialogs_min_score = _safe_float(request.form.get("similar_dialogs_min_score"), service.config.similar_dialogs_min_score, minimum=0.0, maximum=5.0)
        service.config.hitl_enabled = request.form.get("hitl_enabled") == "on"
        service.config.hitl_auto_ready_threshold = _safe_float(request.form.get("hitl_auto_ready_threshold"), service.config.hitl_auto_ready_threshold, minimum=0.0, maximum=1.0)
        service.config.hitl_queue_default_assignee = clean_text(request.form.get("hitl_queue_default_assignee") or service.config.hitl_queue_default_assignee)
        service.config.media_registry_enabled = request.form.get("media_registry_enabled") == "on"
        service.config.media_auto_suggest_enabled = request.form.get("media_auto_suggest_enabled") == "on"
        service.config.media_max_suggestions = _safe_int(request.form.get("media_max_suggestions"), service.config.media_max_suggestions, minimum=1, maximum=12)
        service.config.media_send_enabled = request.form.get("media_send_enabled") == "on"
        service.config.media_send_transport = clean_text(request.form.get("media_send_transport") or service.config.media_send_transport)
        service.config.media_max_send_assets = _safe_int(request.form.get("media_max_send_assets"), service.config.media_max_send_assets, minimum=1, maximum=20)
        service.config.media_send_images_only = request.form.get("media_send_images_only") == "on"
        service.config.media_browser_send_headless = request.form.get("media_browser_send_headless") == "on"
        service.config.media_api_upload_endpoint = clean_text(request.form.get("media_api_upload_endpoint") or service.config.media_api_upload_endpoint)
        service.config.media_api_send_endpoint = clean_text(request.form.get("media_api_send_endpoint") or service.config.media_api_send_endpoint)
        service.config.persist(base_dir=_get_base_dir())
        log_avito_event(
            service.storage,
            channel="ui",
            stage="avito_settings_ai_saved",
            message="Настройки ИИ, базы знаний и медиа для Avito сохранены",
            kind="avito_settings",
            auto_mode=service.config.auto_mode,
            ai_model=service.config.ai_model,
            knowledge_enabled=service.config.knowledge_enabled,
            knowledge_mode=service.config.knowledge_mode,
            similar_dialogs_enabled=service.config.similar_dialogs_enabled,
            similar_dialogs_max_hits=service.config.similar_dialogs_max_hits,
            hitl_enabled=service.config.hitl_enabled,
            hitl_auto_ready_threshold=service.config.hitl_auto_ready_threshold,
            media_registry_enabled=service.config.media_registry_enabled,
            media_auto_suggest_enabled=service.config.media_auto_suggest_enabled,
            media_send_enabled=service.config.media_send_enabled,
            media_send_transport=service.config.media_send_transport,
            media_max_send_assets=service.config.media_max_send_assets,
        )
        flash("Настройки ИИ, базы знаний и медиа для Avito сохранены.", "success")
        return redirect(url_for("avito_module.avito_settings", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/settings/security", methods=["POST"])
def avito_settings_security() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "connect")
        if denied:
            return denied
        def _list(name: str) -> list[str]:
            return [clean_text(x) for x in (request.form.get(name) or "").replace(";", ",").split(",") if clean_text(x)]

        service.config.client_id = clean_text(request.form.get("client_id"))
        service.config.client_secret = clean_text(request.form.get("client_secret"))
        service.config.user_id = clean_text(request.form.get("user_id"))
        service.config.ai_api_key = clean_text(request.form.get("ai_api_key"))
        service.config.ai_base_url = clean_text(request.form.get("ai_base_url"))
        service.config.webhook_secret = clean_text(request.form.get("webhook_secret"))
        service.config.webhook_require_signature = request.form.get("webhook_require_signature") == "on"
        service.config.webhook_query_param = clean_text(request.form.get("webhook_query_param") or service.config.webhook_query_param)
        service.config.webhook_signature_header = clean_text(request.form.get("webhook_signature_header") or service.config.webhook_signature_header)
        service.config.webhook_timestamp_header = clean_text(request.form.get("webhook_timestamp_header") or service.config.webhook_timestamp_header)
        service.config.webhook_nonce_header = clean_text(request.form.get("webhook_nonce_header") or service.config.webhook_nonce_header)
        service.config.webhook_event_id_header = clean_text(request.form.get("webhook_event_id_header") or service.config.webhook_event_id_header)
        service.config.webhook_allowed_skew_seconds = _safe_int(request.form.get("webhook_allowed_skew_seconds"), service.config.webhook_allowed_skew_seconds, minimum=60, maximum=3600)
        service.config.webhook_nonce_ttl_seconds = _safe_int(request.form.get("webhook_nonce_ttl_seconds"), service.config.webhook_nonce_ttl_seconds, minimum=60, maximum=3600)
        service.config.browser_fallback_enabled = request.form.get("browser_fallback_enabled") == "on"
        service.config.polling_fallback_enabled = request.form.get("polling_fallback_enabled") == "on"
        service.config.webhook_first_enabled = request.form.get("webhook_first_enabled") == "on"
        service.config.webhook_auto_generate_draft = request.form.get("webhook_auto_generate_draft") == "on"
        service.config.polling_interval_seconds = _safe_int(request.form.get("polling_interval_seconds"), service.config.polling_interval_seconds, minimum=5, maximum=3600)
        service.config.sync_page_limit = _safe_int(request.form.get("sync_page_limit"), service.config.sync_page_limit, minimum=1, maximum=500)
        service.config.sync_max_pages = _safe_int(request.form.get("sync_max_pages"), service.config.sync_max_pages, minimum=1, maximum=100)
        service.config.browser_bootstrap_timeout_seconds = _safe_int(request.form.get("browser_bootstrap_timeout_seconds"), service.config.browser_bootstrap_timeout_seconds, minimum=30, maximum=3600)
        service.config.api_retry_budget = _safe_int(request.form.get("api_retry_budget"), service.config.api_retry_budget, minimum=1, maximum=10)
        service.config.api_max_requests_per_minute = _safe_int(request.form.get("api_max_requests_per_minute"), service.config.api_max_requests_per_minute, minimum=1, maximum=600)
        service.config.api_min_request_interval_ms = _safe_int(request.form.get("api_min_request_interval_ms"), service.config.api_min_request_interval_ms, minimum=0, maximum=10_000)
        service.config.api_circuit_breaker_threshold = _safe_int(request.form.get("api_circuit_breaker_threshold"), service.config.api_circuit_breaker_threshold, minimum=1, maximum=20)
        service.config.api_circuit_breaker_cooldown_seconds = _safe_int(request.form.get("api_circuit_breaker_cooldown_seconds"), service.config.api_circuit_breaker_cooldown_seconds, minimum=5, maximum=3600)
        service.config.health_alerts_enabled = request.form.get("health_alerts_enabled") == "on"
        service.config.health_webhook_silent_hours = _safe_int(request.form.get("health_webhook_silent_hours"), service.config.health_webhook_silent_hours, minimum=1, maximum=168)
        service.config.health_alert_webhook_verify_fail_pct = _safe_float(request.form.get("health_alert_webhook_verify_fail_pct"), service.config.health_alert_webhook_verify_fail_pct, minimum=0, maximum=100)
        service.config.health_alert_token_refresh_events_60m = _safe_int(request.form.get("health_alert_token_refresh_events_60m"), service.config.health_alert_token_refresh_events_60m, minimum=1, maximum=500)
        service.config.health_alert_circuit_open_events_60m = _safe_int(request.form.get("health_alert_circuit_open_events_60m"), service.config.health_alert_circuit_open_events_60m, minimum=1, maximum=100)
        service.config.health_alert_browser_fallback_share_pct = _safe_float(request.form.get("health_alert_browser_fallback_share_pct"), service.config.health_alert_browser_fallback_share_pct, minimum=0, maximum=100)
        service.config.health_alert_send_errors_24h = _safe_int(request.form.get("health_alert_send_errors_24h"), service.config.health_alert_send_errors_24h, minimum=1, maximum=1000)
        service.config.health_alert_overdue_queue_count = _safe_int(request.form.get("health_alert_overdue_queue_count"), service.config.health_alert_overdue_queue_count, minimum=1, maximum=1000)
        service.config.health_alert_poll_lag_seconds = _safe_int(request.form.get("health_alert_poll_lag_seconds"), service.config.health_alert_poll_lag_seconds, minimum=30, maximum=86400)
        service.config.health_alert_dlq_open_count = _safe_int(request.form.get("health_alert_dlq_open_count"), service.config.health_alert_dlq_open_count, minimum=1, maximum=1000)
        service.config.health_alert_knowledge_hit_rate_min_pct = _safe_float(request.form.get("health_alert_knowledge_hit_rate_min_pct"), service.config.health_alert_knowledge_hit_rate_min_pct, minimum=0, maximum=100)
        service.config.media_allow_external_fetch = request.form.get("media_allow_external_fetch") == "on"
        service.config.media_allowed_external_hosts = _list("media_allowed_external_hosts")
        service.config.media_upload_max_bytes = _safe_int(request.form.get("media_upload_max_bytes"), service.config.media_upload_max_bytes, minimum=1_024, maximum=200 * 1024 * 1024)
        service.config.media_download_max_bytes = _safe_int(request.form.get("media_download_max_bytes"), service.config.media_download_max_bytes, minimum=1_024, maximum=200 * 1024 * 1024)
        service.config.knowledge_upload_max_bytes = _safe_int(request.form.get("knowledge_upload_max_bytes"), service.config.knowledge_upload_max_bytes, minimum=1_024, maximum=100 * 1024 * 1024)
        service.config.media_allowed_mime_types = _list("media_allowed_mime_types") or list(service.config.media_allowed_mime_types)
        service.config.knowledge_allowed_mime_types = _list("knowledge_allowed_mime_types") or list(service.config.knowledge_allowed_mime_types)
        service.config.rbac_view_users = _list("rbac_view_users")
        service.config.rbac_reply_users = _list("rbac_reply_users")
        service.config.rbac_bulk_send_users = _list("rbac_bulk_send_users")
        service.config.rbac_ai_rules_users = _list("rbac_ai_rules_users")
        service.config.rbac_connect_users = _list("rbac_connect_users")
        service.config.rbac_secret_users = _list("rbac_secret_users")
        service.config.rbac_admin_users = _list("rbac_admin_users")
        service.config.persist(base_dir=_get_base_dir())
        log_avito_event(service.storage, channel="security", stage="avito_settings_security_saved", message="Настройки безопасности Avito сохранены", kind="avito_settings_security", webhook_first_enabled=service.config.webhook_first_enabled, api_retry_budget=service.config.api_retry_budget, health_alerts_enabled=service.config.health_alerts_enabled, health_webhook_silent_hours=service.config.health_webhook_silent_hours)
        flash("Настройки безопасности Avito сохранены.", "success")
        return redirect(url_for("avito_module.avito_settings", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/sync", methods=["POST"])
def avito_sync() -> Response:
    queued, payload = _submit_tenant_job(
        kind="avito_sync",
        label="Синхронизация Avito",
        target=run_sync_job,
        unique_key="avito_sync",
        max_chats=_safe_int(request.form.get("max_chats"), 20, minimum=1, maximum=200),
        unread_only=request.form.get("unread_only") in {"1", "true", "yes", "on"},
    )
    if not queued:
        _flash_inline_result("Синхронизация Avito", payload)
    return redirect(url_for("avito_module.avito_index", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/backfill", methods=["POST"])
def avito_backfill() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "admin")
        if denied:
            return denied
    finally:
        service.close()
    queued, payload = _submit_tenant_job(
        kind="avito_backfill",
        label="Историческая дозагрузка Avito",
        target=run_backfill_job,
        unique_key="avito_backfill",
        max_chats=_safe_int(request.form.get("max_chats"), 200, minimum=1, maximum=2000),
        messages_per_chat=_safe_int(request.form.get("messages_per_chat"), 200, minimum=1, maximum=1000),
    )
    if not queued:
        _flash_inline_result("Avito backfill", payload)
    return redirect(url_for("avito_module.avito_index", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/drafts/generate", methods=["POST"])
def avito_generate_drafts() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
    finally:
        service.close()
    queued, payload = _submit_tenant_job(
        kind="avito_drafts_generate",
        label="Генерация черновиков Avito",
        target=run_generate_drafts_job,
        unique_key="avito_drafts_generate",
        limit=_safe_int(request.form.get("limit"), 20, minimum=1, maximum=200),
        force_regenerate=request.form.get("force_regenerate") in {"1", "true", "on", "yes"},
    )
    if not queued:
        _flash_inline_result("Черновики Avito готовы", payload)
    return redirect(url_for("avito_module.avito_index", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/drafts/send", methods=["POST"])
def avito_send_drafts() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "bulk_send")
        if denied:
            return denied
    finally:
        service.close()
    queued, payload = _submit_tenant_job(
        kind="avito_drafts_send",
        label="Отправка Avito-черновиков",
        target=run_send_drafts_job,
        unique_key="avito_drafts_send",
        limit=_safe_int(request.form.get("limit"), 20, minimum=1, maximum=200),
        auto_only=request.form.get("auto_only") == "1",
    )
    if not queued:
        _flash_inline_result("Отправка Avito-черновиков", payload)
    return redirect(url_for("avito_module.avito_index", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/browser/bootstrap", methods=["POST"])
def avito_browser_bootstrap() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "connect")
        if denied:
            return denied
    finally:
        service.close()
    timeout_seconds = _safe_int(request.form.get("timeout_seconds"), 300, minimum=30, maximum=3600)
    queued, payload = _submit_tenant_job(
        kind="avito_browser_bootstrap",
        label="Вход в Avito через браузер",
        target=run_browser_bootstrap_job,
        unique_key="avito_browser_bootstrap",
        timeout_seconds=timeout_seconds,
    )
    if queued:
        flash("Откроется окно браузера Avito. Выполните вход и дождитесь, пока задача сохранит state-файл.", "success")
    else:
        _flash_inline_result("Состояние браузера Avito сохранено", payload)
    return redirect(url_for("avito_module.avito_settings", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/chat/<chat_id>/reply", methods=["POST"])
def avito_manual_reply(chat_id: str) -> Response:
    service = _service()
    audit = AvitoAuditLogger(service.storage, kind="avito_manual_reply", label="Ручной ответ Avito", source="ui")
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        body = request.form.get("body") or ""
        selected_media_ids = []
        for raw in request.form.getlist("selected_media_ids"):
            raw = clean_text(raw)
            if raw.isdigit():
                selected_media_ids.append(int(raw))
        if selected_media_ids:
            service.storage.set_draft_media_assets(chat_id, selected_media_ids, source="manual_reply")
        audit.info("avito_manual_reply_start", "Пользователь отправляет ручной ответ в Avito", channel="ui", percent=0, chat_id=chat_id, selected_media_ids=selected_media_ids)
        if not clean_text(body):
            audit.warn("avito_manual_reply_empty", "Ручной ответ пустой", channel="ui", percent=100, chat_id=chat_id)
            audit.finish("warning", "Ручной ответ не отправлен: пустой текст", chat_id=chat_id)
            flash("Текст ответа пустой.", "error")
            return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
        duplicate = service.detect_recent_duplicate_send(chat_id, body, window_seconds=600)
        if duplicate:
            flash("Похожий ответ уже был отправлен недавно. Повторная отправка остановлена.", "warning")
            return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
        selected_media = service.storage.list_draft_media_assets(chat_id)
        response = service.send_chat_reply(chat_id, body, selected_media=selected_media)
        finalized = service.finalize_successful_send(
            chat_id,
            body,
            response if isinstance(response, dict) else {"result": response},
            selected_media=selected_media,
            draft_context=service.storage.get_draft(chat_id),
            author_name=clean_text(_current_user()) or "manual",
        )
        remote_message_id = clean_text(finalized.get("remote_message_id"))
        delivery_status = clean_text(finalized.get("delivery_status") or (response or {}).get("delivery_status") or "sent")
        audit.finish("completed", "Ручной ответ отправлен в Avito", chat_id=chat_id, remote_message_id=remote_message_id, delivery_status=delivery_status)
        if delivery_status == "partial_sent_text_only":
            flash("Текст отправлен, но вложения не ушли. Проверь медиа и журнал отправки в карточке чата.", "warning")
        else:
            flash("Ответ отправлен в Avito.", "success")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    except Exception as exc:
        audit.fail("Не удалось отправить ответ", chat_id=chat_id, error=str(exc))
        flash(_ui_safe_error_message("Не удалось отправить ответ", exc, run_id=audit.run_id), "error")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/draft/approve", methods=["POST"])
def avito_draft_approve(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        reviewer = _current_user()
        body = request.form.get("body") or None
        review_note = request.form.get("review_note") or ""
        updated = service.approve_draft(chat_id, reviewer=reviewer, review_note=review_note, body=body)
        if not updated:
            flash("Черновик не найден.", "error")
            return redirect(url_for("avito_module.avito_queue", tenant_id=service.tenant_id))
        log_avito_event(service.storage, channel="decision", stage="avito_draft_approved", message="Черновик Avito одобрен человеком", kind="avito_hitl", chat_id=chat_id, reviewer=reviewer, review_note=clean_text(review_note))
        if request.form.get("send_now") == "1":
            selected_media = service.storage.list_draft_media_assets(chat_id)
            duplicate = service.detect_recent_duplicate_send(chat_id, updated.get("body") or "", window_seconds=600)
            if duplicate:
                flash("Похожий ответ уже был отправлен недавно. Повторная отправка остановлена.", "warning")
                return redirect(url_for("avito_module.avito_queue", tenant_id=service.tenant_id))
            response = service.send_chat_reply(chat_id, updated.get("body") or "", selected_media=selected_media, draft_context=updated)
            finalized = service.finalize_successful_send(
                chat_id,
                updated.get("body") or "",
                response if isinstance(response, dict) else {"result": response},
                selected_media=selected_media,
                draft_context=updated,
                author_name=clean_text(_current_user()) or "assistant",
            )
            if clean_text(finalized.get("delivery_status")) == "partial_sent_text_only":
                flash("Черновик одобрен. Текст отправлен, но вложения не ушли.", "warning")
            else:
                flash("Черновик одобрен и отправлен.", "success")
        else:
            flash("Черновик одобрен и перемещён в ready-очередь.", "success")
        return redirect(url_for("avito_module.avito_queue", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/draft/hold", methods=["POST"])
def avito_draft_hold(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        reviewer = _current_user()
        review_note = request.form.get("review_note") or ""
        updated = service.hold_draft(chat_id, reviewer=reviewer, review_note=review_note)
        if not updated:
            flash("Черновик не найден.", "error")
        else:
            log_avito_event(service.storage, channel="decision", stage="avito_draft_held", message="Черновик Avito отложен человеком", kind="avito_hitl", chat_id=chat_id, reviewer=reviewer, review_note=clean_text(review_note))
            flash("Черновик отложен.", "success")
        return redirect(url_for("avito_module.avito_queue", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/draft/reject", methods=["POST"])
def avito_draft_reject(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        reviewer = _current_user()
        review_note = request.form.get("review_note") or ""
        updated = service.reject_draft(chat_id, reviewer=reviewer, review_note=review_note)
        if not updated:
            flash("Черновик не найден.", "error")
        else:
            log_avito_event(service.storage, channel="decision", stage="avito_draft_rejected", message="Черновик Avito отклонён человеком", kind="avito_hitl", chat_id=chat_id, reviewer=reviewer, review_note=clean_text(review_note))
            flash("Черновик отклонён.", "success")
        return redirect(url_for("avito_module.avito_queue", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/draft/regenerate", methods=["POST"])
def avito_draft_regenerate(chat_id: str) -> Response:
    service = _service()
    audit = AvitoAuditLogger(service.storage, kind="avito_draft_regenerate", label="Перегенерация черновика Avito", source="ui")
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        result = service.generate_drafts(limit=1, chat_ids=[chat_id], audit=audit, force_regenerate=True)
        audit.finish("completed", "Перегенерация черновика завершена", chat_id=chat_id, generated=result.generated)
        flash("Черновик перегенерирован.", "success")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    except Exception as exc:
        audit.fail(str(exc), chat_id=chat_id, error=str(exc))
        flash(_ui_safe_error_message("Не удалось перегенерировать черновик", exc, run_id=audit.run_id), "error")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/meta", methods=["POST"])
def avito_chat_meta(chat_id: str) -> Response:
    service = _service()
    try:
        tags = [clean_text(x) for x in (request.form.get("tags") or "").split(",") if clean_text(x)]
        status = clean_text(request.form.get("status"))
        note = request.form.get("note") or ""
        assigned_to = clean_text(request.form.get("assigned_to"))
        priority = clean_text(request.form.get("priority"))
        service.storage.update_chat_meta(chat_id, status=status, note=note, tags=tags, assigned_to=assigned_to, priority=priority, operator_user=_current_user())
        log_avito_event(service.storage, channel="ui", stage="avito_chat_meta_updated", message="Карточка чата Avito обновлена", kind="avito_chat_meta", chat_id=chat_id, status=status, tags=tags, assigned_to=assigned_to, priority=priority, note_preview=clean_text(note)[:240])
        flash("Карточка чата обновлена.", "success")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/promote", methods=["POST"])
def avito_chat_promote(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        mode = clean_text(request.form.get("mode") or "exemplar")
        doc_id = service.promote_chat_example(chat_id, mode=mode, actor=_current_user())
        flash(f"Диалог сохранён в базу знаний как операторский материал. Документ #{doc_id}.", "success")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    except Exception as exc:
        flash(_ui_safe_error_message("Не удалось сохранить диалог как эталон", exc), "error")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/knowledge")
def avito_knowledge() -> str:
    service = _service()
    try:
        search = clean_text(request.args.get("q") or "")
        kind = clean_text(request.args.get("kind") or "all")
        docs = service.storage.list_knowledge_docs(search=search, kind=kind, limit=200)
        return render_template(
            "avito/knowledge.html",
            kb_docs=docs,
            kb_search=search,
            kb_kind=kind,
            kb_kinds=["all", "faq", "listing_card", "policy", "shipping", "condition", "size", "script", "qa"],
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/knowledge/upsert", methods=["POST"])
def avito_knowledge_upsert() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        doc_id_raw = clean_text(request.form.get("doc_id"))
        try:
            uploaded_text = _read_uploaded_text(max_bytes=service.config.knowledge_upload_max_bytes, allowed_mime_types=list(service.config.knowledge_allowed_mime_types or []))
        except ValueError as exc:
            flash(_ui_safe_error_message("Операция Avito завершилась ошибкой", exc), "error")
            return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
        content = str(request.form.get("body_text") or "").strip() or uploaded_text
        if not content:
            flash("Нужно заполнить текст знания или загрузить текстовый файл.", "error")
            return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
        doc_id = int(doc_id_raw) if doc_id_raw.isdigit() else None
        saved_doc_id = service.storage.upsert_knowledge_doc(
            doc_id=doc_id,
            title=clean_text(request.form.get("title") or "Без названия"),
            body_text=content,
            kind=clean_text(request.form.get("kind") or "faq"),
            item_id=clean_text(request.form.get("item_id")),
            item_title=clean_text(request.form.get("item_title")),
            tags=_csv_list(request.form.get("tags")),
            source_name=clean_text(request.form.get("source_name")),
            source_url=clean_text(request.form.get("source_url")),
            active=request.form.get("active") == "on",
            meta={"notes": clean_text(request.form.get("meta_notes"))},
            chunk_chars=service.config.knowledge_chunk_chars,
            overlap_chars=service.config.knowledge_chunk_overlap_chars,
        )
        service.storage.increment_counter("knowledge_docs_saved_total", 1)
        log_avito_event(service.storage, channel="knowledge", stage="avito_knowledge_saved", message="Документ знаний сохранён", kind="avito_knowledge", doc_id=saved_doc_id, title=clean_text(request.form.get("title")), kind_value=clean_text(request.form.get("kind")))
        flash("Документ базы знаний сохранён.", "success")
        return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/knowledge/import", methods=["POST"])
def avito_knowledge_import() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        try:
            payload, filename = _read_uploaded_bytes("import_file", max_bytes=service.config.knowledge_upload_max_bytes, allowed_mime_types=list(service.config.knowledge_allowed_mime_types or []))
        except ValueError as exc:
            flash(_ui_safe_error_message("Операция Avito завершилась ошибкой", exc), "error")
            return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
        if not payload:
            flash("Нужно выбрать файл для импорта базы знаний.", "error")
            return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
        default_kind = clean_text(request.form.get("default_kind") or "faq")
        source_name = clean_text(request.form.get("source_name") or Path(filename or "upload").name)
        result = service.import_knowledge_bytes(payload, filename=filename, default_kind=default_kind, source_name=source_name)
        if result.get("errors"):
            flash(f"Импорт завершён с предупреждениями. Загружено: {result.get('imported', 0)}. Ошибок: {len(result.get('errors') or [])}", "warning")
        else:
            flash(f"Импортировано документов: {result.get('imported', 0)}.", "success")
        return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/knowledge/<int:doc_id>/delete", methods=["POST"])
def avito_knowledge_delete(doc_id: int) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "ai_rules")
        if denied:
            return denied
        service.storage.delete_knowledge_doc(doc_id)
        log_avito_event(service.storage, channel="knowledge", stage="avito_knowledge_deleted", message="Документ знаний удалён", kind="avito_knowledge", doc_id=doc_id)
        flash("Документ базы знаний удалён.", "success")
        return redirect(url_for("avito_module.avito_knowledge", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/media")
def avito_media() -> str:
    service = _service()
    try:
        search = clean_text(request.args.get("q") or "")
        media_kind = clean_text(request.args.get("kind") or "all")
        assets = service.storage.list_media_assets(search=search, media_kind=media_kind, limit=200)
        return render_template(
            "avito/media.html",
            media_assets=assets,
            media_search=search,
            media_kind=media_kind,
            media_kinds=["all", "image", "video", "document", "other"],
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/media/upsert", methods=["POST"])
def avito_media_upsert() -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "connect")
        if denied:
            return denied
        asset_id_raw = clean_text(request.form.get("asset_id"))
        existing = service.storage.get_media_asset(int(asset_id_raw)) if asset_id_raw.isdigit() else None
        try:
            local_path, file_name, mime_type = _save_uploaded_media(service.storage, service.config)
        except ValueError as exc:
            flash(_ui_safe_error_message("Операция Avito завершилась ошибкой", exc), "error")
            return redirect(url_for("avito_module.avito_media", tenant_id=service.tenant_id))
        if existing and not local_path:
            local_path = clean_text(existing.get("local_path"))
            file_name = clean_text(existing.get("file_name"))
            mime_type = clean_text(existing.get("mime_type"))
        raw_external_url = clean_text(request.form.get("external_url"))
        safe_external_url = raw_external_url if _is_safe_external_url(raw_external_url) else ""
        if raw_external_url and not safe_external_url:
            flash("Внешний URL отклонён: разрешены только безопасные публичные http/https-адреса без локальных и приватных хостов.", "warning")
        title = clean_text(request.form.get("title")) or file_name or "Материал"
        asset_id = service.storage.create_media_asset(
            asset_id=int(asset_id_raw) if asset_id_raw.isdigit() else None,
            title=title,
            media_kind=clean_text(request.form.get("media_kind") or "image"),
            caption=clean_text(request.form.get("caption")),
            item_id=clean_text(request.form.get("item_id")),
            item_title=clean_text(request.form.get("item_title")),
            file_name=file_name,
            local_path=local_path,
            external_url=safe_external_url,
            mime_type=mime_type or clean_text(request.form.get("mime_type")),
            tags=_csv_list(request.form.get("tags")),
            active=request.form.get("active") == "on",
            meta={"notes": clean_text(request.form.get("meta_notes"))},
        )
        log_avito_event(service.storage, channel="media", stage="avito_media_saved", message="Медиа-материал сохранён", kind="avito_media", asset_id=asset_id, media_kind=clean_text(request.form.get("media_kind") or "image"), title=title)
        flash("Медиа-материал сохранён.", "success")
        return redirect(url_for("avito_module.avito_media", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/media/<int:asset_id>/delete", methods=["POST"])
def avito_media_delete(asset_id: int) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "connect")
        if denied:
            return denied
        asset = service.storage.get_media_asset(asset_id)
        if asset and clean_text(asset.get("local_path")):
            try:
                path = Path(str(asset["local_path"])).resolve()
                media_root = service.storage.paths.media_dir.resolve()
                if str(path).startswith(str(media_root)) and path.exists():
                    path.unlink()
            except Exception:
                pass
        service.storage.delete_media_asset(asset_id)
        log_avito_event(service.storage, channel="media", stage="avito_media_deleted", message="Медиа-материал удалён", kind="avito_media", asset_id=asset_id)
        flash("Медиа-материал удалён.", "success")
        return redirect(url_for("avito_module.avito_media", tenant_id=service.tenant_id))
    finally:
        service.close()


@avito_bp.route("/avito/media/file/<int:asset_id>")
def avito_media_file(asset_id: int):
    service = _service()
    try:
        asset = service.storage.get_media_asset(asset_id)
        if not asset or not clean_text(asset.get("local_path")):
            abort(404)
        file_path = Path(str(asset["local_path"])).resolve()
        media_root = service.storage.paths.media_dir.resolve()
        if not str(file_path).startswith(str(media_root)) or not file_path.exists():
            abort(404)
        return send_file(file_path)
    finally:
        service.close()


@avito_bp.route("/avito/chat/<chat_id>/media/select", methods=["POST"])
def avito_select_media(chat_id: str) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "reply")
        if denied:
            return denied
        asset_ids = []
        for raw in request.form.getlist("asset_ids"):
            raw = clean_text(raw)
            if raw.isdigit():
                asset_ids.append(int(raw))
        service.storage.set_draft_media_assets(chat_id, asset_ids, source="manual_select")
        log_avito_event(service.storage, channel="media", stage="avito_media_selected", message="Для чата выбраны медиа-материалы", kind="avito_media", chat_id=chat_id, asset_ids=asset_ids)
        flash("Подобранные медиа-материалы сохранены для чата.", "success")
        return redirect(url_for("avito_module.avito_chat", tenant_id=service.tenant_id, chat_id=chat_id))
    finally:
        service.close()


@avito_bp.route("/avito/webhook/<tenant_id>", methods=["POST"])
def avito_webhook(tenant_id: str) -> Response:
    service = _service(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_webhook", label="Webhook Avito", source="webhook")
    try:
        audit.info("avito_webhook_start", "Получен входящий webhook Avito", channel="webhook", percent=0, content_length=len(request.get_data(cache=True) or b""))
        verified, meta = _verify_webhook_request(service)
        if not verified:
            fingerprint_payload = {
                "body_sha256": hashlib.sha256(request.get_data(cache=True) or b"").hexdigest(),
                "path": request.path,
                "reason": meta.get("reason"),
                "timestamp": meta.get("timestamp"),
                "nonce": meta.get("nonce"),
            }
            service.storage.store_webhook_event(
                clean_text(meta.get("event_id") or meta.get("dedupe_key") or f"rejected-{int(time.time())}"),
                fingerprint_payload,
                dedupe_key=clean_text(meta.get("dedupe_key")),
                source_kind="webhook_rejected",
                verified_by="rejected",
                signature=clean_text(meta.get("signature")),
                nonce=clean_text(meta.get("nonce")),
                status="rejected",
            )
            audit.finish("warning", "Webhook Avito отклонён: не пройдена проверка", reason=meta.get("reason"), has_signature=bool(clean_text(request.headers.get(service.config.webhook_signature_header))))
            return jsonify({"ok": False, "error": "webhook_not_verified", **meta, "run_id": audit.run_id}), 403
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {"raw": payload}
        result = service.ingest_webhook(payload, security_meta=meta, audit=audit)
        status_code = 200
        if result.get("duplicate"):
            status_code = 200
        elif result.get("dead_lettered"):
            status_code = 202
        audit.finish("warning" if result.get("dead_lettered") else "completed", "Webhook Avito обработан", verified_by=meta.get("verified_by"), event_id=result.get("event_id", ""), chat_id=result.get("chat_id", ""), duplicate=bool(result.get("duplicate")), dead_lettered=bool(result.get("dead_lettered")))
        result["run_id"] = audit.run_id
        return jsonify(result), status_code
    finally:
        service.close()


@avito_bp.route("/avito/dlq")
def avito_dlq() -> str:
    service = _service()
    try:
        items = service.storage.list_dead_letters(status=clean_text(request.args.get("status") or "open"), limit=100)
        return render_template("avito/dlq.html", dlq_items=items, dlq_status=clean_text(request.args.get("status") or "open"), **_common_context(service))
    finally:
        service.close()


@avito_bp.route("/avito/dlq/<int:dlq_id>/replay", methods=["POST"])
def avito_replay_dlq(dlq_id: int) -> Response:
    service = _service()
    try:
        denied = _check_permission(service.config, "admin")
        if denied:
            return denied
    finally:
        service.close()
    queued, payload = _submit_tenant_job(
        kind="avito_dlq_replay",
        label=f"Переиграть DLQ #{dlq_id}",
        target=run_replay_dlq_job,
        unique_key=f"avito_dlq_replay_{dlq_id}",
        dlq_id=int(dlq_id),
    )
    if not queued:
        _flash_inline_result(f"DLQ #{dlq_id}", payload)
    return redirect(url_for("avito_module.avito_dlq", tenant_id=_active_tenant_id()))


@avito_bp.route("/avito/logs")
def avito_logs() -> str:
    service = _service()
    try:
        runs = service.storage.list_recent_runs(limit=40)
        selected_run_id = clean_text(request.args.get("run_id")) or clean_text((runs[0] if runs else {}).get("run_id"))
        selected_run = service.storage.load_run_summary(selected_run_id) if selected_run_id else {}
        selected_events = service.storage.load_run_events(selected_run_id, limit=400) if selected_run_id else []
        selected_channel = clean_text(request.args.get("channel") or "sync")
        channel_events = service.storage.load_channel_events(selected_channel, limit=200)
        return render_template(
            "avito/logs.html",
            runs=runs,
            selected_run=selected_run,
            selected_run_events=selected_events,
            selected_channel=selected_channel,
            channel_events=channel_events,
            available_channels=service.storage.list_available_channels(),
            **_common_context(service),
        )
    finally:
        service.close()


@avito_bp.route("/avito/api/chats")
def avito_api_chats() -> Response:
    service = _service()
    try:
        return jsonify(service.storage.list_chats(limit=200, offset=0))
    finally:
        service.close()


@avito_bp.route("/avito/api/chat/<chat_id>")
def avito_api_chat(chat_id: str) -> Response:
    service = _service()
    try:
        advisory = service.chat_context_snapshot(chat_id)
        return jsonify({
            "chat": service.storage.get_chat(chat_id),
            "messages": service.storage.get_messages(chat_id, limit=250),
            "draft": service.storage.get_draft(chat_id),
            "knowledge_hits": advisory.get("knowledge_hits") or [],
            "media_suggestions": advisory.get("media_suggestions") or [],
            "similar_dialogs": advisory.get("similar_dialogs") or [],
            "selected_media": service.storage.list_draft_media_assets(chat_id),
        })
    finally:
        service.close()


@avito_bp.route("/avito/api/metrics")
def avito_api_metrics() -> Response:
    service = _service()
    try:
        return jsonify(service.metrics_snapshot())
    finally:
        service.close()



def register_avito_module(app: Flask, *, base_dir: Optional[Path] = None) -> Flask:
    app.register_blueprint(avito_bp)
    app.extensions.setdefault("avito_module", {})["base_dir"] = str(base_dir) if base_dir else os.environ.get("AVITO_MODULE_BASE_DIR", "")

    @app.context_processor
    def _inject_avito_nav() -> Dict[str, Any]:
        return {"avito_module_enabled": True}

    return app



def create_standalone_app(*, base_dir: Optional[Path] = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("AVITO_MODULE_SECRET_KEY") or os.urandom(32).hex()
    register_avito_module(app, base_dir=base_dir)

    @app.route("/")
    def _root() -> Response:
        return redirect(url_for("avito_module.avito_index", tenant_id=app.config.get("AVITO_MODULE_DEFAULT_TENANT_ID", "default")))

    return app
