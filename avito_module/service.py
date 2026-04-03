from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .ai_engine import AvitoAIAgent, DraftDecision
from .api_client import AvitoApiCircuitOpen, AvitoApiClient, AvitoApiError, AvitoApiUnauthorized
from .audit import AvitoAuditLogger, log_avito_event, trim_old_run_files
from .browser_monitor import AvitoBrowserMonitor
from .compat import clean_text
from .config import AvitoModuleConfig
from .importers import load_knowledge_docs_from_bytes
from .knowledge import KnowledgeHit, MediaSuggestion
from .storage import AvitoStorage


@dataclass(slots=True)
class SyncResult:
    chats_seen: int = 0
    chats_updated: int = 0
    messages_added: int = 0
    used_browser_fallback: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass(slots=True)
class BackfillResult:
    chats_seen: int = 0
    chats_updated: int = 0
    messages_added: int = 0
    notes: List[str] = field(default_factory=list)


@dataclass(slots=True)
class DraftRunResult:
    generated: int = 0
    auto_ready: int = 0
    manual_ready: int = 0
    skipped: int = 0
    notes: List[str] = field(default_factory=list)


@dataclass(slots=True)
class SendRunResult:
    sent: int = 0
    failed: int = 0
    notes: List[str] = field(default_factory=list)


class AvitoService:
    def __init__(
        self,
        tenant_id: str,
        *,
        config: Optional[AvitoModuleConfig] = None,
        storage: Optional[AvitoStorage] = None,
        api_client: Optional[AvitoApiClient] = None,
        ai_agent: Optional[AvitoAIAgent] = None,
        base_dir=None,
    ) -> None:
        self.tenant_id = clean_text(tenant_id) or "default"
        self.storage = storage or AvitoStorage(self.tenant_id, base_dir=base_dir)
        self.config = config or AvitoModuleConfig.from_sources(self.tenant_id, base_dir=base_dir)
        self.base_dir = base_dir
        self.api_client = api_client or AvitoApiClient(self.config, tenant_id=self.tenant_id, base_dir=base_dir)
        self.ai_agent = ai_agent or AvitoAIAgent(self.config, tenant_id=self.tenant_id)
        self.browser_monitor = AvitoBrowserMonitor(self.config, self.storage)
        self._current_audit: Optional[AvitoAuditLogger] = None
        if hasattr(self.api_client, "event_writer"):
            self.api_client.event_writer = self._api_event_writer
            if hasattr(self.api_client, "guardian"):
                try:
                    self.api_client.guardian.event_writer = self._api_event_writer
                except Exception:
                    pass
        if hasattr(self.ai_agent, "event_writer"):
            self.ai_agent.event_writer = self._ai_event_writer

    def close(self) -> None:
        try:
            self.api_client.close()
        except Exception:
            pass

    def _bind_audit(self, audit: Optional[AvitoAuditLogger]) -> None:
        self._current_audit = audit
        if hasattr(self.api_client, "event_writer"):
            self.api_client.event_writer = self._api_event_writer
            if hasattr(self.api_client, "guardian"):
                try:
                    self.api_client.guardian.event_writer = self._api_event_writer
                except Exception:
                    pass
        if hasattr(self.ai_agent, "event_writer"):
            self.ai_agent.event_writer = self._ai_event_writer

    def _api_event_writer(self, *, stage: str, message: str = "", level: str = "info", **data: Any) -> None:
        channel = "security" if stage.startswith("api_") and ("circuit" in stage or "rate" in stage) else "sync"
        self._emit(stage, message, channel=channel, level=level, **data)

    def _ai_event_writer(self, *, stage: str, message: str = "", level: str = "info", **data: Any) -> None:
        self._emit(stage, message, channel="ai", level=level, **data)

    def _emit(
        self,
        stage: str,
        message: str = "",
        *,
        channel: str,
        level: str = "info",
        percent: Optional[float] = None,
        **data: Any,
    ) -> None:
        if self._current_audit is not None:
            self._current_audit.stage(stage, message, channel=channel, level=level, percent=percent, **data)
            return
        log_avito_event(
            self.storage,
            channel=channel,
            stage=stage,
            message=message,
            level=level,
            kind=f"avito_{channel}",
            **data,
        )

    def _emit_decision(
        self,
        *,
        chat_id: str,
        route: str,
        confidence: float,
        reason: str,
        policy: str,
        blocked_by: str = "",
        fallback: str = "",
        model_name: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "chat_id": chat_id,
            "route": route,
            "confidence": round(float(confidence or 0.0), 4),
            "reason": clean_text(reason),
            "policy": clean_text(policy),
            "blocked_by": clean_text(blocked_by),
            "fallback": clean_text(fallback),
            "model_name": clean_text(model_name),
        }
        if extra:
            payload.update(extra)
        self._emit(
            "avito_decision",
            f"Decision trail: chat={chat_id} route={route} confidence={round(float(confidence or 0.0), 2)}",
            channel="decision",
            percent=None,
            **payload,
        )

    def _draft_query(self, chat: Dict[str, Any], messages: List[Dict[str, Any]]) -> str:
        incoming = [m for m in messages if clean_text(m.get("direction")) == "in" and clean_text(m.get("text"))]
        latest_text = clean_text(incoming[-1].get("text")) if incoming else ""
        parts = [latest_text, clean_text(chat.get("item_title")), clean_text(chat.get("title")), clean_text(chat.get("note"))]
        return " ".join(part for part in parts if part)

    def _knowledge_hits_for_chat(self, chat: Dict[str, Any], messages: List[Dict[str, Any]], *, track_metrics: bool = False) -> List[KnowledgeHit]:
        if not self.config.knowledge_enabled:
            return []
        query = self._draft_query(chat, messages)
        hits = self.storage.search_knowledge(
            query,
            item_id=clean_text(chat.get("item_id")),
            item_title=clean_text(chat.get("item_title") or chat.get("title")),
            limit=max(1, int(self.config.knowledge_max_hits or 5)),
            min_score=float(self.config.knowledge_min_score or 0.45),
        )
        if track_metrics:
            if hits:
                self.storage.increment_counter("knowledge_hit_total", len(hits))
            else:
                self.storage.increment_counter("knowledge_miss_total", 1)
        return hits

    def _media_suggestions_for_chat(self, chat: Dict[str, Any], messages: List[Dict[str, Any]], *, track_metrics: bool = False) -> List[MediaSuggestion]:
        if not self.config.media_registry_enabled or not self.config.media_auto_suggest_enabled:
            return []
        query = self._draft_query(chat, messages)
        suggestions = self.storage.search_media_assets(
            query,
            item_id=clean_text(chat.get("item_id")),
            item_title=clean_text(chat.get("item_title") or chat.get("title")),
            limit=max(1, int(self.config.media_max_suggestions or 4)),
        )
        if track_metrics and suggestions:
            self.storage.increment_counter("media_suggest_total", len(suggestions))
        return suggestions

    def _similar_dialogs_for_chat(self, chat: Dict[str, Any], messages: List[Dict[str, Any]], *, track_metrics: bool = False) -> List[Dict[str, Any]]:
        if not self.config.similar_dialogs_enabled:
            return []
        query = self._draft_query(chat, messages)
        hits = self.storage.search_similar_dialogs(
            query,
            item_id=clean_text(chat.get("item_id")),
            item_title=clean_text(chat.get("item_title") or chat.get("title")),
            exclude_chat_id=clean_text(chat.get("chat_id")),
            limit=max(1, int(self.config.similar_dialogs_max_hits or 4)),
            min_score=float(self.config.similar_dialogs_min_score or 0.55),
        )
        if track_metrics:
            if hits:
                self.storage.increment_counter("similar_dialog_hit_total", len(hits))
            else:
                self.storage.increment_counter("similar_dialog_miss_total", 1)
        return hits

    def _draft_state_for_hitl(self, draft: DraftDecision, effective_route: str, *, blocked_by: str = "") -> str:
        if not self.config.hitl_enabled:
            return "ready"
        auto_mode = clean_text(self.config.auto_mode or "draft_only")
        if auto_mode == "disabled":
            return "hold"
        if effective_route != "auto":
            return "review"
        if clean_text(blocked_by):
            return "review"
        if draft.confidence >= max(float(self.config.auto_send_confidence_threshold or 0.93), float(self.config.hitl_auto_ready_threshold or 0.985)):
            return "ready"
        return "review"

    def import_knowledge_bytes(
        self,
        payload: bytes,
        *,
        filename: str = "",
        default_kind: str = "faq",
        source_name: str = "upload",
    ) -> Dict[str, Any]:
        result = load_knowledge_docs_from_bytes(payload, filename=filename, default_kind=default_kind, source_name=source_name)
        imported = 0
        doc_ids: List[int] = []
        for doc in result.documents:
            doc_id = self.storage.upsert_knowledge_doc(
                title=doc.title,
                body_text=doc.body_text,
                kind=doc.kind,
                item_id=doc.item_id,
                item_title=doc.item_title,
                tags=doc.tags,
                source_name=doc.source_name,
                source_url=doc.source_url,
                active=doc.active,
                meta=doc.meta,
                chunk_chars=self.config.knowledge_chunk_chars,
                overlap_chars=self.config.knowledge_chunk_overlap_chars,
            )
            imported += 1
            doc_ids.append(int(doc_id))
        self._emit(
            "avito_knowledge_import",
            "Импорт базы знаний Avito завершён",
            channel="knowledge",
            imported=imported,
            detected_format=result.detected_format,
            errors=result.errors,
            doc_ids=doc_ids[:20],
        )
        return {
            "imported": imported,
            "detected_format": result.detected_format,
            "errors": list(result.errors),
            "doc_ids": doc_ids,
        }

    def approve_draft(self, chat_id: str, *, reviewer: str = "", review_note: str = "", body: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self.storage.update_draft_review(chat_id, state="ready", reviewer=reviewer, review_note=review_note, body=body)

    def hold_draft(self, chat_id: str, *, reviewer: str = "", review_note: str = "") -> Optional[Dict[str, Any]]:
        return self.storage.update_draft_review(chat_id, state="hold", reviewer=reviewer, review_note=review_note)

    def reject_draft(self, chat_id: str, *, reviewer: str = "", review_note: str = "") -> Optional[Dict[str, Any]]:
        return self.storage.update_draft_review(chat_id, state="rejected", reviewer=reviewer, review_note=review_note)

    def review_queue_snapshot(self, *, states: Optional[Iterable[str]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.storage.list_review_queue(states=states, limit=limit)

    def chat_context_snapshot(self, chat_id: str) -> Dict[str, Any]:
        chat = self.storage.get_chat(chat_id) or {}
        if not chat:
            return {"chat": None, "knowledge_hits": [], "media_suggestions": [], "selected_media": []}
        messages = self.storage.get_messages(chat_id, limit=max(20, self.config.max_context_messages * 2))
        knowledge_hits = self._knowledge_hits_for_chat(chat, messages, track_metrics=False)
        media_suggestions = self._media_suggestions_for_chat(chat, messages, track_metrics=False)
        selected_media = self.storage.list_draft_media_assets(chat_id)
        similar_dialogs = self._similar_dialogs_for_chat(chat, messages, track_metrics=False)
        return {
            "chat": chat,
            "messages": messages,
            "knowledge_hits": [hit.as_meta() for hit in knowledge_hits],
            "media_suggestions": [item.as_meta() for item in media_suggestions],
            "selected_media": selected_media,
            "similar_dialogs": similar_dialogs,
        }

    def sync_once(
        self,
        *,
        unread_only: Optional[bool] = None,
        max_chats: Optional[int] = None,
        messages_per_chat: int = 100,
        audit: Optional[AvitoAuditLogger] = None,
    ) -> SyncResult:
        self._bind_audit(audit)
        unread_only = self.config.unread_only_sync if unread_only is None else bool(unread_only)
        result = SyncResult()
        target_total = max(1, int(max_chats or self.config.sync_page_limit or 20))
        self.storage.increment_counter("sync_runs_total", 1)
        self._emit(
            "avito_sync_start",
            "Начинаю polling-sync Avito",
            channel="sync",
            percent=0,
            unread_only=unread_only,
            max_chats=max_chats or 0,
            messages_per_chat=messages_per_chat,
            mode="polling_second",
        )
        try:
            for idx, chat in enumerate(self.api_client.iter_chat_previews(unread_only=unread_only, limit=max_chats), start=1):
                result.chats_seen += 1
                self.storage.upsert_chat(chat)
                result.chats_updated += 1
                preview_percent = min(65.0, 5.0 + (idx / target_total) * 45.0)
                self._emit(
                    "avito_sync_chat_preview",
                    f"Получен чат {chat.get('client_name') or chat.get('title') or chat.get('chat_id')}",
                    channel="sync",
                    percent=round(preview_percent, 1),
                    chat_id=chat.get("chat_id"),
                    unread_count=chat.get("unread_count"),
                    item_title=chat.get("item_title"),
                )
                messages = list(self.api_client.iter_messages(chat["chat_id"], limit=messages_per_chat))
                added = self.storage.add_messages(chat["chat_id"], messages)
                result.messages_added += added
                msg_percent = min(82.0, preview_percent + 12.0)
                self._emit(
                    "avito_sync_chat_messages",
                    f"Загружены сообщения по чату {chat['chat_id']}",
                    channel="sync",
                    percent=round(msg_percent, 1),
                    chat_id=chat["chat_id"],
                    messages_total=len(messages),
                    messages_added=added,
                )
            if result.chats_seen == 0:
                result.notes.append("API не вернул ни одного чата")
                self._emit("avito_sync_empty", "Синхронизация завершилась без чатов", channel="sync", level="warning", percent=88)
        except (AvitoApiUnauthorized, AvitoApiCircuitOpen) as exc:
            result.notes.append(f"API недоступен: {exc}")
            self._emit(
                "avito_sync_api_unavailable",
                "Официальный Avito API недоступен, переключаюсь на browser fallback",
                channel="sync",
                level="error",
                percent=18,
                error=str(exc),
            )
            if self.config.browser_fallback_enabled:
                self._emit(
                    "avito_sync_browser_fallback_start",
                    "Пробую получить превью чатов через браузер",
                    channel="browser",
                    percent=24,
                    state_path=str(self.storage.paths.browser_state_file),
                )
                browser_result = self.browser_monitor.collect_chat_previews(max_items=max_chats or 30)
                if browser_result.previews:
                    self.storage.increment_counter("sync_browser_fallback_total", 1)
                    result.used_browser_fallback = True
                    result.notes.extend(browser_result.notes)
                    self._emit(
                        "avito_sync_browser_fallback_used",
                        "Browser fallback вернул список чатов",
                        channel="browser",
                        level="warning",
                        percent=40,
                        preview_count=len(browser_result.previews),
                        notes=browser_result.notes,
                    )
                    browser_total = max(1, len(browser_result.previews))
                    for idx, chat in enumerate(browser_result.previews, start=1):
                        result.chats_seen += 1
                        self.storage.upsert_chat(chat)
                        result.chats_updated += 1
                        preview_percent = min(84.0, 42.0 + (idx / browser_total) * 32.0)
                        self._emit(
                            "avito_sync_browser_preview",
                            f"Сохранено browser-превью чата {chat.get('title') or chat.get('chat_id')}",
                            channel="browser",
                            percent=round(preview_percent, 1),
                            chat_id=chat.get("chat_id"),
                            unread_count=chat.get("unread_count"),
                        )
                else:
                    result.notes.extend(browser_result.notes)
                    self._emit(
                        "avito_sync_browser_fallback_empty",
                        "Browser fallback не дал полезных данных",
                        channel="browser",
                        level="error",
                        percent=52,
                        notes=browser_result.notes,
                    )
        except AvitoApiError as exc:
            result.notes.append(f"Ошибка Avito API: {exc}")
            self._emit("avito_sync_api_error", "Avito API вернул ошибку во время синхронизации", channel="sync", level="error", percent=30, error=str(exc))
        finally:
            self.storage.save_sync_state(
                "last_sync",
                {
                    "chats_seen": result.chats_seen,
                    "chats_updated": result.chats_updated,
                    "messages_added": result.messages_added,
                    "used_browser_fallback": result.used_browser_fallback,
                    "notes": result.notes,
                    "last_run_id": self._current_audit.run_id if self._current_audit else "",
                },
            )
            self._emit(
                "avito_sync_summary",
                "Polling-sync Avito завершил основной цикл",
                channel="sync",
                percent=95,
                chats_seen=result.chats_seen,
                chats_updated=result.chats_updated,
                messages_added=result.messages_added,
                used_browser_fallback=result.used_browser_fallback,
                notes=result.notes,
            )
        return result

    def backfill_history(
        self,
        *,
        max_chats: int = 200,
        messages_per_chat: int = 200,
        audit: Optional[AvitoAuditLogger] = None,
    ) -> BackfillResult:
        self._bind_audit(audit)
        result = BackfillResult()
        self._emit(
            "avito_backfill_start",
            "Начинаю историческую дозагрузку Avito",
            channel="sync",
            percent=0,
            max_chats=max_chats,
            messages_per_chat=messages_per_chat,
            mode="migration_third",
        )
        try:
            total = max(1, max_chats)
            for idx, chat in enumerate(self.api_client.iter_chat_previews(unread_only=False, limit=max_chats), start=1):
                result.chats_seen += 1
                self.storage.upsert_chat(chat)
                result.chats_updated += 1
                percent = min(60.0, 5.0 + (idx / total) * 40.0)
                self._emit("avito_backfill_chat", f"Backfill чата {chat.get('chat_id')}", channel="sync", percent=round(percent, 1), chat_id=chat.get("chat_id"))
                messages = list(self.api_client.iter_messages(chat["chat_id"], limit=messages_per_chat))
                added = self.storage.add_messages(chat["chat_id"], messages)
                result.messages_added += added
            self.storage.save_sync_state(
                "last_backfill",
                {
                    "chats_seen": result.chats_seen,
                    "chats_updated": result.chats_updated,
                    "messages_added": result.messages_added,
                    "last_run_id": self._current_audit.run_id if self._current_audit else "",
                },
            )
        except Exception as exc:
            result.notes.append(str(exc))
            self._emit("avito_backfill_failed", "Историческая дозагрузка Avito завершилась ошибкой", channel="sync", level="error", percent=100, error=str(exc))
            raise
        self._emit(
            "avito_backfill_summary",
            "Историческая дозагрузка Avito завершена",
            channel="sync",
            percent=95,
            chats_seen=result.chats_seen,
            chats_updated=result.chats_updated,
            messages_added=result.messages_added,
        )
        return result

    def generate_drafts(self, *, limit: int = 20, audit: Optional[AvitoAuditLogger] = None, chat_ids: Optional[Iterable[str]] = None) -> DraftRunResult:
        self._bind_audit(audit)
        result = DraftRunResult()
        if chat_ids:
            chats = [self.storage.get_chat(chat_id) for chat_id in chat_ids]
            chats = [chat for chat in chats if isinstance(chat, dict)]
        else:
            chats = self.storage.unanswered_chats(limit=limit)
        self._emit("avito_drafts_start", "Запускаю генерацию AI-черновиков Avito", channel="ai", percent=0, limit=limit, chats_found=len(chats))
        if not chats:
            result.notes.append("Нет чатов, требующих ответа")
            self._emit("avito_drafts_empty", "Нет чатов, требующих ответа", channel="ai", level="warning", percent=100)
            return result

        total = max(1, len(chats))
        for idx, chat in enumerate(chats, start=1):
            pre_percent = 5.0 + (idx - 1) / total * 70.0
            self._emit(
                "avito_draft_chat_start",
                f"Готовлю черновик для чата {chat.get('client_name') or chat.get('chat_id')}",
                channel="ai",
                percent=round(pre_percent, 1),
                chat_id=chat.get("chat_id"),
                item_title=chat.get("item_title"),
            )
            messages = self.storage.get_messages(chat["chat_id"], limit=max(20, self.config.max_context_messages * 2))
            knowledge_hits = self._knowledge_hits_for_chat(chat, messages, track_metrics=True)
            media_suggestions = self._media_suggestions_for_chat(chat, messages, track_metrics=True)
            if knowledge_hits:
                self._emit(
                    "avito_knowledge_hits",
                    f"Найдены источники знаний для чата {chat['chat_id']}",
                    channel="knowledge",
                    percent=round(pre_percent + 2.0, 1),
                    chat_id=chat["chat_id"],
                    hits=[hit.as_meta() for hit in knowledge_hits],
                )
            else:
                self._emit(
                    "avito_knowledge_miss",
                    f"Для чата {chat['chat_id']} не найдено релевантных источников знаний",
                    channel="knowledge",
                    level="warning",
                    percent=round(pre_percent + 2.0, 1),
                    chat_id=chat["chat_id"],
                )
            if media_suggestions:
                self._emit(
                    "avito_media_suggestions",
                    f"Подобраны медиа-материалы для чата {chat['chat_id']}",
                    channel="media",
                    percent=round(pre_percent + 4.0, 1),
                    chat_id=chat["chat_id"],
                    media=[item.as_meta() for item in media_suggestions],
                )
            similar_dialogs = self._similar_dialogs_for_chat(chat, messages, track_metrics=True)
            if similar_dialogs:
                self._emit(
                    "avito_similar_dialogs",
                    f"Найдены похожие прошлые диалоги для чата {chat['chat_id']}",
                    channel="knowledge",
                    percent=round(pre_percent + 5.0, 1),
                    chat_id=chat["chat_id"],
                    similar_dialogs=similar_dialogs[:5],
                )
            draft = self.ai_agent.compose_reply(
                chat,
                messages,
                note=chat.get("note", ""),
                knowledge_hits=knowledge_hits,
                media_suggestions=media_suggestions,
                similar_dialogs=similar_dialogs,
            )
            effective_route = self._effective_route(draft)
            policy = clean_text(self.config.auto_mode or "draft_only")
            blocked_by = clean_text((draft.meta or {}).get("blocked_by") or "")
            fallback = clean_text((draft.meta or {}).get("fallback") or "")
            draft_state = self._draft_state_for_hitl(draft, effective_route, blocked_by=blocked_by)
            if draft.route == "skip" or not clean_text(draft.body):
                result.skipped += 1
                result.notes.append(f"Пропущен чат {chat['chat_id']}: {draft.reason}")
                self._emit(
                    "avito_draft_skipped",
                    f"Чат {chat['chat_id']} пропущен",
                    channel="ai",
                    level="warning",
                    percent=round(pre_percent + 8.0, 1),
                    chat_id=chat["chat_id"],
                    reason=draft.reason,
                )
                self._emit_decision(
                    chat_id=chat["chat_id"],
                    route="skip",
                    confidence=draft.confidence,
                    reason=draft.reason,
                    policy=policy,
                    blocked_by=blocked_by,
                    fallback=fallback,
                    model_name=draft.model_name,
                    extra={
                        "decision": "skip",
                        "knowledge_hits_count": len(knowledge_hits),
                        "media_suggestions_count": len(media_suggestions),
                    },
                )
                continue
            meta = dict(draft.meta or {})
            meta.update(
                {
                    "policy": policy,
                    "blocked_by": blocked_by,
                    "fallback": fallback,
                    "effective_route": effective_route,
                    "draft_state": draft_state,
                    "knowledge_hits": [hit.as_meta() for hit in knowledge_hits],
                    "knowledge_hits_count": len(knowledge_hits),
                    "media_suggestions": [item.as_meta() for item in media_suggestions],
                    "media_suggestions_count": len(media_suggestions),
                    "similar_dialogs": similar_dialogs[:5],
                    "similar_dialogs_count": len(similar_dialogs),
                    "similar_dialog_top_ids": [clean_text(item.get("chat_id")) for item in similar_dialogs[:3]],
                }
            )
            self.storage.replace_draft(
                chat_id=chat["chat_id"],
                body=draft.body,
                confidence=draft.confidence,
                route=effective_route,
                reason=draft.reason,
                source_message_ids=[m["message_id"] for m in messages if clean_text(m.get("direction")) == "in"],
                model_name=draft.model_name,
                state=draft_state,
                meta=meta,
            )
            if media_suggestions:
                suggested_ids = [item.asset_id for item in media_suggestions]
                self.storage.set_draft_media_assets(chat["chat_id"], suggested_ids, source="suggested")
            result.generated += 1
            if draft_state == "ready" and effective_route == "auto":
                result.auto_ready += 1
            else:
                result.manual_ready += 1
            self._emit(
                "avito_draft_ready",
                f"Черновик для чата {chat['chat_id']} готов",
                channel="ai",
                percent=round(pre_percent + 12.0, 1),
                chat_id=chat["chat_id"],
                route=effective_route,
                draft_state=draft_state,
                confidence=draft.confidence,
                model_name=draft.model_name,
                reason=draft.reason,
            )
            self._emit_decision(
                chat_id=chat["chat_id"],
                route=effective_route,
                confidence=draft.confidence,
                reason=draft.reason,
                policy=policy,
                blocked_by=blocked_by,
                fallback=fallback,
                model_name=draft.model_name,
                extra={
                    "decision": "draft_ready",
                    "message_preview": clean_text(draft.body)[:240],
                    "knowledge_hits_count": len(knowledge_hits),
                    "knowledge_top_titles": [hit.title for hit in knowledge_hits[:3]],
                    "media_suggestions_count": len(media_suggestions),
                    "media_kinds": sorted({item.media_kind for item in media_suggestions}),
                    "similar_dialogs_count": len(similar_dialogs),
                    "similar_dialog_top_ids": [clean_text(item.get("chat_id")) for item in similar_dialogs[:3]],
                    "draft_state": draft_state,
                },
            )

        self._emit(
            "avito_drafts_summary",
            "Генерация черновиков Avito завершена",
            channel="ai",
            percent=95,
            generated=result.generated,
            auto_ready=result.auto_ready,
            manual_ready=result.manual_ready,
            skipped=result.skipped,
            notes=result.notes,
        )
        return result

    def send_ready_drafts(self, *, limit: int = 20, auto_only: bool = False, audit: Optional[AvitoAuditLogger] = None) -> SendRunResult:
        self._bind_audit(audit)
        routes = ["auto"] if auto_only else None
        drafts = self.storage.list_pending_drafts(routes=routes, limit=limit)
        result = SendRunResult()
        self._emit(
            "avito_send_start",
            "Начинаю отправку готовых Avito-черновиков",
            channel="send",
            percent=0,
            auto_only=auto_only,
            drafts_found=len(drafts),
            limit=limit,
        )
        if not drafts:
            result.notes.append("Нет готовых черновиков для отправки")
            self._emit("avito_send_empty", "Нет готовых черновиков для отправки", channel="send", level="warning", percent=100)
            return result

        total = max(1, len(drafts))
        for idx, draft in enumerate(drafts, start=1):
            start_percent = 5.0 + (idx - 1) / total * 70.0
            self._emit(
                "avito_send_chat_start",
                f"Отправляю ответ в чат {draft['chat_id']}",
                channel="send",
                percent=round(start_percent, 1),
                chat_id=draft["chat_id"],
                route=draft.get("route"),
            )
            try:
                selected_media = self.storage.list_draft_media_assets(draft["chat_id"])
                if selected_media and not self.config.media_send_enabled:
                    self._emit(
                        "avito_send_media_pending",
                        f"Для чата {draft['chat_id']} подготовлены медиа, но live media send не включён",
                        channel="media",
                        level="warning",
                        percent=round(start_percent + 1.0, 1),
                        chat_id=draft["chat_id"],
                        asset_ids=[item.get("asset_id") for item in selected_media],
                    )
                response = self.send_chat_reply(draft["chat_id"], draft["body"], selected_media=selected_media, draft_context=draft)
                remote_message_id = clean_text((response.get("message") or {}).get("id") or response.get("id")) if isinstance(response, dict) else ""
                self.storage.mark_draft_sent(draft["chat_id"], remote_message_id=remote_message_id)
                attachments_payload = []
                if selected_media:
                    attachments_payload = [
                        {
                            "asset_id": item.get("asset_id"),
                            "media_kind": item.get("media_kind"),
                            "title": item.get("title"),
                            "file_name": item.get("file_name"),
                            "external_url": item.get("external_url"),
                            "local_path": item.get("local_path"),
                        }
                        for item in selected_media
                    ]
                if remote_message_id:
                    self.storage.add_messages(
                        draft["chat_id"],
                        [
                            {
                                "message_id": remote_message_id,
                                "direction": "out",
                                "is_read": True,
                                "author_name": "assistant",
                                "message_ts": "",
                                "text": draft["body"],
                                "attachments": attachments_payload,
                                "raw": response,
                            }
                        ],
                    )
                try:
                    self.api_client.mark_chat_as_read(draft["chat_id"])
                except Exception as exc:
                    self._emit(
                        "avito_send_mark_read_failed",
                        f"Не удалось пометить чат {draft['chat_id']} как прочитанный",
                        channel="send",
                        level="warning",
                        percent=round(start_percent + 6.0, 1),
                        chat_id=draft["chat_id"],
                        error=str(exc),
                    )
                result.sent += 1
                self._emit(
                    "avito_send_chat_ok",
                    f"Ответ отправлен в чат {draft['chat_id']}",
                    channel="send",
                    percent=round(start_percent + 12.0, 1),
                    chat_id=draft["chat_id"],
                    remote_message_id=remote_message_id,
                    transport=clean_text((response or {}).get("transport")) if isinstance(response, dict) else "",
                    media_fallback=clean_text((response or {}).get("media_fallback")) if isinstance(response, dict) else "",
                )
                meta = draft.get("meta") or {}
                self._emit_decision(
                    chat_id=draft["chat_id"],
                    route=clean_text(draft.get("route") or "manual"),
                    confidence=float(draft.get("confidence") or 0.0),
                    reason=clean_text(draft.get("reason") or "sent"),
                    policy=clean_text(meta.get("policy") or self.config.auto_mode),
                    blocked_by=clean_text(meta.get("blocked_by") or ""),
                    fallback=clean_text(meta.get("fallback") or ""),
                    model_name=clean_text(draft.get("model_name") or ""),
                    extra={"decision": "sent", "remote_message_id": remote_message_id},
                )
            except Exception as exc:
                self.storage.mark_draft_error(draft["chat_id"], str(exc))
                result.failed += 1
                result.notes.append(f"{draft['chat_id']}: {exc}")
                self._emit(
                    "avito_send_chat_failed",
                    f"Не удалось отправить ответ в чат {draft['chat_id']}",
                    channel="send",
                    level="error",
                    percent=round(start_percent + 12.0, 1),
                    chat_id=draft["chat_id"],
                    error=str(exc),
                )
        self._emit("avito_send_summary", "Отправка Avito-черновиков завершена", channel="send", percent=95, sent=result.sent, failed=result.failed, notes=result.notes)
        return result

    def send_chat_reply(
        self,
        chat_id: str,
        body: str,
        *,
        selected_media: Optional[List[Dict[str, Any]]] = None,
        draft_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        selected_media = list(selected_media or [])
        transport = clean_text(self.config.media_send_transport or "browser_first")
        filtered_media: List[Dict[str, Any]] = []
        for item in selected_media[: max(1, int(self.config.media_max_send_assets or 4))]:
            if self.config.media_send_images_only and clean_text(item.get("media_kind") or "image") != "image":
                continue
            filtered_media.append(item)

        media_error = ""
        if filtered_media and self.config.media_send_enabled and transport != "disabled":
            if transport in {"browser_first", "browser_only"}:
                try:
                    response = self.browser_monitor.send_message_with_media(chat_id, body, filtered_media)
                    response.setdefault("transport", "browser")
                    return response
                except Exception as exc:
                    media_error = str(exc)
                    self._emit(
                        "avito_media_send_browser_failed",
                        f"Browser transport не смог отправить фото в чат {chat_id}",
                        channel="media",
                        level="warning",
                        chat_id=chat_id,
                        error=str(exc),
                        asset_ids=[item.get("asset_id") for item in filtered_media],
                    )
                    if transport == "browser_only":
                        raise
            if transport in {"api_first", "api_only"} and hasattr(self.api_client, "send_message_with_media"):
                try:
                    response = self.api_client.send_message_with_media(chat_id, body, filtered_media)
                    if isinstance(response, dict):
                        response.setdefault("transport", "api")
                    return response
                except Exception as exc:
                    media_error = media_error or str(exc)
                    self._emit(
                        "avito_media_send_api_failed",
                        f"API transport не смог отправить фото в чат {chat_id}",
                        channel="media",
                        level="warning",
                        chat_id=chat_id,
                        error=str(exc),
                        asset_ids=[item.get("asset_id") for item in filtered_media],
                    )
                    if transport == "api_only":
                        raise
        response = self.api_client.send_text_message(chat_id, body)
        if isinstance(response, dict):
            response.setdefault("transport", "text")
            if filtered_media:
                response["media_fallback"] = "text_only"
                response["media_error"] = media_error
                response["selected_media_ids"] = [item.get("asset_id") for item in filtered_media]
        return response

    def ingest_webhook(
        self,
        payload: Dict[str, Any],
        *,
        security_meta: Optional[Dict[str, Any]] = None,
        audit: Optional[AvitoAuditLogger] = None,
        allow_duplicate_reprocess: bool = False,
    ) -> Dict[str, Any]:
        self._bind_audit(audit)
        security_meta = dict(security_meta or {})
        extracted = self._extract_webhook_entities(payload)
        raw_hash = hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()
        event_id = clean_text(security_meta.get("event_id") or extracted.get("event_id") or payload.get("id") or payload.get("event_id") or payload.get("uuid")) or f"event-{raw_hash[:16]}"
        dedupe_key = clean_text(security_meta.get("dedupe_key") or extracted.get("dedupe_key")) or f"{event_id}:{clean_text(extracted.get('message_id') or extracted.get('chat_id') or raw_hash[:12])}"
        self._emit(
            "avito_webhook_store_start",
            "Получен webhook Avito",
            channel="webhook",
            percent=10,
            event_id=event_id,
            dedupe_key=dedupe_key,
            keys=list(payload.keys())[:20],
        )
        store_result = self.storage.store_webhook_event(
            event_id,
            payload,
            dedupe_key=dedupe_key,
            source_kind=clean_text(security_meta.get("source_kind") or "webhook"),
            verified_by=clean_text(security_meta.get("verified_by")),
            signature=clean_text(security_meta.get("signature")),
            nonce=clean_text(security_meta.get("nonce")),
            status="received",
        )
        if store_result.get("duplicate") and not allow_duplicate_reprocess:
            self._emit(
                "avito_webhook_duplicate",
                "Webhook Avito уже был обработан ранее",
                channel="webhook",
                level="warning",
                percent=100,
                event_id=event_id,
                dedupe_key=dedupe_key,
            )
            return {
                "ok": True,
                "duplicate": True,
                "event_id": clean_text(store_result.get("event_id") or event_id),
                "dedupe_key": dedupe_key,
                "chat_id": clean_text(extracted.get("chat_id")),
                "run_id": self._current_audit.run_id if self._current_audit else "",
            }
        try:
            process_result = self._process_webhook_payload(event_id, extracted, payload)
            process_payload = dict(process_result or {})
            process_payload.pop("event_id", None)
            self.storage.mark_webhook_event(event_id, status="processed", processed=True, increment_attempt=True)
            self.storage.save_sync_state("webhook_last_chat", {"chat_id": process_result.get("chat_id", ""), "event_id": event_id})
            self._emit(
                "avito_webhook_processed",
                "Webhook Avito обработан и записан в inbox",
                channel="webhook",
                percent=100,
                event_id=event_id,
                dedupe_key=dedupe_key,
                **process_payload,
            )
            return {
                "ok": True,
                "event_id": event_id,
                "dedupe_key": dedupe_key,
                "run_id": self._current_audit.run_id if self._current_audit else "",
                **process_payload,
            }
        except Exception as exc:
            dlq_id = self.storage.create_dead_letter(
                source_kind="webhook",
                payload=payload,
                error_text=str(exc),
                dedupe_key=dedupe_key,
                event_id=event_id,
                last_run_id=self._current_audit.run_id if self._current_audit else "",
            )
            self.storage.mark_webhook_event(event_id, status="dead_letter", error_text=str(exc), processed=False, increment_attempt=True)
            self._emit(
                "avito_webhook_dead_letter",
                "Webhook Avito ушёл в DLQ",
                channel="webhook",
                level="error",
                percent=100,
                event_id=event_id,
                dedupe_key=dedupe_key,
                dlq_id=dlq_id,
                error=str(exc),
            )
            return {
                "ok": False,
                "accepted": True,
                "dead_lettered": True,
                "event_id": event_id,
                "dedupe_key": dedupe_key,
                "dlq_id": dlq_id,
                "error": str(exc),
                "run_id": self._current_audit.run_id if self._current_audit else "",
            }

    def replay_dead_letter(self, dlq_id: int, *, audit: Optional[AvitoAuditLogger] = None) -> Dict[str, Any]:
        self._bind_audit(audit)
        item = self.storage.get_dead_letter(dlq_id)
        if not item:
            raise ValueError(f"DLQ item not found: {dlq_id}")
        self._emit("avito_dlq_replay_start", "Пробую переиграть DLQ-событие", channel="ops", percent=0, dlq_id=dlq_id, source_kind=item.get("source_kind"))
        try:
            if clean_text(item.get("source_kind")) == "webhook":
                result = self.ingest_webhook(
                    item.get("payload") or {},
                    security_meta={
                        "event_id": clean_text(item.get("event_id")) or f"replay-{dlq_id}",
                        "dedupe_key": clean_text(item.get("dedupe_key")) or f"dlq:{dlq_id}",
                        "source_kind": "webhook_replay",
                        "verified_by": "replay_console",
                    },
                    audit=audit,
                    allow_duplicate_reprocess=True,
                )
            else:
                raise ValueError(f"Unsupported DLQ source: {item.get('source_kind')}")
            self.storage.mark_dead_letter(dlq_id, status="resolved", last_run_id=self._current_audit.run_id if self._current_audit else "", increment_attempt=True)
            self._emit("avito_dlq_replay_done", "DLQ-событие успешно переиграно", channel="ops", percent=100, dlq_id=dlq_id)
            return result
        except Exception as exc:
            self.storage.mark_dead_letter(dlq_id, status="open", error_text=str(exc), last_run_id=self._current_audit.run_id if self._current_audit else "", increment_attempt=True)
            self._emit("avito_dlq_replay_failed", "Переиграть DLQ-событие не удалось", channel="ops", level="error", percent=100, dlq_id=dlq_id, error=str(exc))
            raise

    def bootstrap_browser_state(self, timeout_seconds: int = 0, audit: Optional[AvitoAuditLogger] = None) -> Dict[str, Any]:
        self._bind_audit(audit)
        self._emit(
            "avito_browser_bootstrap_start",
            "Открываю браузер Avito для сохранения state",
            channel="browser",
            percent=0,
            timeout_seconds=timeout_seconds or self.config.browser_bootstrap_timeout_seconds,
            state_path=str(self.storage.paths.browser_state_file),
        )
        payload = self.browser_monitor.bootstrap_interactive_login(timeout_seconds=timeout_seconds)
        self._emit(
            "avito_browser_bootstrap_saved",
            clean_text(payload.get("notes", ["Состояние браузера Avito сохранено"])[0] if isinstance(payload.get("notes"), list) else "Состояние браузера Avito сохранено"),
            channel="browser",
            percent=100,
            state_path=payload.get("state_path", ""),
            saved_at=payload.get("saved_at", ""),
        )
        return payload

    def metrics_snapshot(self) -> Dict[str, Any]:
        snapshot = self.storage.compute_metrics()
        self._emit("avito_metrics_snapshot", "Собран snapshot оперативных метрик Avito", channel="ops", incoming_5m=snapshot.get("incoming_5m"), incoming_60m=snapshot.get("incoming_60m"), auto_reply_share=snapshot.get("auto_reply_share"))
        return snapshot

    def _process_webhook_payload(self, event_id: str, extracted: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        chat_payload = extracted.get("chat") or {}
        chat_id = clean_text(extracted.get("chat_id") or chat_payload.get("chat_id") or chat_payload.get("id"))
        messages = list(extracted.get("messages") or [])
        if chat_id and not chat_payload:
            chat_payload = {"chat_id": chat_id, "title": clean_text(extracted.get("title") or chat_id)}
        if chat_id and messages:
            latest = messages[-1]
            chat_payload.setdefault("last_message_text", clean_text(latest.get("text")))
            chat_payload.setdefault("last_message_ts", clean_text(latest.get("message_ts") or latest.get("created_at")))
            chat_payload.setdefault("unread_count", 1 if clean_text(latest.get("direction") or "") == "in" else 0)
        if chat_id:
            self.storage.upsert_chat(chat_payload or {"chat_id": chat_id})
        added = 0
        if chat_id and messages:
            added = self.storage.add_messages(chat_id, messages)
        generated = 0
        if chat_id and self.config.webhook_auto_generate_draft and self.storage.chat_needs_reply(chat_id):
            draft_result = self.generate_drafts(limit=1, chat_ids=[chat_id], audit=self._current_audit)
            generated = draft_result.generated
        return {
            "chat_id": chat_id,
            "messages_added": added,
            "drafts_generated": generated,
            "message_id": clean_text(extracted.get("message_id")),
            "event_id": event_id,
        }

    def _extract_webhook_entities(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        root = payload if isinstance(payload, dict) else {"raw": payload}
        nested = root.get("payload") if isinstance(root.get("payload"), dict) else root.get("data") if isinstance(root.get("data"), dict) else root
        chat_block = nested.get("chat") if isinstance(nested.get("chat"), dict) else root.get("chat") if isinstance(root.get("chat"), dict) else {}
        message_candidates: List[Dict[str, Any]] = []
        for candidate in (
            nested.get("message"),
            root.get("message"),
            (nested.get("messages") or [None])[0] if isinstance(nested.get("messages"), list) and nested.get("messages") else None,
            (root.get("messages") or [None])[0] if isinstance(root.get("messages"), list) and root.get("messages") else None,
        ):
            if isinstance(candidate, dict):
                message_candidates.append(candidate)
        first_message = message_candidates[0] if message_candidates else {}
        chat_id = clean_text(
            nested.get("chat_id")
            or nested.get("chatId")
            or nested.get("conversation_id")
            or chat_block.get("id")
            or first_message.get("chat_id")
            or first_message.get("chatId")
            or root.get("chat_id")
            or root.get("chatId")
        )
        text = clean_text(
            ((first_message.get("content") or {}).get("text") if isinstance(first_message.get("content"), dict) else "")
            or first_message.get("text")
            or nested.get("text")
            or root.get("text")
        )
        direction = clean_text(first_message.get("direction") or nested.get("direction") or root.get("direction") or "in")
        if direction not in {"in", "out"}:
            direction = "in"
        message_id = clean_text(first_message.get("id") or nested.get("message_id") or root.get("message_id") or root.get("id"))
        message_ts = clean_text(first_message.get("created") or first_message.get("created_at") or nested.get("created") or root.get("created") or payload.get("timestamp"))
        client_name = clean_text(((first_message.get("author") or {}).get("name") if isinstance(first_message.get("author"), dict) else "") or ((nested.get("user") or {}).get("name") if isinstance(nested.get("user"), dict) else "") or ((root.get("user") or {}).get("name") if isinstance(root.get("user"), dict) else ""))
        item = chat_block.get("item") if isinstance(chat_block.get("item"), dict) else nested.get("item") if isinstance(nested.get("item"), dict) else {}
        chat_payload = {
            "chat_id": chat_id,
            "title": clean_text(chat_block.get("title") or item.get("title") or client_name or chat_id),
            "client_name": client_name,
            "item_id": clean_text(item.get("id") or nested.get("item_id") or root.get("item_id")),
            "item_title": clean_text(item.get("title") or nested.get("item_title") or root.get("item_title")),
            "unread_count": 1 if direction == "in" else 0,
            "last_message_text": text,
            "last_message_ts": message_ts,
            "raw": payload,
        }
        messages: List[Dict[str, Any]] = []
        if chat_id and (text or message_id):
            messages.append(
                {
                    "message_id": message_id or f"webhook-{hashlib.sha256((chat_id + text + message_ts).encode('utf-8')).hexdigest()[:16]}",
                    "direction": direction,
                    "is_read": False if direction == "in" else True,
                    "author_name": client_name or ("buyer" if direction == "in" else "assistant"),
                    "message_ts": message_ts,
                    "text": text,
                    "attachments": first_message.get("attachments") or [],
                    "raw": first_message or payload,
                }
            )
        return {
            "event_id": clean_text(root.get("event_id") or root.get("id") or nested.get("event_id")),
            "dedupe_key": clean_text(root.get("dedupe_key") or root.get("idempotency_key") or root.get("id") or message_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "chat": chat_payload if chat_id else {},
            "messages": messages,
            "title": clean_text(chat_payload.get("title")),
        }

    def _effective_route(self, draft: DraftDecision) -> str:
        auto_mode = clean_text(self.config.auto_mode or "draft_only")
        if auto_mode == "disabled":
            return "manual"
        if auto_mode == "draft_only":
            return "manual"
        if auto_mode == "simple_only":
            return "auto" if draft.reason.startswith("FAQ") and draft.confidence >= self.config.auto_send_confidence_threshold else "manual"
        return draft.route if draft.route in {"auto", "manual"} else "manual"


def _result_message(payload: Dict[str, Any], fallback: str) -> str:
    message = clean_text(payload.get("message"))
    return message or fallback


def run_sync_job(tenant_id: str, *, max_chats: int = 20, unread_only: Optional[bool] = None) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_sync", label="Синхронизация Avito", source="background_job")
    try:
        result = service.sync_once(max_chats=max_chats, unread_only=unread_only, audit=audit)
        payload = asdict(result)
        payload["run_id"] = audit.run_id
        payload["message"] = f"Avito sync завершён. Чатов: {result.chats_seen}, сообщений добавлено: {result.messages_added}."
        audit.finish(
            "warning" if result.notes else "completed",
            payload["message"],
            chats_seen=result.chats_seen,
            chats_updated=result.chats_updated,
            messages_added=result.messages_added,
            used_browser_fallback=result.used_browser_fallback,
            notes=result.notes,
        )
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), max_chats=max_chats, unread_only=bool(unread_only))
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()


def run_backfill_job(tenant_id: str, *, max_chats: int = 200, messages_per_chat: int = 200) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_backfill", label="Историческая дозагрузка Avito", source="background_job")
    try:
        result = service.backfill_history(max_chats=max_chats, messages_per_chat=messages_per_chat, audit=audit)
        payload = asdict(result)
        payload["run_id"] = audit.run_id
        payload["message"] = f"Backfill завершён. Чатов: {result.chats_seen}, сообщений добавлено: {result.messages_added}."
        audit.finish("warning" if result.notes else "completed", payload["message"], **payload)
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), max_chats=max_chats, messages_per_chat=messages_per_chat)
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()


def run_generate_drafts_job(tenant_id: str, *, limit: int = 20) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_drafts_generate", label="Генерация черновиков Avito", source="background_job")
    try:
        result = service.generate_drafts(limit=limit, audit=audit)
        payload = asdict(result)
        payload["run_id"] = audit.run_id
        payload["message"] = f"Готово. Черновиков: {result.generated}, авто: {result.auto_ready}, ручных: {result.manual_ready}."
        audit.finish(
            "warning" if result.notes else "completed",
            payload["message"],
            generated=result.generated,
            auto_ready=result.auto_ready,
            manual_ready=result.manual_ready,
            skipped=result.skipped,
            notes=result.notes,
        )
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), limit=limit)
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()


def run_send_drafts_job(tenant_id: str, *, limit: int = 20, auto_only: bool = False) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_drafts_send", label="Отправка Avito-черновиков", source="background_job")
    try:
        result = service.send_ready_drafts(limit=limit, auto_only=auto_only, audit=audit)
        payload = asdict(result)
        payload["run_id"] = audit.run_id
        payload["message"] = f"Отправлено: {result.sent}. Ошибок: {result.failed}."
        audit.finish(
            "warning" if result.failed or result.notes else "completed",
            payload["message"],
            sent=result.sent,
            failed=result.failed,
            auto_only=auto_only,
            notes=result.notes,
        )
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), limit=limit, auto_only=auto_only)
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()


def run_browser_bootstrap_job(tenant_id: str, *, timeout_seconds: int = 0) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_browser_bootstrap", label="Вход в Avito через браузер", source="background_job")
    try:
        payload = service.bootstrap_browser_state(timeout_seconds=timeout_seconds, audit=audit)
        payload["run_id"] = audit.run_id
        payload["message"] = _result_message(payload, "Состояние браузера Avito сохранено.")
        audit.finish(
            "completed",
            payload["message"],
            state_path=payload.get("state_path", ""),
            saved_at=payload.get("saved_at", ""),
            notes=payload.get("notes", []),
        )
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), timeout_seconds=timeout_seconds)
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()


def run_replay_dlq_job(tenant_id: str, *, dlq_id: int) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_dlq_replay", label="Переиграть DLQ Avito", source="background_job")
    try:
        payload = service.replay_dead_letter(dlq_id, audit=audit)
        payload["run_id"] = audit.run_id
        payload["message"] = f"DLQ #{dlq_id} переигран."
        audit.finish("completed", payload["message"], dlq_id=dlq_id)
        trim_old_run_files(service.storage)
        return payload
    except Exception as exc:
        audit.fail(str(exc), error=str(exc), dlq_id=dlq_id)
        trim_old_run_files(service.storage)
        raise
    finally:
        service.close()
