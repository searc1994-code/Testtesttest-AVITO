from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timedelta, timezone
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .ai_engine import AvitoAIAgent, DraftDecision
from .api_client import AvitoApiCircuitOpen, AvitoApiClient, AvitoApiError, AvitoApiUnauthorized
from .audit import AvitoAuditLogger, log_avito_event, trim_old_run_files
from .browser_monitor import AvitoBrowserMonitor
from .compat import clean_text, utc_now_iso
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
    partial_sent: int = 0
    suppressed: int = 0
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

    def _classify_dialog_scenario(self, chat: Dict[str, Any], messages: List[Dict[str, Any]]) -> str:
        incoming_text = " ".join(clean_text(m.get("text")) for m in messages if clean_text(m.get("direction")) == "in")
        haystack = f"{incoming_text} {clean_text(chat.get('last_message_text'))} {clean_text(chat.get('note'))}".lower()
        if re.search(r"(скидк|дешевл|торг|уступ|последн\w* цен)", haystack):
            return "bargain"
        if re.search(r"(фото|видео|покаж|ракурс|сним|комплект)", haystack):
            return "media_request"
        if re.search(r"(телефон|whatsapp|ватсап|telegram|телеграм|ссылк|номер)", haystack):
            return "external_contact"
        if re.search(r"(дефект|царап|скол|пятн|состояни|неисправ|трещин)", haystack):
            return "condition"
        if re.search(r"(размер|характеристик|параметр|габарит|вес)", haystack):
            return "characteristics"
        if re.search(r"(доставк|отправ|самовывоз|сдэк|почт|авито доставк)", haystack):
            return "delivery"
        if re.search(r"(актуальн|в наличии|наличии|еще есть|остал)", haystack):
            return "availability"
        if re.search(r"(жалоб|спор|обман|верн|возврат|претензи)", haystack):
            return "complaint"
        return "general"

    def _policy_decision_level(self, *, effective_route: str, draft_state: str) -> str:
        if effective_route == "auto" and clean_text(draft_state) == "ready":
            return "auto_send"
        if effective_route == "auto":
            return "recommendation"
        return "draft"

    def _apply_policy_overrides(
        self,
        *,
        chat: Dict[str, Any],
        messages: List[Dict[str, Any]],
        draft: DraftDecision,
        knowledge_hits: List[KnowledgeHit],
        media_suggestions: List[MediaSuggestion],
    ) -> Tuple[str, str, str, str]:
        scenario = self._classify_dialog_scenario(chat, messages)
        effective_route = self._effective_route(draft)
        blocked_by = clean_text((draft.meta or {}).get("blocked_by") or "")
        fallback = clean_text((draft.meta or {}).get("fallback") or "")
        if scenario in {"bargain", "complaint", "external_contact"}:
            effective_route = "manual"
            blocked_by = blocked_by or f"policy_manual:{scenario}"
        elif scenario == "media_request" and not media_suggestions:
            effective_route = "manual"
            blocked_by = blocked_by or "policy_manual:media_without_asset"
        elif scenario == "characteristics" and not knowledge_hits:
            effective_route = "manual"
            blocked_by = blocked_by or "policy_manual:characteristics_without_kb"
        return scenario, effective_route, blocked_by, fallback

    def detect_recent_duplicate_send(self, chat_id: str, body: str, *, window_seconds: int = 600) -> Optional[Dict[str, Any]]:
        return self.storage.find_recent_duplicate_send(chat_id, body, window_seconds=window_seconds)


    def _build_attachment_payload(self, selected_media: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for item in list(selected_media or []):
            payload.append(
                {
                    "asset_id": item.get("asset_id"),
                    "media_kind": item.get("media_kind"),
                    "title": item.get("title"),
                    "file_name": item.get("file_name"),
                    "external_url": item.get("external_url"),
                    "local_path": item.get("local_path"),
                    "mime_type": item.get("mime_type"),
                }
            )
        return payload

    def finalize_successful_send(
        self,
        chat_id: str,
        body: str,
        response: Optional[Dict[str, Any]] = None,
        *,
        selected_media: Optional[List[Dict[str, Any]]] = None,
        draft_context: Optional[Dict[str, Any]] = None,
        author_name: str = "assistant",
        mark_read: bool = True,
    ) -> Dict[str, Any]:
        response = dict(response or {})
        selected_media = list(selected_media or [])
        message_payload = response.get("message") if isinstance(response.get("message"), dict) else {}
        remote_message_id = clean_text(message_payload.get("id") or response.get("id"))
        message_ts = clean_text(message_payload.get("created") or message_payload.get("message_ts") or response.get("message_ts")) or utc_now_iso()
        local_message_id = remote_message_id or f"local-out-{hashlib.sha256((clean_text(chat_id) + message_ts + clean_text(body)).encode('utf-8')).hexdigest()[:16]}"
        attachments_payload = self._build_attachment_payload(selected_media)
        self.storage.add_messages(
            chat_id,
            [
                {
                    "message_id": local_message_id,
                    "direction": "out",
                    "is_read": True,
                    "author_name": clean_text(author_name) or "assistant",
                    "message_ts": message_ts,
                    "text": body,
                    "attachments": attachments_payload,
                    "raw": response or {"message": {"id": local_message_id, "text": body}},
                }
            ],
        )
        self.storage.touch_chat_after_send(chat_id, body=body, message_ts=message_ts, unread_count=0)
        current_draft = draft_context or self.storage.get_draft(chat_id) or {}
        if current_draft:
            self.storage.mark_draft_sent(chat_id, remote_message_id=remote_message_id or local_message_id)
        mark_read_error = ""
        if mark_read:
            try:
                self.api_client.mark_chat_as_read(chat_id)
            except Exception as exc:
                mark_read_error = str(exc)
                self._emit(
                    "avito_send_mark_read_failed",
                    f"Не удалось пометить чат {chat_id} как прочитанный",
                    channel="send",
                    level="warning",
                    chat_id=chat_id,
                    error=str(exc),
                )
        self._emit(
            "avito_send_finalized",
            f"Ответ в чат {chat_id} локально зафиксирован и помечен как отправленный",
            channel="send",
            chat_id=chat_id,
            remote_message_id=remote_message_id or local_message_id,
            message_ts=message_ts,
            attachments_count=len(attachments_payload),
            transport=clean_text(response.get("transport")),
            media_fallback=clean_text(response.get("media_fallback")),
            mark_read_error=mark_read_error,
        )
        return {
            "remote_message_id": remote_message_id or local_message_id,
            "local_message_id": local_message_id,
            "message_ts": message_ts,
            "attachments": attachments_payload,
            "mark_read_error": mark_read_error,
        }

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
            return {"chat": None, "knowledge_hits": [], "media_suggestions": [], "selected_media": [], "send_history": [], "flags": {}}
        messages = self.storage.get_messages(chat_id, limit=max(20, self.config.max_context_messages * 2))
        knowledge_hits = self._knowledge_hits_for_chat(chat, messages, track_metrics=False)
        media_suggestions = self._media_suggestions_for_chat(chat, messages, track_metrics=False)
        selected_media = self.storage.list_draft_media_assets(chat_id)
        similar_dialogs = self._similar_dialogs_for_chat(chat, messages, track_metrics=False)
        draft = self.storage.get_draft(chat_id) or {}
        return {
            "chat": chat,
            "messages": messages,
            "knowledge_hits": [hit.as_meta() for hit in knowledge_hits],
            "media_suggestions": [item.as_meta() for item in media_suggestions],
            "selected_media": selected_media,
            "similar_dialogs": similar_dialogs,
            "send_history": self.storage.list_send_events(chat_id, limit=20),
            "flags": self.storage.chat_flags(chat_id),
            "draft": draft,
            "decision_level": clean_text((draft.get("meta") or {}).get("decision_level")),
            "scenario": clean_text((draft.get("meta") or {}).get("scenario")),
            "blocked_by": clean_text((draft.get("meta") or {}).get("blocked_by")),
        }

    @staticmethod
    def _sticky_draft_states() -> set[str]:
        return {"review", "hold", "rejected", "sent", "ready", "sending"}

    def _is_sticky_draft_state(self, state: str) -> bool:
        return clean_text(state) in self._sticky_draft_states()

    def _should_skip_generation_for_chat(self, chat_id: str, *, force_regenerate: bool = False) -> tuple[bool, dict[str, Any]]:
        draft = self.storage.get_draft(chat_id) or {}
        if not draft or force_regenerate:
            return False, draft
        if self._is_sticky_draft_state(clean_text(draft.get("state"))):
            return True, draft
        return False, draft

    def _build_attachment_payload(self, selected_media: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        attachments_payload: List[Dict[str, Any]] = []
        for item in list(selected_media or []):
            attachments_payload.append(
                {
                    "asset_id": item.get("asset_id"),
                    "media_kind": item.get("media_kind"),
                    "title": item.get("title"),
                    "caption": item.get("caption"),
                    "file_name": item.get("file_name"),
                    "external_url": item.get("external_url"),
                    "local_path": item.get("local_path"),
                    "mime_type": item.get("mime_type"),
                }
            )
        return attachments_payload

    def _finalize_successful_send(
        self,
        chat_id: str,
        body: str,
        response: Dict[str, Any],
        *,
        selected_media: Optional[List[Dict[str, Any]]] = None,
        draft_context: Optional[Dict[str, Any]] = None,
        lease_id: str = "",
        author_name: str = "assistant",
    ) -> Dict[str, Any]:
        response = dict(response or {})
        message_payload = response.get("message") if isinstance(response.get("message"), dict) else {}
        remote_message_id = clean_text(message_payload.get("id") or response.get("id"))
        if not remote_message_id:
            remote_message_id = f"local-out-{hash((chat_id, body, utc_now_iso())) & 0xffffffffffff:x}"
        message_ts = clean_text(message_payload.get("created") or message_payload.get("created_at") or message_payload.get("timestamp") or response.get("message_ts")) or utc_now_iso()
        attachments_payload = self._build_attachment_payload(selected_media)
        self.storage.add_messages(
            chat_id,
            [
                {
                    "message_id": remote_message_id,
                    "direction": "out",
                    "is_read": True,
                    "author_name": clean_text(author_name) or "assistant",
                    "message_ts": message_ts,
                    "text": body,
                    "attachments": attachments_payload,
                    "raw": response,
                }
            ],
        )
        mark_read_error = ""
        try:
            self.api_client.mark_chat_as_read(chat_id)
        except Exception as exc:
            mark_read_error = str(exc)
            self._emit(
                "avito_send_mark_read_failed",
                f"Не удалось пометить чат {chat_id} как прочитанный",
                channel="send",
                level="warning",
                chat_id=chat_id,
                error=str(exc),
            )
        if draft_context or self.storage.get_draft(chat_id):
            self.storage.mark_draft_sent(chat_id, remote_message_id=remote_message_id, lease_id=lease_id)
        self._emit(
            "avito_send_finalized",
            f"Локальная история и статус чата {chat_id} обновлены после отправки",
            channel="send",
            chat_id=chat_id,
            remote_message_id=remote_message_id,
            message_ts=message_ts,
            attachments_count=len(attachments_payload),
            transport=clean_text(response.get("transport")),
            media_fallback=clean_text(response.get("media_fallback")),
            mark_read_error=mark_read_error,
        )
        return {
            "remote_message_id": remote_message_id,
            "message_ts": message_ts,
            "attachments": attachments_payload,
            "mark_read_error": mark_read_error,
        }

    def finalize_successful_send(
        self,
        chat_id: str,
        body: str,
        response: Dict[str, Any],
        *,
        selected_media: Optional[List[Dict[str, Any]]] = None,
        draft_context: Optional[Dict[str, Any]] = None,
        lease_id: str = "",
        author_name: str = "assistant",
    ) -> Dict[str, Any]:
        return self._finalize_successful_send(
            chat_id,
            body,
            response,
            selected_media=selected_media,
            draft_context=draft_context,
            lease_id=lease_id,
            author_name=author_name,
        )

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

    def generate_drafts(
        self,
        *,
        limit: int = 20,
        audit: Optional[AvitoAuditLogger] = None,
        chat_ids: Optional[Iterable[str]] = None,
        force_regenerate: bool = False,
    ) -> DraftRunResult:
        self._bind_audit(audit)
        result = DraftRunResult()
        sticky_states = set() if force_regenerate else self._sticky_draft_states()
        if chat_ids:
            chats: List[Dict[str, Any]] = []
            for chat_id in chat_ids:
                chat = self.storage.get_chat(chat_id)
                if isinstance(chat, dict):
                    chats.append(chat)
        else:
            chats = self.storage.unanswered_chats(limit=limit, sticky_states=list(sticky_states) if sticky_states else [])
        self._emit(
            "avito_drafts_start",
            "Запускаю генерацию AI-черновиков Avito",
            channel="ai",
            percent=0,
            limit=limit,
            chats_found=len(chats),
            force_regenerate=force_regenerate,
        )
        if not chats:
            result.notes.append("Нет чатов, требующих ответа")
            self._emit("avito_drafts_empty", "Нет чатов, требующих ответа", channel="ai", level="warning", percent=100)
            return result

        total = max(1, len(chats))
        for idx, chat in enumerate(chats, start=1):
            pre_percent = 5.0 + (idx - 1) / total * 70.0
            existing_draft = self.storage.get_draft(chat["chat_id"])
            existing_state = clean_text((existing_draft or {}).get("state"))
            if existing_draft and existing_state in sticky_states:
                result.skipped += 1
                note = f"Пропущен чат {chat['chat_id']}: уже есть черновик в состоянии {existing_state}"
                result.notes.append(note)
                self._emit(
                    "avito_draft_sticky_skip",
                    f"Чат {chat['chat_id']} пропущен: липкое состояние {existing_state}",
                    channel="ai",
                    level="warning",
                    percent=round(pre_percent, 1),
                    chat_id=chat["chat_id"],
                    existing_state=existing_state,
                )
                continue
            if existing_draft and force_regenerate:
                self._emit(
                    "avito_draft_force_regenerate",
                    f"Чат {chat['chat_id']} будет перегенерирован принудительно",
                    channel="ai",
                    percent=round(pre_percent, 1),
                    chat_id=chat["chat_id"],
                    previous_state=existing_state,
                )
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
            similar_dialogs = self._similar_dialogs_for_chat(chat, messages, track_metrics=True)
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
            policy = clean_text(self.config.auto_mode or "draft_only")
            scenario, effective_route, blocked_by, fallback = self._apply_policy_overrides(
                chat=chat,
                messages=messages,
                draft=draft,
                knowledge_hits=knowledge_hits,
                media_suggestions=media_suggestions,
            )
            draft_state = self._draft_state_for_hitl(draft, effective_route, blocked_by=blocked_by)
            decision_level = self._policy_decision_level(effective_route=effective_route, draft_state=draft_state)
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
                    extra={"decision": "skip", "knowledge_hits_count": len(knowledge_hits), "media_suggestions_count": len(media_suggestions)},
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
                    "force_regenerate": force_regenerate,
                    "scenario": scenario,
                    "decision_level": decision_level,
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
                    "draft_state": draft_state,
                    "knowledge_hits_count": len(knowledge_hits),
                    "media_suggestions_count": len(media_suggestions),
                    "similar_dialogs_count": len(similar_dialogs),
                    "scenario": scenario,
                    "decision_level": decision_level,
                },
            )
        self._emit(
            "avito_drafts_summary",
            "Генерация AI-черновиков Avito завершена",
            channel="ai",
            percent=95,
            generated=result.generated,
            auto_ready=result.auto_ready,
            manual_ready=result.manual_ready,
            skipped=result.skipped,
            notes=result.notes,
        )
        return result

    def _build_attachment_payload(self, selected_media: Optional[List[Dict[str, Any]]] = None, *, include_payload: bool = True) -> List[Dict[str, Any]]:
        if not include_payload:
            return []
        return [
            {
                "asset_id": item.get("asset_id"),
                "media_kind": item.get("media_kind"),
                "title": item.get("title"),
                "file_name": item.get("file_name"),
                "external_url": item.get("external_url"),
                "local_path": item.get("local_path"),
                "mime_type": item.get("mime_type"),
            }
            for item in list(selected_media or [])
        ]

    def _extract_response_message_id(self, response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        message = response.get("message") if isinstance(response.get("message"), dict) else {}
        return clean_text(message.get("id") or response.get("id") or message.get("message_id"))

    def _extract_response_message_ts(self, response: Any) -> str:
        if not isinstance(response, dict):
            return utc_now_iso()
        message = response.get("message") if isinstance(response.get("message"), dict) else {}
        return clean_text(
            message.get("message_ts")
            or message.get("created_at")
            or message.get("created")
            or response.get("message_ts")
            or response.get("created_at")
            or response.get("created")
            or utc_now_iso()
        )

    def _mark_chat_read_safely(self, chat_id: str, *, percent: Optional[float] = None) -> str:
        try:
            self.api_client.mark_chat_as_read(chat_id)
            return ""
        except Exception as exc:
            self._emit(
                "avito_send_mark_read_failed",
                f"Не удалось пометить чат {chat_id} как прочитанный",
                channel="send",
                level="warning",
                percent=percent,
                chat_id=chat_id,
                error=str(exc),
            )
            return str(exc)

    def _finalize_successful_send(
        self,
        chat_id: str,
        body: str,
        response: Dict[str, Any],
        *,
        selected_media: Optional[List[Dict[str, Any]]] = None,
        draft_context: Optional[Dict[str, Any]] = None,
        lease_id: str = "",
        author_name: str = "assistant",
    ) -> Dict[str, Any]:
        response = dict(response or {})
        transport = clean_text(response.get("transport"))
        media_fallback = clean_text(response.get("media_fallback"))
        delivery_status = clean_text(response.get("delivery_status")) or ("partial_sent_text_only" if media_fallback == "text_only" else "sent")
        detail = clean_text(response.get("detail") or media_fallback or response.get("media_error") or "")
        message_payload = response.get("message") if isinstance(response.get("message"), dict) else {}
        remote_message_id = clean_text(message_payload.get("id") or response.get("id"))
        if not remote_message_id:
            remote_message_id = f"local-out-{hashlib.sha256(f'{chat_id}:{body}:{utc_now_iso()}'.encode('utf-8')).hexdigest()[:16]}"
        message_ts = self._extract_response_message_ts(response)
        media_sent = bool(selected_media) and delivery_status == "sent" and transport in {"browser", "api"}
        attachments_payload = self._build_attachment_payload(selected_media, include_payload=media_sent)

        self.storage.record_send_event(
            chat_id,
            body=body,
            transport=transport or "text",
            delivery_status=delivery_status or "sent",
            remote_message_id=remote_message_id,
            attachments=attachments_payload,
            detail={
                "media_fallback": media_fallback,
                "media_error": clean_text(response.get("media_error")),
                "selected_media_ids": [item.get("asset_id") for item in list(selected_media or [])],
            },
        )
        if draft_context or self.storage.get_draft(chat_id):
            extra_meta = {"delivery_status": delivery_status, "send_transport": transport, "media_fallback": media_fallback, "finalized_at": utc_now_iso()}
            if clean_text(response.get("media_error")):
                extra_meta["media_error"] = clean_text(response.get("media_error"))
            if delivery_status == "partial_sent_text_only":
                self.storage.mark_draft_partial_sent(chat_id, remote_message_id=remote_message_id, lease_id=lease_id, extra_meta=extra_meta)
            else:
                self.storage.mark_draft_sent(chat_id, remote_message_id=remote_message_id, lease_id=lease_id, extra_meta=extra_meta)

        local_errors: List[str] = []
        try:
            self.storage.add_messages(
                chat_id,
                [
                    {
                        "message_id": remote_message_id,
                        "direction": "out",
                        "is_read": True,
                        "author_name": clean_text(author_name) or "assistant",
                        "message_ts": message_ts,
                        "created_at": message_ts,
                        "text": body,
                        "attachments": attachments_payload,
                        "raw": response,
                    }
                ],
            )
        except Exception as exc:
            local_errors.append(f"local_add_message_failed:{exc}")

        try:
            self.storage.touch_chat_after_send(
                chat_id,
                body=body,
                message_ts=message_ts,
                unread_count=0,
                delivery_status=delivery_status or "sent",
                detail=detail,
                operator_user=author_name,
            )
        except Exception as exc:
            local_errors.append(f"local_chat_touch_failed:{exc}")

        mark_read_error = self._mark_chat_read_safely(chat_id)
        if mark_read_error:
            local_errors.append(f"mark_read_failed:{mark_read_error}")

        self._emit(
            "avito_send_finalize",
            f"Удалённая отправка в чат {chat_id} подтверждена, локальное состояние обновлено",
            channel="send",
            chat_id=chat_id,
            remote_message_id=remote_message_id,
            message_ts=message_ts,
            attachments_count=len(attachments_payload),
            transport=transport,
            media_fallback=media_fallback,
            delivery_status=delivery_status,
            finalize_ok=not local_errors,
            local_errors=local_errors,
        )
        return {
            "remote_message_id": remote_message_id,
            "message_ts": message_ts,
            "attachments": attachments_payload,
            "mark_read_error": mark_read_error,
            "delivery_status": delivery_status,
            "local_errors": local_errors,
            "finalize_ok": not local_errors,
        }

    def send_ready_drafts(self, *, limit: int = 20, auto_only: bool = False, audit: Optional[AvitoAuditLogger] = None) -> SendRunResult:
        self._bind_audit(audit)
        routes = ["auto"] if auto_only else None
        lease_id = hashlib.sha256(f"{self.tenant_id}:{utc_now_iso()}:{limit}:{auto_only}".encode("utf-8")).hexdigest()[:20]
        drafts = self.storage.claim_ready_drafts(routes=routes, limit=limit, lease_seconds=300, lease_id=lease_id)
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
            current_lease_id = clean_text(draft.get("lease_id") or lease_id)
            chat_id = clean_text(draft.get("chat_id"))
            self._emit(
                "avito_send_chat_start",
                f"Отправляю ответ в чат {chat_id}",
                channel="send",
                percent=round(start_percent, 1),
                chat_id=chat_id,
                route=draft.get("route"),
                lease_id=current_lease_id,
            )
            try:
                duplicate = self.detect_recent_duplicate_send(chat_id, clean_text(draft.get("body")), window_seconds=600)
                if duplicate:
                    self.storage.release_draft_lease(chat_id, state="hold")
                    result.suppressed += 1
                    result.notes.append(f"Повторная отправка подавлена для чата {chat_id}")
                    self._emit(
                        "avito_send_duplicate_suppressed",
                        f"Повторная отправка в чат {chat_id} подавлена",
                        channel="send",
                        level="warning",
                        percent=round(start_percent + 2.0, 1),
                        chat_id=chat_id,
                        duplicate_send_id=duplicate.get("send_id"),
                    )
                    continue

                selected_media = self.storage.list_draft_media_assets(chat_id)
                if selected_media and not self.config.media_send_enabled:
                    self._emit(
                        "avito_send_media_pending",
                        f"Для чата {chat_id} подготовлены медиа, но отправка вложений выключена",
                        channel="media",
                        level="warning",
                        percent=round(start_percent + 1.0, 1),
                        chat_id=chat_id,
                        asset_ids=[item.get("asset_id") for item in selected_media],
                    )
                response = self.send_chat_reply(chat_id, clean_text(draft.get("body")), selected_media=selected_media, draft_context=draft)
                finalized = self._finalize_successful_send(
                    chat_id,
                    clean_text(draft.get("body")),
                    response if isinstance(response, dict) else {"result": response},
                    selected_media=selected_media,
                    draft_context=draft,
                    lease_id=current_lease_id,
                    author_name="assistant",
                )
                remote_message_id = clean_text(finalized.get("remote_message_id"))
                delivery_status = clean_text(finalized.get("delivery_status") or (response or {}).get("delivery_status") or "sent")
                if delivery_status == "partial_sent_text_only":
                    result.partial_sent += 1
                else:
                    result.sent += 1
                self._emit(
                    "avito_send_chat_ok",
                    f"Ответ отправлен в чат {chat_id}",
                    channel="send",
                    percent=round(start_percent + 12.0, 1),
                    chat_id=chat_id,
                    remote_message_id=remote_message_id,
                    transport=clean_text((response or {}).get("transport")) if isinstance(response, dict) else "",
                    media_fallback=clean_text((response or {}).get("media_fallback")) if isinstance(response, dict) else "",
                    attachments_count=len(finalized.get("attachments") or []),
                    delivery_status=delivery_status,
                    finalize_ok=bool(finalized.get("finalize_ok", True)),
                    local_errors=finalized.get("local_errors") or [],
                )
                meta = draft.get("meta") or {}
                self._emit_decision(
                    chat_id=chat_id,
                    route=clean_text(draft.get("route") or "manual"),
                    confidence=float(draft.get("confidence") or 0.0),
                    reason=clean_text(draft.get("reason") or "sent"),
                    policy=clean_text(meta.get("policy") or self.config.auto_mode),
                    blocked_by=clean_text(meta.get("blocked_by") or ""),
                    fallback=clean_text(meta.get("fallback") or ""),
                    model_name=clean_text(draft.get("model_name") or ""),
                    extra={
                        "decision": "sent",
                        "draft_state": delivery_status,
                        "finalize_ok": bool(finalized.get("finalize_ok", True)),
                        "local_errors": finalized.get("local_errors") or [],
                        "scenario": clean_text(meta.get("scenario") or ""),
                        "decision_level": clean_text(meta.get("decision_level") or ""),
                    },
                )
            except Exception as exc:
                result.failed += 1
                self.storage.mark_draft_error(chat_id, str(exc), lease_id=current_lease_id)
                result.notes.append(f"Чат {chat_id}: {exc}")
                self._emit(
                    "avito_send_chat_failed",
                    f"Ошибка отправки в чат {chat_id}",
                    channel="send",
                    level="error",
                    percent=round(start_percent + 12.0, 1),
                    chat_id=chat_id,
                    error=str(exc),
                    lease_id=current_lease_id,
                )

        self._emit(
            "avito_send_summary",
            "Отправка готовых Avito-черновиков завершена",
            channel="send",
            percent=95,
            sent=result.sent,
            partial_sent=result.partial_sent,
            suppressed=result.suppressed,
            failed=result.failed,
            notes=result.notes,
        )
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
                    response.setdefault("delivery_status", "sent")
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
                        response.setdefault("delivery_status", "sent")
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
                response["delivery_status"] = "partial_sent_text_only"
                response["detail"] = "Медиа не были отправлены, доставлен только текст"
            else:
                response.setdefault("delivery_status", "sent")
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
        defaults = {
            "incoming_5m": 0,
            "incoming_15m": 0,
            "incoming_60m": 0,
            "avg_first_response_minutes": None,
            "auto_reply_share": 0.0,
            "escalation_share": 0.0,
            "token_refresh_events_60m": 0,
            "circuit_open_events_60m": 0,
            "webhook_verify_fail_pct_24h": 0.0,
            "webhook_process_lag_seconds_avg": None,
            "browser_fallback_share": 0.0,
            "sync_runs_considered": 0,
            "drafts_total": 0,
            "sent_drafts_total": 0,
            "partial_sent_total": 0,
            "review_queue_count": 0,
            "hold_queue_count": 0,
            "approved_queue_count": 0,
            "error_queue_count": 0,
            "webhook_events_24h": 0,
            "knowledge_docs_total": 0,
            "knowledge_docs_active": 0,
            "media_assets_total": 0,
            "media_assets_active": 0,
            "knowledge_hit_rate": 0.0,
            "new_chats_count": 0,
            "in_progress_chats_count": 0,
            "waiting_customer_chats_count": 0,
            "closed_chats_count": 0,
            "escalation_chats_count": 0,
            "overdue_queue_count": 0,
            "answered_chats_count": 0,
            "send_errors_24h": 0,
            "dlq_open_count": 0,
            "poll_lag_seconds": None,
        }
        try:
            snapshot = {**defaults, **(self.storage.compute_metrics() or {})}
        except Exception as exc:
            snapshot = dict(defaults)
            self._emit("avito_metrics_snapshot_failed", "Не удалось собрать оперативные метрики Avito", channel="ops", level="error", error=str(exc))
        self._emit("avito_metrics_snapshot", "Собран снимок оперативных метрик Avito", channel="ops", incoming_5m=snapshot.get("incoming_5m"), incoming_60m=snapshot.get("incoming_60m"), auto_reply_share=snapshot.get("auto_reply_share"))
        return snapshot

    @staticmethod
    def _parse_iso_dt(value: Any) -> Optional[datetime]:
        raw = clean_text(value)
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _component_state(status: str, title: str, summary: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return {
            "status": clean_text(status) or "info",
            "title": clean_text(title),
            "summary": clean_text(summary),
            "details": details or {},
        }

    def _health_alert_rules(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.config.health_alerts_enabled),
            "webhook_silent_hours": int(self.config.health_webhook_silent_hours or 24),
            "webhook_verify_fail_pct": float(self.config.health_alert_webhook_verify_fail_pct or 20.0),
            "token_refresh_events_60m": int(self.config.health_alert_token_refresh_events_60m or 5),
            "circuit_open_events_60m": int(self.config.health_alert_circuit_open_events_60m or 1),
            "browser_fallback_share_pct": float(self.config.health_alert_browser_fallback_share_pct or 50.0),
            "send_errors_24h": int(self.config.health_alert_send_errors_24h or 1),
            "overdue_queue_count": int(self.config.health_alert_overdue_queue_count or 1),
            "poll_lag_seconds": int(self.config.health_alert_poll_lag_seconds or 600),
            "dlq_open_count": int(self.config.health_alert_dlq_open_count or 1),
            "knowledge_hit_rate_min_pct": float(self.config.health_alert_knowledge_hit_rate_min_pct or 20.0),
        }

    def health_snapshot(self, *, persist_alerts: bool = True) -> Dict[str, Any]:
        metrics = self.metrics_snapshot()
        rules = self._health_alert_rules()
        now_dt = datetime.now(timezone.utc)

        last_sync_record = self.storage.load_sync_state_record("last_sync", {})
        last_backfill_record = self.storage.load_sync_state_record("last_backfill", {})
        last_sync_at = clean_text(last_sync_record.get("updated_at"))
        last_backfill_at = clean_text(last_backfill_record.get("updated_at"))
        last_sync_dt = self._parse_iso_dt(last_sync_at)
        last_backfill_dt = self._parse_iso_dt(last_backfill_at)
        poll_lag_seconds = int(max(0.0, (now_dt - last_sync_dt).total_seconds())) if last_sync_dt else None
        metrics["poll_lag_seconds"] = poll_lag_seconds

        recent_webhooks = self.storage.list_webhook_events(limit=20)
        latest_webhook = recent_webhooks[0] if recent_webhooks else {}
        latest_webhook_at = clean_text((latest_webhook or {}).get("received_at"))
        latest_webhook_dt = self._parse_iso_dt(latest_webhook_at)
        webhook_age_seconds = int(max(0.0, (now_dt - latest_webhook_dt).total_seconds())) if latest_webhook_dt else None

        dlq_open_count = self.storage.count_dead_letters(status="open")
        metrics["dlq_open_count"] = dlq_open_count

        alerts_input: List[Dict[str, Any]] = []
        if rules["enabled"]:
            if self.config.webhook_first_enabled:
                silent_hours = max(1, int(rules["webhook_silent_hours"] or 24))
                webhook_is_silent = latest_webhook_dt is None or (webhook_age_seconds is not None and webhook_age_seconds >= silent_hours * 3600)
                if webhook_is_silent:
                    alerts_input.append({
                        "code": "webhook_silent",
                        "severity": "warning",
                        "title": "Webhook не подаёт признаков жизни",
                        "message": "Webhook давно не присылал событий. Проверь внешний вызов и доступность URL.",
                        "details": {"silent_hours": silent_hours, "latest_webhook_at": latest_webhook_at, "webhook_age_seconds": webhook_age_seconds},
                    })
            if float(metrics.get("webhook_verify_fail_pct_24h") or 0.0) >= float(rules["webhook_verify_fail_pct"] or 20.0):
                alerts_input.append({
                    "code": "webhook_verify_fail",
                    "severity": "error",
                    "title": "Webhook часто отклоняется по подписи",
                    "message": "Высокая доля отклонённых webhook-событий. Проверь подпись, время и nonce.",
                    "details": {"verify_fail_pct": metrics.get("webhook_verify_fail_pct_24h"), "threshold": rules["webhook_verify_fail_pct"]},
                })
            if int(metrics.get("token_refresh_events_60m") or 0) >= int(rules["token_refresh_events_60m"] or 5):
                alerts_input.append({
                    "code": "token_refresh_spike",
                    "severity": "warning",
                    "title": "Токен обновляется слишком часто",
                    "message": "Слишком частые refresh-операции могут говорить о проблемах с OAuth или нестабильности API.",
                    "details": {"events": metrics.get("token_refresh_events_60m"), "threshold": rules["token_refresh_events_60m"]},
                })
            if int(metrics.get("circuit_open_events_60m") or 0) >= int(rules["circuit_open_events_60m"] or 1):
                alerts_input.append({
                    "code": "api_circuit_open",
                    "severity": "error",
                    "title": "Открывается предохранитель API",
                    "message": "Circuit breaker срабатывает слишком часто. Проверь Avito API и лимиты запросов.",
                    "details": {"events": metrics.get("circuit_open_events_60m"), "threshold": rules["circuit_open_events_60m"]},
                })
            if float(metrics.get("browser_fallback_share") or 0.0) >= float(rules["browser_fallback_share_pct"] or 50.0):
                alerts_input.append({
                    "code": "browser_fallback_high",
                    "severity": "warning",
                    "title": "Слишком часто включается браузерный резерв",
                    "message": "Контур слишком часто уходит в browser fallback. Это признак деградации API или webhook.",
                    "details": {"share_pct": metrics.get("browser_fallback_share"), "threshold": rules["browser_fallback_share_pct"]},
                })
            if int(metrics.get("send_errors_24h") or 0) >= int(rules["send_errors_24h"] or 1):
                alerts_input.append({
                    "code": "send_errors",
                    "severity": "error",
                    "title": "Есть ошибки отправки сообщений",
                    "message": "За последние 24 часа были ошибки отправки. Нужна проверка контура доставки.",
                    "details": {"send_errors_24h": metrics.get("send_errors_24h"), "threshold": rules["send_errors_24h"]},
                })
            if int(metrics.get("overdue_queue_count") or 0) >= int(rules["overdue_queue_count"] or 1):
                alerts_input.append({
                    "code": "sla_overdue",
                    "severity": "warning",
                    "title": "Есть просроченные диалоги по SLA",
                    "message": "В операторской очереди есть диалоги, по которым просрочен первый ответ.",
                    "details": {"overdue_queue_count": metrics.get("overdue_queue_count"), "threshold": rules["overdue_queue_count"]},
                })
            if self.config.polling_fallback_enabled and poll_lag_seconds is not None and poll_lag_seconds >= int(rules["poll_lag_seconds"] or 600):
                alerts_input.append({
                    "code": "poll_lag",
                    "severity": "warning",
                    "title": "Резервный poll-sync отстаёт",
                    "message": "Последний poll-sync был слишком давно. Проверь фоновый воркер или scheduler.",
                    "details": {"poll_lag_seconds": poll_lag_seconds, "threshold": rules["poll_lag_seconds"], "last_sync_at": last_sync_at},
                })
            if int(dlq_open_count or 0) >= int(rules["dlq_open_count"] or 1):
                alerts_input.append({
                    "code": "dlq_backlog",
                    "severity": "error",
                    "title": "В очереди ошибок есть застрявшие события",
                    "message": "Открытые DLQ-события требуют внимания. Часть webhook или send-контуров не была обработана до конца.",
                    "details": {"dlq_open_count": dlq_open_count, "threshold": rules["dlq_open_count"]},
                })
            if int(metrics.get("knowledge_docs_active") or 0) > 0 and float(metrics.get("knowledge_hit_rate") or 0.0) < float(rules["knowledge_hit_rate_min_pct"] or 20.0):
                alerts_input.append({
                    "code": "knowledge_low_hit_rate",
                    "severity": "warning",
                    "title": "База знаний редко помогает в ответах",
                    "message": "Низкий процент попаданий базы знаний. Возможно, KB нужно расширить или улучшить теги/формулировки.",
                    "details": {"knowledge_hit_rate": metrics.get("knowledge_hit_rate"), "threshold": rules["knowledge_hit_rate_min_pct"]},
                })

        sync_result = self.storage.sync_health_alerts(alerts_input) if persist_alerts else {"active": self.storage.list_health_alerts(status="active", limit=50), "opened": [], "resolved": []}
        if persist_alerts:
            for item in sync_result.get("opened") or []:
                self._emit("avito_health_alert_opened", f"Открыт сигнал здоровья: {item.get('title')}", channel="health", level=item.get("severity") or "warning", code=item.get("code"), status="open")
            for item in sync_result.get("resolved") or []:
                self._emit("avito_health_alert_resolved", f"Сигнал здоровья закрыт: {item.get('title')}", channel="health", level="info", code=item.get("code"), status="resolved")

        alerts = []
        for item in sync_result.get("active") or []:
            enriched = dict(item)
            enriched["level"] = clean_text(item.get("severity") or item.get("level") or "warning")
            alerts.append(enriched)

        components: Dict[str, Dict[str, Any]] = {}
        if not self.config.webhook_first_enabled:
            components["webhook"] = self._component_state("info", "Webhook", "Webhook не является основным каналом.", {"latest_webhook_at": latest_webhook_at})
        elif any(clean_text(item.get("code")) == "webhook_verify_fail" for item in alerts):
            components["webhook"] = self._component_state("error", "Webhook", "Подпись webhook часто не проходит проверку.", {"latest_webhook_at": latest_webhook_at, "verify_fail_pct": metrics.get("webhook_verify_fail_pct_24h")})
        elif any(clean_text(item.get("code")) == "webhook_silent" for item in alerts):
            components["webhook"] = self._component_state("warning", "Webhook", "Webhook давно не присылал событий.", {"latest_webhook_at": latest_webhook_at, "age_seconds": webhook_age_seconds})
        else:
            components["webhook"] = self._component_state("ok", "Webhook", "Webhook-канал выглядит живым.", {"latest_webhook_at": latest_webhook_at, "events_24h": metrics.get("webhook_events_24h")})

        if not self.config.polling_fallback_enabled:
            components["polling"] = self._component_state("info", "Poll-sync", "Резервный опрос отключён.", {"last_sync_at": last_sync_at})
        elif any(clean_text(item.get("code")) == "poll_lag" for item in alerts):
            components["polling"] = self._component_state("warning", "Poll-sync", "Резервный опрос отстаёт от ожидаемого интервала.", {"last_sync_at": last_sync_at, "poll_lag_seconds": poll_lag_seconds})
        else:
            components["polling"] = self._component_state("ok", "Poll-sync", "Резервный polling работает в ожидаемом окне.", {"last_sync_at": last_sync_at, "poll_lag_seconds": poll_lag_seconds})

        if any(clean_text(item.get("code")) == "api_circuit_open" for item in alerts):
            components["api"] = self._component_state("error", "API / circuit breaker", "Circuit breaker срабатывает слишком часто.", {"circuit_open_events_60m": metrics.get("circuit_open_events_60m")})
        elif any(clean_text(item.get("code")) == "token_refresh_spike" for item in alerts):
            components["api"] = self._component_state("warning", "API / токен", "Токен обновляется чаще обычного.", {"token_refresh_events_60m": metrics.get("token_refresh_events_60m")})
        else:
            components["api"] = self._component_state("ok", "API / токен", "API-контур и токен-хранитель выглядят стабильно.", {"token_refresh_events_60m": metrics.get("token_refresh_events_60m"), "circuit_open_events_60m": metrics.get("circuit_open_events_60m")})

        if any(clean_text(item.get("code")) == "send_errors" for item in alerts):
            components["delivery"] = self._component_state("error", "Доставка ответов", "Есть ошибки отправки сообщений за последние 24 часа.", {"send_errors_24h": metrics.get("send_errors_24h"), "partial_sent_total": metrics.get("partial_sent_total")})
        elif int(metrics.get("partial_sent_total") or 0) > 0:
            components["delivery"] = self._component_state("warning", "Доставка ответов", "Есть частичные отправки: текст ушёл, вложения — нет.", {"partial_sent_total": metrics.get("partial_sent_total")})
        else:
            components["delivery"] = self._component_state("ok", "Доставка ответов", "Ошибок доставки не видно.", {"sent_drafts_total": metrics.get("sent_drafts_total")})

        if any(clean_text(item.get("code")) == "knowledge_low_hit_rate" for item in alerts):
            components["knowledge"] = self._component_state("warning", "База знаний", "База знаний редко помогает в ответах.", {"knowledge_hit_rate": metrics.get("knowledge_hit_rate"), "knowledge_docs_active": metrics.get("knowledge_docs_active")})
        elif int(metrics.get("knowledge_docs_active") or 0) <= 0:
            components["knowledge"] = self._component_state("info", "База знаний", "Активных документов базы знаний пока нет.", {"knowledge_docs_total": metrics.get("knowledge_docs_total")})
        else:
            components["knowledge"] = self._component_state("ok", "База знаний", "База знаний активно участвует в ответах.", {"knowledge_hit_rate": metrics.get("knowledge_hit_rate"), "knowledge_docs_active": metrics.get("knowledge_docs_active")})

        if any(clean_text(item.get("code")) == "sla_overdue" for item in alerts):
            components["queue"] = self._component_state("warning", "Операторская очередь", "Есть диалоги, просроченные по SLA.", {"overdue_queue_count": metrics.get("overdue_queue_count"), "review_queue_count": metrics.get("review_queue_count")})
        else:
            components["queue"] = self._component_state("ok", "Операторская очередь", "Просроченных SLA-диалогов не видно.", {"overdue_queue_count": metrics.get("overdue_queue_count"), "review_queue_count": metrics.get("review_queue_count")})

        if any(clean_text(item.get("code")) == "dlq_backlog" for item in alerts):
            components["dlq"] = self._component_state("error", "Очередь ошибок", "Есть открытые события в DLQ, требующие разбора.", {"dlq_open_count": dlq_open_count})
        else:
            components["dlq"] = self._component_state("ok", "Очередь ошибок", "Открытых DLQ-событий нет или они в норме.", {"dlq_open_count": dlq_open_count})

        if any(clean_text(item.get("code")) == "browser_fallback_high" for item in alerts):
            components["browser"] = self._component_state("warning", "Браузерный резерв", "Резерв через браузер используется слишком часто.", {"browser_fallback_share": metrics.get("browser_fallback_share")})
        else:
            components["browser"] = self._component_state("ok", "Браузерный резерв", "Browser fallback используется в пределах нормы.", {"browser_fallback_share": metrics.get("browser_fallback_share")})

        alert_counts = {
            "error": sum(1 for item in alerts if clean_text(item.get("level")) == "error"),
            "warning": sum(1 for item in alerts if clean_text(item.get("level")) == "warning"),
            "acknowledged": sum(1 for item in alerts if clean_text(item.get("status")) == "acknowledged"),
        }
        overall_status = "error" if alert_counts["error"] else ("warning" if alert_counts["warning"] else "ok")
        history = self.storage.list_health_alerts(status="all", limit=60)
        return {
            "metrics": metrics,
            "alerts": alerts,
            "alert_counts": alert_counts,
            "overall_status": overall_status,
            "components": components,
            "component_order": ["webhook", "polling", "api", "delivery", "knowledge", "queue", "dlq", "browser"],
            "rules": rules,
            "latest_webhook": latest_webhook,
            "latest_webhook_at": latest_webhook_at,
            "webhook_age_seconds": webhook_age_seconds,
            "last_sync": last_sync_record,
            "last_sync_at": last_sync_at,
            "last_backfill": last_backfill_record,
            "last_backfill_at": last_backfill_at,
            "alert_history": history,
            "alert_history_open_count": sum(1 for item in history if clean_text(item.get("status")) == "open"),
            "alert_history_ack_count": sum(1 for item in history if clean_text(item.get("status")) == "acknowledged"),
            "alert_history_resolved_count": sum(1 for item in history if clean_text(item.get("status")) == "resolved"),
        }

    def health_dashboard_snapshot(self, *, persist_alerts: bool = True) -> Dict[str, Any]:
        snapshot = self.health_snapshot(persist_alerts=persist_alerts)
        snapshot["recent_runs"] = self.storage.list_recent_runs(limit=20)
        snapshot["recent_webhooks"] = self.storage.list_webhook_events(limit=15)
        snapshot["recent_dlq"] = self.storage.list_dead_letters(limit=15, status="all")
        return snapshot

    def list_operator_assignees(self) -> List[str]:
        values = list(self.storage.list_assignees(limit=200))
        default_assignee = clean_text(self.config.hitl_queue_default_assignee)
        if default_assignee and default_assignee not in values:
            values.append(default_assignee)
        return sorted({clean_text(item) for item in values if clean_text(item)}, key=str.casefold)

    @staticmethod
    def _sla_state_for_chat(chat: Dict[str, Any]) -> str:
        first_response_epoch = int(chat.get("first_response_epoch") or 0)
        due_epoch = int(chat.get("first_response_due_epoch") or 0)
        if first_response_epoch:
            return "answered"
        if not due_epoch:
            return "none"
        now_epoch = int(time.time())
        if due_epoch <= now_epoch:
            return "overdue"
        if due_epoch - now_epoch <= 5 * 60:
            return "due_soon"
        return "on_track"

    def _operator_bucket_match(self, bucket: str, row: Dict[str, Any], *, actor: str, assignee: str) -> bool:
        bucket = clean_text(bucket) or "team"
        assigned_to = clean_text(row.get("assigned_to"))
        status = clean_text(row.get("status") or "new")
        flags = row.get("flags") or {}
        if assignee and assigned_to != assignee:
            return False
        if bucket == "team":
            return True
        if bucket == "mine":
            return bool(actor and assigned_to == actor)
        if bucket == "unassigned":
            return not assigned_to
        if bucket == "overdue":
            return bool(flags.get("overdue"))
        if bucket == "human":
            return bool(flags.get("needs_human"))
        if bucket == "escalation":
            return status == "escalation"
        if bucket == "waiting":
            return status == "waiting_customer"
        if bucket == "with_media":
            return bool(flags.get("has_media_selected"))
        if bucket == "bargain":
            return bool(flags.get("has_bargain"))
        return True

    def _operator_row(self, chat: Dict[str, Any]) -> Dict[str, Any]:
        chat_id = clean_text(chat.get("chat_id"))
        draft = self.storage.get_draft(chat_id) or {}
        flags = self.storage.chat_flags(chat_id)
        selected_media = self.storage.list_draft_media_assets(chat_id)
        draft_meta = draft.get("meta") or {}
        blocked_by = clean_text(draft_meta.get("blocked_by")) if isinstance(draft_meta, dict) else ""
        queue_reason = blocked_by
        if not queue_reason and flags.get("needs_human"):
            queue_reason = "Диалог требует ручной проверки по правилам маршрутизации."
        if not queue_reason and flags.get("has_bargain"):
            queue_reason = "Клиент просит скидку или торгуется."
        if not queue_reason and flags.get("asks_media") and not selected_media:
            queue_reason = "Клиент просит фото или видео, но материалы ещё не выбраны."
        enriched = dict(chat)
        enriched.update(
            {
                "flags": flags,
                "draft": draft,
                "draft_state": clean_text(draft.get("state")),
                "decision_level": clean_text((draft_meta or {}).get("decision_level")) if isinstance(draft_meta, dict) else "",
                "scenario": clean_text((draft_meta or {}).get("scenario")) if isinstance(draft_meta, dict) else "",
                "blocked_by": blocked_by,
                "selected_media_count": len(selected_media),
                "sla_state": self._sla_state_for_chat(chat),
                "queue_reason": queue_reason,
            }
        )
        return enriched

    def operator_queue_snapshot(
        self,
        *,
        bucket: str = "team",
        actor: str = "",
        assignee: str = "",
        status: str = "all",
        limit: int = 120,
        only_unanswered: bool = False,
    ) -> List[Dict[str, Any]]:
        target_limit = max(int(limit or 120), 1)
        prefetch = min(max(target_limit * 5, 300), 1500)
        status_filter = clean_text(status) or "all"
        assignee_filter = "" if clean_text(bucket) == "unassigned" else clean_text(assignee)
        chats = self.storage.list_chats(
            status=status_filter,
            only_unanswered=only_unanswered,
            assigned_to=assignee_filter,
            limit=prefetch,
            offset=0,
        )
        rows: List[Dict[str, Any]] = []
        actor_name = clean_text(actor)
        for chat in chats:
            row = self._operator_row(chat)
            if not self._operator_bucket_match(clean_text(bucket), row, actor=actor_name, assignee=clean_text(assignee)):
                continue
            rows.append(row)

        def _sort_key(item: Dict[str, Any]):
            overdue_rank = {"overdue": 0, "due_soon": 1, "on_track": 2, "answered": 3, "none": 4}.get(clean_text(item.get("sla_state")), 5)
            human_rank = 0 if (item.get("flags") or {}).get("needs_human") else 1
            due_epoch = int(item.get("first_response_due_epoch") or 0) or 10 ** 12
            unread_rank = -int(item.get("unread_count") or 0)
            last_message_rank = -int(item.get("last_message_ts_epoch") or 0)
            return (overdue_rank, human_rank, due_epoch, unread_rank, last_message_rank, clean_text(item.get("chat_id")))

        rows.sort(key=_sort_key)
        return rows[:target_limit]

    def operator_dashboard_snapshot(self, *, actor: str = "") -> Dict[str, Any]:
        chats = self.storage.list_chats(status="all", limit=1500, offset=0)
        actor_name = clean_text(actor)
        counts = {
            "total": 0,
            "mine": 0,
            "unassigned": 0,
            "overdue": 0,
            "human": 0,
            "escalation": 0,
            "waiting": 0,
            "with_media": 0,
            "bargain": 0,
            "closed": 0,
        }
        assignee_counts: Dict[str, int] = {}
        for chat in chats:
            row = self._operator_row(chat)
            counts["total"] += 1
            assigned_to = clean_text(row.get("assigned_to"))
            if actor_name and assigned_to == actor_name:
                counts["mine"] += 1
            if not assigned_to:
                counts["unassigned"] += 1
            if clean_text(row.get("status")) == "closed":
                counts["closed"] += 1
            if clean_text(row.get("status")) == "escalation":
                counts["escalation"] += 1
            if clean_text(row.get("status")) == "waiting_customer":
                counts["waiting"] += 1
            if (row.get("flags") or {}).get("overdue"):
                counts["overdue"] += 1
            if (row.get("flags") or {}).get("needs_human"):
                counts["human"] += 1
            if (row.get("flags") or {}).get("has_media_selected"):
                counts["with_media"] += 1
            if (row.get("flags") or {}).get("has_bargain"):
                counts["bargain"] += 1
            if assigned_to:
                assignee_counts[assigned_to] = assignee_counts.get(assigned_to, 0) + 1
        top_assignees = [
            {"name": name, "count": count}
            for name, count in sorted(assignee_counts.items(), key=lambda item: (-item[1], item[0].casefold()))[:10]
        ]
        return {
            "counts": counts,
            "assignees": self.list_operator_assignees(),
            "top_assignees": top_assignees,
            "current_user": actor_name,
            "truncated": len(chats) >= 1500,
        }

    def apply_operator_action(
        self,
        chat_ids: Iterable[str],
        *,
        action: str,
        actor: str = "",
        assignee: str = "",
        note: str = "",
    ) -> Dict[str, Any]:
        safe_action = clean_text(action)
        safe_actor = clean_text(actor)
        safe_assignee = clean_text(assignee)
        safe_note = clean_text(note)
        chat_id_list = [clean_text(chat_id) for chat_id in chat_ids if clean_text(chat_id)]
        if not chat_id_list:
            raise ValueError("Не выбраны диалоги для действия.")
        if safe_action in {"claim_me", "start"} and not safe_actor:
            raise ValueError("Невозможно определить текущего оператора для выбранного действия.")
        if safe_action == "assign" and not safe_assignee:
            raise ValueError("Укажи оператора, на которого нужно назначить диалоги.")

        action_labels = {
            "claim_me": "Назначить на себя",
            "assign": "Назначить оператора",
            "release": "Снять назначение",
            "start": "Взять в работу",
            "waiting": "Ждать клиента",
            "escalate": "Эскалация",
            "close": "Закрыть",
            "reopen": "Переоткрыть",
            "priority_high": "Высокий приоритет",
            "priority_normal": "Обычный приоритет",
            "priority_low": "Низкий приоритет",
        }
        updated = 0
        notes: List[str] = []
        for chat_id in chat_id_list:
            chat = self.storage.get_chat(chat_id) or {}
            if not chat:
                continue
            next_status: Optional[str] = None
            next_assigned_to: Optional[str] = None
            next_priority: Optional[str] = None
            if safe_action == "claim_me":
                next_assigned_to = safe_actor
                if clean_text(chat.get("status")) in {"", "new", "escalation"}:
                    next_status = "in_progress"
            elif safe_action == "assign":
                next_assigned_to = safe_assignee
            elif safe_action == "release":
                next_assigned_to = ""
            elif safe_action == "start":
                next_status = "in_progress"
                next_assigned_to = clean_text(chat.get("assigned_to")) or safe_actor
            elif safe_action == "waiting":
                next_status = "waiting_customer"
            elif safe_action == "escalate":
                next_status = "escalation"
                next_assigned_to = clean_text(chat.get("assigned_to")) or safe_actor
            elif safe_action == "close":
                next_status = "closed"
            elif safe_action == "reopen":
                next_status = "in_progress"
            elif safe_action == "priority_high":
                next_priority = "high"
            elif safe_action == "priority_normal":
                next_priority = "normal"
            elif safe_action == "priority_low":
                next_priority = "low"
            else:
                raise ValueError(f"Неизвестное операторское действие: {safe_action}")

            merged_note: Optional[str] = None
            if safe_note:
                previous = clean_text(chat.get("note"))
                merged_note = f"{previous}\n{safe_note}".strip() if previous else safe_note
            self.storage.update_chat_meta(
                chat_id,
                status=next_status,
                assigned_to=next_assigned_to,
                priority=next_priority,
                note=merged_note,
                operator_user=safe_actor,
            )
            updated += 1
            notes.append(f"{clean_text(chat.get('client_name') or chat.get('title') or chat_id)}: {action_labels.get(safe_action, safe_action)}")
            log_avito_event(
                self.storage,
                channel="ui",
                stage="avito_operator_action_applied",
                message="Операторское действие применено к диалогу Avito",
                kind="avito_operator_action",
                chat_id=chat_id,
                action=safe_action,
                actor=safe_actor,
                assigned_to=next_assigned_to,
                status=next_status,
                priority=next_priority,
            )
        return {
            "updated": updated,
            "action": safe_action,
            "action_label": action_labels.get(safe_action, safe_action),
            "notes": notes,
        }

    def promote_chat_example(self, chat_id: str, *, mode: str = "exemplar", actor: str = "") -> int:
        chat = self.storage.get_chat(chat_id) or {}
        if not chat:
            raise ValueError("Чат не найден")
        messages = self.storage.get_messages(chat_id, limit=50)
        latest_in = next((clean_text(m.get("text")) for m in reversed(messages) if clean_text(m.get("direction")) == "in" and clean_text(m.get("text"))), "")
        latest_out = next((clean_text(m.get("text")) for m in reversed(messages) if clean_text(m.get("direction")) == "out" and clean_text(m.get("text"))), "")
        draft = self.storage.get_draft(chat_id) or {}
        draft_body = clean_text(draft.get("body"))
        answer_text = draft_body or latest_out
        if not latest_in or not answer_text:
            raise ValueError("Недостаточно данных для создания примера")
        safe_mode = clean_text(mode) or "exemplar"
        title = f"Avito пример — {chat.get('item_title') or chat.get('title') or chat_id}"
        body_text = f"Вопрос клиента:\n{latest_in}\n\nРекомендуемый ответ:\n{answer_text}"
        tags = ["avito", safe_mode]
        kind = "qa" if safe_mode in {"exemplar", "dialog_example"} else "script"
        doc_id = self.storage.upsert_knowledge_doc(
            title=title,
            body_text=body_text,
            kind=kind,
            item_id=clean_text(chat.get("item_id")),
            item_title=clean_text(chat.get("item_title") or chat.get("title")),
            tags=tags,
            source_name=clean_text(actor) or "operator",
            active=True,
            meta={
                "chat_id": chat_id,
                "promotion_mode": safe_mode,
                "latest_in": latest_in,
                "latest_out": latest_out,
                "draft_state": clean_text(draft.get("state")),
            },
            chunk_chars=self.config.knowledge_chunk_chars,
            overlap_chars=self.config.knowledge_chunk_overlap_chars,
        )
        self._emit(
            "avito_chat_promoted",
            "Диалог Avito сохранён как эталонный материал",
            channel="knowledge",
            chat_id=chat_id,
            doc_id=doc_id,
            promotion_mode=safe_mode,
            actor=clean_text(actor),
        )
        return int(doc_id)

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


def run_generate_drafts_job(tenant_id: str, *, limit: int = 20, force_regenerate: bool = False) -> Dict[str, Any]:
    service = AvitoService(tenant_id)
    audit = AvitoAuditLogger(service.storage, kind="avito_drafts_generate", label="Генерация черновиков Avito", source="background_job")
    try:
        result = service.generate_drafts(limit=limit, audit=audit, force_regenerate=force_regenerate)
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
        audit.fail(str(exc), error=str(exc), limit=limit, force_regenerate=force_regenerate)
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
