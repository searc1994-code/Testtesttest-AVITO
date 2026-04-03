from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

from .compat import clean_text, log_event
from .config import AvitoModuleConfig
from .knowledge import KnowledgeHit, MediaSuggestion

_PHONE_RE = re.compile(r"(?:\+7|8)?[\s\-\(]*\d{3}[\)\s\-]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}")
_LINK_RE = re.compile(r"https?://|www\.", re.I)
_PRICE_BARGAIN_RE = re.compile(r"скидк|торг|дешевл|уступ|последн(яя|ий) цена", re.I)
_AVAILABILITY_RE = re.compile(r"в наличии|наличии|актуал|не продан|можно забрать", re.I)
_DELIVERY_RE = re.compile(r"доставк|отправк|сдэк|почт|авито доставк", re.I)
_MEETING_RE = re.compile(r"когда|сегодня|завтра|встрет|самовывоз|адрес", re.I)
_CONDITION_RE = re.compile(r"состояни|царап|дефект|работает|комплект", re.I)
_SIZE_RE = re.compile(r"размер|габарит|длина|ширина|вес|рост", re.I)
_MEDIA_RE = re.compile(r"фото|видео|виде|фотк|снимк|показать|дополнител(ьн|н)ые материалы", re.I)


@dataclass(slots=True)
class DraftDecision:
    body: str
    confidence: float
    route: str
    reason: str
    model_name: str
    meta: Dict[str, Any] = field(default_factory=dict)


class AvitoAIAgent:
    def __init__(self, config: AvitoModuleConfig, tenant_id: str, event_writer: Optional[Callable[..., Any]] = None) -> None:
        self.config = config
        self.tenant_id = tenant_id
        self.event_writer = event_writer

    def _emit_event(self, stage: str, message: str = "", *, level: str = "info", **data: Any) -> None:
        if callable(self.event_writer):
            try:
                self.event_writer(stage=stage, message=message, level=level, **data)
                return
            except Exception:
                pass
        log_event("avito_ai", stage, tenant_id=self.tenant_id, level=level, **data)

    def compose_reply(
        self,
        chat: Dict[str, Any],
        messages: Iterable[Dict[str, Any]],
        *,
        note: str = "",
        knowledge_hits: Optional[List[KnowledgeHit]] = None,
        media_suggestions: Optional[List[MediaSuggestion]] = None,
        similar_dialogs: Optional[List[Dict[str, Any]]] = None,
    ) -> DraftDecision:
        history = [m for m in messages]
        incoming = [m for m in history if clean_text(m.get("direction")) == "in" and clean_text(m.get("text"))]
        latest_text = clean_text(incoming[-1].get("text")) if incoming else ""
        hits = list(knowledge_hits or [])
        media = list(media_suggestions or [])
        similar = list(similar_dialogs or [])
        if not latest_text:
            return DraftDecision(
                body="",
                confidence=0.0,
                route="skip",
                reason="Нет входящего текста для ответа",
                model_name="rule-engine",
                meta={"intent": "empty", "knowledge_hits": [hit.as_meta() for hit in hits], "media_suggestions": [item.as_meta() for item in media], "similar_dialogs": similar[:5]},
            )

        safety = self._safety_gate(latest_text)
        if safety:
            return DraftDecision(
                body="Спасибо за сообщение! Передам владельцу диалога, чтобы он ответил вручную.",
                confidence=0.15,
                route="manual",
                reason=safety,
                model_name="rule-engine",
                meta={
                    "intent": "manual_escalation",
                    "blocked_by": safety,
                    "fallback": "safety_gate",
                    "knowledge_hits": [hit.as_meta() for hit in hits],
                    "media_suggestions": [item.as_meta() for item in media],
                    "similar_dialogs": similar[:5],
                },
            )

        if _MEDIA_RE.search(latest_text) and media and not self.config.media_send_enabled:
            kinds = ", ".join(sorted({item.media_kind for item in media if clean_text(item.media_kind)})) or "материалы"
            return DraftDecision(
                body=(
                    "Спасибо за сообщение! Дополнительные материалы по товару есть. "
                    f"Я подготовил {kinds} для отправки менеджером, чтобы не обещать вложения до ручной проверки."
                ),
                confidence=0.72,
                route="manual",
                reason="Клиент просит фото/видео, подготовлены media suggestions",
                model_name="rule-engine",
                meta={
                    "intent": "media_request",
                    "blocked_by": "media_attachment_required",
                    "fallback": "media_registry",
                    "knowledge_hits": [hit.as_meta() for hit in hits],
                    "media_suggestions": [item.as_meta() for item in media],
                    "similar_dialogs": similar[:5],
                },
            )

        knowledge_reply = self._knowledge_guided_reply(chat, latest_text, hits, media)
        if knowledge_reply is not None:
            knowledge_reply.meta.setdefault("similar_dialogs", similar[:5])
            return knowledge_reply

        similar_reply = self._similar_dialog_reply(chat, latest_text, similar, media)
        if similar_reply is not None:
            similar_reply.meta.setdefault("knowledge_hits", [hit.as_meta() for hit in hits])
            return similar_reply

        heuristic = self._heuristic_reply(chat, latest_text)
        if heuristic is not None:
            heuristic.meta.setdefault("knowledge_hits", [hit.as_meta() for hit in hits])
            heuristic.meta.setdefault("media_suggestions", [item.as_meta() for item in media])
            return heuristic

        llm_decision = self._llm_reply(chat=chat, messages=history, note=note, knowledge_hits=hits, media_suggestions=media, similar_dialogs=similar)
        if llm_decision is not None:
            return llm_decision

        if hits:
            top = hits[0]
            body = (
                f"Здравствуйте! По товару {clean_text(chat.get('item_title') or chat.get('title') or 'из объявления')} могу подтвердить следующее: "
                f"{top.excerpt} Если нужно, уточните удобный для вас сценарий — доставка, самовывоз или дополнительные детали."
            )
            return DraftDecision(
                body=body,
                confidence=min(0.88, max(0.56, float(top.score) / 2.2)),
                route="manual" if self.config.knowledge_mode == "require_for_auto" else "manual",
                reason="Сработал knowledge-based fallback без LLM",
                model_name="knowledge-fallback",
                meta={
                    "intent": "knowledge_fallback",
                    "fallback": "knowledge_registry",
                    "knowledge_hits": [hit.as_meta() for hit in hits],
                    "media_suggestions": [item.as_meta() for item in media],
                    "similar_dialogs": similar[:5],
                },
            )

        return DraftDecision(
            body="Здравствуйте! Спасибо за сообщение. Уточню детали и вернусь с точным ответом чуть позже.",
            confidence=0.42,
            route="manual",
            reason="Сработал безопасный дефолт без LLM",
            model_name="fallback-template",
            meta={
                "intent": "fallback",
                "fallback": "safe_template",
                "knowledge_hits": [hit.as_meta() for hit in hits],
                "media_suggestions": [item.as_meta() for item in media],
                "similar_dialogs": similar[:5],
            },
        )

    def _safety_gate(self, text: str) -> str:
        if _PHONE_RE.search(text):
            return "В сообщении есть телефон — лучше ручная обработка"
        if _LINK_RE.search(text):
            return "В сообщении есть ссылка — лучше ручная обработка"
        if len(text) > 1200:
            return "Слишком длинное сообщение — лучше ручная обработка"
        return ""

    def _knowledge_guided_reply(
        self,
        chat: Dict[str, Any],
        latest_text: str,
        knowledge_hits: List[KnowledgeHit],
        media_suggestions: List[MediaSuggestion],
    ) -> Optional[DraftDecision]:
        if not self.config.knowledge_enabled or not knowledge_hits:
            return None
        top = knowledge_hits[0]
        item_title = clean_text(chat.get("item_title") or chat.get("title"))
        intro = "Здравствуйте!"
        if clean_text(chat.get("client_name")):
            intro = f"Здравствуйте, {chat['client_name']}!"

        if top.score < max(0.05, float(self.config.knowledge_min_score or 0.45)):
            return None

        if _AVAILABILITY_RE.search(latest_text):
            body = (
                f"{intro} Объявление по товару “{item_title or 'этому товару'}” актуально. "
                f"По базе знаний есть такая опора: {top.excerpt} Если удобно, уточните формат — самовывоз или доставка."
            )
            return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge: наличие")

        if _DELIVERY_RE.search(latest_text):
            body = (
                f"{intro} По доставке могу опереться на такую информацию: {top.excerpt} "
                "Если нужен конкретный способ отправки, напишите город и предпочитаемый вариант."
            )
            return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge: доставка", prefer_manual=True)

        if _CONDITION_RE.search(latest_text) or _SIZE_RE.search(latest_text):
            body = (
                f"{intro} По описанию товара у меня есть такая проверенная информация: {top.excerpt} "
                "Если нужно, могу уточнить дополнительные детали по объявлению."
            )
            return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge: характеристики")

        if _PRICE_BARGAIN_RE.search(latest_text):
            body = (
                f"{intro} По текущему описанию товара могу подтвердить следующее: {top.excerpt} "
                "По скидке и финальной цене лучше ответить вручную, чтобы не пообещать лишнего."
            )
            return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge: торг требует ручной проверки", prefer_manual=True, blocked_by="pricing_manual")

        if top.kind in {"faq", "qa", "listing_card", "policy", "shipping", "condition", "size"}:
            body = f"{intro} {top.excerpt}"
            if media_suggestions and _MEDIA_RE.search(latest_text):
                body += " Дополнительные материалы по товару подобраны и готовы к ручной отправке менеджером."
                return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge + media registry", prefer_manual=True, blocked_by="media_attachment_required")
            body += " Если нужен ещё один ракурс или уточнение по объявлению — напишите, что именно важно."
            return self._knowledge_decision(body, top, media_suggestions, reason="Knowledge match")
        return None

    def _knowledge_decision(
        self,
        body: str,
        top_hit: KnowledgeHit,
        media_suggestions: List[MediaSuggestion],
        *,
        reason: str,
        prefer_manual: bool = False,
        blocked_by: str = "",
    ) -> DraftDecision:
        base_confidence = min(0.97, max(0.58, float(top_hit.score) / 2.05))
        route = "manual" if prefer_manual else "auto"
        if self.config.knowledge_mode == "require_for_auto" and not prefer_manual:
            route = "auto"
        meta = {
            "intent": "knowledge",
            "knowledge_supported": True,
            "knowledge_hits": [top_hit.as_meta()],
            "media_suggestions": [item.as_meta() for item in media_suggestions],
            "fallback": "knowledge_registry",
            "blocked_by": blocked_by,
        }
        return DraftDecision(
            body=body,
            confidence=base_confidence,
            route=route,
            reason=reason,
            model_name="knowledge-engine",
            meta=meta,
        )

    def _similar_dialog_reply(
        self,
        chat: Dict[str, Any],
        latest_text: str,
        similar_dialogs: List[Dict[str, Any]],
        media_suggestions: List[MediaSuggestion],
    ) -> Optional[DraftDecision]:
        if not self.config.similar_dialogs_enabled or not similar_dialogs:
            return None
        top = similar_dialogs[0]
        top_score = float(top.get("score") or 0.0)
        last_out = clean_text(top.get("latest_out_text"))
        if top_score < max(0.05, float(self.config.similar_dialogs_min_score or 0.55)) or not last_out:
            return None
        body = last_out
        reason = "Похожий прошлый диалог"
        route = "manual"
        confidence = min(0.94, max(0.52, top_score / 2.0))
        if top_score >= 1.2 and not _PRICE_BARGAIN_RE.search(latest_text) and not _MEETING_RE.search(latest_text):
            route = "manual"
        if media_suggestions and _MEDIA_RE.search(latest_text):
            body = body.rstrip() + " Дополнительные фото подготовлены и будут отправлены вместе с ответом после подтверждения."
            route = "manual"
        return DraftDecision(
            body=body,
            confidence=confidence,
            route=route,
            reason=reason,
            model_name="similar-dialog-engine",
            meta={
                "intent": "similar_dialog",
                "fallback": "similar_dialogs",
                "similar_dialogs": similar_dialogs[:5],
                "similar_dialog_top_score": round(top_score, 4),
                "similar_chat_id": clean_text(top.get("chat_id")),
                "similar_latest_out_text": last_out[:500],
                "media_suggestions": [item.as_meta() for item in media_suggestions],
            },
        )

    def _heuristic_reply(self, chat: Dict[str, Any], latest_text: str) -> Optional[DraftDecision]:
        item_title = clean_text(chat.get("item_title") or chat.get("title"))
        intro = "Здравствуйте!"
        if clean_text(chat.get("client_name")):
            intro = f"Здравствуйте, {chat['client_name']}!"

        if _AVAILABILITY_RE.search(latest_text):
            body = (
                f"{intro} Да, объявление по товару “{item_title or 'этому товару'}” актуально. "
                "Если удобно, напишите, интересует самовывоз или доставка — сориентирую по следующему шагу."
            )
            return DraftDecision(body=body, confidence=0.97, route="auto", reason="FAQ: наличие", model_name="rule-engine", meta={"intent": "availability"})

        if _DELIVERY_RE.search(latest_text):
            body = (
                f"{intro} По доставке можем сориентировать, но точный вариант зависит от объявления и вашего города. "
                "Напишите, пожалуйста, какой способ вам нужен: Авито Доставка, самовывоз или отправка перевозчиком."
            )
            return DraftDecision(body=body, confidence=0.91, route="manual", reason="Нужна конкретизация доставки", model_name="rule-engine", meta={"intent": "delivery"})

        if _PRICE_BARGAIN_RE.search(latest_text):
            body = (
                f"{intro} Спасибо за интерес к товару. По цене лучше уточнить у владельца объявления, "
                "чтобы не обещать лишнего. Я передал запрос на ручную обработку."
            )
            return DraftDecision(body=body, confidence=0.35, route="manual", reason="Торг/скидка — ручной сценарий", model_name="rule-engine", meta={"intent": "bargain"})

        if _MEETING_RE.search(latest_text):
            body = (
                f"{intro} Спасибо! По времени встречи и адресу лучше сразу согласовать вручную, "
                "чтобы подтвердить актуальное окно. Передал ваш вопрос владельцу объявления."
            )
            return DraftDecision(body=body, confidence=0.31, route="manual", reason="Встреча/адрес — ручной сценарий", model_name="rule-engine", meta={"intent": "meeting"})

        if _CONDITION_RE.search(latest_text):
            body = (
                f"{intro} По состоянию товара лучше ответить максимально точно. "
                "Я передал вопрос на ручную проверку, чтобы не написать неточно."
            )
            return DraftDecision(body=body, confidence=0.39, route="manual", reason="Состояние/дефекты — лучше ручной ответ", model_name="rule-engine", meta={"intent": "condition"})

        if _SIZE_RE.search(latest_text):
            body = (
                f"{intro} Уточню точные параметры по товару{f' “{item_title}”' if item_title else ''} и вернусь с ответом. "
                "Не хочу назвать размеры наугад."
            )
            return DraftDecision(body=body, confidence=0.38, route="manual", reason="Размеры/характеристики — нужна проверка", model_name="rule-engine", meta={"intent": "size"})

        return None

    def _llm_reply(
        self,
        *,
        chat: Dict[str, Any],
        messages: List[Dict[str, Any]],
        note: str,
        knowledge_hits: List[KnowledgeHit],
        media_suggestions: List[MediaSuggestion],
        similar_dialogs: List[Dict[str, Any]],
    ) -> Optional[DraftDecision]:
        api_key = clean_text(self.config.ai_api_key)
        if not api_key:
            return None
        try:
            try:
                from openai import OpenAI  # type: ignore
            except Exception:
                return None
            client_kwargs: Dict[str, Any] = {"api_key": api_key}
            if clean_text(self.config.ai_base_url):
                client_kwargs["base_url"] = self.config.ai_base_url
            client = OpenAI(**client_kwargs)
            prompt_messages = self._build_prompt(chat=chat, messages=messages, note=note, knowledge_hits=knowledge_hits, media_suggestions=media_suggestions, similar_dialogs=similar_dialogs)
            response = client.chat.completions.create(
                model=self.config.ai_model,
                messages=prompt_messages,
                temperature=self.config.ai_temperature,
                max_tokens=450,
            )
            body = clean_text((response.choices[0].message.content if response.choices else "") or "")
            if not body:
                return None
            confidence = self._estimate_confidence(messages, knowledge_hits)
            route = "auto" if confidence >= self.config.auto_send_confidence_threshold else "manual"
            if self.config.knowledge_mode == "require_for_auto" and not knowledge_hits:
                route = "manual"
            return DraftDecision(
                body=body,
                confidence=confidence,
                route=route,
                reason="LLM-черновик",
                model_name=self.config.ai_model,
                meta={
                    "intent": "llm",
                    "fallback": "llm",
                    "knowledge_hits": [hit.as_meta() for hit in knowledge_hits],
                    "media_suggestions": [item.as_meta() for item in media_suggestions],
                    "similar_dialogs": similar_dialogs[:5],
                    "usage": getattr(response, "usage", None).__dict__ if getattr(response, "usage", None) else {},
                },
            )
        except Exception as exc:
            self._emit_event("llm_failed", "LLM не смог сгенерировать ответ для Avito", level="error", error=str(exc))
            return None

    def _build_prompt(
        self,
        *,
        chat: Dict[str, Any],
        messages: List[Dict[str, Any]],
        note: str,
        knowledge_hits: List[KnowledgeHit],
        media_suggestions: List[MediaSuggestion],
        similar_dialogs: List[Dict[str, Any]],
    ) -> List[Dict[str, str]]:
        history = messages[-max(1, self.config.max_context_messages) :]
        formatted_history = []
        for msg in history:
            role = "assistant" if clean_text(msg.get("direction")) == "out" else "user"
            text = clean_text(msg.get("text"))
            if not text:
                continue
            formatted_history.append({"role": role, "content": text})
        preamble = [
            f"Клиент: {clean_text(chat.get('client_name') or 'не указан')}",
            f"Заголовок чата/объявления: {clean_text(chat.get('item_title') or chat.get('title'))}",
        ]
        if clean_text(note):
            preamble.append(f"Заметка менеджера: {clean_text(note)}")
        if clean_text(self.config.knowledge_text):
            preamble.append(f"Базовые правила: {self.config.knowledge_text}")
        if knowledge_hits:
            lines = []
            for idx, hit in enumerate(knowledge_hits[: max(1, self.config.knowledge_max_hits)], start=1):
                src = hit.source_name or hit.title or f"Источник {idx}"
                lines.append(f"[{idx}] {src} | kind={hit.kind} | score={round(hit.score, 3)} | excerpt={hit.excerpt}")
            preamble.append("Подтверждённые знания:\n" + "\n".join(lines))
        if media_suggestions:
            lines = []
            for idx, item in enumerate(media_suggestions[: max(1, self.config.media_max_suggestions)], start=1):
                location = item.external_url or item.local_path or item.title
                lines.append(f"[{idx}] {item.media_kind}: {item.title} | {location}")
            preamble.append(
                "Доступные медиа-материалы (не утверждай, что они уже прикреплены, если инструмент не подтвердил отправку):\n"
                + "\n".join(lines)
            )
        if similar_dialogs:
            lines = []
            for idx, item in enumerate(similar_dialogs[: max(1, self.config.similar_dialogs_max_hits)], start=1):
                lines.append(
                    f"[{idx}] score={round(float(item.get('score') or 0.0), 3)} | item={clean_text(item.get('item_title') or item.get('item_id'))} | "
                    f"client={clean_text(item.get('client_name') or item.get('title'))} | customer={clean_text(item.get('latest_in_text'))[:180]} | answer={clean_text(item.get('latest_out_text'))[:220]}"
                )
            preamble.append("Похожие прошлые диалоги (используй как примеры, но не копируй слепо факты):\n" + "\n".join(lines))
        system_message = {
            "role": "system",
            "content": self.config.system_prompt
            + "\n\nПравила ответа:\n"
            + "1) Не выдумывай факты, которых нет в подтверждённых знаниях или истории.\n"
            + "2) Если клиент просит фото/видео, можно упомянуть наличие подготовленных материалов, но не утверждай, что они уже отправлены, пока система не подтвердила отправку.\n"
            + "3) Не обещай скидку, адрес или встречу без явного подтверждения.\n\nКонтекст:\n"
            + "\n".join(preamble),
        }
        return [system_message, *formatted_history]

    def _estimate_confidence(self, messages: List[Dict[str, Any]], knowledge_hits: List[KnowledgeHit]) -> float:
        incoming_count = sum(1 for msg in messages if clean_text(msg.get("direction")) == "in")
        base = 0.7 if incoming_count <= 2 else 0.62
        if knowledge_hits:
            base += min(0.2, max(0.0, float(knowledge_hits[0].score) / 5.0))
        return min(0.97, max(0.18, round(base, 4)))
