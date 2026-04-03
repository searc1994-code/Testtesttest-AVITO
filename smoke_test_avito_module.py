from __future__ import annotations

import tempfile
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from avito_module.ai_engine import AvitoAIAgent
from avito_module.api_client import AvitoApiClient
from avito_module.audit import AvitoAuditLogger
from avito_module.config import AvitoModuleConfig
from avito_module.service import AvitoService
from avito_module.storage import AvitoStorage


class FakeCurrentApp:
    view_functions = {
        "index": object(),
        "questions": object(),
        "complaints": object(),
        "automation": object(),
        "diagnostics": object(),
        "tenants": object(),
    }


class FakeApiClient(AvitoApiClient):
    def __init__(self, config, tenant_id, *, base_dir=None):
        super().__init__(config, tenant_id, base_dir=base_dir)
        self.sent_messages = []

    def ensure_token(self):
        return "fake-token"

    def iter_chat_previews(self, unread_only=False, limit=None):
        yield {
            "chat_id": "chat-1",
            "id": "chat-1",
            "title": "Покупатель 1",
            "client_name": "Иван",
            "item_id": "item-1",
            "item_title": "Коляска",
            "unread_count": 1,
            "last_message_text": "Здравствуйте, объявление актуально?",
            "last_message_ts": "2026-04-02T12:00:00+00:00",
            "raw": {"source": "fake"},
        }
        if not unread_only:
            yield {
                "chat_id": "chat-2",
                "id": "chat-2",
                "title": "Покупатель 2",
                "client_name": "Пётр",
                "item_id": "item-2",
                "item_title": "Автокресло",
                "unread_count": 0,
                "last_message_text": "Спасибо",
                "last_message_ts": "2026-04-02T12:10:00+00:00",
                "raw": {"source": "fake"},
            }

    def iter_messages(self, chat_id, limit=200):
        if chat_id == "chat-1":
            yield {
                "message_id": "m1",
                "direction": "in",
                "is_read": False,
                "author_name": "Иван",
                "message_ts": "2026-04-02T12:00:00+00:00",
                "text": "Здравствуйте, объявление актуально?",
                "attachments": [],
                "raw": {"source": "fake"},
            }
        else:
            yield {
                "message_id": "m2",
                "direction": "in",
                "is_read": False,
                "author_name": "Пётр",
                "message_ts": "2026-04-02T12:10:00+00:00",
                "text": "Спасибо",
                "attachments": [],
                "raw": {"source": "fake"},
            }

    def send_text_message(self, chat_id, text):
        self.sent_messages.append((chat_id, text))
        return {"id": f"out-{len(self.sent_messages)}"}

    def mark_chat_as_read(self, chat_id):
        return {"ok": True}


class FailingProcessService(AvitoService):
    def _process_webhook_payload(self, event_id, extracted, payload):  # pragma: no cover - test helper
        raise RuntimeError("forced webhook failure")


class FakeBrowserMonitor:
    def __init__(self):
        self.sent_payloads = []

    def send_message_with_media(self, chat_id, text, assets, **kwargs):
        self.sent_payloads.append({"chat_id": chat_id, "text": text, "assets": list(assets), "kwargs": dict(kwargs)})
        return {"ok": True, "transport": "browser", "message": {"id": f"browser-out-{len(self.sent_payloads)}", "text": text}}



def _template_context() -> dict:
    return {
        "page_title": "Avito Inbox",
        "active_tenant_id": "tenant-a",
        "active_tenant": {"name": "Tenant A"},
        "avito_config": {
            "configured": True,
            "auto_mode": "all",
            "browser_fallback_enabled": True,
            "polling_fallback_enabled": True,
            "webhook_first_enabled": True,
            "polling_interval_seconds": 60,
            "sync_page_limit": 100,
            "sync_max_pages": 10,
            "webhook_auto_generate_draft": True,
            "auto_send_confidence_threshold": 0.93,
            "system_prompt": "prompt",
            "knowledge_text": "knowledge",
            "knowledge_enabled": True,
            "knowledge_mode": "assist",
            "knowledge_max_hits": 5,
            "knowledge_min_score": 0.45,
            "knowledge_chunk_chars": 900,
            "knowledge_chunk_overlap_chars": 120,
            "knowledge_answer_style": "grounded",
            "similar_dialogs_enabled": True,
            "similar_dialogs_max_hits": 4,
            "similar_dialogs_min_score": 0.55,
            "hitl_enabled": True,
            "hitl_auto_ready_threshold": 0.985,
            "hitl_queue_default_assignee": "",
            "media_registry_enabled": True,
            "media_auto_suggest_enabled": True,
            "media_max_suggestions": 4,
            "media_send_enabled": True,
            "media_send_transport": "browser_first",
            "media_max_send_assets": 4,
            "media_send_images_only": True,
            "media_browser_send_headless": True,
            "media_api_upload_endpoint": "",
            "media_api_send_endpoint": "",
            "ai_model": "gpt-4o-mini",
            "ai_base_url": "",
            "max_context_messages": 12,
            "webhook_query_param": "token",
            "webhook_signature_header": "x-avito-messenger-signature",
            "webhook_timestamp_header": "x-avito-webhook-timestamp",
            "webhook_nonce_header": "x-avito-webhook-nonce",
            "webhook_event_id_header": "x-avito-webhook-id",
            "webhook_require_signature": True,
            "webhook_allowed_skew_seconds": 900,
            "webhook_nonce_ttl_seconds": 900,
            "browser_bootstrap_timeout_seconds": 300,
            "api_retry_budget": 3,
            "api_backoff_base_seconds": 1.0,
            "api_backoff_max_seconds": 30.0,
            "api_max_requests_per_minute": 60,
            "api_min_request_interval_ms": 250,
            "api_circuit_breaker_threshold": 5,
            "api_circuit_breaker_cooldown_seconds": 120,
            "rbac_view_users": [],
            "rbac_reply_users": [],
            "rbac_bulk_send_users": [],
            "rbac_ai_rules_users": [],
            "rbac_connect_users": [],
            "rbac_secret_users": [],
            "rbac_admin_users": [],
        },
        "config_edit": {
            "client_id": "cid",
            "client_secret": "secret",
            "user_id": "123",
            "ai_api_key": "k",
            "ai_base_url": "",
            "webhook_secret": "secret-token",
            "can_view_secrets": True,
        },
        "avito_permissions": {
            "view": True,
            "reply": True,
            "bulk_send": True,
            "ai_rules": True,
            "connect": True,
            "secret_view": True,
            "admin": True,
        },
        "avito_metrics": {
            "incoming_5m": 1,
            "incoming_15m": 2,
            "incoming_60m": 3,
            "avg_first_response_minutes": 4.5,
            "auto_reply_share": 0.5,
            "escalation_share": 0.5,
            "token_refresh_events_60m": 1,
            "circuit_open_events_60m": 0,
            "webhook_verify_fail_pct_24h": 0.0,
            "webhook_process_lag_seconds_avg": 1.2,
            "browser_fallback_share": 0.0,
            "sync_runs_considered": 2,
            "drafts_total": 1,
            "sent_drafts_total": 1,
            "review_queue_count": 1,
            "hold_queue_count": 0,
            "approved_queue_count": 0,
            "error_queue_count": 0,
            "webhook_events_24h": 2,
            "knowledge_docs_total": 1,
            "knowledge_docs_active": 1,
            "media_assets_total": 1,
            "media_assets_active": 1,
        },
        "avito_recent_dlq": [
            {"dlq_id": 11, "status": "open", "source_kind": "webhook", "error_text": "forced webhook failure", "updated_at": "2026-04-03T00:10:00Z"}
        ],
        "avito_recent_webhooks": [
            {"event_id": "evt-1", "status": "processed", "updated_at": "2026-04-03T00:00:00Z", "source_kind": "webhook"}
        ],
        "avito_last_sync": {"notes": ["ok"]},
        "avito_last_backfill": {"notes": ["ok"]},
        "avito_browser_state": {"exists": False, "path": "/tmp/tenant-a/auth/avito_state.json"},
        "avito_jobs": [],
        "background_jobs_latest": [],
        "avito_recent_runs": [
            {
                "run_id": "run-1",
                "kind": "avito_sync",
                "status": "completed",
                "last_stage": "done",
                "updated_at": "2026-04-03T00:00:00Z",
                "created_at": "2026-04-03T00:00:00Z",
                "steps_count": 3,
                "duration_ms": 120,
                "label": "Sync",
                "last_message": "ok",
            }
        ],
        "avito_latest_run": {"run_id": "run-1", "kind": "avito_sync", "status": "completed"},
        "avito_logs_root": "/tmp/tenant-a/logs/avito",
        "avito_webhook_url": "https://example.test/avito/webhook/tenant-a?token=secret-token",
        "avito_media_send_enabled": False,
        "get_flashed_messages": lambda with_categories=False: [],
        "url_for": lambda name, **kwargs: f"/{name}",
        "current_app": FakeCurrentApp(),
    }



def _check_templates() -> None:
    template_dir = Path(__file__).parent / "avito_module" / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), undefined=StrictUndefined)
    base_context = _template_context()

    env.get_template("avito/index.html").render(
        chats=[
            {
                "chat_id": "chat-1",
                "client_name": "Иван",
                "title": "Покупатель 1",
                "item_title": "Коляска",
                "last_message_text": "Здравствуйте",
                "last_message_ts": "2026-04-03T00:00:00Z",
                "unread_count": 1,
                "status": "open",
                "assigned_to": "manager-1",
                "draft": None,
            }
        ],
        status="all",
        unanswered_only=False,
        **base_context,
    )
    env.get_template("avito/queue.html").render(
        queue_items=[
            {
                "chat_id": "chat-1",
                "body": "Да, объявление актуально",
                "route": "manual",
                "state": "review",
                "confidence": 0.8,
                "reason": "FAQ match",
                "meta": {"policy": "draft_only", "blocked_by": "", "similar_dialogs": [{"chat_id": "chat-old", "score": 1.4, "excerpt": "Актуально?", "latest_in_text": "Актуально?", "latest_out_text": "Да"}]},
                "chat": {"chat_id": "chat-1", "client_name": "Иван", "title": "Покупатель 1", "item_title": "Коляска", "item_id": "item-1", "status": "open", "priority": "normal", "assigned_to": "manager-1", "note": "", "tags": [], "last_message_ts": "2026-04-03T00:00:00Z", "last_message_text": "Здравствуйте"},
            }
        ],
        queue_states=["review", "hold", "error"],
        **base_context,
    )
    env.get_template("avito/settings.html").render(**base_context)
    env.get_template("avito/chat.html").render(
        chat={
            "client_name": "Иван",
            "title": "Диалог",
            "chat_id": "chat-1",
            "item_title": "Коляска",
            "status": "open",
            "priority": "normal",
            "assigned_to": "manager-1",
            "tags": ["faq", "lead"],
            "note": "перезвонить",
        },
        messages=[
            {"direction": "in", "author_name": "Иван", "message_ts": "2026-04-03T00:00:00Z", "text": "Здравствуйте"},
            {"direction": "out", "author_name": "assistant", "message_ts": "2026-04-03T00:01:00Z", "text": "Да, актуально"},
        ],
        draft={"body": "Да, актуально", "state": "review", "route": "manual", "confidence": 0.8, "model_name": "gpt-4o-mini", "reason": "FAQ match", "meta": {"policy": "draft_only", "knowledge_hits_count": 1, "media_suggestions_count": 1}},
        knowledge_hits=[{"title": "FAQ по коляске", "kind": "faq", "score": 1.5, "item_title": "Коляска", "item_id": "item-1", "excerpt": "Коляска в наличии, возможен самовывоз и доставка.", "source_name": "Менеджер", "source_url": ""}],
        media_suggestions=[{"asset_id": 1, "title": "Фото коляски сбоку", "media_kind": "image", "score": 1.2, "item_title": "Коляска", "item_id": "item-1", "caption": "Дополнительный ракурс", "external_url": "https://example.test/stroller.jpg", "local_path": ""}],
        similar_dialogs=[{"chat_id": "chat-old", "client_name": "Мария", "title": "Старый диалог", "score": 1.4, "latest_in_text": "Актуально?", "latest_out_text": "Да, актуально.", "excerpt": "Покупатель спрашивал про наличие."}],
        selected_media=[{"asset_id": 1, "title": "Фото коляски сбоку"}],
        **base_context,
    )
    env.get_template("avito/logs.html").render(
        runs=base_context["avito_recent_runs"],
        selected_run={
            "run_id": "run-1",
            "kind": "avito_sync",
            "status": "completed",
            "label": "Sync",
            "created_at": "2026-04-03T00:00:00Z",
            "finished_at": "2026-04-03T00:01:00Z",
            "last_stage": "done",
            "last_message": "ok",
            "steps_count": 3,
            "duration_ms": 120,
            "summary": {"chats_seen": 1},
        },
        selected_run_events=[
            {"stage": "start", "level": "info", "channel": "sync", "ts": "2026-04-03T00:00:00Z", "message": "start", "data": {"x": 1}, "run_id": "run-1", "percent": 0}
        ],
        selected_channel="sync",
        channel_events=[{"stage": "oauth_ok", "level": "info", "ts": "2026-04-03T00:00:00Z", "message": "ok", "run_id": "run-1"}],
        available_channels=["sync", "ai", "send", "browser", "webhook", "ui", "ops", "decision", "security"],
        **base_context,
    )
    env.get_template("avito/dlq.html").render(
        dlq_status="open",
        dlq_items=[
            {
                "dlq_id": 11,
                "status": "open",
                "attempts": 1,
                "source_kind": "webhook",
                "event_id": "evt-1",
                "error_text": "forced webhook failure",
                "updated_at": "2026-04-03T00:10:00Z",
                "payload": {"chat_id": "chat-3", "message": "bad"},
            }
        ],
        **base_context,
    )
    env.get_template("avito/knowledge.html").render(
        kb_docs=[
            {
                "doc_id": 1,
                "title": "FAQ по коляске",
                "kind": "faq",
                "item_id": "item-1",
                "item_title": "Коляска",
                "tags": ["faq", "наличие"],
                "source_name": "Менеджер",
                "source_url": "",
                "body_text": "Коляска в наличии, возможен самовывоз и доставка.",
                "active": True,
                "updated_at": "2026-04-03T00:00:00Z",
            }
        ],
        kb_search="",
        kb_kind="all",
        kb_kinds=["all", "faq"],
        **base_context,
    )
    env.get_template("avito/media.html").render(
        media_assets=[
            {
                "asset_id": 1,
                "title": "Фото коляски сбоку",
                "media_kind": "image",
                "caption": "Дополнительный ракурс",
                "item_id": "item-1",
                "item_title": "Коляска",
                "external_url": "https://example.test/stroller.jpg",
                "local_path": "",
                "mime_type": "image/jpeg",
                "tags": ["фото"],
                "active": True,
                "updated_at": "2026-04-03T00:00:00Z",
            }
        ],
        media_search="",
        media_kind="all",
        media_kinds=["all", "image"],
        **base_context,
    )



def run_smoke() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        config = AvitoModuleConfig(
            tenant_id="tenant-a",
            tenant_name="Tenant A",
            client_id="cid",
            client_secret="secret",
            user_id="123",
            auto_mode="all",
            ai_api_key="",
            webhook_secret="secret-token",
            webhook_first_enabled=True,
            polling_fallback_enabled=True,
            browser_fallback_enabled=False,
            knowledge_enabled=True,
            knowledge_mode="assist",
            media_registry_enabled=True,
            media_auto_suggest_enabled=True,
            media_max_suggestions=4,
            media_send_enabled=True,
            media_send_transport="browser_first",
            media_max_send_assets=4,
            media_send_images_only=True,
            media_browser_send_headless=True,
        )
        config.persist(base_dir=base_dir)
        storage = AvitoStorage("tenant-a", base_dir=base_dir)
        storage.upsert_knowledge_doc(title="FAQ по коляске", body_text="Коляска в наличии. Возможен самовывоз и доставка по договорённости.", kind="faq", item_id="item-1", item_title="Коляска", tags=["faq", "наличие"], source_name="Менеджер", active=True)
        storage.create_media_asset(title="Фото коляски сбоку", media_kind="image", caption="Дополнительный ракурс", item_id="item-1", item_title="Коляска", external_url="https://example.test/stroller.jpg", mime_type="image/jpeg", tags=["фото"], active=True)
        storage.upsert_chat({
            "chat_id": "chat-old",
            "id": "chat-old",
            "title": "Старый диалог",
            "client_name": "Мария",
            "item_id": "item-1",
            "item_title": "Коляска",
            "unread_count": 0,
            "last_message_text": "Актуально?",
            "last_message_ts": "2026-04-01T12:00:00+00:00",
            "raw": {"source": "seed"},
        })
        storage.add_messages("chat-old", [
            {"message_id": "old-in-1", "direction": "in", "is_read": True, "author_name": "Мария", "message_ts": "2026-04-01T12:00:00+00:00", "text": "Здравствуйте, объявление актуально?", "attachments": [], "raw": {"source": "seed"}},
            {"message_id": "old-out-1", "direction": "out", "is_read": True, "author_name": "assistant", "message_ts": "2026-04-01T12:01:00+00:00", "text": "Здравствуйте! Да, объявление актуально.", "attachments": [], "raw": {"source": "seed"}},
        ])
        fake_api = FakeApiClient(config, "tenant-a", base_dir=base_dir)
        service = AvitoService(
            "tenant-a",
            config=config,
            storage=storage,
            api_client=fake_api,
            ai_agent=AvitoAIAgent(config, "tenant-a"),
            base_dir=base_dir,
        )
        fake_browser = FakeBrowserMonitor()
        service.browser_monitor = fake_browser
        try:
            sync_audit = AvitoAuditLogger(storage, kind="smoke_sync", label="Smoke sync", source="test")
            sync_result = service.sync_once(max_chats=5, audit=sync_audit)
            sync_audit.finish("completed", "sync ok", chats_seen=sync_result.chats_seen, messages_added=sync_result.messages_added)
            assert sync_result.chats_seen >= 1, sync_result
            assert storage.get_chat("chat-1"), "Chat not stored"

            draft_audit = AvitoAuditLogger(storage, kind="smoke_drafts", label="Smoke drafts", source="test")
            draft_result = service.generate_drafts(limit=5, audit=draft_audit)
            draft_audit.finish("completed", "drafts ok", generated=draft_result.generated)
            assert draft_result.generated >= 1, draft_result
            draft = storage.get_draft("chat-1")
            assert draft and draft["body"], draft
            assert draft.get("state") in {"review", "ready"}, draft
            assert int((draft.get("meta") or {}).get("knowledge_hits_count") or 0) >= 1, draft
            assert int((draft.get("meta") or {}).get("media_suggestions_count") or 0) >= 1, draft
            assert int((draft.get("meta") or {}).get("similar_dialogs_count") or 0) >= 1, draft
            assert storage.list_draft_media_assets("chat-1"), "Selected media was not stored"
            queue_snapshot = service.review_queue_snapshot(states=["review", "hold", "error"], limit=20)
            assert any(item.get("chat_id") == "chat-1" for item in queue_snapshot), queue_snapshot
            approved = service.approve_draft("chat-1", reviewer="smoke", review_note="ok")
            assert approved and approved.get("state") == "ready", approved

            send_audit = AvitoAuditLogger(storage, kind="smoke_send", label="Smoke send", source="test")
            send_result = service.send_ready_drafts(limit=5, audit=send_audit)
            send_audit.finish("completed", "send ok", sent=send_result.sent)
            assert send_result.sent >= 1, send_result
            assert fake_browser.sent_payloads, "Live media send did not use browser transport"
            assert not fake_api.sent_messages, "Text-only API send was used instead of media browser transport"

            webhook_payload = {
                "event_id": "evt-ok-1",
                "chat": {"id": "chat-3", "title": "Покупатель 3", "item": {"id": "item-3", "title": "Стул"}},
                "message": {
                    "id": "wm-1",
                    "direction": "in",
                    "author": {"name": "Мария"},
                    "content": {"text": "Можно забрать сегодня?"},
                    "created": "2026-04-03T02:00:00Z",
                },
            }
            webhook_result = service.ingest_webhook(
                webhook_payload,
                security_meta={
                    "event_id": "evt-ok-1",
                    "dedupe_key": "evt-ok-1:wm-1",
                    "source_kind": "webhook",
                    "verified_by": "smoke_test",
                    "signature": "sig",
                    "nonce": "nonce-1",
                },
            )
            assert webhook_result["ok"] is True, webhook_result
            duplicate_result = service.ingest_webhook(
                webhook_payload,
                security_meta={
                    "event_id": "evt-ok-1",
                    "dedupe_key": "evt-ok-1:wm-1",
                    "source_kind": "webhook",
                    "verified_by": "smoke_test",
                    "signature": "sig",
                    "nonce": "nonce-1",
                },
            )
            assert duplicate_result.get("duplicate") is True, duplicate_result
            assert storage.get_chat("chat-3"), "Webhook chat not stored"

            failing_service = FailingProcessService(
                "tenant-a",
                config=config,
                storage=storage,
                api_client=fake_api,
                ai_agent=AvitoAIAgent(config, "tenant-a"),
                base_dir=base_dir,
            )
            try:
                dlq_result = failing_service.ingest_webhook(
                    {"event_id": "evt-bad-1", "chat": {"id": "chat-err"}, "message": {"id": "wm-bad", "direction": "in", "content": {"text": "сломайся"}}},
                    security_meta={"event_id": "evt-bad-1", "dedupe_key": "evt-bad-1:wm-bad", "source_kind": "webhook", "verified_by": "smoke_test"},
                )
                assert dlq_result.get("dead_lettered") is True, dlq_result
            finally:
                failing_service.close()

            dlq_items = storage.list_dead_letters(limit=10)
            assert dlq_items, "DLQ should contain failed webhook"
            replay_result = service.replay_dead_letter(dlq_items[0]["dlq_id"])
            assert replay_result.get("ok") is True, replay_result

            backfill_audit = AvitoAuditLogger(storage, kind="smoke_backfill", label="Smoke backfill", source="test")
            backfill_result = service.backfill_history(max_chats=5, messages_per_chat=5, audit=backfill_audit)
            backfill_audit.finish("completed", "backfill ok", chats_seen=backfill_result.chats_seen, messages_added=backfill_result.messages_added)
            assert backfill_result.chats_seen >= 1, backfill_result

            metrics = service.metrics_snapshot()
            for key in [
                "incoming_5m",
                "incoming_15m",
                "incoming_60m",
                "avg_first_response_minutes",
                "auto_reply_share",
                "escalation_share",
                "token_refresh_events_60m",
                "webhook_verify_fail_pct_24h",
                "browser_fallback_share",
                "knowledge_docs_total",
                "media_assets_total",
            ]:
                assert key in metrics, f"Metric missing: {key}"

            run_index = storage.list_recent_runs(limit=20)
            assert run_index, "Run index is empty"
            assert storage.paths.avito_logs_dir.exists(), "Avito logs dir missing"
            assert storage.paths.channel_logs_dir.exists(), "Channel logs dir missing"
            assert storage.paths.run_logs_dir.exists(), "Run logs dir missing"
            assert any(storage.paths.channel_logs_dir.glob("*.jsonl")), "Channel log files missing"
            assert any(storage.paths.run_logs_dir.glob("*.json")), "Run summary files missing"
            assert any(storage.paths.run_logs_dir.glob("*.jsonl")), "Run timeline files missing"
            assert storage.paths.secret_file.exists(), "Secret file missing"
        finally:
            service.close()

    _check_templates()


if __name__ == "__main__":
    run_smoke()
    print("smoke ok")
