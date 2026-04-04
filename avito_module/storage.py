from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from uuid import uuid4

from .compat import clean_text, read_jsonl, resolve_paths, utc_now_iso
from .knowledge import KnowledgeHit, MediaSuggestion, compact_excerpt, normalize_for_search, score_match, split_text_into_chunks


def _parse_dt(value: Any) -> Optional[datetime]:
    text = clean_text(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _to_epoch_seconds(value: Any) -> int:
    dt = _parse_dt(value)
    if dt is None:
        return 0
    return int(dt.timestamp())


_BARGAIN_RE = re.compile(r"скидк|торг|дешевл|уступ|последн(яя|ий) цена", re.I)
_MEDIA_RE = re.compile(r"фото|видео|виде|фотк|снимк|прикреп", re.I)


@dataclass(slots=True)
class AvitoPaths:
    tenant_root: Path
    data_dir: Path
    auth_dir: Path
    logs_dir: Path
    avito_logs_dir: Path
    channel_logs_dir: Path
    run_logs_dir: Path
    run_index_file: Path
    last_run_file: Path
    db_file: Path
    settings_file: Path
    rules_file: Path
    secret_file: Path
    guardian_state_file: Path
    browser_state_file: Path
    browser_profile_file: Path
    exports_dir: Path
    media_dir: Path
    knowledge_dir: Path


class AvitoStorage:
    def __init__(self, tenant_id: str, base_dir: Optional[Path] = None) -> None:
        self.tenant_id = clean_text(tenant_id) or "default"
        raw_paths = resolve_paths(self.tenant_id, base_dir=base_dir)
        self.paths = AvitoPaths(
            tenant_root=raw_paths["tenant_root"],
            data_dir=raw_paths["data_dir"],
            auth_dir=raw_paths["auth_dir"],
            logs_dir=raw_paths.get("logs_dir", raw_paths["data_dir"].parent / "logs"),
            avito_logs_dir=raw_paths["avito_logs_dir"],
            channel_logs_dir=raw_paths["avito_channel_logs_dir"],
            run_logs_dir=raw_paths["avito_run_logs_dir"],
            run_index_file=raw_paths["avito_run_index_file"],
            last_run_file=raw_paths["avito_last_run_file"],
            db_file=raw_paths["avito_db_file"],
            settings_file=raw_paths["avito_settings_file"],
            rules_file=raw_paths["avito_rules_file"],
            secret_file=raw_paths["avito_secret_file"],
            guardian_state_file=raw_paths["avito_guardian_state_file"],
            browser_state_file=raw_paths["avito_browser_state_file"],
            browser_profile_file=raw_paths["avito_browser_profile_file"],
            exports_dir=raw_paths["avito_exports_dir"],
            media_dir=raw_paths["avito_media_dir"],
            knowledge_dir=raw_paths["avito_knowledge_dir"],
        )
        for path in (
            self.paths.logs_dir,
            self.paths.avito_logs_dir,
            self.paths.channel_logs_dir,
            self.paths.run_logs_dir,
            self.paths.exports_dir,
            self.paths.media_dir,
            self.paths.knowledge_dir,
            self.paths.data_dir,
            self.paths.auth_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.paths.db_file, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS chats (
                    tenant_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    client_name TEXT,
                    title TEXT,
                    item_id TEXT,
                    item_title TEXT,
                    unread_count INTEGER DEFAULT 0,
                    last_message_text TEXT,
                    last_message_ts TEXT,
                    last_message_ts_epoch INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'new',
                    priority TEXT DEFAULT 'normal',
                    assigned_to TEXT,
                    note TEXT,
                    tags_json TEXT DEFAULT '[]',
                    first_response_due_at TEXT,
                    first_response_due_epoch INTEGER DEFAULT 0,
                    first_response_at TEXT,
                    first_response_epoch INTEGER DEFAULT 0,
                    last_operator_user TEXT,
                    last_operator_action_at TEXT,
                    last_send_status TEXT,
                    last_send_detail TEXT,
                    raw_json TEXT DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, chat_id)
                );

                CREATE TABLE IF NOT EXISTS messages (
                    tenant_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    is_read INTEGER DEFAULT 0,
                    author_name TEXT,
                    message_ts TEXT,
                    message_ts_epoch INTEGER DEFAULT 0,
                    text TEXT,
                    attachments_json TEXT DEFAULT '[]',
                    raw_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (tenant_id, chat_id, message_ts_epoch, message_id);

                CREATE TABLE IF NOT EXISTS send_events (
                    tenant_id TEXT NOT NULL,
                    send_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    body_hash TEXT NOT NULL,
                    body_preview TEXT,
                    transport TEXT,
                    delivery_status TEXT,
                    remote_message_id TEXT,
                    attachments_json TEXT DEFAULT '[]',
                    detail_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_send_events_chat ON send_events (tenant_id, chat_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_send_events_hash ON send_events (tenant_id, chat_id, body_hash, created_at);

                CREATE TABLE IF NOT EXISTS drafts (
                    tenant_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    body TEXT NOT NULL,
                    confidence REAL DEFAULT 0,
                    route TEXT DEFAULT 'manual',
                    reason TEXT,
                    source_message_ids_json TEXT DEFAULT '[]',
                    model_name TEXT,
                    meta_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    lease_id TEXT,
                    lease_until TEXT,
                    lease_acquired_at TEXT,
                    sent_at TEXT,
                    PRIMARY KEY (tenant_id, chat_id)
                );
                CREATE INDEX IF NOT EXISTS idx_drafts_state ON drafts (tenant_id, state, updated_at);
                CREATE INDEX IF NOT EXISTS idx_drafts_lease ON drafts (tenant_id, state, lease_until);

                CREATE TABLE IF NOT EXISTS webhook_events (
                    tenant_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    dedupe_key TEXT,
                    source_kind TEXT DEFAULT 'webhook',
                    received_at TEXT NOT NULL,
                    processed_at TEXT,
                    verified_by TEXT,
                    signature TEXT,
                    nonce TEXT,
                    status TEXT DEFAULT 'received',
                    attempts INTEGER DEFAULT 0,
                    last_attempt_at TEXT,
                    last_error TEXT,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (tenant_id, status, received_at);
                CREATE INDEX IF NOT EXISTS idx_webhook_events_dedupe ON webhook_events (tenant_id, dedupe_key);

                CREATE TABLE IF NOT EXISTS webhook_nonces (
                    tenant_id TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    seen_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, nonce)
                );

                CREATE TABLE IF NOT EXISTS dead_letters (
                    tenant_id TEXT NOT NULL,
                    dlq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_kind TEXT NOT NULL,
                    dedupe_key TEXT,
                    event_id TEXT,
                    payload_json TEXT NOT NULL,
                    error_text TEXT,
                    status TEXT DEFAULT 'open',
                    attempts INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    resolved_at TEXT,
                    last_run_id TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_dead_letters_status ON dead_letters (tenant_id, status, updated_at);

                CREATE TABLE IF NOT EXISTS knowledge_docs (
                    tenant_id TEXT NOT NULL,
                    doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    kind TEXT DEFAULT 'faq',
                    item_id TEXT,
                    item_title TEXT,
                    tags_json TEXT DEFAULT '[]',
                    source_name TEXT,
                    source_url TEXT,
                    body_text TEXT NOT NULL,
                    active INTEGER DEFAULT 1,
                    meta_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_knowledge_docs_item ON knowledge_docs (tenant_id, item_id, active);
                CREATE INDEX IF NOT EXISTS idx_knowledge_docs_kind ON knowledge_docs (tenant_id, kind, active);

                CREATE TABLE IF NOT EXISTS knowledge_chunks (
                    tenant_id TEXT NOT NULL,
                    chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id INTEGER NOT NULL,
                    chunk_index INTEGER DEFAULT 0,
                    title TEXT,
                    kind TEXT DEFAULT 'faq',
                    item_id TEXT,
                    item_title TEXT,
                    tags_json TEXT DEFAULT '[]',
                    text TEXT NOT NULL,
                    norm_text TEXT NOT NULL,
                    token_count INTEGER DEFAULT 0,
                    meta_json TEXT DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(doc_id) REFERENCES knowledge_docs(doc_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_doc ON knowledge_chunks (tenant_id, doc_id, chunk_index);
                CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_item ON knowledge_chunks (tenant_id, item_id);

                CREATE TABLE IF NOT EXISTS media_assets (
                    tenant_id TEXT NOT NULL,
                    asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    media_kind TEXT DEFAULT 'image',
                    title TEXT NOT NULL,
                    caption TEXT,
                    item_id TEXT,
                    item_title TEXT,
                    file_name TEXT,
                    local_path TEXT,
                    external_url TEXT,
                    mime_type TEXT,
                    tags_json TEXT DEFAULT '[]',
                    active INTEGER DEFAULT 1,
                    meta_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_media_assets_item ON media_assets (tenant_id, item_id, active);
                CREATE INDEX IF NOT EXISTS idx_media_assets_kind ON media_assets (tenant_id, media_kind, active);

                CREATE TABLE IF NOT EXISTS draft_media_links (
                    tenant_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    source TEXT DEFAULT 'suggested',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, chat_id, asset_id),
                    FOREIGN KEY(asset_id) REFERENCES media_assets(asset_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_draft_media_links_chat ON draft_media_links (tenant_id, chat_id);

                CREATE TABLE IF NOT EXISTS sync_state (
                    tenant_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, key)
                );

                CREATE TABLE IF NOT EXISTS health_alerts (
                    tenant_id TEXT NOT NULL,
                    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details_json TEXT DEFAULT '{}',
                    status TEXT DEFAULT 'open',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    acknowledged_at TEXT,
                    acknowledged_by TEXT,
                    resolved_at TEXT,
                    occurrence_count INTEGER DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_health_alerts_status ON health_alerts (tenant_id, status, last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_health_alerts_code ON health_alerts (tenant_id, code, status);
                """
            )
            self._ensure_column(conn, "drafts", "lease_id", "TEXT")
            self._ensure_column(conn, "drafts", "lease_until", "TEXT")
            self._ensure_column(conn, "drafts", "lease_taken_at", "TEXT")
            self._ensure_column(conn, "drafts", "send_attempts", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "drafts", "last_error", "TEXT")
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_drafts_state ON drafts (tenant_id, state, updated_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_drafts_lease ON drafts (tenant_id, lease_until)")
            except Exception:
                pass
            self._ensure_column(conn, "webhook_events", "dedupe_key", "TEXT")
            self._ensure_column(conn, "webhook_events", "source_kind", "TEXT DEFAULT 'webhook'")
            self._ensure_column(conn, "webhook_events", "processed_at", "TEXT")
            self._ensure_column(conn, "webhook_events", "verified_by", "TEXT")
            self._ensure_column(conn, "webhook_events", "signature", "TEXT")
            self._ensure_column(conn, "webhook_events", "nonce", "TEXT")
            self._ensure_column(conn, "webhook_events", "status", "TEXT DEFAULT 'received'")
            self._ensure_column(conn, "webhook_events", "attempts", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "webhook_events", "last_attempt_at", "TEXT")
            self._ensure_column(conn, "webhook_events", "last_error", "TEXT")
            self._ensure_column(conn, "drafts", "lease_id", "TEXT")
            self._ensure_column(conn, "drafts", "lease_until", "TEXT")
            self._ensure_column(conn, "drafts", "sending_attempts", "INTEGER DEFAULT 0")
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (tenant_id, status, received_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_webhook_events_dedupe ON webhook_events (tenant_id, dedupe_key)")
            except Exception:
                pass
            self._ensure_column(conn, "messages", "message_ts_epoch", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "chats", "last_message_ts_epoch", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "chats", "first_response_due_at", "TEXT")
            self._ensure_column(conn, "chats", "first_response_due_epoch", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "chats", "first_response_at", "TEXT")
            self._ensure_column(conn, "chats", "first_response_epoch", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "chats", "last_operator_user", "TEXT")
            self._ensure_column(conn, "chats", "last_operator_action_at", "TEXT")
            self._ensure_column(conn, "chats", "last_send_status", "TEXT")
            self._ensure_column(conn, "chats", "last_send_detail", "TEXT")
            try:
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_events_dedupe_unique ON webhook_events (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL AND dedupe_key != ''")
            except Exception:
                pass
            self._ensure_column(conn, "drafts", "lease_id", "TEXT")
            self._ensure_column(conn, "drafts", "lease_until", "TEXT")
            self._ensure_column(conn, "drafts", "lease_acquired_at", "TEXT")
            self._ensure_column(conn, "drafts", "sent_at", "TEXT")
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_drafts_state ON drafts (tenant_id, state, updated_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_drafts_lease ON drafts (tenant_id, state, lease_until)")
            except Exception:
                pass

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column in columns:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def upsert_chat(self, chat: Dict[str, Any]) -> None:
        now = utc_now_iso()
        tags = chat.get("tags") or []
        unread_count = int(chat.get("unread_count") or 0)
        last_message_ts = clean_text(chat.get("last_message_ts"))
        last_message_ts_epoch = int(chat.get("last_message_ts_epoch") or _to_epoch_seconds(last_message_ts))
        status_value = clean_text(chat.get("status") or ("new" if unread_count > 0 else "waiting_customer")) or "new"
        first_response_due_at = clean_text(chat.get("first_response_due_at"))
        first_response_due_epoch = int(chat.get("first_response_due_epoch") or _to_epoch_seconds(first_response_due_at))
        first_response_at = clean_text(chat.get("first_response_at"))
        first_response_epoch = int(chat.get("first_response_epoch") or _to_epoch_seconds(first_response_at))
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO chats (
                    tenant_id, chat_id, client_name, title, item_id, item_title,
                    unread_count, last_message_text, last_message_ts, last_message_ts_epoch, status,
                    priority, assigned_to, note, tags_json, first_response_due_at, first_response_due_epoch,
                    first_response_at, first_response_epoch, last_operator_user, last_operator_action_at,
                    last_send_status, last_send_detail, raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, chat_id) DO UPDATE SET
                    client_name=excluded.client_name,
                    title=excluded.title,
                    item_id=excluded.item_id,
                    item_title=excluded.item_title,
                    unread_count=excluded.unread_count,
                    last_message_text=excluded.last_message_text,
                    last_message_ts=excluded.last_message_ts,
                    last_message_ts_epoch=excluded.last_message_ts_epoch,
                    status=COALESCE(NULLIF(excluded.status, ''), chats.status),
                    priority=COALESCE(NULLIF(excluded.priority, ''), chats.priority),
                    assigned_to=COALESCE(NULLIF(excluded.assigned_to, ''), chats.assigned_to),
                    note=COALESCE(NULLIF(excluded.note, ''), chats.note),
                    tags_json=CASE WHEN excluded.tags_json != '[]' THEN excluded.tags_json ELSE chats.tags_json END,
                    first_response_due_at=CASE WHEN COALESCE(chats.first_response_due_at, '') = '' THEN excluded.first_response_due_at ELSE chats.first_response_due_at END,
                    first_response_due_epoch=CASE WHEN COALESCE(chats.first_response_due_epoch, 0) = 0 THEN excluded.first_response_due_epoch ELSE chats.first_response_due_epoch END,
                    first_response_at=CASE WHEN COALESCE(chats.first_response_at, '') = '' THEN excluded.first_response_at ELSE chats.first_response_at END,
                    first_response_epoch=CASE WHEN COALESCE(chats.first_response_epoch, 0) = 0 THEN excluded.first_response_epoch ELSE chats.first_response_epoch END,
                    raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                (
                    self.tenant_id,
                    clean_text(chat.get("chat_id") or chat.get("id")),
                    clean_text(chat.get("client_name")),
                    clean_text(chat.get("title")),
                    clean_text(chat.get("item_id")),
                    clean_text(chat.get("item_title")),
                    unread_count,
                    clean_text(chat.get("last_message_text")),
                    last_message_ts,
                    last_message_ts_epoch,
                    status_value,
                    clean_text(chat.get("priority") or "normal"),
                    clean_text(chat.get("assigned_to")),
                    clean_text(chat.get("note")),
                    json.dumps(tags, ensure_ascii=False),
                    first_response_due_at,
                    first_response_due_epoch,
                    first_response_at,
                    first_response_epoch,
                    clean_text(chat.get("last_operator_user")),
                    clean_text(chat.get("last_operator_action_at")),
                    clean_text(chat.get("last_send_status")),
                    clean_text(chat.get("last_send_detail")),
                    json.dumps(chat, ensure_ascii=False),
                    now,
                ),
            )

    def add_messages(self, chat_id: str, messages: Iterable[Dict[str, Any]]) -> int:
        now = utc_now_iso()
        inserted = 0
        with self.connect() as conn:
            for message in messages:
                message_id = clean_text(message.get("message_id") or message.get("id"))
                if not message_id:
                    continue
                created_at = clean_text(message.get("created_at") or message.get("message_ts") or message.get("created") or message.get("created_at")) or now
                message_ts = clean_text(message.get("message_ts") or message.get("created") or message.get("created_at") or created_at) or created_at
                message_ts_epoch = int(message.get("message_ts_epoch") or _to_epoch_seconds(message_ts or created_at))
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO messages (
                        tenant_id, message_id, chat_id, direction, is_read, author_name,
                        message_ts, message_ts_epoch, text, attachments_json, raw_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.tenant_id,
                        message_id,
                        clean_text(chat_id),
                        clean_text(message.get("direction") or "unknown"),
                        1 if bool(message.get("is_read")) else 0,
                        clean_text(message.get("author_name")),
                        message_ts,
                        message_ts_epoch,
                        clean_text(message.get("text")),
                        json.dumps(message.get("attachments") or [], ensure_ascii=False),
                        json.dumps(message, ensure_ascii=False),
                        created_at,
                    ),
                )
                inserted += int(cur.rowcount or 0)
        return inserted

    def list_chats(self, status: str = "all", only_unanswered: bool = False, limit: int = 100, offset: int = 0, *, assigned_to: str = "", overdue_only: bool = False, needs_human_only: bool = False, with_media_only: bool = False, with_bargain_only: bool = False) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if status != "all":
            where.append("status = ?")
            params.append(status)
        if clean_text(assigned_to):
            where.append("assigned_to = ?")
            params.append(clean_text(assigned_to))
        if overdue_only:
            where.append("COALESCE(first_response_epoch, 0) = 0")
            where.append("COALESCE(first_response_due_epoch, 0) > 0")
            where.append("first_response_due_epoch < ?")
            params.append(_to_epoch_seconds(utc_now_iso()))
        if only_unanswered:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM messages m1
                    WHERE m1.tenant_id = chats.tenant_id
                      AND m1.chat_id = chats.chat_id
                      AND m1.direction = 'in'
                      AND NOT EXISTS (
                          SELECT 1 FROM messages m2
                          WHERE m2.tenant_id = m1.tenant_id
                            AND m2.chat_id = m1.chat_id
                            AND m2.direction = 'out'
                            AND COALESCE(m2.message_ts_epoch, 0) >= COALESCE(m1.message_ts_epoch, 0)
                      )
                )
                """
            )
        query = f"""
            SELECT * FROM chats
            WHERE {' AND '.join(where)}
            ORDER BY COALESCE(last_message_ts_epoch, 0) DESC, updated_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([max(1, limit), max(0, offset)])
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            chat = self._row_to_chat(row)
            flags = self.chat_flags(chat.get("chat_id") or "")
            chat["flags"] = flags
            if needs_human_only and not flags.get("needs_human"):
                continue
            if with_media_only and not flags.get("has_media_selected"):
                continue
            if with_bargain_only and not flags.get("has_bargain"):
                continue
            result.append(chat)
        return result

    def get_chat(self, chat_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM chats WHERE tenant_id = ? AND chat_id = ?",
                (self.tenant_id, clean_text(chat_id)),
            ).fetchone()
        return self._row_to_chat(row) if row else None

    def get_messages(self, chat_id: str, limit: int = 200) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM messages
                WHERE tenant_id = ? AND chat_id = ?
                ORDER BY COALESCE(message_ts_epoch, 0), message_id
                LIMIT ?
                """,
                (self.tenant_id, clean_text(chat_id), max(1, limit)),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def replace_draft(
        self,
        chat_id: str,
        body: str,
        confidence: float,
        route: str,
        reason: str,
        source_message_ids: Iterable[str],
        model_name: str,
        state: str = "ready",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO drafts (
                    tenant_id, chat_id, state, body, confidence, route, reason,
                    source_message_ids_json, model_name, meta_json, created_at, updated_at,
                    lease_id, lease_until, lease_acquired_at, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
                ON CONFLICT(tenant_id, chat_id) DO UPDATE SET
                    state=excluded.state,
                    body=excluded.body,
                    confidence=excluded.confidence,
                    route=excluded.route,
                    reason=excluded.reason,
                    source_message_ids_json=excluded.source_message_ids_json,
                    model_name=excluded.model_name,
                    meta_json=excluded.meta_json,
                    updated_at=excluded.updated_at,
                    lease_id=NULL,
                    lease_until=NULL,
                    lease_acquired_at=NULL
                """,
                (
                    self.tenant_id,
                    clean_text(chat_id),
                    clean_text(state) or "ready",
                    body,
                    float(confidence),
                    clean_text(route) or "manual",
                    clean_text(reason),
                    json.dumps([clean_text(x) for x in source_message_ids if clean_text(x)], ensure_ascii=False),
                    clean_text(model_name),
                    json.dumps(meta or {}, ensure_ascii=False),
                    now,
                    now,
                ),
            )

    def get_draft(self, chat_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM drafts WHERE tenant_id = ? AND chat_id = ?",
                (self.tenant_id, clean_text(chat_id)),
            ).fetchone()
        if not row:
            return None
        return self._row_to_draft(row)

    def list_review_queue(self, *, states: Optional[Iterable[str]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        state_values = [clean_text(x) for x in (states or ["review", "hold", "error"]) if clean_text(x)]
        params: List[Any] = [self.tenant_id]
        where = ["d.tenant_id = ?"]
        if state_values:
            where.append("d.state IN (%s)" % ",".join("?" for _ in state_values))
            params.extend(state_values)
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    d.*,
                    c.client_name AS chat_client_name,
                    c.title AS chat_title,
                    c.item_id AS chat_item_id,
                    c.item_title AS chat_item_title,
                    c.status AS chat_status,
                    c.priority AS chat_priority,
                    c.assigned_to AS chat_assigned_to,
                    c.note AS chat_note,
                    c.tags_json AS chat_tags_json,
                    c.last_message_ts AS chat_last_message_ts,
                    c.last_message_text AS chat_last_message_text
                FROM drafts d
                LEFT JOIN chats c ON c.tenant_id = d.tenant_id AND c.chat_id = d.chat_id
                WHERE {' AND '.join(where)}
                ORDER BY COALESCE(c.last_message_ts, '' ) DESC, d.updated_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            payload = self._row_to_draft(row)
            payload["chat"] = {
                "chat_id": payload.get("chat_id"),
                "client_name": row["chat_client_name"] or "",
                "title": row["chat_title"] or payload.get("chat_id") or "",
                "item_id": row["chat_item_id"] or "",
                "item_title": row["chat_item_title"] or "",
                "status": row["chat_status"] or "new",
                "priority": row["chat_priority"] or "normal",
                "assigned_to": row["chat_assigned_to"] or "",
                "note": row["chat_note"] or "",
                "tags": json.loads(row["chat_tags_json"] or "[]"),
                "last_message_ts": row["chat_last_message_ts"] or "",
                "last_message_text": row["chat_last_message_text"] or "",
            }
            items.append(payload)
        return items

    def update_draft_review(
        self,
        chat_id: str,
        *,
        state: str,
        reviewer: str = "",
        review_note: str = "",
        body: Optional[str] = None,
        route: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        draft = self.get_draft(chat_id)
        if not draft:
            return None
        meta = dict(draft.get("meta") or {})
        history = list(meta.get("review_history") or [])
        history.append({
            "ts": utc_now_iso(),
            "state": clean_text(state) or draft.get("state") or "review",
            "reviewer": clean_text(reviewer),
            "note": clean_text(review_note),
        })
        meta["review_history"] = history[-50:]
        meta["reviewer"] = clean_text(reviewer)
        if clean_text(review_note):
            meta["review_note"] = clean_text(review_note)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE drafts
                SET state = ?, body = ?, route = ?, meta_json = ?, updated_at = ?, lease_id = NULL, lease_until = NULL, lease_acquired_at = NULL
                WHERE tenant_id = ? AND chat_id = ?
                """,
                (
                    clean_text(state) or draft.get("state") or "review",
                    str(body if body is not None else draft.get("body") or ""),
                    clean_text(route if route is not None else draft.get("route") or "manual"),
                    json.dumps(meta, ensure_ascii=False),
                    utc_now_iso(),
                    self.tenant_id,
                    clean_text(chat_id),
                ),
            )
        return self.get_draft(chat_id)

    def count_drafts_by_state(self) -> Dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS cnt FROM drafts WHERE tenant_id = ? GROUP BY state",
                (self.tenant_id,),
            ).fetchall()
        return {clean_text(row[0]) or "unknown": int(row[1] or 0) for row in rows}

    def list_pending_drafts(self, routes: Optional[Iterable[str]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        params: List[Any] = [self.tenant_id, utc_now_iso()]
        where = ["tenant_id = ?", "state = 'ready'", "(lease_until IS NULL OR lease_until = '' OR lease_until < ?)"]
        route_values = [clean_text(x) for x in (routes or []) if clean_text(x)]
        if route_values:
            where.append("route IN (%s)" % ",".join("?" for _ in route_values))
            params.extend(route_values)
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM drafts WHERE {' AND '.join(where)} ORDER BY updated_at ASC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_draft(row) for row in rows]


    def claim_ready_drafts(
        self,
        *,
        routes: Optional[Iterable[str]] = None,
        limit: int = 100,
        lease_seconds: int = 120,
        lease_id: str = "",
    ) -> List[Dict[str, Any]]:
        route_values = [clean_text(x) for x in (routes or []) if clean_text(x)]
        lease_token = clean_text(lease_id) or str(uuid4())
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_until = (now + timedelta(seconds=max(30, int(lease_seconds or 120)))).isoformat()
        params: List[Any] = [self.tenant_id, now_iso]
        where = ["tenant_id = ?", "state = 'ready'", "(lease_until IS NULL OR lease_until = '' OR lease_until < ?)"]
        if route_values:
            where.append("route IN (%s)" % ",".join("?" for _ in route_values))
            params.extend(route_values)
        params.append(max(1, limit))
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                f"SELECT chat_id FROM drafts WHERE {' AND '.join(where)} ORDER BY updated_at ASC LIMIT ?",
                params,
            ).fetchall()
            chat_ids = [clean_text(row[0]) for row in rows if clean_text(row[0])]
            if not chat_ids:
                return []
            conn.executemany(
                "UPDATE drafts SET state = 'sending', lease_id = ?, lease_until = ?, lease_acquired_at = ?, updated_at = ? WHERE tenant_id = ? AND chat_id = ?",
                [(lease_token, lease_until, now_iso, now_iso, self.tenant_id, chat_id) for chat_id in chat_ids],
            )
            placeholders = ",".join("?" for _ in chat_ids)
            selected = conn.execute(
                f"SELECT * FROM drafts WHERE tenant_id = ? AND lease_id = ? AND chat_id IN ({placeholders}) ORDER BY updated_at ASC",
                [self.tenant_id, lease_token, *chat_ids],
            ).fetchall()
        return [self._row_to_draft(row) for row in selected]

    def claim_pending_drafts(
        self,
        *,
        routes: Optional[Iterable[str]] = None,
        limit: int = 100,
        lease_seconds: int = 120,
        lease_id: str = "",
    ) -> List[Dict[str, Any]]:
        return self.claim_ready_drafts(routes=routes, limit=limit, lease_seconds=lease_seconds, lease_id=lease_id)

    def release_draft_lease(self, chat_id: str, *, state: Optional[str] = None) -> None:
        with self.connect() as conn:
            if clean_text(state):
                conn.execute(
                    "UPDATE drafts SET state = ?, lease_id = NULL, lease_until = NULL, lease_acquired_at = NULL, updated_at = ? WHERE tenant_id = ? AND chat_id = ?",
                    (clean_text(state), utc_now_iso(), self.tenant_id, clean_text(chat_id)),
                )
            else:
                conn.execute(
                    "UPDATE drafts SET lease_id = NULL, lease_until = NULL, lease_acquired_at = NULL, updated_at = ? WHERE tenant_id = ? AND chat_id = ?",
                    (utc_now_iso(), self.tenant_id, clean_text(chat_id)),
                )

    def touch_chat_after_send(self, chat_id: str, *, body: str, message_ts: str, unread_count: int = 0, delivery_status: str = "sent", detail: str = "", operator_user: str = "") -> None:
        epoch = _to_epoch_seconds(message_ts)
        with self.connect() as conn:
            row = conn.execute("SELECT first_response_at FROM chats WHERE tenant_id = ? AND chat_id = ?", (self.tenant_id, clean_text(chat_id))).fetchone()
            first_missing = not row or not clean_text(row[0])
            if first_missing:
                conn.execute(
                    "UPDATE chats SET unread_count = ?, last_message_text = ?, last_message_ts = ?, last_message_ts_epoch = ?, status = 'waiting_customer', first_response_at = ?, first_response_epoch = ?, last_send_status = ?, last_send_detail = ?, last_operator_user = COALESCE(NULLIF(?, ''), last_operator_user), last_operator_action_at = ?, updated_at = ? WHERE tenant_id = ? AND chat_id = ?",
                    (max(0, int(unread_count)), clean_text(body), clean_text(message_ts), epoch, clean_text(message_ts), epoch, clean_text(delivery_status), clean_text(detail), clean_text(operator_user), utc_now_iso(), utc_now_iso(), self.tenant_id, clean_text(chat_id)),
                )
            else:
                conn.execute(
                    "UPDATE chats SET unread_count = ?, last_message_text = ?, last_message_ts = ?, last_message_ts_epoch = ?, status = 'waiting_customer', last_send_status = ?, last_send_detail = ?, last_operator_user = COALESCE(NULLIF(?, ''), last_operator_user), last_operator_action_at = ?, updated_at = ? WHERE tenant_id = ? AND chat_id = ?",
                    (max(0, int(unread_count)), clean_text(body), clean_text(message_ts), epoch, clean_text(delivery_status), clean_text(detail), clean_text(operator_user), utc_now_iso(), utc_now_iso(), self.tenant_id, clean_text(chat_id)),
                )

    def _mark_draft_state(self, chat_id: str, *, state: str, remote_message_id: str = "", lease_id: str = "", extra_meta: Optional[Dict[str, Any]] = None) -> None:
        draft = self.get_draft(chat_id) or {}
        meta = dict(draft.get("meta") or {})
        if clean_text(remote_message_id):
            meta["remote_message_id"] = clean_text(remote_message_id)
        if extra_meta:
            meta.update(extra_meta)
        with self.connect() as conn:
            now = utc_now_iso()
            sent_marker = now if clean_text(state) in {"sent", "partial_sent_text_only"} else ""
            params = [clean_text(state) or draft.get("state") or "ready", now, sent_marker, sent_marker, json.dumps(meta, ensure_ascii=False), self.tenant_id, clean_text(chat_id)]
            query = "UPDATE drafts SET state = ?, updated_at = ?, sent_at = CASE WHEN ? != '' THEN ? ELSE sent_at END, lease_id = NULL, lease_until = NULL, lease_acquired_at = NULL, meta_json = ? WHERE tenant_id = ? AND chat_id = ?"
            if clean_text(lease_id):
                query += " AND (lease_id = ? OR lease_id IS NULL OR lease_id = '')"
                params.append(clean_text(lease_id))
            conn.execute(query, params)

    def mark_draft_sent(self, chat_id: str, remote_message_id: str = "", lease_id: str = "", *, extra_meta: Optional[Dict[str, Any]] = None) -> None:
        self._mark_draft_state(chat_id, state="sent", remote_message_id=remote_message_id, lease_id=lease_id, extra_meta=extra_meta)

    def mark_draft_partial_sent(self, chat_id: str, remote_message_id: str = "", lease_id: str = "", *, extra_meta: Optional[Dict[str, Any]] = None) -> None:
        self._mark_draft_state(chat_id, state="partial_sent_text_only", remote_message_id=remote_message_id, lease_id=lease_id, extra_meta=extra_meta)

    def mark_draft_error(self, chat_id: str, error_text: str, lease_id: str = "") -> None:
        draft = self.get_draft(chat_id) or {}
        if clean_text(draft.get("state")) in {"sent", "partial_sent_text_only"}:
            return
        meta = dict(draft.get("meta") or {})
        meta["error"] = clean_text(error_text)
        with self.connect() as conn:
            params = [utc_now_iso(), json.dumps(meta, ensure_ascii=False), self.tenant_id, clean_text(chat_id)]
            query = "UPDATE drafts SET state = 'error', updated_at = ?, lease_id = NULL, lease_until = NULL, lease_acquired_at = NULL, meta_json = ? WHERE tenant_id = ? AND chat_id = ? AND state NOT IN ('sent', 'partial_sent_text_only')"
            if clean_text(lease_id):
                query += " AND (lease_id = ? OR lease_id IS NULL OR lease_id = '')"
                params.append(clean_text(lease_id))
            conn.execute(query, params)

    def _update_chat_meta_conn(
        self,
        conn: sqlite3.Connection,
        chat_id: str,
        *,
        status: Optional[str] = None,
        note: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        assigned_to: Optional[str] = None,
        priority: Optional[str] = None,
        operator_user: str = "",
    ) -> bool:
        fields: List[str] = []
        params: List[Any] = []
        if status is not None:
            fields.append("status = ?")
            params.append(clean_text(status) or "new")
        if note is not None:
            fields.append("note = ?")
            params.append(clean_text(note))
        if tags is not None:
            fields.append("tags_json = ?")
            params.append(json.dumps([clean_text(x) for x in tags if clean_text(x)], ensure_ascii=False))
        if assigned_to is not None:
            fields.append("assigned_to = ?")
            params.append(clean_text(assigned_to))
        if priority is not None:
            fields.append("priority = ?")
            params.append(clean_text(priority) or "normal")
        if not fields:
            return False
        fields.append("last_operator_user = ?")
        params.append(clean_text(operator_user))
        now_iso = utc_now_iso()
        fields.append("last_operator_action_at = ?")
        params.append(now_iso)
        fields.append("updated_at = ?")
        params.append(now_iso)
        params.extend([self.tenant_id, clean_text(chat_id)])
        cur = conn.execute(
            f"UPDATE chats SET {', '.join(fields)} WHERE tenant_id = ? AND chat_id = ?",
            params,
        )
        return bool(cur.rowcount)

    def update_chat_meta(self, chat_id: str, *, status: Optional[str] = None, note: Optional[str] = None, tags: Optional[Iterable[str]] = None, assigned_to: Optional[str] = None, priority: Optional[str] = None, operator_user: str = "") -> None:
        with self.connect() as conn:
            self._update_chat_meta_conn(
                conn,
                chat_id,
                status=status,
                note=note,
                tags=tags,
                assigned_to=assigned_to,
                priority=priority,
                operator_user=operator_user,
            )

    def bulk_update_chat_meta(
        self,
        chat_ids: Iterable[str],
        *,
        status: Optional[str] = None,
        note: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        assigned_to: Optional[str] = None,
        priority: Optional[str] = None,
        operator_user: str = "",
    ) -> int:
        chat_id_list = [clean_text(chat_id) for chat_id in chat_ids if clean_text(chat_id)]
        if not chat_id_list:
            return 0
        updated = 0
        with self.connect() as conn:
            for chat_id in chat_id_list:
                if self._update_chat_meta_conn(
                    conn,
                    chat_id,
                    status=status,
                    note=note,
                    tags=tags,
                    assigned_to=assigned_to,
                    priority=priority,
                    operator_user=operator_user,
                ):
                    updated += 1
        return updated

    def list_assignees(self, limit: int = 100) -> List[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT value FROM (
                    SELECT assigned_to AS value FROM chats WHERE tenant_id = ? AND COALESCE(assigned_to, '') != ''
                    UNION
                    SELECT last_operator_user AS value FROM chats WHERE tenant_id = ? AND COALESCE(last_operator_user, '') != ''
                )
                ORDER BY value COLLATE NOCASE ASC
                LIMIT ?
                """,
                (self.tenant_id, self.tenant_id, max(1, int(limit or 100))),
            ).fetchall()
        return [clean_text(row[0]) for row in rows if clean_text(row[0])]


    def upsert_knowledge_doc(
        self,
        *,
        title: str,
        body_text: str,
        kind: str = "faq",
        item_id: str = "",
        item_title: str = "",
        tags: Optional[Iterable[str]] = None,
        source_name: str = "",
        source_url: str = "",
        active: bool = True,
        meta: Optional[Dict[str, Any]] = None,
        doc_id: Optional[int] = None,
        chunk_chars: int = 900,
        overlap_chars: int = 120,
    ) -> int:
        now = utc_now_iso()
        safe_title = clean_text(title) or "Без названия"
        safe_kind = clean_text(kind) or "faq"
        safe_item_id = clean_text(item_id)
        safe_item_title = clean_text(item_title)
        safe_tags = [clean_text(x) for x in (tags or []) if clean_text(x)]
        raw_text = str(body_text or "").strip()
        if not raw_text:
            raise ValueError("body_text is required")
        doc_meta = dict(meta or {})
        chunks = split_text_into_chunks(raw_text, max_chars=chunk_chars, overlap_chars=overlap_chars)
        if not chunks:
            chunks = [raw_text]
        with self.connect() as conn:
            if doc_id:
                conn.execute(
                    """
                    UPDATE knowledge_docs
                    SET title = ?, kind = ?, item_id = ?, item_title = ?, tags_json = ?, source_name = ?, source_url = ?, body_text = ?, active = ?, meta_json = ?, updated_at = ?
                    WHERE tenant_id = ? AND doc_id = ?
                    """,
                    (
                        safe_title,
                        safe_kind,
                        safe_item_id,
                        safe_item_title,
                        json.dumps(safe_tags, ensure_ascii=False),
                        clean_text(source_name),
                        clean_text(source_url),
                        raw_text,
                        1 if active else 0,
                        json.dumps(doc_meta, ensure_ascii=False),
                        now,
                        self.tenant_id,
                        int(doc_id),
                    ),
                )
                current_doc_id = int(doc_id)
                conn.execute("DELETE FROM knowledge_chunks WHERE tenant_id = ? AND doc_id = ?", (self.tenant_id, current_doc_id))
            else:
                cur = conn.execute(
                    """
                    INSERT INTO knowledge_docs (
                        tenant_id, title, kind, item_id, item_title, tags_json, source_name, source_url,
                        body_text, active, meta_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.tenant_id,
                        safe_title,
                        safe_kind,
                        safe_item_id,
                        safe_item_title,
                        json.dumps(safe_tags, ensure_ascii=False),
                        clean_text(source_name),
                        clean_text(source_url),
                        raw_text,
                        1 if active else 0,
                        json.dumps(doc_meta, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                current_doc_id = int(cur.lastrowid or 0)
            for idx, chunk in enumerate(chunks):
                norm_text = normalize_for_search(chunk)
                conn.execute(
                    """
                    INSERT INTO knowledge_chunks (
                        tenant_id, doc_id, chunk_index, title, kind, item_id, item_title,
                        tags_json, text, norm_text, token_count, meta_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.tenant_id,
                        current_doc_id,
                        idx,
                        safe_title,
                        safe_kind,
                        safe_item_id,
                        safe_item_title,
                        json.dumps(safe_tags, ensure_ascii=False),
                        chunk,
                        norm_text,
                        len(norm_text.split()),
                        json.dumps(doc_meta, ensure_ascii=False),
                        now,
                    ),
                )
        return current_doc_id

    def get_knowledge_doc(self, doc_id: int) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge_docs WHERE tenant_id = ? AND doc_id = ?",
                (self.tenant_id, int(doc_id)),
            ).fetchone()
        return self._row_to_knowledge_doc(row) if row else None

    def list_knowledge_docs(self, *, search: str = "", kind: str = "all", limit: int = 200) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if kind != "all":
            where.append("kind = ?")
            params.append(clean_text(kind))
        if clean_text(search):
            query = f"%{clean_text(search).lower()}%"
            where.append("(LOWER(title) LIKE ? OR LOWER(body_text) LIKE ? OR LOWER(item_title) LIKE ? OR LOWER(tags_json) LIKE ?)")
            params.extend([query, query, query, query])
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM knowledge_docs WHERE {' AND '.join(where)} ORDER BY active DESC, updated_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_knowledge_doc(row) for row in rows]

    def delete_knowledge_doc(self, doc_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM knowledge_chunks WHERE tenant_id = ? AND doc_id = ?", (self.tenant_id, int(doc_id)))
            conn.execute("DELETE FROM knowledge_docs WHERE tenant_id = ? AND doc_id = ?", (self.tenant_id, int(doc_id)))

    def search_knowledge(
        self,
        query: str,
        *,
        item_id: str = "",
        item_title: str = "",
        limit: int = 5,
        min_score: float = 0.45,
    ) -> List[KnowledgeHit]:
        query = clean_text(query)
        if not query:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT c.*
                FROM knowledge_chunks c
                JOIN knowledge_docs d ON d.doc_id = c.doc_id AND d.tenant_id = c.tenant_id
                WHERE c.tenant_id = ? AND d.active = 1
                ORDER BY c.updated_at DESC
                LIMIT 5000
                """,
                (self.tenant_id,),
            ).fetchall()
        hits: List[KnowledgeHit] = []
        safe_item_id = clean_text(item_id)
        safe_item_title = clean_text(item_title)
        for row in rows:
            tags = json.loads(row["tags_json"] or "[]")
            score = score_match(
                query,
                text=row["text"],
                title=row["title"],
                item_id=safe_item_id or row["item_id"],
                item_title=safe_item_title or row["item_title"],
                tags=tags,
            )
            row_item_id = clean_text(row["item_id"])
            row_item_title = clean_text(row["item_title"])
            if safe_item_id and row_item_id and safe_item_id == row_item_id:
                score += 0.8
            if safe_item_title and row_item_title and normalize_for_search(safe_item_title) in normalize_for_search(row_item_title):
                score += 0.35
            if score < float(min_score or 0.0):
                continue
            hits.append(
                KnowledgeHit(
                    doc_id=int(row["doc_id"] or 0),
                    chunk_id=int(row["chunk_id"] or 0),
                    title=clean_text(row["title"]),
                    kind=clean_text(row["kind"]),
                    item_id=row_item_id,
                    item_title=row_item_title,
                    score=round(score, 6),
                    excerpt=compact_excerpt(row["text"], query),
                    tags=[clean_text(x) for x in tags if clean_text(x)],
                    meta=json.loads(row["meta_json"] or "{}"),
                )
            )
        hits.sort(key=lambda item: (item.score, item.doc_id, item.chunk_id), reverse=True)
        deduped: List[KnowledgeHit] = []
        seen_chunks: set[int] = set()
        for hit in hits:
            if hit.chunk_id in seen_chunks:
                continue
            seen_chunks.add(hit.chunk_id)
            doc = self.get_knowledge_doc(hit.doc_id) or {}
            hit.source_name = clean_text(doc.get("source_name"))
            hit.source_url = clean_text(doc.get("source_url"))
            if not hit.tags:
                hit.tags = list(doc.get("tags") or [])
            deduped.append(hit)
            if len(deduped) >= max(1, int(limit or 5)):
                break
        return deduped

    def create_media_asset(
        self,
        *,
        title: str,
        media_kind: str = "image",
        caption: str = "",
        item_id: str = "",
        item_title: str = "",
        file_name: str = "",
        local_path: str = "",
        external_url: str = "",
        mime_type: str = "",
        tags: Optional[Iterable[str]] = None,
        active: bool = True,
        meta: Optional[Dict[str, Any]] = None,
        asset_id: Optional[int] = None,
    ) -> int:
        now = utc_now_iso()
        safe_tags = [clean_text(x) for x in (tags or []) if clean_text(x)]
        payload = (
            clean_text(media_kind) or "image",
            clean_text(title) or "Без названия",
            clean_text(caption),
            clean_text(item_id),
            clean_text(item_title),
            clean_text(file_name),
            clean_text(local_path),
            clean_text(external_url),
            clean_text(mime_type),
            json.dumps(safe_tags, ensure_ascii=False),
            1 if active else 0,
            json.dumps(meta or {}, ensure_ascii=False),
            now,
        )
        with self.connect() as conn:
            if asset_id:
                conn.execute(
                    """
                    UPDATE media_assets
                    SET media_kind = ?, title = ?, caption = ?, item_id = ?, item_title = ?, file_name = ?, local_path = ?, external_url = ?, mime_type = ?, tags_json = ?, active = ?, meta_json = ?, updated_at = ?
                    WHERE tenant_id = ? AND asset_id = ?
                    """,
                    (*payload, self.tenant_id, int(asset_id)),
                )
                return int(asset_id)
            cur = conn.execute(
                """
                INSERT INTO media_assets (
                    tenant_id, media_kind, title, caption, item_id, item_title, file_name,
                    local_path, external_url, mime_type, tags_json, active, meta_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (self.tenant_id, *payload, now),
            )
            return int(cur.lastrowid or 0)

    def get_media_asset(self, asset_id: int) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_assets WHERE tenant_id = ? AND asset_id = ?",
                (self.tenant_id, int(asset_id)),
            ).fetchone()
        return self._row_to_media_asset(row) if row else None

    def list_media_assets(self, *, media_kind: str = "all", search: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if media_kind != "all":
            where.append("media_kind = ?")
            params.append(clean_text(media_kind))
        if clean_text(search):
            query = f"%{clean_text(search).lower()}%"
            where.append("(LOWER(title) LIKE ? OR LOWER(caption) LIKE ? OR LOWER(item_title) LIKE ? OR LOWER(tags_json) LIKE ?)")
            params.extend([query, query, query, query])
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM media_assets WHERE {' AND '.join(where)} ORDER BY active DESC, updated_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_media_asset(row) for row in rows]

    def delete_media_asset(self, asset_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM draft_media_links WHERE tenant_id = ? AND asset_id = ?", (self.tenant_id, int(asset_id)))
            conn.execute("DELETE FROM media_assets WHERE tenant_id = ? AND asset_id = ?", (self.tenant_id, int(asset_id)))

    def search_media_assets(
        self,
        query: str,
        *,
        item_id: str = "",
        item_title: str = "",
        limit: int = 4,
        media_kinds: Optional[Iterable[str]] = None,
    ) -> List[MediaSuggestion]:
        query = clean_text(query)
        if not query:
            return []
        allowed_kinds = {clean_text(x).lower() for x in (media_kinds or []) if clean_text(x)}
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM media_assets WHERE tenant_id = ? AND active = 1 ORDER BY updated_at DESC LIMIT 2000",
                (self.tenant_id,),
            ).fetchall()
        suggestions: List[MediaSuggestion] = []
        safe_item_id = clean_text(item_id)
        safe_item_title = clean_text(item_title)
        for row in rows:
            kind = clean_text(row["media_kind"]).lower() or "image"
            if allowed_kinds and kind not in allowed_kinds:
                continue
            tags = json.loads(row["tags_json"] or "[]")
            score = score_match(
                query,
                text=f"{row['caption'] or ''} {row['title'] or ''}",
                title=row["title"],
                item_id=safe_item_id or row["item_id"],
                item_title=safe_item_title or row["item_title"],
                tags=tags,
                media_kind=kind,
            )
            row_item_id = clean_text(row["item_id"])
            row_item_title = clean_text(row["item_title"])
            if safe_item_id and row_item_id and safe_item_id == row_item_id:
                score += 0.9
            if safe_item_title and row_item_title and normalize_for_search(safe_item_title) in normalize_for_search(row_item_title):
                score += 0.35
            if score <= 0.0:
                continue
            local_path = clean_text(row["local_path"])
            preview_url = clean_text(row["external_url"])
            suggestions.append(
                MediaSuggestion(
                    asset_id=int(row["asset_id"] or 0),
                    media_kind=kind,
                    title=clean_text(row["title"]),
                    caption=clean_text(row["caption"]),
                    item_id=row_item_id,
                    item_title=row_item_title,
                    mime_type=clean_text(row["mime_type"]),
                    external_url=clean_text(row["external_url"]),
                    local_path=local_path,
                    preview_url=preview_url,
                    score=round(score, 6),
                    tags=[clean_text(x) for x in tags if clean_text(x)],
                    meta=json.loads(row["meta_json"] or "{}"),
                )
            )
        suggestions.sort(key=lambda item: (item.score, item.asset_id), reverse=True)
        return suggestions[: max(1, int(limit or 4))]

    def set_draft_media_assets(self, chat_id: str, asset_ids: Iterable[int], *, source: str = "selected") -> None:
        safe_chat_id = clean_text(chat_id)
        now = utc_now_iso()
        normalized_ids = sorted({int(asset_id) for asset_id in asset_ids if int(asset_id) > 0})
        with self.connect() as conn:
            conn.execute("DELETE FROM draft_media_links WHERE tenant_id = ? AND chat_id = ?", (self.tenant_id, safe_chat_id))
            for asset_id in normalized_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO draft_media_links (tenant_id, chat_id, asset_id, source, created_at) VALUES (?, ?, ?, ?, ?)",
                    (self.tenant_id, safe_chat_id, int(asset_id), clean_text(source) or "selected", now),
                )
        draft = self.get_draft(safe_chat_id)
        if draft:
            meta = dict(draft.get("meta") or {})
            meta["prepared_media_asset_ids"] = normalized_ids
            meta["media_selection_source"] = clean_text(source) or "selected"
            self.replace_draft(
                chat_id=safe_chat_id,
                body=draft.get("body") or "",
                confidence=float(draft.get("confidence") or 0.0),
                route=draft.get("route") or "manual",
                reason=draft.get("reason") or "",
                source_message_ids=draft.get("source_message_ids") or [],
                model_name=draft.get("model_name") or "",
                state=draft.get("state") or "ready",
                meta=meta,
            )

    def list_draft_media_assets(self, chat_id: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT a.*
                FROM draft_media_links l
                JOIN media_assets a ON a.asset_id = l.asset_id AND a.tenant_id = l.tenant_id
                WHERE l.tenant_id = ? AND l.chat_id = ?
                ORDER BY l.created_at ASC, a.asset_id ASC
                """,
                (self.tenant_id, clean_text(chat_id)),
            ).fetchall()
        return [self._row_to_media_asset(row) for row in rows]

    def record_send_event(
        self,
        chat_id: str,
        *,
        body: str,
        transport: str,
        delivery_status: str,
        remote_message_id: str = "",
        attachments: Optional[List[Dict[str, Any]]] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> int:
        import hashlib
        body_hash = hashlib.sha256(clean_text(body).encode("utf-8")).hexdigest()
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO send_events (tenant_id, chat_id, body_hash, body_preview, transport, delivery_status, remote_message_id, attachments_json, detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.tenant_id, clean_text(chat_id), body_hash, clean_text(body)[:400], clean_text(transport), clean_text(delivery_status), clean_text(remote_message_id), json.dumps(attachments or [], ensure_ascii=False), json.dumps(detail or {}, ensure_ascii=False), utc_now_iso()),
            )
            return int(cur.lastrowid or 0)

    def list_send_events(self, chat_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM send_events WHERE tenant_id = ? AND chat_id = ? ORDER BY created_at DESC LIMIT ?", (self.tenant_id, clean_text(chat_id), max(1, limit))).fetchall()
        return [{"send_id": int(row["send_id"] or 0), "chat_id": row["chat_id"] or "", "body_hash": row["body_hash"] or "", "body_preview": row["body_preview"] or "", "transport": row["transport"] or "", "delivery_status": row["delivery_status"] or "", "remote_message_id": row["remote_message_id"] or "", "attachments": json.loads(row["attachments_json"] or "[]"), "detail": json.loads(row["detail_json"] or "{}"), "created_at": row["created_at"] or ""} for row in rows]

    def find_recent_duplicate_send(self, chat_id: str, body: str, *, window_seconds: int = 600) -> Optional[Dict[str, Any]]:
        import hashlib
        body_hash = hashlib.sha256(clean_text(body).encode("utf-8")).hexdigest()
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(1, int(window_seconds)))).isoformat()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM send_events WHERE tenant_id = ? AND chat_id = ? AND body_hash = ? AND created_at >= ? ORDER BY created_at DESC LIMIT 1", (self.tenant_id, clean_text(chat_id), body_hash, cutoff)).fetchone()
        if not row:
            return None
        return {"send_id": int(row["send_id"] or 0), "delivery_status": row["delivery_status"] or "", "remote_message_id": row["remote_message_id"] or "", "created_at": row["created_at"] or "", "body_hash": row["body_hash"] or ""}

    def chat_flags(self, chat_id: str) -> Dict[str, Any]:
        chat = self.get_chat(chat_id) or {}
        draft = self.get_draft(chat_id) or {}
        selected_media = self.list_draft_media_assets(chat_id)
        messages = self.get_messages(chat_id, limit=30)
        last_in = next((m for m in reversed(messages) if clean_text(m.get("direction")) == "in"), {})
        last_in_text = clean_text(last_in.get("text"))
        overdue = bool(int(chat.get("first_response_due_epoch") or 0) and not int(chat.get("first_response_epoch") or 0) and int(chat.get("first_response_due_epoch") or 0) < _to_epoch_seconds(utc_now_iso()))
        needs_human = clean_text(chat.get("status")) == "escalation" or clean_text(draft.get("route")) in {"manual", "escalate"} or bool((draft.get("meta") or {}).get("blocked_by"))
        return {
            "needs_reply": self.chat_needs_reply(chat_id),
            "overdue": overdue,
            "needs_human": needs_human,
            "has_media_selected": bool(selected_media),
            "has_bargain": bool(_BARGAIN_RE.search(last_in_text)),
            "asks_media": bool(_MEDIA_RE.search(last_in_text)),
        }

    def search_similar_dialogs(
        self,
        query: str,
        *,
        item_id: str = "",
        item_title: str = "",
        exclude_chat_id: str = "",
        limit: int = 5,
        min_score: float = 0.55,
    ) -> List[Dict[str, Any]]:
        query = clean_text(query)
        if not query:
            return []
        safe_item_id = clean_text(item_id)
        safe_item_title = clean_text(item_title)
        exclude_chat_id = clean_text(exclude_chat_id)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.*,
                    d.state AS draft_state,
                    d.route AS draft_route,
                    d.body AS draft_body,
                    d.reason AS draft_reason,
                    d.meta_json AS draft_meta_json,
                    (
                        SELECT m.text FROM messages m
                        WHERE m.tenant_id = c.tenant_id AND m.chat_id = c.chat_id AND m.direction = 'in'
                        ORDER BY COALESCE(m.message_ts, m.created_at) DESC, m.message_id DESC
                        LIMIT 1
                    ) AS latest_in_text,
                    (
                        SELECT m.text FROM messages m
                        WHERE m.tenant_id = c.tenant_id AND m.chat_id = c.chat_id AND m.direction = 'out'
                        ORDER BY COALESCE(m.message_ts, m.created_at) DESC, m.message_id DESC
                        LIMIT 1
                    ) AS latest_out_text
                FROM chats c
                LEFT JOIN drafts d ON d.tenant_id = c.tenant_id AND d.chat_id = c.chat_id
                WHERE c.tenant_id = ?
                ORDER BY COALESCE(c.last_message_ts, '' ) DESC, c.updated_at DESC
                LIMIT 1200
                """,
                (self.tenant_id,),
            ).fetchall()
        hits: List[Dict[str, Any]] = []
        for row in rows:
            chat_id = clean_text(row["chat_id"])
            if exclude_chat_id and chat_id == exclude_chat_id:
                continue
            tags = json.loads(row["tags_json"] or "[]")
            draft_meta = json.loads(row["draft_meta_json"] or "{}")
            candidate_text = " ".join(
                part for part in [
                    clean_text(row["latest_in_text"]),
                    clean_text(row["latest_out_text"]),
                    clean_text(row["draft_body"]),
                    clean_text(row["note"]),
                    clean_text(draft_meta.get("review_note")),
                ] if part
            )
            score = score_match(
                query,
                text=candidate_text,
                title=row["title"],
                item_id=safe_item_id or row["item_id"],
                item_title=safe_item_title or row["item_title"],
                tags=tags,
            )
            row_item_id = clean_text(row["item_id"])
            row_item_title = clean_text(row["item_title"])
            if safe_item_id and row_item_id and safe_item_id == row_item_id:
                score += 0.9
            if safe_item_title and row_item_title and normalize_for_search(safe_item_title) in normalize_for_search(row_item_title):
                score += 0.35
            if clean_text(row["latest_out_text"]):
                score += 0.1
            if score < float(min_score or 0.55):
                continue
            hits.append({
                "chat_id": chat_id,
                "client_name": clean_text(row["client_name"]),
                "title": clean_text(row["title"]),
                "item_id": row_item_id,
                "item_title": row_item_title,
                "status": clean_text(row["status"]),
                "priority": clean_text(row["priority"]),
                "assigned_to": clean_text(row["assigned_to"]),
                "draft_state": clean_text(row["draft_state"]),
                "draft_route": clean_text(row["draft_route"]),
                "draft_reason": clean_text(row["draft_reason"]),
                "latest_in_text": clean_text(row["latest_in_text"]),
                "latest_out_text": clean_text(row["latest_out_text"]),
                "last_message_ts": clean_text(row["last_message_ts"]),
                "score": round(float(score), 6),
                "excerpt": compact_excerpt(candidate_text, query),
                "tags": [clean_text(x) for x in tags if clean_text(x)],
            })
        hits.sort(key=lambda item: (float(item.get("score") or 0.0), clean_text(item.get("last_message_ts"))), reverse=True)
        return hits[: max(1, int(limit or 5))]

    def save_sync_state(self, key: str, payload: Any) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (tenant_id, key, value_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(tenant_id, key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
                """,
                (self.tenant_id, clean_text(key), json.dumps(payload, ensure_ascii=False), utc_now_iso()),
            )

    def load_sync_state(self, key: str, default: Any = None) -> Any:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM sync_state WHERE tenant_id = ? AND key = ?",
                (self.tenant_id, clean_text(key)),
            ).fetchone()
        if not row:
            return default
        try:
            return json.loads(str(row[0]))
        except Exception:
            return default

    def load_sync_state_record(self, key: str, default: Any = None) -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value_json, updated_at FROM sync_state WHERE tenant_id = ? AND key = ?",
                (self.tenant_id, clean_text(key)),
            ).fetchone()
        if not row:
            return {"value": default, "updated_at": ""}
        try:
            value = json.loads(str(row[0]))
        except Exception:
            value = default
        return {"value": value, "updated_at": row[1] or ""}

    def increment_counter(self, key: str, amount: int = 1) -> int:
        payload = self.load_sync_state(f"counter::{clean_text(key)}", {"value": 0})
        current = int((payload or {}).get("value") or 0) + int(amount)
        self.save_sync_state(f"counter::{clean_text(key)}", {"value": current, "updated_at": utc_now_iso()})
        return current

    def count_dead_letters(self, status: str = "open") -> int:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        safe_status = clean_text(status)
        if safe_status and safe_status != "all":
            where.append("status = ?")
            params.append(safe_status)
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM dead_letters WHERE {' AND '.join(where)}",
                params,
            ).fetchone()
        return int((row[0] if row else 0) or 0)

    def sync_health_alerts(self, alerts: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        now = utc_now_iso()
        safe_alerts: List[Dict[str, Any]] = []
        for raw in alerts:
            code = clean_text((raw or {}).get("code"))
            if not code:
                continue
            safe_alerts.append({
                "code": code,
                "severity": clean_text((raw or {}).get("severity") or "warning") or "warning",
                "title": clean_text((raw or {}).get("title") or code),
                "message": clean_text((raw or {}).get("message") or code),
                "details": (raw or {}).get("details") if isinstance((raw or {}).get("details"), dict) else {},
            })
        opened: List[Dict[str, Any]] = []
        resolved: List[Dict[str, Any]] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM health_alerts WHERE tenant_id = ? AND status IN ('open', 'acknowledged') ORDER BY alert_id DESC",
                (self.tenant_id,),
            ).fetchall()
            existing_by_code = {}
            for row in rows:
                code = clean_text(row["code"])
                if code and code not in existing_by_code:
                    existing_by_code[code] = row
            active_codes = set()
            for alert in safe_alerts:
                code = alert["code"]
                active_codes.add(code)
                details_json = json.dumps(alert["details"], ensure_ascii=False, sort_keys=True)
                existing = existing_by_code.get(code)
                if existing is None:
                    cur = conn.execute(
                        """
                        INSERT INTO health_alerts (
                            tenant_id, code, severity, title, message, details_json,
                            status, first_seen_at, last_seen_at, acknowledged_at, acknowledged_by, resolved_at, occurrence_count
                        ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, '', '', '', 1)
                        """,
                        (self.tenant_id, code, alert["severity"], alert["title"], alert["message"], details_json, now, now),
                    )
                    opened.append({
                        "alert_id": int(cur.lastrowid or 0),
                        "code": code,
                        "severity": alert["severity"],
                        "title": alert["title"],
                        "message": alert["message"],
                        "details": alert["details"],
                        "status": "open",
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "acknowledged_at": "",
                        "acknowledged_by": "",
                        "resolved_at": "",
                        "occurrence_count": 1,
                    })
                    continue
                existing_details_json = existing["details_json"] or "{}"
                changed = (
                    clean_text(existing["severity"]) != alert["severity"]
                    or clean_text(existing["title"]) != alert["title"]
                    or clean_text(existing["message"]) != alert["message"]
                    or clean_text(existing_details_json) != details_json
                )
                status = clean_text(existing["status"] or "open")
                occurrence_count = int(existing["occurrence_count"] or 1) + (1 if changed else 0)
                conn.execute(
                    """
                    UPDATE health_alerts
                    SET severity = ?, title = ?, message = ?, details_json = ?,
                        status = ?, last_seen_at = ?, resolved_at = '', occurrence_count = ?
                    WHERE tenant_id = ? AND alert_id = ?
                    """,
                    (
                        alert["severity"],
                        alert["title"],
                        alert["message"],
                        details_json,
                        "acknowledged" if status == "acknowledged" else "open",
                        now,
                        occurrence_count,
                        self.tenant_id,
                        int(existing["alert_id"] or 0),
                    ),
                )
            for code, existing in existing_by_code.items():
                if code in active_codes:
                    continue
                conn.execute(
                    "UPDATE health_alerts SET status = 'resolved', resolved_at = ?, last_seen_at = ? WHERE tenant_id = ? AND alert_id = ?",
                    (now, now, self.tenant_id, int(existing["alert_id"] or 0)),
                )
                resolved.append({
                    "alert_id": int(existing["alert_id"] or 0),
                    "code": clean_text(existing["code"]),
                    "severity": clean_text(existing["severity"]),
                    "title": clean_text(existing["title"]),
                    "message": clean_text(existing["message"]),
                    "status": "resolved",
                    "resolved_at": now,
                })
        return {"active": self.list_health_alerts(status="active", limit=100), "opened": opened, "resolved": resolved}

    def acknowledge_health_alert(self, alert_id: int, *, actor: str = "") -> bool:
        now = utc_now_iso()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT alert_id FROM health_alerts WHERE tenant_id = ? AND alert_id = ? AND status IN ('open', 'acknowledged')",
                (self.tenant_id, int(alert_id)),
            ).fetchone()
            if not row:
                return False
            conn.execute(
                "UPDATE health_alerts SET status = 'acknowledged', acknowledged_at = ?, acknowledged_by = ?, last_seen_at = ? WHERE tenant_id = ? AND alert_id = ?",
                (now, clean_text(actor), now, self.tenant_id, int(alert_id)),
            )
        return True

    def list_health_alerts(self, status: str = "active", limit: int = 100) -> List[Dict[str, Any]]:
        safe_status = clean_text(status) or "active"
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if safe_status == "active":
            where.append("status IN ('open', 'acknowledged')")
        elif safe_status != "all":
            where.append("status = ?")
            params.append(safe_status)
        params.append(max(1, int(limit or 100)))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM health_alerts WHERE {' AND '.join(where)} ORDER BY COALESCE(last_seen_at, first_seen_at) DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_health_alert(row) for row in rows]

    def list_recent_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        from .audit import list_recent_runs

        return list_recent_runs(self, limit=limit)

    def load_run_summary(self, run_id: str) -> Dict[str, Any]:
        from .audit import load_run_summary

        return load_run_summary(self, run_id)

    def load_run_events(self, run_id: str, limit: int = 300) -> List[Dict[str, Any]]:
        from .audit import load_run_events

        return load_run_events(self, run_id, limit=limit)

    def load_channel_events(self, channel: str, limit: int = 200) -> List[Dict[str, Any]]:
        from .audit import load_channel_events

        return load_channel_events(self, channel, limit=limit)

    def list_available_channels(self) -> List[str]:
        defaults = ["sync", "ai", "decision", "send", "browser", "webhook", "ui", "ops", "security", "knowledge", "media", "health"]
        found = {path.stem for path in self.paths.channel_logs_dir.glob("*.jsonl")}
        return list(dict.fromkeys(defaults + sorted(found)))

    def store_webhook_event(
        self,
        event_id: str,
        payload: Dict[str, Any],
        *,
        dedupe_key: str = "",
        source_kind: str = "webhook",
        verified_by: str = "",
        signature: str = "",
        nonce: str = "",
        status: str = "received",
    ) -> Dict[str, Any]:
        event_id = clean_text(event_id)
        dedupe_key = clean_text(dedupe_key) or event_id
        now = utc_now_iso()
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT event_id, dedupe_key, status FROM webhook_events WHERE tenant_id = ? AND (event_id = ? OR dedupe_key = ?)",
                (self.tenant_id, event_id, dedupe_key),
            ).fetchone()
            if existing:
                return {
                    "stored": False,
                    "duplicate": True,
                    "event_id": clean_text(existing["event_id"]),
                    "dedupe_key": clean_text(existing["dedupe_key"]),
                    "status": clean_text(existing["status"]),
                }
            try:
                conn.execute(
                    """
                    INSERT INTO webhook_events (
                        tenant_id, event_id, dedupe_key, source_kind, received_at,
                        processed_at, verified_by, signature, nonce, status,
                        attempts, last_attempt_at, last_error, payload_json
                    ) VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 0, ?, '', ?)
                    """,
                    (
                        self.tenant_id,
                        event_id,
                        dedupe_key,
                        clean_text(source_kind) or "webhook",
                        now,
                        clean_text(verified_by),
                        clean_text(signature),
                        clean_text(nonce),
                        clean_text(status) or "received",
                        now,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
            except sqlite3.IntegrityError:
                existing = conn.execute(
                    "SELECT event_id, dedupe_key, status FROM webhook_events WHERE tenant_id = ? AND (event_id = ? OR dedupe_key = ?)",
                    (self.tenant_id, event_id, dedupe_key),
                ).fetchone()
                return {
                    "stored": False,
                    "duplicate": True,
                    "event_id": clean_text((existing or {"event_id": event_id})["event_id"]),
                    "dedupe_key": clean_text((existing or {"dedupe_key": dedupe_key})["dedupe_key"]),
                    "status": clean_text((existing or {"status": status})["status"]),
                }
        return {"stored": True, "duplicate": False, "event_id": event_id, "dedupe_key": dedupe_key, "status": status}

    def mark_webhook_event(
        self,
        event_id: str,
        *,
        status: str,
        error_text: str = "",
        processed: bool = False,
        increment_attempt: bool = True,
    ) -> None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT attempts FROM webhook_events WHERE tenant_id = ? AND event_id = ?",
                (self.tenant_id, clean_text(event_id)),
            ).fetchone()
            attempts = int(row[0] or 0) if row else 0
            if increment_attempt:
                attempts += 1
            conn.execute(
                """
                UPDATE webhook_events
                SET status = ?, attempts = ?, last_attempt_at = ?, last_error = ?, processed_at = CASE WHEN ? THEN ? ELSE processed_at END
                WHERE tenant_id = ? AND event_id = ?
                """,
                (
                    clean_text(status),
                    attempts,
                    utc_now_iso(),
                    clean_text(error_text),
                    1 if processed else 0,
                    utc_now_iso(),
                    self.tenant_id,
                    clean_text(event_id),
                ),
            )

    def get_webhook_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM webhook_events WHERE tenant_id = ? AND event_id = ?",
                (self.tenant_id, clean_text(event_id)),
            ).fetchone()
        if not row:
            return None
        return self._row_to_webhook_event(row)

    def list_webhook_events(self, status: str = "all", limit: int = 100) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if status != "all":
            where.append("status = ?")
            params.append(status)
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM webhook_events WHERE {' AND '.join(where)} ORDER BY received_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_webhook_event(row) for row in rows]

    def remember_nonce(self, nonce: str, *, ttl_seconds: int = 900) -> bool:
        nonce = clean_text(nonce)
        if not nonce:
            return True
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(1, ttl_seconds))).isoformat()
        with self.connect() as conn:
            conn.execute("DELETE FROM webhook_nonces WHERE tenant_id = ? AND seen_at < ?", (self.tenant_id, cutoff))
            existing = conn.execute(
                "SELECT nonce FROM webhook_nonces WHERE tenant_id = ? AND nonce = ?",
                (self.tenant_id, nonce),
            ).fetchone()
            if existing:
                return False
            conn.execute(
                "INSERT INTO webhook_nonces (tenant_id, nonce, seen_at) VALUES (?, ?, ?)",
                (self.tenant_id, nonce, utc_now_iso()),
            )
        return True

    def create_dead_letter(
        self,
        *,
        source_kind: str,
        payload: Dict[str, Any],
        error_text: str,
        dedupe_key: str = "",
        event_id: str = "",
        last_run_id: str = "",
    ) -> int:
        now = utc_now_iso()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO dead_letters (
                    tenant_id, source_kind, dedupe_key, event_id, payload_json, error_text,
                    status, attempts, created_at, updated_at, resolved_at, last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, 'open', 0, ?, ?, '', ?)
                """,
                (
                    self.tenant_id,
                    clean_text(source_kind),
                    clean_text(dedupe_key),
                    clean_text(event_id),
                    json.dumps(payload, ensure_ascii=False),
                    clean_text(error_text),
                    now,
                    now,
                    clean_text(last_run_id),
                ),
            )
            return int(cur.lastrowid or 0)

    def get_dead_letter(self, dlq_id: int) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM dead_letters WHERE tenant_id = ? AND dlq_id = ?",
                (self.tenant_id, int(dlq_id)),
            ).fetchone()
        return self._row_to_dead_letter(row) if row else None

    def list_dead_letters(self, status: str = "open", limit: int = 100) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if status != "all":
            where.append("status = ?")
            params.append(status)
        params.append(max(1, limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM dead_letters WHERE {' AND '.join(where)} ORDER BY updated_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_dead_letter(row) for row in rows]

    def mark_dead_letter(self, dlq_id: int, *, status: str, error_text: str = "", last_run_id: str = "", increment_attempt: bool = False) -> None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT attempts FROM dead_letters WHERE tenant_id = ? AND dlq_id = ?",
                (self.tenant_id, int(dlq_id)),
            ).fetchone()
            attempts = int(row[0] or 0) if row else 0
            if increment_attempt:
                attempts += 1
            resolved_at = utc_now_iso() if clean_text(status) in {"resolved", "discarded"} else ""
            conn.execute(
                """
                UPDATE dead_letters
                SET status = ?, attempts = ?, error_text = ?, updated_at = ?, resolved_at = CASE WHEN ? != '' THEN ? ELSE resolved_at END, last_run_id = ?
                WHERE tenant_id = ? AND dlq_id = ?
                """,
                (
                    clean_text(status),
                    attempts,
                    clean_text(error_text),
                    utc_now_iso(),
                    resolved_at,
                    resolved_at,
                    clean_text(last_run_id),
                    self.tenant_id,
                    int(dlq_id),
                ),
            )

    def chat_needs_reply(self, chat_id: str) -> bool:
        with self.connect() as conn:
            incoming = conn.execute(
                "SELECT message_id, COALESCE(message_ts_epoch, 0) AS ts_epoch FROM messages WHERE tenant_id = ? AND chat_id = ? AND direction = 'in' ORDER BY ts_epoch DESC, message_id DESC LIMIT 1",
                (self.tenant_id, clean_text(chat_id)),
            ).fetchone()
            if not incoming:
                return False
            outgoing = conn.execute(
                "SELECT message_id FROM messages WHERE tenant_id = ? AND chat_id = ? AND direction = 'out' AND COALESCE(message_ts_epoch, 0) >= ? ORDER BY COALESCE(message_ts_epoch, 0) DESC LIMIT 1",
                (self.tenant_id, clean_text(chat_id), int(incoming[1] or 0)),
            ).fetchone()
        return outgoing is None

    def unanswered_chats(self, limit: int = 50, *, sticky_states: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        chats = self.list_chats(status="all", only_unanswered=True, limit=limit, offset=0)
        source_states = ["ready", "review", "hold", "rejected", "sent", "partial_sent_text_only", "sending"] if sticky_states is None else list(sticky_states)
        sticky = {clean_text(state) for state in source_states if clean_text(state)}
        result: List[Dict[str, Any]] = []
        for chat in chats:
            draft = self.get_draft(chat["chat_id"])
            if draft and sticky and clean_text(draft.get("state")) in sticky:
                continue
            result.append(chat)
        return result

    def compute_metrics(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        incoming: List[datetime] = []
        first_response_samples: List[float] = []
        with self.connect() as conn:
            msg_rows = conn.execute(
                "SELECT * FROM messages WHERE tenant_id = ? ORDER BY chat_id, COALESCE(message_ts_epoch, 0), message_id",
                (self.tenant_id,),
            ).fetchall()
            for row in msg_rows:
                message = self._row_to_message(row)
                dt = _parse_dt(message.get("message_ts") or message.get("created_at"))
                if message.get("direction") == "in" and dt is not None:
                    incoming.append(dt)
            by_chat: Dict[str, List[Dict[str, Any]]] = {}
            for row in msg_rows:
                message = self._row_to_message(row)
                by_chat.setdefault(message["chat_id"], []).append(message)
            for messages in by_chat.values():
                first_in = None
                first_out = None
                for msg in messages:
                    dt = _parse_dt(msg.get("message_ts") or msg.get("created_at"))
                    if dt is None:
                        continue
                    if msg.get("direction") == "in" and first_in is None:
                        first_in = dt
                    if first_in is not None and msg.get("direction") == "out" and dt >= first_in:
                        first_out = dt
                        break
                if first_in is not None and first_out is not None:
                    first_response_samples.append(max(0.0, (first_out - first_in).total_seconds() / 60.0))

            draft_rows = conn.execute(
                "SELECT state, route FROM drafts WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchall()
            webhook_rows = conn.execute(
                "SELECT received_at, processed_at, status FROM webhook_events WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchall()
            knowledge_counts = conn.execute("SELECT COUNT(*), SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) FROM knowledge_docs WHERE tenant_id = ?", (self.tenant_id,)).fetchone()
            media_counts = conn.execute("SELECT COUNT(*), SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) FROM media_assets WHERE tenant_id = ?", (self.tenant_id,)).fetchone()
            chat_rows = conn.execute(
                "SELECT status, first_response_due_epoch, first_response_epoch FROM chats WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchall()
            send_rows = conn.execute(
                "SELECT delivery_status, created_at FROM send_events WHERE tenant_id = ?",
                (self.tenant_id,),
            ).fetchall()

        def count_window(minutes: int) -> int:
            cutoff = now - timedelta(minutes=minutes)
            return sum(1 for dt in incoming if dt >= cutoff)

        sent_total = 0
        partial_sent_total = 0
        sent_auto = 0
        escalation_total = 0
        draft_total = 0
        for row in draft_rows:
            state = clean_text(row[0])
            route = clean_text(row[1])
            if state in {"sent", "partial_sent_text_only"}:
                sent_total += 1
                if state == "partial_sent_text_only":
                    partial_sent_total += 1
                if route == "auto":
                    sent_auto += 1
            if route in {"manual", "escalate"}:
                escalation_total += 1
            draft_total += 1

        process_lags: List[float] = []
        webhook_recent = 0
        webhook_rejected_recent = 0
        cutoff_24h = now - timedelta(hours=24)
        for row in webhook_rows:
            received_at = _parse_dt(row[0])
            processed_at = _parse_dt(row[1])
            status = clean_text(row[2])
            if received_at and received_at >= cutoff_24h:
                webhook_recent += 1
                if status in {"rejected", "unverified"}:
                    webhook_rejected_recent += 1
            if received_at and processed_at:
                process_lags.append(max(0.0, (processed_at - received_at).total_seconds()))

        recent_runs = self.list_recent_runs(limit=100)
        sync_runs = [run for run in recent_runs if clean_text(run.get("kind")) == "avito_sync"]
        browser_fallback_runs = 0
        for run in sync_runs:
            summary = run.get("summary") or {}
            if isinstance(summary, dict) and summary.get("used_browser_fallback"):
                browser_fallback_runs += 1

        sync_events = self.load_channel_events("sync", limit=2000)
        security_events = self.load_channel_events("security", limit=2000)
        cutoff_60m = now - timedelta(minutes=60)
        token_refresh_events = 0
        circuit_open_events = 0
        for event in sync_events + security_events:
            ts = _parse_dt(event.get("ts"))
            if not ts or ts < cutoff_60m:
                continue
            stage = clean_text(event.get("stage"))
            if stage in {"oauth_ok", "oauth_refresh_success", "auth_retry"}:
                token_refresh_events += 1
            if stage in {"api_circuit_open", "api_circuit_half_open"}:
                circuit_open_events += 1

        send_errors_24h = 0
        for row in send_rows:
            created_at = _parse_dt(row[1])
            if not created_at or created_at < cutoff_24h:
                continue
            if clean_text(row[0]) in {"error", "failed"}:
                send_errors_24h += 1

        draft_state_counts = self.count_drafts_by_state()
        status_counts: Dict[str, int] = {"new": 0, "in_progress": 0, "waiting_customer": 0, "closed": 0, "escalation": 0}
        overdue_queue_count = 0
        answered_total = 0
        for row in chat_rows:
            status = clean_text(row[0]) or "new"
            status_counts[status] = int(status_counts.get(status, 0) or 0) + 1
            due_epoch = int(row[1] or 0)
            first_response_epoch = int(row[2] or 0)
            if due_epoch and not first_response_epoch and due_epoch <= int(now.timestamp()):
                overdue_queue_count += 1
            if first_response_epoch:
                answered_total += 1

        knowledge_hits_total = int((self.load_sync_state("counter::knowledge_hit_total", {"value": 0}) or {}).get("value") or 0)
        knowledge_miss_total = int((self.load_sync_state("counter::knowledge_miss_total", {"value": 0}) or {}).get("value") or 0)
        knowledge_decisions_total = knowledge_hits_total + knowledge_miss_total
        knowledge_hit_rate = round((knowledge_hits_total / knowledge_decisions_total) * 100.0, 2) if knowledge_decisions_total else 0.0

        return {
            "incoming_5m": count_window(5),
            "incoming_15m": count_window(15),
            "incoming_60m": count_window(60),
            "avg_first_response_minutes": round(sum(first_response_samples) / len(first_response_samples), 2) if first_response_samples else None,
            "auto_reply_share": round((sent_auto / sent_total) * 100.0, 2) if sent_total else 0.0,
            "escalation_share": round((escalation_total / draft_total) * 100.0, 2) if draft_total else 0.0,
            "token_refresh_events_60m": token_refresh_events,
            "circuit_open_events_60m": circuit_open_events,
            "webhook_verify_fail_pct_24h": round((webhook_rejected_recent / webhook_recent) * 100.0, 2) if webhook_recent else 0.0,
            "webhook_process_lag_seconds_avg": round(sum(process_lags) / len(process_lags), 2) if process_lags else None,
            "browser_fallback_share": round((browser_fallback_runs / len(sync_runs)) * 100.0, 2) if sync_runs else 0.0,
            "sync_runs_considered": len(sync_runs),
            "drafts_total": draft_total,
            "sent_drafts_total": sent_total,
            "partial_sent_total": partial_sent_total,
            "review_queue_count": int(draft_state_counts.get("review", 0) or 0),
            "hold_queue_count": int(draft_state_counts.get("hold", 0) or 0),
            "approved_queue_count": int(draft_state_counts.get("ready", 0) or 0),
            "error_queue_count": int(draft_state_counts.get("error", 0) or 0),
            "webhook_events_24h": webhook_recent,
            "knowledge_docs_total": int((knowledge_counts[0] if knowledge_counts else 0) or 0),
            "knowledge_docs_active": int((knowledge_counts[1] if knowledge_counts else 0) or 0),
            "media_assets_total": int((media_counts[0] if media_counts else 0) or 0),
            "media_assets_active": int((media_counts[1] if media_counts else 0) or 0),
            "knowledge_hit_rate": knowledge_hit_rate,
            "new_chats_count": int(status_counts.get("new", 0) or 0),
            "in_progress_chats_count": int(status_counts.get("in_progress", 0) or 0),
            "waiting_customer_chats_count": int(status_counts.get("waiting_customer", 0) or 0),
            "closed_chats_count": int(status_counts.get("closed", 0) or 0),
            "escalation_chats_count": int(status_counts.get("escalation", 0) or 0),
            "overdue_queue_count": overdue_queue_count,
            "answered_chats_count": answered_total,
            "send_errors_24h": send_errors_24h,
        }

    @staticmethod
    def _row_to_knowledge_doc(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "doc_id": int(row["doc_id"] or 0),
            "title": row["title"] or "",
            "kind": row["kind"] or "faq",
            "item_id": row["item_id"] or "",
            "item_title": row["item_title"] or "",
            "tags": json.loads(row["tags_json"] or "[]"),
            "source_name": row["source_name"] or "",
            "source_url": row["source_url"] or "",
            "body_text": row["body_text"] or "",
            "active": bool(row["active"]),
            "meta": json.loads(row["meta_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _row_to_media_asset(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "asset_id": int(row["asset_id"] or 0),
            "media_kind": row["media_kind"] or "image",
            "title": row["title"] or "",
            "caption": row["caption"] or "",
            "item_id": row["item_id"] or "",
            "item_title": row["item_title"] or "",
            "file_name": row["file_name"] or "",
            "local_path": row["local_path"] or "",
            "external_url": row["external_url"] or "",
            "mime_type": row["mime_type"] or "",
            "tags": json.loads(row["tags_json"] or "[]"),
            "active": bool(row["active"]),
            "meta": json.loads(row["meta_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _row_to_chat(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "chat_id": row["chat_id"],
            "client_name": row["client_name"],
            "title": row["title"],
            "item_id": row["item_id"],
            "item_title": row["item_title"],
            "unread_count": int(row["unread_count"] or 0),
            "last_message_text": row["last_message_text"] or "",
            "last_message_ts": row["last_message_ts"] or "",
            "last_message_ts_epoch": int(row["last_message_ts_epoch"] or 0) if "last_message_ts_epoch" in row.keys() else _to_epoch_seconds(row["last_message_ts"] or ""),
            "status": row["status"] or "new",
            "priority": row["priority"] or "normal",
            "assigned_to": row["assigned_to"] or "",
            "note": row["note"] or "",
            "tags": json.loads(row["tags_json"] or "[]"),
            "first_response_due_at": row["first_response_due_at"] if "first_response_due_at" in row.keys() else "",
            "first_response_due_epoch": int(row["first_response_due_epoch"] or 0) if "first_response_due_epoch" in row.keys() else 0,
            "first_response_at": row["first_response_at"] if "first_response_at" in row.keys() else "",
            "first_response_epoch": int(row["first_response_epoch"] or 0) if "first_response_epoch" in row.keys() else 0,
            "last_operator_user": row["last_operator_user"] if "last_operator_user" in row.keys() else "",
            "last_operator_action_at": row["last_operator_action_at"] if "last_operator_action_at" in row.keys() else "",
            "last_send_status": row["last_send_status"] if "last_send_status" in row.keys() else "",
            "last_send_detail": row["last_send_detail"] if "last_send_detail" in row.keys() else "",
            "raw": json.loads(row["raw_json"] or "{}"),
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "message_id": row["message_id"],
            "chat_id": row["chat_id"],
            "direction": row["direction"],
            "is_read": bool(row["is_read"]),
            "author_name": row["author_name"] or "",
            "message_ts": row["message_ts"] or "",
            "message_ts_epoch": int(row["message_ts_epoch"] or 0) if "message_ts_epoch" in row.keys() else _to_epoch_seconds(row["message_ts"] or row["created_at"] or ""),
            "text": row["text"] or "",
            "attachments": json.loads(row["attachments_json"] or "[]"),
            "raw": json.loads(row["raw_json"] or "{}"),
            "created_at": row["created_at"],
        }

    @staticmethod
    def _row_to_draft(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "chat_id": row["chat_id"],
            "state": row["state"],
            "body": row["body"],
            "confidence": float(row["confidence"] or 0),
            "route": row["route"] or "manual",
            "reason": row["reason"] or "",
            "source_message_ids": json.loads(row["source_message_ids_json"] or "[]"),
            "model_name": row["model_name"] or "",
            "meta": json.loads(row["meta_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "lease_id": row["lease_id"] if "lease_id" in row.keys() else "",
            "lease_until": row["lease_until"] if "lease_until" in row.keys() else "",
            "lease_acquired_at": row["lease_acquired_at"] if "lease_acquired_at" in row.keys() else "",
            "sent_at": row["sent_at"] if "sent_at" in row.keys() else "",
        }

    @staticmethod
    def _row_to_health_alert(row: sqlite3.Row) -> Dict[str, Any]:
        details = {}
        try:
            details = json.loads(row["details_json"] or "{}")
        except Exception:
            details = {}
        return {
            "tenant_id": row["tenant_id"],
            "alert_id": int(row["alert_id"] or 0),
            "code": row["code"] or "",
            "severity": row["severity"] or "warning",
            "title": row["title"] or "",
            "message": row["message"] or "",
            "details": details,
            "status": row["status"] or "open",
            "first_seen_at": row["first_seen_at"] or "",
            "last_seen_at": row["last_seen_at"] or "",
            "acknowledged_at": row["acknowledged_at"] or "",
            "acknowledged_by": row["acknowledged_by"] or "",
            "resolved_at": row["resolved_at"] or "",
            "occurrence_count": int(row["occurrence_count"] or 0),
        }

    @staticmethod
    def _row_to_webhook_event(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "event_id": row["event_id"],
            "dedupe_key": row["dedupe_key"] or "",
            "source_kind": row["source_kind"] or "webhook",
            "received_at": row["received_at"] or "",
            "processed_at": row["processed_at"] or "",
            "verified_by": row["verified_by"] or "",
            "signature": row["signature"] or "",
            "nonce": row["nonce"] or "",
            "status": row["status"] or "received",
            "attempts": int(row["attempts"] or 0),
            "last_attempt_at": row["last_attempt_at"] or "",
            "last_error": row["last_error"] or "",
            "payload": json.loads(row["payload_json"] or "{}"),
        }

    @staticmethod
    def _row_to_dead_letter(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "dlq_id": int(row["dlq_id"] or 0),
            "source_kind": row["source_kind"] or "",
            "dedupe_key": row["dedupe_key"] or "",
            "event_id": row["event_id"] or "",
            "payload": json.loads(row["payload_json"] or "{}"),
            "error_text": row["error_text"] or "",
            "status": row["status"] or "open",
            "attempts": int(row["attempts"] or 0),
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
            "resolved_at": row["resolved_at"] or "",
            "last_run_id": row["last_run_id"] or "",
        }
