from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

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
        conn = sqlite3.connect(self.paths.db_file)
        conn.row_factory = sqlite3.Row
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
                    status TEXT DEFAULT 'open',
                    priority TEXT DEFAULT 'normal',
                    assigned_to TEXT,
                    note TEXT,
                    tags_json TEXT DEFAULT '[]',
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
                    text TEXT,
                    attachments_json TEXT DEFAULT '[]',
                    raw_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (tenant_id, chat_id, message_ts);

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
                    PRIMARY KEY (tenant_id, chat_id)
                );

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
                """
            )
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
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (tenant_id, status, received_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_webhook_events_dedupe ON webhook_events (tenant_id, dedupe_key)")
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
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO chats (
                    tenant_id, chat_id, client_name, title, item_id, item_title,
                    unread_count, last_message_text, last_message_ts, status,
                    priority, assigned_to, note, tags_json, raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, chat_id) DO UPDATE SET
                    client_name=excluded.client_name,
                    title=excluded.title,
                    item_id=excluded.item_id,
                    item_title=excluded.item_title,
                    unread_count=excluded.unread_count,
                    last_message_text=excluded.last_message_text,
                    last_message_ts=excluded.last_message_ts,
                    status=COALESCE(NULLIF(excluded.status, ''), chats.status),
                    priority=COALESCE(NULLIF(excluded.priority, ''), chats.priority),
                    assigned_to=COALESCE(NULLIF(excluded.assigned_to, ''), chats.assigned_to),
                    note=COALESCE(NULLIF(excluded.note, ''), chats.note),
                    tags_json=CASE WHEN excluded.tags_json != '[]' THEN excluded.tags_json ELSE chats.tags_json END,
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
                    int(chat.get("unread_count") or 0),
                    clean_text(chat.get("last_message_text")),
                    clean_text(chat.get("last_message_ts")),
                    clean_text(chat.get("status") or "open"),
                    clean_text(chat.get("priority") or "normal"),
                    clean_text(chat.get("assigned_to")),
                    clean_text(chat.get("note")),
                    json.dumps(tags, ensure_ascii=False),
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
                created_at = clean_text(message.get("message_ts") or message.get("created") or message.get("created_at")) or now
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO messages (
                        tenant_id, message_id, chat_id, direction, is_read, author_name,
                        message_ts, text, attachments_json, raw_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.tenant_id,
                        message_id,
                        clean_text(chat_id),
                        clean_text(message.get("direction") or "unknown"),
                        1 if bool(message.get("is_read")) else 0,
                        clean_text(message.get("author_name")),
                        clean_text(message.get("message_ts") or message.get("created") or message.get("created_at")),
                        clean_text(message.get("text")),
                        json.dumps(message.get("attachments") or [], ensure_ascii=False),
                        json.dumps(message, ensure_ascii=False),
                        created_at,
                    ),
                )
                inserted += int(cur.rowcount or 0)
        return inserted

    def list_chats(self, status: str = "all", only_unanswered: bool = False, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        where = ["tenant_id = ?"]
        params: List[Any] = [self.tenant_id]
        if status != "all":
            where.append("status = ?")
            params.append(status)
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
                            AND COALESCE(m2.message_ts, '') >= COALESCE(m1.message_ts, '')
                      )
                )
                """
            )
        query = f"""
            SELECT * FROM chats
            WHERE {' AND '.join(where)}
            ORDER BY COALESCE(last_message_ts, '') DESC, updated_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([max(1, limit), max(0, offset)])
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_chat(row) for row in rows]

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
                ORDER BY COALESCE(message_ts, ''), message_id
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
                    source_message_ids_json, model_name, meta_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, chat_id) DO UPDATE SET
                    state=excluded.state,
                    body=excluded.body,
                    confidence=excluded.confidence,
                    route=excluded.route,
                    reason=excluded.reason,
                    source_message_ids_json=excluded.source_message_ids_json,
                    model_name=excluded.model_name,
                    meta_json=excluded.meta_json,
                    updated_at=excluded.updated_at
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
                "status": row["chat_status"] or "open",
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
                SET state = ?, body = ?, route = ?, meta_json = ?, updated_at = ?
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
        params: List[Any] = [self.tenant_id]
        where = ["tenant_id = ?", "state = 'ready'"]
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

    def mark_draft_sent(self, chat_id: str, remote_message_id: str = "") -> None:
        draft = self.get_draft(chat_id) or {}
        meta = dict(draft.get("meta") or {})
        if clean_text(remote_message_id):
            meta["remote_message_id"] = clean_text(remote_message_id)
        with self.connect() as conn:
            conn.execute(
                "UPDATE drafts SET state = 'sent', updated_at = ?, meta_json = ? WHERE tenant_id = ? AND chat_id = ?",
                (utc_now_iso(), json.dumps(meta, ensure_ascii=False), self.tenant_id, clean_text(chat_id)),
            )

    def mark_draft_error(self, chat_id: str, error_text: str) -> None:
        draft = self.get_draft(chat_id) or {}
        meta = dict(draft.get("meta") or {})
        meta["error"] = clean_text(error_text)
        with self.connect() as conn:
            conn.execute(
                "UPDATE drafts SET state = 'error', updated_at = ?, meta_json = ? WHERE tenant_id = ? AND chat_id = ?",
                (utc_now_iso(), json.dumps(meta, ensure_ascii=False), self.tenant_id, clean_text(chat_id)),
            )

    def update_chat_meta(self, chat_id: str, *, status: Optional[str] = None, note: Optional[str] = None, tags: Optional[Iterable[str]] = None, assigned_to: Optional[str] = None, priority: Optional[str] = None) -> None:
        fields: List[str] = []
        params: List[Any] = []
        if status is not None:
            fields.append("status = ?")
            params.append(clean_text(status) or "open")
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
            return
        fields.append("updated_at = ?")
        params.append(utc_now_iso())
        params.extend([self.tenant_id, clean_text(chat_id)])
        with self.connect() as conn:
            conn.execute(
                f"UPDATE chats SET {', '.join(fields)} WHERE tenant_id = ? AND chat_id = ?",
                params,
            )


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

    def increment_counter(self, key: str, amount: int = 1) -> int:
        payload = self.load_sync_state(f"counter::{clean_text(key)}", {"value": 0})
        current = int((payload or {}).get("value") or 0) + int(amount)
        self.save_sync_state(f"counter::{clean_text(key)}", {"value": current, "updated_at": utc_now_iso()})
        return current

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
        defaults = ["sync", "ai", "decision", "send", "browser", "webhook", "ui", "ops", "security", "knowledge", "media"]
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
                "SELECT message_id, COALESCE(message_ts, '') AS ts FROM messages WHERE tenant_id = ? AND chat_id = ? AND direction = 'in' ORDER BY ts DESC, message_id DESC LIMIT 1",
                (self.tenant_id, clean_text(chat_id)),
            ).fetchone()
            if not incoming:
                return False
            outgoing = conn.execute(
                "SELECT message_id FROM messages WHERE tenant_id = ? AND chat_id = ? AND direction = 'out' AND COALESCE(message_ts, '') >= ? ORDER BY COALESCE(message_ts, '') DESC LIMIT 1",
                (self.tenant_id, clean_text(chat_id), str(incoming[1] or "")),
            ).fetchone()
        return outgoing is None

    def unanswered_chats(self, limit: int = 50) -> List[Dict[str, Any]]:
        chats = self.list_chats(status="all", only_unanswered=True, limit=limit, offset=0)
        result: List[Dict[str, Any]] = []
        for chat in chats:
            draft = self.get_draft(chat["chat_id"])
            if draft and draft.get("state") == "ready":
                continue
            result.append(chat)
        return result

    def compute_metrics(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        incoming = []
        first_response_samples: List[float] = []
        with self.connect() as conn:
            msg_rows = conn.execute(
                "SELECT * FROM messages WHERE tenant_id = ? ORDER BY chat_id, COALESCE(message_ts, created_at), message_id",
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

        def count_window(minutes: int) -> int:
            cutoff = now - timedelta(minutes=minutes)
            return sum(1 for dt in incoming if dt >= cutoff)

        sent_total = 0
        sent_auto = 0
        escalation_total = 0
        draft_total = 0
        for row in draft_rows:
            state = clean_text(row[0])
            route = clean_text(row[1])
            if state == "sent":
                sent_total += 1
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

        draft_state_counts = self.count_drafts_by_state()
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
            "review_queue_count": int(draft_state_counts.get("review", 0) or 0),
            "hold_queue_count": int(draft_state_counts.get("hold", 0) or 0),
            "approved_queue_count": int(draft_state_counts.get("ready", 0) or 0),
            "error_queue_count": int(draft_state_counts.get("error", 0) or 0),
            "webhook_events_24h": webhook_recent,
            "knowledge_docs_total": int((knowledge_counts[0] if knowledge_counts else 0) or 0),
            "knowledge_docs_active": int((knowledge_counts[1] if knowledge_counts else 0) or 0),
            "media_assets_total": int((media_counts[0] if media_counts else 0) or 0),
            "media_assets_active": int((media_counts[1] if media_counts else 0) or 0),
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
            "status": row["status"] or "open",
            "priority": row["priority"] or "normal",
            "assigned_to": row["assigned_to"] or "",
            "note": row["note"] or "",
            "tags": json.loads(row["tags_json"] or "[]"),
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
