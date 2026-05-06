#!/usr/bin/env python3
"""Postgres-backed Hermes session storage.

This module implements the same operational surface as ``hermes_state.SessionDB``
for deployments that need multiple Hermes processes to share one durable state
store. SQLite remains the default local backend; this backend is selected with
``HERMES_STATE_BACKEND=postgres``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, TypeVar

from agent.memory_manager import sanitize_context
from hermes_state import SQLiteSessionDB

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PostgresSessionDB:
    """Postgres-backed session storage with lexical/vector hybrid search."""

    _PG_CONTENT_JSON_PREFIX = "__hermes_json__:"
    _CONTENT_JSON_PREFIX = SQLiteSessionDB._CONTENT_JSON_PREFIX
    MAX_TITLE_LENGTH = SQLiteSessionDB.MAX_TITLE_LENGTH
    _RRF_K = 60.0

    def __init__(self, db_path: Path = None):
        del db_path  # Kept for SessionDB constructor compatibility.
        try:
            import psycopg
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                "HERMES_STATE_BACKEND=postgres requires psycopg[binary,pool]."
            ) from exc

        self._psycopg = psycopg
        self._dict_row = dict_row
        self._schema = self._resolve_schema()
        self._schema_sql = self._quote_ident(self._schema)
        self._search_mode = os.getenv("HERMES_STATE_SEARCH_MODE", "hybrid").strip().lower()
        if self._search_mode not in {"hybrid", "lexical", "vector"}:
            logger.warning(
                "Ignoring unsupported HERMES_STATE_SEARCH_MODE=%r; using hybrid",
                self._search_mode,
            )
            self._search_mode = "hybrid"
        self._embedding_model = os.getenv(
            "HERMES_STATE_EMBEDDING_MODEL",
            os.getenv("LLM_EMBEDDING_MODEL", "text-embedding-3-small"),
        )
        self._embedding_dimensions = int(os.getenv("HERMES_STATE_EMBEDDING_DIMENSIONS", "1536"))
        self._embedding_batch_size = max(
            1, min(int(os.getenv("HERMES_STATE_EMBEDDING_BATCH_SIZE", "32")), 128)
        )
        self._embeddings_enabled = self._resolve_bool(
            "HERMES_STATE_EMBEDDINGS_ENABLED",
            default=self._search_mode in {"hybrid", "vector"},
        )
        self._embedding_thread_lock = threading.Lock()
        self._embedding_thread: Optional[threading.Thread] = None
        self._embedding_client = None

        conninfo = self._normalize_conninfo(self._resolve_conninfo())
        pool_size = max(1, int(os.getenv("HERMES_STATE_POSTGRES_POOL_SIZE", "5")))
        self._pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=pool_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )
        self._init_schema()

        if self._resolve_bool("HERMES_STATE_EMBEDDING_RECONCILE_ON_START", default=False):
            self._kick_embedding_reconciler()

    # ------------------------------------------------------------------
    # Configuration / SQL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _normalize_conninfo(conninfo: str) -> str:
        if conninfo.startswith("postgresql+psycopg://"):
            return "postgresql://" + conninfo[len("postgresql+psycopg://"):]
        if conninfo.startswith("postgres+psycopg://"):
            return "postgresql://" + conninfo[len("postgres+psycopg://"):]
        return conninfo

    @staticmethod
    def _resolve_conninfo() -> str:
        conninfo = (
            os.getenv("HERMES_STATE_POSTGRES_URL")
            or os.getenv("RSI_POSTGRES_URL")
            or os.getenv("DATABASE_URL")
            or ""
        ).strip()
        if not conninfo:
            raise RuntimeError(
                "HERMES_STATE_BACKEND=postgres requires HERMES_STATE_POSTGRES_URL "
                "(or RSI_POSTGRES_URL / DATABASE_URL)."
            )
        return conninfo

    @staticmethod
    def _resolve_schema() -> str:
        schema = os.getenv("HERMES_STATE_POSTGRES_SCHEMA", "hermes_state").strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema):
            raise ValueError(f"Invalid HERMES_STATE_POSTGRES_SCHEMA={schema!r}")
        return schema

    @staticmethod
    def _quote_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _table(self, name: str) -> str:
        return f"{self._schema_sql}.{self._quote_ident(name)}"

    @staticmethod
    def _clean_text(value: Any) -> Any:
        if isinstance(value, str):
            return value.replace("\x00", "")
        return value

    @classmethod
    def _encode_content(cls, content: Any) -> Any:
        encoded = SQLiteSessionDB._encode_content(content)
        if isinstance(encoded, str) and encoded.startswith(cls._CONTENT_JSON_PREFIX):
            return cls._PG_CONTENT_JSON_PREFIX + encoded[len(cls._CONTENT_JSON_PREFIX):]
        return cls._clean_text(encoded)

    @classmethod
    def _decode_content(cls, content: Any) -> Any:
        if isinstance(content, str) and content.startswith(cls._PG_CONTENT_JSON_PREFIX):
            try:
                return json.loads(content[len(cls._PG_CONTENT_JSON_PREFIX):])
            except (json.JSONDecodeError, TypeError):
                logger.warning("Failed to decode Postgres JSON-encoded message content")
                return content
        return SQLiteSessionDB._decode_content(content)

    @staticmethod
    def sanitize_title(title: Optional[str]) -> Optional[str]:
        return SQLiteSessionDB.sanitize_title(title)

    def _init_schema(self) -> None:
        sessions = self._table("sessions")
        messages = self._table("messages")
        meta = self._table("state_meta")
        embeddings = self._table("message_embeddings")

        with self._pool.connection() as conn:
            with conn.transaction():
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_sql}")
                conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sessions} (
                        id TEXT PRIMARY KEY,
                        source TEXT NOT NULL,
                        user_id TEXT,
                        model TEXT,
                        model_config TEXT,
                        system_prompt TEXT,
                        parent_session_id TEXT REFERENCES {sessions}(id) DEFERRABLE INITIALLY DEFERRED,
                        started_at DOUBLE PRECISION NOT NULL,
                        ended_at DOUBLE PRECISION,
                        end_reason TEXT,
                        message_count INTEGER DEFAULT 0,
                        tool_call_count INTEGER DEFAULT 0,
                        input_tokens INTEGER DEFAULT 0,
                        output_tokens INTEGER DEFAULT 0,
                        cache_read_tokens INTEGER DEFAULT 0,
                        cache_write_tokens INTEGER DEFAULT 0,
                        reasoning_tokens INTEGER DEFAULT 0,
                        billing_provider TEXT,
                        billing_base_url TEXT,
                        billing_mode TEXT,
                        estimated_cost_usd DOUBLE PRECISION,
                        actual_cost_usd DOUBLE PRECISION,
                        cost_status TEXT,
                        cost_source TEXT,
                        pricing_version TEXT,
                        title TEXT,
                        api_call_count INTEGER DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {messages} (
                        id BIGSERIAL PRIMARY KEY,
                        session_id TEXT NOT NULL REFERENCES {sessions}(id) ON DELETE CASCADE,
                        role TEXT NOT NULL,
                        content TEXT,
                        tool_call_id TEXT,
                        tool_calls TEXT,
                        tool_name TEXT,
                        timestamp DOUBLE PRECISION NOT NULL,
                        token_count INTEGER,
                        finish_reason TEXT,
                        reasoning TEXT,
                        reasoning_content TEXT,
                        reasoning_details TEXT,
                        codex_reasoning_items TEXT,
                        codex_message_items TEXT
                    )
                    """
                )
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {meta} (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                    """
                )
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {embeddings} (
                        id BIGSERIAL PRIMARY KEY,
                        message_id BIGINT NOT NULL UNIQUE REFERENCES {messages}(id) ON DELETE CASCADE,
                        content TEXT NOT NULL,
                        embedding vector({self._embedding_dimensions}),
                        embedding_model TEXT,
                        sync_state TEXT NOT NULL DEFAULT 'pending',
                        sync_error TEXT,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_retry_at DOUBLE PRECISION,
                        last_sync_at DOUBLE PRECISION,
                        created_at DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
                        updated_at DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
                    )
                    """
                )
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hermes_sessions_source ON {sessions}(source)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hermes_sessions_parent ON {sessions}(parent_session_id)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hermes_sessions_started ON {sessions}(started_at DESC)")
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_hermes_sessions_title_unique "
                    f"ON {sessions}(title) WHERE title IS NOT NULL"
                )
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hermes_messages_session ON {messages}(session_id, timestamp, id)")
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_hermes_messages_fts_simple ON {messages} "
                    f"USING gin (to_tsvector('simple', "
                    f"COALESCE(content, '') || ' ' || COALESCE(tool_name, '') || ' ' || COALESCE(tool_calls, '')))"
                )
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_hermes_message_embeddings_state "
                    f"ON {embeddings}(sync_state, next_retry_at, updated_at)"
                )
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_hermes_message_embeddings_hnsw "
                    f"ON {embeddings} USING hnsw (embedding vector_cosine_ops) "
                    f"WHERE embedding IS NOT NULL"
                )

    def close(self) -> None:
        self._pool.close()

    def _execute_write(
        self,
        fn: Callable[[Any], T],
        *,
        lock_key: Optional[str] = None,
    ) -> T:
        with self._pool.connection() as conn:
            with conn.transaction():
                if lock_key:
                    conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (lock_key,),
                    )
                return fn(conn)

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _insert_session_row(
        self,
        session_id: str,
        source: str,
        model: str = None,
        model_config: Dict[str, Any] = None,
        system_prompt: str = None,
        user_id: str = None,
        parent_session_id: str = None,
    ) -> None:
        def _do(conn):
            conn.execute(
                f"""
                INSERT INTO {self._table("sessions")} (
                    id, source, user_id, model, model_config, system_prompt,
                    parent_session_id, started_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    session_id,
                    source,
                    user_id,
                    model,
                    json.dumps(model_config) if model_config else None,
                    system_prompt,
                    parent_session_id,
                    time.time(),
                ),
            )

        self._execute_write(_do, lock_key=session_id)

    def create_session(self, session_id: str, source: str, **kwargs) -> str:
        self._insert_session_row(session_id, source, **kwargs)
        return session_id

    def ensure_session(
        self,
        session_id: str,
        source: str = "unknown",
        model: str = None,
        **kwargs,
    ) -> str:
        self._insert_session_row(session_id, source, model=model, **kwargs)
        return session_id

    def end_session(self, session_id: str, end_reason: str) -> None:
        def _do(conn):
            conn.execute(
                f"UPDATE {self._table('sessions')} SET ended_at = %s, end_reason = %s "
                f"WHERE id = %s AND ended_at IS NULL",
                (time.time(), end_reason, session_id),
            )

        self._execute_write(_do, lock_key=session_id)

    def reopen_session(self, session_id: str) -> None:
        def _do(conn):
            conn.execute(
                f"UPDATE {self._table('sessions')} SET ended_at = NULL, end_reason = NULL WHERE id = %s",
                (session_id,),
            )

        self._execute_write(_do, lock_key=session_id)

    def update_system_prompt(self, session_id: str, system_prompt: str) -> None:
        def _do(conn):
            conn.execute(
                f"UPDATE {self._table('sessions')} SET system_prompt = %s WHERE id = %s",
                (system_prompt, session_id),
            )

        self._execute_write(_do, lock_key=session_id)

    def update_token_counts(
        self,
        session_id: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        model: str = None,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
        reasoning_tokens: int = 0,
        estimated_cost_usd: Optional[float] = None,
        actual_cost_usd: Optional[float] = None,
        cost_status: Optional[str] = None,
        cost_source: Optional[str] = None,
        pricing_version: Optional[str] = None,
        billing_provider: Optional[str] = None,
        billing_base_url: Optional[str] = None,
        billing_mode: Optional[str] = None,
        api_call_count: int = 0,
        absolute: bool = False,
    ) -> None:
        if absolute:
            sql = f"""UPDATE {self._table('sessions')} SET
                input_tokens = %s,
                output_tokens = %s,
                cache_read_tokens = %s,
                cache_write_tokens = %s,
                reasoning_tokens = %s,
                estimated_cost_usd = COALESCE(%s, 0),
                actual_cost_usd = CASE WHEN %s IS NULL THEN actual_cost_usd ELSE %s END,
                cost_status = COALESCE(%s, cost_status),
                cost_source = COALESCE(%s, cost_source),
                pricing_version = COALESCE(%s, pricing_version),
                billing_provider = COALESCE(billing_provider, %s),
                billing_base_url = COALESCE(billing_base_url, %s),
                billing_mode = COALESCE(billing_mode, %s),
                model = COALESCE(model, %s),
                api_call_count = %s
                WHERE id = %s"""
        else:
            sql = f"""UPDATE {self._table('sessions')} SET
                input_tokens = input_tokens + %s,
                output_tokens = output_tokens + %s,
                cache_read_tokens = cache_read_tokens + %s,
                cache_write_tokens = cache_write_tokens + %s,
                reasoning_tokens = reasoning_tokens + %s,
                estimated_cost_usd = COALESCE(estimated_cost_usd, 0) + COALESCE(%s, 0),
                actual_cost_usd = CASE WHEN %s IS NULL THEN actual_cost_usd ELSE COALESCE(actual_cost_usd, 0) + %s END,
                cost_status = COALESCE(%s, cost_status),
                cost_source = COALESCE(%s, cost_source),
                pricing_version = COALESCE(%s, pricing_version),
                billing_provider = COALESCE(billing_provider, %s),
                billing_base_url = COALESCE(billing_base_url, %s),
                billing_mode = COALESCE(billing_mode, %s),
                model = COALESCE(model, %s),
                api_call_count = COALESCE(api_call_count, 0) + %s
                WHERE id = %s"""
        params = (
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            reasoning_tokens,
            estimated_cost_usd,
            actual_cost_usd,
            actual_cost_usd,
            cost_status,
            cost_source,
            pricing_version,
            billing_provider,
            billing_base_url,
            billing_mode,
            model,
            api_call_count,
            session_id,
        )

        def _do(conn):
            conn.execute(sql, params)

        self._execute_write(_do, lock_key=session_id)

    def prune_empty_ghost_sessions(self, sessions_dir: "Optional[Path]" = None) -> int:
        cutoff = time.time() - 86400

        def _do(conn):
            rows = conn.execute(
                f"""
                DELETE FROM {self._table('sessions')} s
                WHERE s.source = 'tui'
                  AND s.title IS NULL
                  AND s.ended_at IS NOT NULL
                  AND s.started_at < %s
                  AND NOT EXISTS (
                      SELECT 1 FROM {self._table('messages')} m WHERE m.session_id = s.id
                  )
                RETURNING s.id
                """,
                (cutoff,),
            ).fetchall()
            return [r["id"] for r in rows]

        removed_ids = self._execute_write(_do) or []
        if sessions_dir and removed_ids:
            for sid in removed_ids:
                SQLiteSessionDB._remove_session_files(sessions_dir, sid)
        return len(removed_ids)

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT * FROM {self._table('sessions')} WHERE id = %s",
                (session_id,),
            ).fetchone()
        return dict(row) if row else None

    def resolve_session_id(self, session_id_or_prefix: str) -> Optional[str]:
        exact = self.get_session(session_id_or_prefix)
        if exact:
            return exact["id"]
        escaped = self._escape_like(session_id_or_prefix)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"SELECT id FROM {self._table('sessions')} "
                f"WHERE id LIKE %s ESCAPE '\\' ORDER BY started_at DESC LIMIT 2",
                (f"{escaped}%",),
            ).fetchall()
        matches = [r["id"] for r in rows]
        return matches[0] if len(matches) == 1 else None

    def set_session_title(self, session_id: str, title: str) -> bool:
        title = self.sanitize_title(title)

        def _do(conn):
            if title:
                conflict = conn.execute(
                    f"SELECT id FROM {self._table('sessions')} WHERE title = %s AND id != %s",
                    (title, session_id),
                ).fetchone()
                if conflict:
                    raise ValueError(
                        f"Title '{title}' is already in use by session {conflict['id']}"
                    )
            cur = conn.execute(
                f"UPDATE {self._table('sessions')} SET title = %s WHERE id = %s",
                (title, session_id),
            )
            return cur.rowcount

        return self._execute_write(_do, lock_key=session_id) > 0

    def get_session_title(self, session_id: str) -> Optional[str]:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT title FROM {self._table('sessions')} WHERE id = %s",
                (session_id,),
            ).fetchone()
        return row["title"] if row else None

    def get_session_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT * FROM {self._table('sessions')} WHERE title = %s",
                (title,),
            ).fetchone()
        return dict(row) if row else None

    def resolve_session_by_title(self, title: str) -> Optional[str]:
        exact = self.get_session_by_title(title)
        escaped = self._escape_like(title)
        with self._pool.connection() as conn:
            numbered = conn.execute(
                f"SELECT id, title, started_at FROM {self._table('sessions')} "
                f"WHERE title LIKE %s ESCAPE '\\' ORDER BY started_at DESC",
                (f"{escaped} #%",),
            ).fetchall()
        if numbered:
            return numbered[0]["id"]
        if exact:
            return exact["id"]
        return None

    def get_next_title_in_lineage(self, base_title: str) -> str:
        match = re.match(r"^(.*?) #(\d+)$", base_title)
        base = match.group(1) if match else base_title
        escaped = self._escape_like(base)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"SELECT title FROM {self._table('sessions')} "
                f"WHERE title = %s OR title LIKE %s ESCAPE '\\'",
                (base, f"{escaped} #%"),
            ).fetchall()
        existing = [r["title"] for r in rows]
        if not existing:
            return base
        max_num = 1
        for title in existing:
            m = re.match(r"^.* #(\d+)$", title or "")
            if m:
                max_num = max(max_num, int(m.group(1)))
        return f"{base} #{max_num + 1}"

    def get_compression_tip(self, session_id: str) -> Optional[str]:
        current = session_id
        for _ in range(100):
            with self._pool.connection() as conn:
                row = conn.execute(
                    f"""
                    SELECT id FROM {self._table('sessions')}
                    WHERE parent_session_id = %s
                      AND started_at >= (
                          SELECT ended_at FROM {self._table('sessions')}
                          WHERE id = %s AND end_reason = 'compression'
                      )
                    ORDER BY started_at DESC LIMIT 1
                    """,
                    (current, current),
                ).fetchone()
            if row is None:
                return current
            current = row["id"]
        return current

    # ------------------------------------------------------------------
    # Listing / export helpers
    # ------------------------------------------------------------------

    def list_sessions_rich(
        self,
        source: str = None,
        exclude_sources: List[str] = None,
        limit: int = 20,
        offset: int = 0,
        include_children: bool = False,
        project_compression_tips: bool = True,
        order_by_last_active: bool = False,
    ) -> List[Dict[str, Any]]:
        where = []
        params: list[Any] = []
        if not include_children:
            where.append(
                "(s.parent_session_id IS NULL OR EXISTS ("
                f"SELECT 1 FROM {self._table('sessions')} p "
                "WHERE p.id = s.parent_session_id "
                "AND p.end_reason = 'branched' "
                "AND s.started_at >= p.ended_at))"
            )
        if source:
            where.append("s.source = %s")
            params.append(source)
        if exclude_sources:
            where.append(f"s.source NOT IN ({','.join(['%s'] * len(exclude_sources))})")
            params.extend(exclude_sources)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order_sql = (
            "ORDER BY last_active DESC, s.started_at DESC, s.id DESC"
            if order_by_last_active
            else "ORDER BY s.started_at DESC"
        )
        query = f"""
            SELECT s.*,
                COALESCE((
                    SELECT SUBSTR(REPLACE(REPLACE(m.content, CHR(10), ' '), CHR(13), ' '), 1, 63)
                    FROM {self._table('messages')} m
                    WHERE m.session_id = s.id AND m.role = 'user' AND m.content IS NOT NULL
                    ORDER BY m.timestamp, m.id LIMIT 1
                ), '') AS _preview_raw,
                COALESCE((
                    SELECT MAX(m2.timestamp) FROM {self._table('messages')} m2
                    WHERE m2.session_id = s.id
                ), s.started_at) AS last_active
            FROM {self._table('sessions')} s
            {where_sql}
            {order_sql}
            LIMIT %s OFFSET %s
        """
        with self._pool.connection() as conn:
            rows = conn.execute(query, tuple(params + [limit, offset])).fetchall()
        sessions = [self._row_with_preview(dict(row)) for row in rows]
        if project_compression_tips and not include_children:
            projected = []
            for session in sessions:
                if session.get("end_reason") != "compression":
                    projected.append(session)
                    continue
                tip_id = self.get_compression_tip(session["id"])
                if tip_id == session["id"]:
                    projected.append(session)
                    continue
                tip_row = self._get_session_rich_row(tip_id)
                if not tip_row:
                    projected.append(session)
                    continue
                merged = dict(session)
                for key in (
                    "id", "ended_at", "end_reason", "message_count",
                    "tool_call_count", "title", "last_active", "preview",
                    "model", "system_prompt",
                ):
                    if key in tip_row:
                        merged[key] = tip_row[key]
                merged["_lineage_root_id"] = session["id"]
                projected.append(merged)
            sessions = projected
        return sessions

    def _row_with_preview(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raw = (row.pop("_preview_raw", "") or "").strip()
        if raw:
            decoded = self._decode_content(raw)
            if not isinstance(decoded, str):
                decoded = ""
            text = decoded[:60]
            row["preview"] = text + ("..." if len(decoded) > 60 else "")
        else:
            row["preview"] = ""
        row.pop("_effective_last_active", None)
        return row

    def _get_session_rich_row(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT s.*,
                    COALESCE((
                        SELECT SUBSTR(REPLACE(REPLACE(m.content, CHR(10), ' '), CHR(13), ' '), 1, 63)
                        FROM {self._table('messages')} m
                        WHERE m.session_id = s.id AND m.role = 'user' AND m.content IS NOT NULL
                        ORDER BY m.timestamp, m.id LIMIT 1
                    ), '') AS _preview_raw,
                    COALESCE((
                        SELECT MAX(m2.timestamp) FROM {self._table('messages')} m2
                        WHERE m2.session_id = s.id
                    ), s.started_at) AS last_active
                FROM {self._table('sessions')} s
                WHERE s.id = %s
                """,
                (session_id,),
            ).fetchone()
        return self._row_with_preview(dict(row)) if row else None

    def search_sessions(
        self,
        source: str = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        where = "WHERE s.source = %s" if source else ""
        params: tuple[Any, ...] = (source, limit, offset) if source else (limit, offset)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT s.*, COALESCE(m.last_active, s.started_at) AS last_active
                FROM {self._table('sessions')} s
                LEFT JOIN (
                    SELECT session_id, MAX(timestamp) AS last_active
                    FROM {self._table('messages')} GROUP BY session_id
                ) m ON m.session_id = s.id
                {where}
                ORDER BY last_active DESC, s.started_at DESC, s.id DESC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def session_count(self, source: str = None) -> int:
        with self._pool.connection() as conn:
            if source:
                row = conn.execute(
                    f"SELECT COUNT(*) AS count FROM {self._table('sessions')} WHERE source = %s",
                    (source,),
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT COUNT(*) AS count FROM {self._table('sessions')}"
                ).fetchone()
        return int(row["count"])

    def message_count(self, session_id: str = None) -> int:
        with self._pool.connection() as conn:
            if session_id:
                row = conn.execute(
                    f"SELECT COUNT(*) AS count FROM {self._table('messages')} WHERE session_id = %s",
                    (session_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT COUNT(*) AS count FROM {self._table('messages')}"
                ).fetchone()
        return int(row["count"])

    def export_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = self.get_session(session_id)
        if not session:
            return None
        return {**session, "messages": self.get_messages(session_id)}

    def export_all(self, source: str = None) -> List[Dict[str, Any]]:
        return [
            {**session, "messages": self.get_messages(session["id"])}
            for session in self.search_sessions(source=source, limit=100000)
        ]

    # ------------------------------------------------------------------
    # Message storage
    # ------------------------------------------------------------------

    def append_message(
        self,
        session_id: str,
        role: str,
        content: str = None,
        tool_name: str = None,
        tool_calls: Any = None,
        tool_call_id: str = None,
        token_count: int = None,
        finish_reason: str = None,
        reasoning: str = None,
        reasoning_content: str = None,
        reasoning_details: Any = None,
        codex_reasoning_items: Any = None,
        codex_message_items: Any = None,
    ) -> int:
        reasoning_details_json = json.dumps(reasoning_details) if reasoning_details else None
        codex_items_json = json.dumps(codex_reasoning_items) if codex_reasoning_items else None
        codex_message_items_json = (
            json.dumps(codex_message_items) if codex_message_items else None
        )
        tool_calls_json = json.dumps(tool_calls) if tool_calls else None
        stored_content = self._encode_content(content)
        search_text = self._message_search_text(stored_content, tool_name, tool_calls_json)
        num_tool_calls = len(tool_calls) if isinstance(tool_calls, list) else (1 if tool_calls else 0)

        def _do(conn):
            row = conn.execute(
                f"""
                INSERT INTO {self._table('messages')} (
                    session_id, role, content, tool_call_id, tool_calls, tool_name,
                    timestamp, token_count, finish_reason, reasoning,
                    reasoning_content, reasoning_details, codex_reasoning_items,
                    codex_message_items
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    session_id,
                    role,
                    stored_content,
                    tool_call_id,
                    tool_calls_json,
                    tool_name,
                    time.time(),
                    token_count,
                    finish_reason,
                    reasoning,
                    reasoning_content,
                    reasoning_details_json,
                    codex_items_json,
                    codex_message_items_json,
                ),
            ).fetchone()
            msg_id = int(row["id"])
            if num_tool_calls > 0:
                conn.execute(
                    f"UPDATE {self._table('sessions')} "
                    f"SET message_count = message_count + 1, tool_call_count = tool_call_count + %s "
                    f"WHERE id = %s",
                    (num_tool_calls, session_id),
                )
            else:
                conn.execute(
                    f"UPDATE {self._table('sessions')} SET message_count = message_count + 1 WHERE id = %s",
                    (session_id,),
                )
            self._insert_pending_embedding(conn, msg_id, search_text)
            return msg_id

        msg_id = self._execute_write(_do, lock_key=session_id)
        self._kick_embedding_reconciler()
        return msg_id

    def replace_messages(self, session_id: str, messages: List[Dict[str, Any]]) -> None:
        def _do(conn):
            conn.execute(f"DELETE FROM {self._table('messages')} WHERE session_id = %s", (session_id,))
            conn.execute(
                f"UPDATE {self._table('sessions')} SET message_count = 0, tool_call_count = 0 WHERE id = %s",
                (session_id,),
            )
            now_ts = time.time()
            total_messages = 0
            total_tool_calls = 0
            for msg in messages:
                role = msg.get("role", "unknown")
                tool_calls = msg.get("tool_calls")
                tool_calls_json = json.dumps(tool_calls) if tool_calls else None
                reasoning_details = msg.get("reasoning_details") if role == "assistant" else None
                codex_reasoning_items = msg.get("codex_reasoning_items") if role == "assistant" else None
                codex_message_items = msg.get("codex_message_items") if role == "assistant" else None
                stored_content = self._encode_content(msg.get("content"))
                row = conn.execute(
                    f"""
                    INSERT INTO {self._table('messages')} (
                        session_id, role, content, tool_call_id, tool_calls, tool_name,
                        timestamp, token_count, finish_reason, reasoning,
                        reasoning_content, reasoning_details, codex_reasoning_items,
                        codex_message_items
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        session_id,
                        role,
                        stored_content,
                        msg.get("tool_call_id"),
                        tool_calls_json,
                        msg.get("tool_name"),
                        now_ts,
                        msg.get("token_count"),
                        msg.get("finish_reason"),
                        msg.get("reasoning") if role == "assistant" else None,
                        msg.get("reasoning_content") if role == "assistant" else None,
                        json.dumps(reasoning_details) if reasoning_details else None,
                        json.dumps(codex_reasoning_items) if codex_reasoning_items else None,
                        json.dumps(codex_message_items) if codex_message_items else None,
                    ),
                ).fetchone()
                self._insert_pending_embedding(
                    conn,
                    int(row["id"]),
                    self._message_search_text(stored_content, msg.get("tool_name"), tool_calls_json),
                )
                total_messages += 1
                if tool_calls is not None:
                    total_tool_calls += len(tool_calls) if isinstance(tool_calls, list) else 1
                now_ts += 1e-6
            conn.execute(
                f"UPDATE {self._table('sessions')} SET message_count = %s, tool_call_count = %s WHERE id = %s",
                (total_messages, total_tool_calls, session_id),
            )

        self._execute_write(_do, lock_key=session_id)
        self._kick_embedding_reconciler()

    def get_messages(self, session_id: str) -> List[Dict[str, Any]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM {self._table('messages')} WHERE session_id = %s ORDER BY timestamp, id",
                (session_id,),
            ).fetchall()
        result = []
        for row in rows:
            msg = dict(row)
            msg["content"] = self._decode_content(msg.get("content"))
            if msg.get("tool_calls"):
                try:
                    msg["tool_calls"] = json.loads(msg["tool_calls"])
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Failed to deserialize tool_calls in get_messages")
                    msg["tool_calls"] = []
            result.append(msg)
        return result

    def resolve_resume_session_id(self, session_id: str) -> str:
        if not session_id:
            return session_id
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT 1 FROM {self._table('messages')} WHERE session_id = %s LIMIT 1",
                (session_id,),
            ).fetchone()
            if row is not None:
                return session_id
            current = session_id
            seen = {current}
            for _ in range(32):
                child = conn.execute(
                    f"SELECT id FROM {self._table('sessions')} "
                    f"WHERE parent_session_id = %s ORDER BY started_at DESC, id DESC LIMIT 1",
                    (current,),
                ).fetchone()
                if child is None or not child["id"] or child["id"] in seen:
                    return session_id
                child_id = child["id"]
                seen.add(child_id)
                msg = conn.execute(
                    f"SELECT 1 FROM {self._table('messages')} WHERE session_id = %s LIMIT 1",
                    (child_id,),
                ).fetchone()
                if msg is not None:
                    return child_id
                current = child_id
        return session_id

    def get_messages_as_conversation(
        self, session_id: str, include_ancestors: bool = False
    ) -> List[Dict[str, Any]]:
        session_ids = self._session_lineage_root_to_tip(session_id) if include_ancestors else [session_id]
        if not session_ids:
            return []
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT role, content, tool_call_id, tool_calls, tool_name,
                       finish_reason, reasoning, reasoning_content, reasoning_details,
                       codex_reasoning_items, codex_message_items
                FROM {self._table('messages')}
                WHERE session_id = ANY(%s)
                ORDER BY timestamp, id
                """,
                (session_ids,),
            ).fetchall()
        messages = []
        for row in rows:
            content = self._decode_content(row["content"])
            if row["role"] in {"user", "assistant"} and isinstance(content, str):
                content = sanitize_context(content).strip()
            msg = {"role": row["role"], "content": content}
            if row.get("tool_call_id"):
                msg["tool_call_id"] = row["tool_call_id"]
            if row.get("tool_name"):
                msg["tool_name"] = row["tool_name"]
            if row.get("tool_calls"):
                try:
                    msg["tool_calls"] = json.loads(row["tool_calls"])
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Failed to deserialize tool_calls in conversation replay")
                    msg["tool_calls"] = []
            if row["role"] == "assistant":
                if row.get("finish_reason"):
                    msg["finish_reason"] = row["finish_reason"]
                if row.get("reasoning"):
                    msg["reasoning"] = row["reasoning"]
                if row.get("reasoning_content") is not None:
                    msg["reasoning_content"] = row["reasoning_content"]
                for field in ("reasoning_details", "codex_reasoning_items", "codex_message_items"):
                    if row.get(field):
                        try:
                            msg[field] = json.loads(row[field])
                        except (json.JSONDecodeError, TypeError):
                            logger.warning("Failed to deserialize %s", field)
                            msg[field] = None
            if include_ancestors and SQLiteSessionDB._is_duplicate_replayed_user_message(messages, msg):
                continue
            messages.append(msg)
        return messages

    def _session_lineage_root_to_tip(self, session_id: str) -> List[str]:
        if not session_id:
            return [session_id]
        chain = []
        current = session_id
        seen = set()
        with self._pool.connection() as conn:
            for _ in range(100):
                if not current or current in seen:
                    break
                seen.add(current)
                chain.append(current)
                row = conn.execute(
                    f"SELECT parent_session_id FROM {self._table('sessions')} WHERE id = %s",
                    (current,),
                ).fetchone()
                if row is None:
                    break
                current = row["parent_session_id"]
        return list(reversed(chain)) or [session_id]

    def clear_messages(self, session_id: str) -> None:
        def _do(conn):
            conn.execute(f"DELETE FROM {self._table('messages')} WHERE session_id = %s", (session_id,))
            conn.execute(
                f"UPDATE {self._table('sessions')} SET message_count = 0, tool_call_count = 0 WHERE id = %s",
                (session_id,),
            )

        self._execute_write(_do, lock_key=session_id)

    def delete_session(self, session_id: str, sessions_dir: Optional[Path] = None) -> bool:
        def _do(conn):
            exists = conn.execute(
                f"SELECT COUNT(*) AS count FROM {self._table('sessions')} WHERE id = %s",
                (session_id,),
            ).fetchone()
            if int(exists["count"]) == 0:
                return False
            conn.execute(
                f"UPDATE {self._table('sessions')} SET parent_session_id = NULL WHERE parent_session_id = %s",
                (session_id,),
            )
            conn.execute(f"DELETE FROM {self._table('sessions')} WHERE id = %s", (session_id,))
            return True

        deleted = self._execute_write(_do, lock_key=session_id)
        if deleted:
            SQLiteSessionDB._remove_session_files(sessions_dir, session_id)
        return deleted

    def prune_sessions(
        self,
        older_than_days: int = 90,
        source: str = None,
        sessions_dir: Optional[Path] = None,
    ) -> int:
        cutoff = time.time() - (older_than_days * 86400)
        removed_ids: list[str] = []

        def _do(conn):
            if source:
                rows = conn.execute(
                    f"SELECT id FROM {self._table('sessions')} "
                    f"WHERE started_at < %s AND ended_at IS NOT NULL AND source = %s",
                    (cutoff, source),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT id FROM {self._table('sessions')} "
                    f"WHERE started_at < %s AND ended_at IS NOT NULL",
                    (cutoff,),
                ).fetchall()
            ids = [r["id"] for r in rows]
            if not ids:
                return 0
            conn.execute(
                f"UPDATE {self._table('sessions')} SET parent_session_id = NULL "
                f"WHERE parent_session_id = ANY(%s)",
                (ids,),
            )
            conn.execute(f"DELETE FROM {self._table('sessions')} WHERE id = ANY(%s)", (ids,))
            removed_ids.extend(ids)
            return len(ids)

        count = self._execute_write(_do)
        for sid in removed_ids:
            SQLiteSessionDB._remove_session_files(sessions_dir, sid)
        return count

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    @staticmethod
    def _contains_cjk(text: str) -> bool:
        return SQLiteSessionDB._contains_cjk(text)

    @staticmethod
    def _sanitize_fts5_query(query: str) -> str:
        return SQLiteSessionDB._sanitize_fts5_query(query)

    @classmethod
    def _parse_compatible_query(cls, query: str) -> Dict[str, Any]:
        raw = query.strip()
        phrases = [m.group(1).strip() for m in re.finditer(r'"([^"]+)"', raw) if m.group(1).strip()]
        without_phrases = re.sub(r'"[^"]+"', " ", raw)
        tokens = [t for t in re.split(r"\s+", without_phrases.strip()) if t]
        positives: list[str] = []
        negatives: list[str] = []
        prefixes: list[str] = []
        has_or = False
        negate_next = False
        for token in tokens:
            upper = token.upper()
            if upper == "OR":
                has_or = True
                continue
            if upper == "AND":
                continue
            if upper == "NOT":
                negate_next = True
                continue
            cleaned = re.sub(r"^[^\w]+|[^\w*._-]+$", "", token)
            if not cleaned:
                continue
            if cleaned.endswith("*") and len(cleaned) > 1:
                cleaned = cleaned[:-1]
                prefixes.append(cleaned)
            if negate_next:
                negatives.append(cleaned)
                negate_next = False
            else:
                positives.append(cleaned)
        positives.extend(phrases)
        websearch = raw.replace("*", "")
        websearch = re.sub(r"\bNOT\b", "-", websearch, flags=re.IGNORECASE)
        return {
            "raw": raw,
            "websearch": websearch,
            "positive": [p for p in positives if p],
            "negative": [n for n in negatives if n],
            "prefix": prefixes,
            "has_or": has_or,
        }

    def search_messages(
        self,
        query: str,
        source_filter: List[str] = None,
        exclude_sources: List[str] = None,
        role_filter: List[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        if not query or not query.strip():
            return []
        query = self._sanitize_fts5_query(query)
        if not query:
            return []
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        fetch_limit = max(limit + offset, 50)
        compat = self._parse_compatible_query(query)

        lexical_rows: list[dict[str, Any]] = []
        vector_rows: list[dict[str, Any]] = []
        if self._search_mode in {"hybrid", "lexical"}:
            lexical_rows = self._search_lexical(
                compat, source_filter, exclude_sources, role_filter, fetch_limit
            )
        query_embedding = None
        if self._search_mode in {"hybrid", "vector"} and self._embeddings_enabled:
            query_embedding = self._embed_query(query)
        if query_embedding:
            vector_rows = self._search_vector(
                query_embedding, source_filter, exclude_sources, role_filter, fetch_limit
            )

        combined: dict[int, dict[str, Any]] = {}
        for rank, row in enumerate(lexical_rows, start=1):
            msg_id = int(row["id"])
            item = combined.setdefault(msg_id, dict(row))
            item["_score"] = item.get("_score", 0.0) + 1.0 / (self._RRF_K + rank)
            item["_lexical_rank"] = rank
        for rank, row in enumerate(vector_rows, start=1):
            msg_id = int(row["id"])
            item = combined.setdefault(msg_id, dict(row))
            item["_score"] = item.get("_score", 0.0) + 1.0 / (self._RRF_K + rank)
            item["_vector_rank"] = rank

        matches = sorted(
            combined.values(),
            key=lambda r: (r.get("_score", 0.0), r.get("timestamp") or 0.0),
            reverse=True,
        )[offset:offset + limit]

        for match in matches:
            if not match.get("snippet"):
                match["snippet"] = self._make_snippet(match.get("content"), compat)
            match["context"] = self._load_context(match["id"])
            match.pop("content", None)
            match.pop("_score", None)
            match.pop("_lexical_rank", None)
            match.pop("_vector_rank", None)
        return matches

    def _filter_sql(
        self,
        source_filter: Optional[Sequence[str]],
        exclude_sources: Optional[Sequence[str]],
        role_filter: Optional[Sequence[str]],
        params: list[Any],
    ) -> str:
        clauses = []
        if source_filter is not None:
            if not source_filter:
                clauses.append("FALSE")
            else:
                clauses.append(f"s.source = ANY(%s)")
                params.append(list(source_filter))
        if exclude_sources:
            clauses.append("NOT (s.source = ANY(%s))")
            params.append(list(exclude_sources))
        if role_filter:
            clauses.append("m.role = ANY(%s)")
            params.append(list(role_filter))
        return (" AND " + " AND ".join(clauses)) if clauses else ""

    def _base_match_select(self) -> str:
        return f"""
            SELECT
                m.id,
                m.session_id,
                m.role,
                m.content,
                m.timestamp,
                m.tool_name,
                s.source,
                s.model,
                s.started_at AS session_started
            FROM {{from_clause}}
            JOIN {self._table('sessions')} s ON s.id = m.session_id
        """

    def _search_lexical(
        self,
        compat: Dict[str, Any],
        source_filter: List[str] = None,
        exclude_sources: List[str] = None,
        role_filter: List[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        params: list[Any] = [compat["websearch"]]
        filter_sql = self._filter_sql(source_filter, exclude_sources, role_filter, params)
        text_expr = "COALESCE(m.content, '') || ' ' || COALESCE(m.tool_name, '') || ' ' || COALESCE(m.tool_calls, '')"
        sql = f"""
            WITH q AS (SELECT websearch_to_tsquery('simple', %s) AS tsq)
            SELECT
                m.id, m.session_id, m.role, m.content, m.timestamp, m.tool_name,
                s.source, s.model, s.started_at AS session_started,
                ts_rank_cd(to_tsvector('simple', {text_expr}), q.tsq) AS lexical_score
            FROM {self._table('messages')} m
            JOIN {self._table('sessions')} s ON s.id = m.session_id
            CROSS JOIN q
            WHERE q.tsq @@ to_tsvector('simple', {text_expr})
            {filter_sql}
            ORDER BY lexical_score DESC, m.timestamp DESC
            LIMIT %s
        """
        try:
            with self._pool.connection() as conn:
                rows = [dict(r) for r in conn.execute(sql, tuple(params + [limit])).fetchall()]
        except Exception as exc:
            logger.debug("Postgres lexical tsquery search failed; falling back to ILIKE: %s", exc)
            rows = []

        # Fallback also catches dotted/hyphenated terms, prefixes, CJK substrings,
        # and unsupported FTS5-ish syntax that websearch_to_tsquery drops.
        if rows and not self._contains_cjk(compat["raw"]):
            for row in rows:
                row["snippet"] = self._make_snippet(row.get("content"), compat)
            return rows
        fallback = self._search_like(compat, source_filter, exclude_sources, role_filter, limit)
        seen = {int(r["id"]) for r in rows}
        rows.extend(r for r in fallback if int(r["id"]) not in seen)
        for row in rows:
            row["snippet"] = self._make_snippet(row.get("content"), compat)
        return rows[:limit]

    def _search_like(
        self,
        compat: Dict[str, Any],
        source_filter: List[str] = None,
        exclude_sources: List[str] = None,
        role_filter: List[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        positives = compat["positive"] or [compat["raw"]]
        patterns = [f"%{self._escape_like(term)}%" for term in positives if term]
        prefix_patterns = [f"%{self._escape_like(term)}%" for term in compat["prefix"] if term]
        negative_patterns = [f"%{self._escape_like(term)}%" for term in compat["negative"] if term]
        if not patterns and not prefix_patterns:
            return []
        text_expr = "COALESCE(m.content, '') || ' ' || COALESCE(m.tool_name, '') || ' ' || COALESCE(m.tool_calls, '')"
        params: list[Any] = []
        positive_clauses = []
        all_patterns = patterns + prefix_patterns
        joiner = " OR " if compat["has_or"] else " AND "
        for pattern in all_patterns:
            positive_clauses.append(f"{text_expr} ILIKE %s ESCAPE '\\'")
            params.append(pattern)
        negative_clauses = []
        for pattern in negative_patterns:
            negative_clauses.append(f"{text_expr} NOT ILIKE %s ESCAPE '\\'")
            params.append(pattern)
        where = [f"({joiner.join(positive_clauses)})"]
        where.extend(negative_clauses)
        filter_sql = self._filter_sql(source_filter, exclude_sources, role_filter, params)
        sql = f"""
            SELECT
                m.id, m.session_id, m.role, m.content, m.timestamp, m.tool_name,
                s.source, s.model, s.started_at AS session_started
            FROM {self._table('messages')} m
            JOIN {self._table('sessions')} s ON s.id = m.session_id
            WHERE {' AND '.join(where)}
            {filter_sql}
            ORDER BY m.timestamp DESC
            LIMIT %s
        """
        with self._pool.connection() as conn:
            return [dict(r) for r in conn.execute(sql, tuple(params + [limit])).fetchall()]

    def _search_vector(
        self,
        embedding: Sequence[float],
        source_filter: List[str] = None,
        exclude_sources: List[str] = None,
        role_filter: List[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        vector = self._vector_literal(embedding)
        params: list[Any] = [vector]
        filter_sql = self._filter_sql(source_filter, exclude_sources, role_filter, params)
        sql = f"""
            SELECT
                m.id, m.session_id, m.role, m.content, m.timestamp, m.tool_name,
                s.source, s.model, s.started_at AS session_started,
                1 - (me.embedding <=> %s::vector) AS vector_score
            FROM {self._table('message_embeddings')} me
            JOIN {self._table('messages')} m ON m.id = me.message_id
            JOIN {self._table('sessions')} s ON s.id = m.session_id
            WHERE me.embedding IS NOT NULL
            {filter_sql}
            ORDER BY me.embedding <=> %s::vector
            LIMIT %s
        """
        with self._pool.connection() as conn:
            rows = conn.execute(sql, tuple(params + [vector, limit])).fetchall()
        return [dict(r) for r in rows]

    def _make_snippet(self, stored_content: Any, compat: Dict[str, Any]) -> str:
        decoded = self._decode_content(stored_content)
        if isinstance(decoded, list):
            text = " ".join(
                p.get("text", "") for p in decoded
                if isinstance(p, dict) and p.get("type") == "text"
            ).strip() or "[multimodal content]"
        elif decoded is None:
            text = ""
        else:
            text = str(decoded)
        terms = compat["positive"] or compat["prefix"] or [compat["raw"]]
        match = None
        for term in terms:
            if not term:
                continue
            match = re.search(re.escape(term), text, flags=re.IGNORECASE)
            if match:
                break
        if not match:
            return text[:160]
        start = max(0, match.start() - 60)
        end = min(len(text), match.end() + 80)
        snippet = text[start:match.start()] + ">>>" + text[match.start():match.end()] + "<<<" + text[match.end():end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet += "..."
        return snippet

    def _load_context(self, msg_id: int) -> List[Dict[str, str]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                WITH target AS (
                    SELECT session_id, timestamp, id
                    FROM {self._table('messages')}
                    WHERE id = %s
                )
                SELECT role, content
                FROM (
                    SELECT m.id, m.timestamp, m.role, m.content
                    FROM {self._table('messages')} m
                    JOIN target t ON t.session_id = m.session_id
                    WHERE (m.timestamp < t.timestamp)
                       OR (m.timestamp = t.timestamp AND m.id < t.id)
                    ORDER BY m.timestamp DESC, m.id DESC
                    LIMIT 1
                ) before_msg
                UNION ALL
                SELECT role, content
                FROM {self._table('messages')}
                WHERE id = %s
                UNION ALL
                SELECT role, content
                FROM (
                    SELECT m.id, m.timestamp, m.role, m.content
                    FROM {self._table('messages')} m
                    JOIN target t ON t.session_id = m.session_id
                    WHERE (m.timestamp > t.timestamp)
                       OR (m.timestamp = t.timestamp AND m.id > t.id)
                    ORDER BY m.timestamp ASC, m.id ASC
                    LIMIT 1
                ) after_msg
                """,
                (msg_id, msg_id),
            ).fetchall()
        context = []
        for row in rows:
            decoded = self._decode_content(row["content"])
            if isinstance(decoded, list):
                preview = " ".join(
                    p.get("text", "") for p in decoded
                    if isinstance(p, dict) and p.get("type") == "text"
                ).strip() or "[multimodal content]"
            elif isinstance(decoded, str):
                preview = decoded
            else:
                preview = ""
            context.append({"role": row["role"], "content": preview[:200]})
        return context

    # ------------------------------------------------------------------
    # Embedding reconciliation
    # ------------------------------------------------------------------

    def _message_search_text(self, stored_content: Any, tool_name: Any, tool_calls: Any) -> str:
        decoded = self._decode_content(stored_content)
        if isinstance(decoded, list):
            content = " ".join(
                p.get("text", "") for p in decoded
                if isinstance(p, dict) and p.get("type") == "text"
            )
        elif decoded is None:
            content = ""
        else:
            content = str(decoded)
        return " ".join(
            part for part in (content, str(tool_name or ""), str(tool_calls or "")) if part
        )[:16000]

    def _insert_pending_embedding(self, conn, message_id: int, content: str) -> None:
        if not self._embeddings_enabled or not content.strip():
            return
        now = time.time()
        conn.execute(
            f"""
            INSERT INTO {self._table('message_embeddings')} (
                message_id, content, sync_state, embedding_model, created_at, updated_at
            )
            VALUES (%s, %s, 'pending', %s, %s, %s)
            ON CONFLICT (message_id) DO UPDATE SET
                content = EXCLUDED.content,
                embedding = NULL,
                embedding_model = EXCLUDED.embedding_model,
                sync_state = 'pending',
                sync_error = NULL,
                next_retry_at = NULL,
                updated_at = EXCLUDED.updated_at
            """,
            (message_id, content, self._embedding_model, now, now),
        )

    def _kick_embedding_reconciler(self) -> None:
        if not self._embeddings_enabled:
            return
        with self._embedding_thread_lock:
            if self._embedding_thread and self._embedding_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._embedding_reconciler_once,
                name="hermes-state-pg-embedding-reconciler",
                daemon=True,
            )
            self._embedding_thread = thread
            thread.start()

    def _embedding_reconciler_once(self) -> None:
        try:
            self.sync_pending_embeddings(limit=self._embedding_batch_size)
        except Exception:
            logger.debug("Hermes Postgres embedding reconciliation failed", exc_info=True)

    def sync_pending_embeddings(self, limit: int = 100) -> int:
        if not self._embeddings_enabled:
            return 0
        rows = self._claim_pending_embeddings(limit)
        if not rows:
            return 0
        texts = [row["content"] for row in rows]
        try:
            vectors = self._embed_batch(texts)
        except Exception as exc:
            self._mark_embeddings_failed([row["id"] for row in rows], str(exc))
            return 0
        synced = 0
        for row, vector in zip(rows, vectors):
            if vector and len(vector) == self._embedding_dimensions:
                self._store_embedding(row["id"], vector)
                synced += 1
            else:
                self._mark_embeddings_failed([row["id"]], "embedding dimension mismatch")
        return synced

    def _claim_pending_embeddings(self, limit: int) -> List[Dict[str, Any]]:
        now = time.time()
        stale_syncing_cutoff = now - 900

        def _do(conn):
            rows = conn.execute(
                f"""
                SELECT id, content
                FROM {self._table('message_embeddings')}
                WHERE (
                    sync_state IN ('pending', 'failed')
                    OR (sync_state = 'syncing' AND updated_at < %s)
                )
                  AND (next_retry_at IS NULL OR next_retry_at <= %s)
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (stale_syncing_cutoff, now, limit),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                conn.execute(
                    f"""
                    UPDATE {self._table('message_embeddings')}
                    SET sync_state = 'syncing',
                        attempts = attempts + 1,
                        updated_at = %s
                    WHERE id = ANY(%s)
                    """,
                    (now, ids),
                )
            return [dict(row) for row in rows]

        return self._execute_write(_do)

    def _mark_embeddings_failed(self, ids: Sequence[int], error: str) -> None:
        if not ids:
            return
        now = time.time()
        retry_at = now + 300

        def _do(conn):
            conn.execute(
                f"""
                UPDATE {self._table('message_embeddings')}
                SET sync_state = 'failed',
                    sync_error = %s,
                    next_retry_at = %s,
                    updated_at = %s
                WHERE id = ANY(%s)
                """,
                (error[:1000], retry_at, now, list(ids)),
            )

        self._execute_write(_do)

    def _store_embedding(self, row_id: int, embedding: Sequence[float]) -> None:
        vector = self._vector_literal(embedding)
        now = time.time()

        def _do(conn):
            conn.execute(
                f"""
                UPDATE {self._table('message_embeddings')}
                SET embedding = %s::vector,
                    embedding_model = %s,
                    sync_state = 'synced',
                    sync_error = NULL,
                    next_retry_at = NULL,
                    last_sync_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (vector, self._embedding_model, now, now, row_id),
            )

        self._execute_write(_do)

    @staticmethod
    def _vector_literal(embedding: Sequence[float]) -> str:
        return "[" + ",".join(f"{float(v):.8g}" for v in embedding) + "]"

    def _get_embedding_client(self):
        if self._embedding_client is not None:
            return self._embedding_client
        api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured for Hermes state embeddings")
        from openai import OpenAI

        kwargs: dict[str, Any] = {"api_key": api_key}
        base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("LLM_OPENAI_BASE_URL")
        if base_url:
            kwargs["base_url"] = base_url
        self._embedding_client = OpenAI(**kwargs)
        return self._embedding_client

    def _embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        client = self._get_embedding_client()
        response = client.embeddings.create(model=self._embedding_model, input=list(texts))
        return [list(item.embedding) for item in response.data]

    def _embed_query(self, query: str) -> Optional[List[float]]:
        try:
            vectors = self._embed_batch([query])
        except Exception as exc:
            logger.debug("Hermes Postgres query embedding unavailable: %s", exc)
            return None
        if not vectors or len(vectors[0]) != self._embedding_dimensions:
            logger.debug("Hermes Postgres query embedding dimension mismatch")
            return None
        return vectors[0]

    # ------------------------------------------------------------------
    # Migration/import helpers
    # ------------------------------------------------------------------

    def import_session_snapshot(
        self,
        session: Dict[str, Any],
        messages: Iterable[Dict[str, Any]],
    ) -> None:
        session = dict(session)
        messages = [dict(m) for m in messages]

        def _do(conn):
            conn.execute(
                f"""
                INSERT INTO {self._table('sessions')} (
                    id, source, user_id, model, model_config, system_prompt,
                    parent_session_id, started_at, ended_at, end_reason,
                    message_count, tool_call_count, input_tokens, output_tokens,
                    cache_read_tokens, cache_write_tokens, reasoning_tokens,
                    billing_provider, billing_base_url, billing_mode,
                    estimated_cost_usd, actual_cost_usd, cost_status, cost_source,
                    pricing_version, title, api_call_count
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id) DO UPDATE SET
                    source = EXCLUDED.source,
                    user_id = EXCLUDED.user_id,
                    model = EXCLUDED.model,
                    model_config = EXCLUDED.model_config,
                    system_prompt = EXCLUDED.system_prompt,
                    parent_session_id = EXCLUDED.parent_session_id,
                    started_at = EXCLUDED.started_at,
                    ended_at = EXCLUDED.ended_at,
                    end_reason = EXCLUDED.end_reason,
                    message_count = EXCLUDED.message_count,
                    tool_call_count = EXCLUDED.tool_call_count,
                    input_tokens = EXCLUDED.input_tokens,
                    output_tokens = EXCLUDED.output_tokens,
                    cache_read_tokens = EXCLUDED.cache_read_tokens,
                    cache_write_tokens = EXCLUDED.cache_write_tokens,
                    reasoning_tokens = EXCLUDED.reasoning_tokens,
                    billing_provider = EXCLUDED.billing_provider,
                    billing_base_url = EXCLUDED.billing_base_url,
                    billing_mode = EXCLUDED.billing_mode,
                    estimated_cost_usd = EXCLUDED.estimated_cost_usd,
                    actual_cost_usd = EXCLUDED.actual_cost_usd,
                    cost_status = EXCLUDED.cost_status,
                    cost_source = EXCLUDED.cost_source,
                    pricing_version = EXCLUDED.pricing_version,
                    title = EXCLUDED.title,
                    api_call_count = EXCLUDED.api_call_count
                """,
                (
                    session.get("id"),
                    session.get("source") or "unknown",
                    session.get("user_id"),
                    session.get("model"),
                    session.get("model_config"),
                    session.get("system_prompt"),
                    session.get("parent_session_id"),
                    session.get("started_at") or time.time(),
                    session.get("ended_at"),
                    session.get("end_reason"),
                    session.get("message_count") or 0,
                    session.get("tool_call_count") or 0,
                    session.get("input_tokens") or 0,
                    session.get("output_tokens") or 0,
                    session.get("cache_read_tokens") or 0,
                    session.get("cache_write_tokens") or 0,
                    session.get("reasoning_tokens") or 0,
                    session.get("billing_provider"),
                    session.get("billing_base_url"),
                    session.get("billing_mode"),
                    session.get("estimated_cost_usd"),
                    session.get("actual_cost_usd"),
                    session.get("cost_status"),
                    session.get("cost_source"),
                    session.get("pricing_version"),
                    session.get("title"),
                    session.get("api_call_count") or 0,
                ),
            )
            conn.execute(
                f"DELETE FROM {self._table('messages')} WHERE session_id = %s",
                (session.get("id"),),
            )
            for msg in messages:
                stored_content = self._encode_content(SQLiteSessionDB._decode_content(msg.get("content")))
                row = conn.execute(
                    f"""
                    INSERT INTO {self._table('messages')} (
                        session_id, role, content, tool_call_id, tool_calls, tool_name,
                        timestamp, token_count, finish_reason, reasoning,
                        reasoning_content, reasoning_details, codex_reasoning_items,
                        codex_message_items
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        session.get("id"),
                        msg.get("role") or "unknown",
                        stored_content,
                        msg.get("tool_call_id"),
                        msg.get("tool_calls"),
                        msg.get("tool_name"),
                        msg.get("timestamp") or time.time(),
                        msg.get("token_count"),
                        msg.get("finish_reason"),
                        msg.get("reasoning"),
                        msg.get("reasoning_content"),
                        msg.get("reasoning_details"),
                        msg.get("codex_reasoning_items"),
                        msg.get("codex_message_items"),
                    ),
                ).fetchone()
                self._insert_pending_embedding(
                    conn,
                    int(row["id"]),
                    self._message_search_text(stored_content, msg.get("tool_name"), msg.get("tool_calls")),
                )

        self._execute_write(_do, lock_key=session.get("id"))
        self._kick_embedding_reconciler()

    # ------------------------------------------------------------------
    # Meta / maintenance
    # ------------------------------------------------------------------

    def get_meta(self, key: str) -> Optional[str]:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT value FROM {self._table('state_meta')} WHERE key = %s",
                (key,),
            ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        def _do(conn):
            conn.execute(
                f"""
                INSERT INTO {self._table('state_meta')} (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )

        self._execute_write(_do)

    def vacuum(self) -> None:
        with self._pool.connection() as conn:
            conn.execute(f"ANALYZE {self._table('sessions')}")
            conn.execute(f"ANALYZE {self._table('messages')}")
            conn.execute(f"ANALYZE {self._table('message_embeddings')}")

    def maybe_auto_prune_and_vacuum(
        self,
        retention_days: int = 90,
        min_interval_hours: int = 24,
        vacuum: bool = True,
        sessions_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"skipped": False, "pruned": 0, "vacuumed": False}
        try:
            last_raw = self.get_meta("last_auto_prune")
            now = time.time()
            if last_raw:
                try:
                    last_ts = float(last_raw)
                    if now - last_ts < min_interval_hours * 3600:
                        result["skipped"] = True
                        return result
                except (TypeError, ValueError):
                    pass

            pruned = self.prune_sessions(
                older_than_days=retention_days,
                sessions_dir=sessions_dir,
            )
            result["pruned"] = pruned
            if vacuum and pruned > 0:
                try:
                    self.vacuum()
                    result["vacuumed"] = True
                except Exception as exc:
                    logger.warning("Hermes Postgres state maintenance ANALYZE failed: %s", exc)
            self.set_meta("last_auto_prune", str(now))
        except Exception as exc:
            logger.warning("Hermes Postgres state maintenance failed: %s", exc)
            result["error"] = str(exc)
        return result
