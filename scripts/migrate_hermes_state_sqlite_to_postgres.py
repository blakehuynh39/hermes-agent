#!/usr/bin/env python3
"""One-time SQLite state.db to Postgres migration for Hermes sessions."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_constants import get_hermes_home
from hermes_state_postgres import PostgresSessionDB


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _load_sessions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM sessions ORDER BY started_at, id").fetchall()
    by_id = {row["id"]: _row_dict(row) for row in rows}
    ordered: list[dict[str, Any]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(session_id: str) -> None:
        if session_id in visited:
            return
        if session_id in visiting:
            ordered.append(by_id[session_id])
            visited.add(session_id)
            return
        visiting.add(session_id)
        parent_id = by_id[session_id].get("parent_session_id")
        if parent_id in by_id:
            visit(parent_id)
        visiting.discard(session_id)
        if session_id not in visited:
            ordered.append(by_id[session_id])
            visited.add(session_id)

    for sid in by_id:
        visit(sid)
    return ordered


def _load_messages(conn: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, id",
        (session_id,),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def migrate(args: argparse.Namespace) -> int:
    sqlite_path = Path(args.sqlite_path).expanduser()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite state DB not found: {sqlite_path}")

    if args.postgres_url:
        os.environ["HERMES_STATE_POSTGRES_URL"] = args.postgres_url
    if args.schema:
        os.environ["HERMES_STATE_POSTGRES_SCHEMA"] = args.schema
    if args.embedding_batch_size:
        os.environ["HERMES_STATE_EMBEDDING_BATCH_SIZE"] = str(args.embedding_batch_size)
    if args.disable_embeddings:
        os.environ["HERMES_STATE_EMBEDDINGS_ENABLED"] = "false"

    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    try:
        sessions = _load_sessions(conn)
        message_counts = {
            session["id"]: conn.execute(
                "SELECT COUNT(*) AS count FROM messages WHERE session_id = ?",
                (session["id"],),
            ).fetchone()["count"]
            for session in sessions
        }
        total_messages = sum(int(count) for count in message_counts.values())
        if args.dry_run:
            print(
                f"Would migrate {len(sessions)} sessions and {total_messages} messages "
                f"from {sqlite_path}"
            )
            return 0

        target = PostgresSessionDB()
        try:
            known_session_ids = {session["id"] for session in sessions}
            for index, session in enumerate(sessions, start=1):
                session = dict(session)
                if session.get("parent_session_id") not in known_session_ids:
                    session["parent_session_id"] = None
                messages = _load_messages(conn, session["id"])
                target.import_session_snapshot(session, messages)
                if args.verbose and (index % 25 == 0 or index == len(sessions)):
                    print(f"Migrated {index}/{len(sessions)} sessions")

            if args.backfill_embeddings and not args.disable_embeddings:
                while True:
                    synced = target.sync_pending_embeddings(limit=args.embedding_batch_size)
                    if synced <= 0:
                        break
                    if args.verbose:
                        print(f"Backfilled {synced} embeddings")
        finally:
            target.close()
    finally:
        conn.close()

    print(f"Migrated {len(sessions)} sessions and {total_messages} messages from {sqlite_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate Hermes SQLite state.db sessions/messages to Postgres."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(get_hermes_home() / "state.db"),
        help="Path to the source SQLite state.db. Defaults to $HERMES_HOME/state.db.",
    )
    parser.add_argument(
        "--postgres-url",
        default="",
        help="Target Postgres URL. Defaults to HERMES_STATE_POSTGRES_URL / RSI_POSTGRES_URL.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("HERMES_STATE_POSTGRES_SCHEMA", "hermes_state"),
        help="Target Postgres schema. Defaults to hermes_state.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows without writing to Postgres.",
    )
    parser.add_argument(
        "--disable-embeddings",
        action="store_true",
        help="Import sessions/messages without creating pending embedding rows.",
    )
    parser.add_argument(
        "--backfill-embeddings",
        action="store_true",
        help="Synchronously backfill pending embeddings after message import.",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=int(os.getenv("HERMES_STATE_EMBEDDING_BATCH_SIZE", "32")),
        help="Embedding reconciliation batch size.",
    )
    parser.add_argument("--verbose", action="store_true")
    return migrate(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
