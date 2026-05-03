"""Durable Hermes self-review queue.

The queue is additive to ``hermes_state.SessionDB``. It uses its own tables and
schema marker in ``state_meta`` so upstream SessionDB schema versioning stays
independent.
"""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from hermes_constants import get_hermes_home
from hermes_state import SessionDB
from self_review_contracts import SelfReviewObservationV1

logger = logging.getLogger(__name__)

REVIEW_SCHEMA_META_KEY = "self_review_schema_version"
REVIEW_SCHEMA_VERSION = "1"
RETRYABLE_PROMOTION_STATUSES = {"candidate", "validated", "pending_promote_retry"}
BLOCKING_WORK_STATUSES = {"pending", "running", "interrupted"}


def _now() -> float:
    return time.time()


def _string(value: Any) -> str:
    return str(value or "").strip()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _redact_error(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    for prefix in (str(get_hermes_home()), str(Path.home())):
        if prefix:
            text = text.replace(prefix, "<path>")
    return text[:800]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)
    try:
        flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
        dir_fd = os.open(str(path.parent), flags)
    except OSError:
        dir_fd = None
    if dir_fd is not None:
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    return _sha256_file(path), path.stat().st_size


@dataclass(frozen=True)
class SelfReviewConfig:
    hermes_home: Path
    state_db_path: Path
    snapshot_root: Path
    agent_identity: str
    executor_instance_id: str = ""
    pod_generation: str = ""
    stale_after_seconds: float = 900.0
    max_batch_rows: int = 4
    max_batch_tokens: int = 24000
    credential_profile: str = ""
    memory_backend: str = ""
    honcho_workspace: str = ""
    honcho_environment: str = ""
    model: str = ""
    provider: str = ""
    base_url: str = ""
    api_mode: str = ""

    @classmethod
    def from_env(cls, **overrides: Any) -> "SelfReviewConfig":
        hermes_home = Path(
            _string(overrides.get("hermes_home"))
            or _string(os.getenv("HERMES_HOME"))
            or str(get_hermes_home())
        ).expanduser()
        identity = (
            _string(overrides.get("agent_identity"))
            or _string(os.getenv("RSI_HERMES_SELF_REVIEW_IDENTITY"))
            or _string(os.getenv("RSI_HONCHO_AI_PEER"))
            or f"rsi:{_string(os.getenv('RSI_HONCHO_ENVIRONMENT')) or _string(os.getenv('RSI_ENV')) or 'unknown'}:company"
        )
        return cls(
            hermes_home=hermes_home,
            state_db_path=Path(
                _string(overrides.get("state_db_path"))
                or _string(os.getenv("HERMES_STATE_DB_PATH"))
                or str(hermes_home / "state.db")
            ).expanduser(),
            snapshot_root=Path(
                _string(overrides.get("snapshot_root"))
                or _string(os.getenv("RSI_HERMES_SELF_REVIEW_SNAPSHOT_ROOT"))
                or str(hermes_home / "self-review" / "snapshots")
            ).expanduser(),
            agent_identity=identity,
            executor_instance_id=_string(overrides.get("executor_instance_id")) or _string(os.getenv("RSI_HERMES_EXECUTOR_INSTANCE_ID")) or _string(os.getenv("HOSTNAME")),
            pod_generation=_string(overrides.get("pod_generation")) or _string(os.getenv("POD_UID")) or _string(os.getenv("HOSTNAME")),
            stale_after_seconds=float(overrides.get("stale_after_seconds") or os.getenv("RSI_HERMES_SELF_REVIEW_STALE_AFTER_SECONDS") or 900),
            max_batch_rows=int(overrides.get("max_batch_rows") or os.getenv("RSI_HERMES_SELF_REVIEW_MAX_BATCH_ROWS") or 4),
            max_batch_tokens=int(overrides.get("max_batch_tokens") or os.getenv("RSI_HERMES_SELF_REVIEW_MAX_BATCH_TOKENS") or 24000),
            credential_profile=_string(overrides.get("credential_profile")) or _string(os.getenv("RSI_HERMES_SELF_REVIEW_CREDENTIAL_PROFILE")),
            memory_backend=_string(overrides.get("memory_backend")) or _string(os.getenv("RSI_MEMORY_BACKEND")),
            honcho_workspace=_string(overrides.get("honcho_workspace")) or _string(os.getenv("RSI_HONCHO_WORKSPACE")),
            honcho_environment=_string(overrides.get("honcho_environment")) or _string(os.getenv("RSI_HONCHO_ENVIRONMENT")),
            model=_string(overrides.get("model")) or _string(os.getenv("RSI_MODEL")) or _string(os.getenv("HERMES_MODEL")),
            provider=_string(overrides.get("provider")) or _string(os.getenv("RSI_PROVIDER")) or "openrouter",
            base_url=_string(overrides.get("base_url")) or _string(os.getenv("OPENAI_BASE_URL")),
            api_mode=_string(overrides.get("api_mode")) or _string(os.getenv("RSI_HERMES_API_MODE")),
        )


def _db(config: SelfReviewConfig) -> SessionDB:
    db = SessionDB(db_path=config.state_db_path)
    ensure_review_schema(db)
    return db


def ensure_review_schema(db: SessionDB) -> None:
    def _do(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS self_review_candidates (
                candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL UNIQUE,
                agent_identity TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                candidate_owner_executor_instance_id TEXT,
                candidate_owner_generation TEXT,
                native_envelope_validated_at REAL,
                delivered_at REAL,
                result_hash TEXT,
                result_ref TEXT,
                memory_turn_delta INTEGER NOT NULL DEFAULT 0,
                skill_iteration_delta INTEGER NOT NULL DEFAULT 0,
                memory_nudge_interval INTEGER NOT NULL DEFAULT 0,
                skill_nudge_interval INTEGER NOT NULL DEFAULT 0,
                memory_eligible INTEGER NOT NULL DEFAULT 0,
                skill_eligible INTEGER NOT NULL DEFAULT 0,
                observation_json TEXT NOT NULL,
                memory_target_json TEXT NOT NULL DEFAULT '{}',
                skill_target_json TEXT NOT NULL DEFAULT '{}',
                snapshot_ref TEXT,
                snapshot_path TEXT,
                snapshot_hash TEXT,
                snapshot_size INTEGER NOT NULL DEFAULT 0,
                promotion_attempts INTEGER NOT NULL DEFAULT 0,
                promoted_at REAL,
                trigger_decision_json TEXT,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_self_review_candidates_identity_order
                ON self_review_candidates(agent_identity, created_at, candidate_id);
            CREATE INDEX IF NOT EXISTS idx_self_review_candidates_status
                ON self_review_candidates(status);

            CREATE TABLE IF NOT EXISTS self_review_counters (
                agent_identity TEXT PRIMARY KEY,
                memory_turns INTEGER NOT NULL DEFAULT 0,
                skill_iterations INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS self_review_work_items (
                work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL,
                execution_id TEXT NOT NULL,
                agent_identity TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                review_owner_executor_instance_id TEXT,
                review_owner_generation TEXT,
                lease_heartbeat_at REAL,
                last_progress_at REAL,
                batch_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                result_summary TEXT,
                last_error TEXT,
                UNIQUE(candidate_id, kind),
                FOREIGN KEY(candidate_id) REFERENCES self_review_candidates(candidate_id)
            );
            CREATE INDEX IF NOT EXISTS idx_self_review_work_status
                ON self_review_work_items(status, kind, agent_identity, created_at, work_id);
            CREATE INDEX IF NOT EXISTS idx_self_review_work_owner
                ON self_review_work_items(review_owner_executor_instance_id, review_owner_generation);
            """
        )
        conn.execute(
            "INSERT INTO state_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (REVIEW_SCHEMA_META_KEY, REVIEW_SCHEMA_VERSION),
        )

    db._execute_write(_do)


def apply_turn_review_candidate(
    config: SelfReviewConfig,
    observation: SelfReviewObservationV1 | Dict[str, Any],
) -> Dict[str, Any]:
    obs = observation if isinstance(observation, SelfReviewObservationV1) else SelfReviewObservationV1.from_dict(observation)
    if not obs.agent_identity:
        obs.agent_identity = config.agent_identity
    obs.validate()
    snapshot_ref = f"{obs.agent_identity.replace(':', '_')}/{obs.execution_id}.json"
    snapshot_path = (config.snapshot_root / snapshot_ref).resolve()
    snapshot_payload = {
        "schema": "hermes.self_review_snapshot.v1",
        "created_at": _now(),
        "observation": obs.to_dict(),
        "messages": obs.messages,
        "final_response": obs.final_response,
    }
    snapshot_hash, snapshot_size = _atomic_write_json(snapshot_path, snapshot_payload)
    created = _now()
    db = _db(config)

    def _do(conn: sqlite3.Connection) -> Dict[str, Any]:
        existing = conn.execute(
            "SELECT candidate_id, status, snapshot_ref, snapshot_hash, snapshot_size FROM self_review_candidates WHERE execution_id=?",
            (obs.execution_id,),
        ).fetchone()
        if existing:
            return {
                "candidate_id": existing["candidate_id"],
                "status": existing["status"],
                "snapshot_ref": existing["snapshot_ref"],
                "snapshot_hash": existing["snapshot_hash"],
                "snapshot_size": existing["snapshot_size"],
            }
        conn.execute(
            """
            INSERT INTO self_review_candidates(
                execution_id, agent_identity, status, created_at, updated_at,
                candidate_owner_executor_instance_id, candidate_owner_generation,
                memory_turn_delta, skill_iteration_delta, memory_nudge_interval,
                skill_nudge_interval, memory_eligible, skill_eligible,
                observation_json, memory_target_json, skill_target_json,
                snapshot_ref, snapshot_path, snapshot_hash, snapshot_size
            )
            VALUES (?, ?, 'candidate', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                obs.execution_id,
                obs.agent_identity,
                created,
                created,
                config.executor_instance_id,
                config.pod_generation,
                obs.memory_turn_delta,
                obs.skill_iteration_delta,
                obs.memory_nudge_interval,
                obs.skill_nudge_interval,
                1 if obs.memory_eligible else 0,
                1 if obs.skill_eligible else 0,
                _json_dumps(obs.to_dict()),
                _json_dumps(obs.memory_target),
                _json_dumps(obs.skill_target),
                snapshot_ref,
                str(snapshot_path),
                snapshot_hash,
                snapshot_size,
            ),
        )
        candidate_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        return {
            "candidate_id": candidate_id,
            "status": "candidate",
            "snapshot_ref": snapshot_ref,
            "snapshot_hash": snapshot_hash,
            "snapshot_size": snapshot_size,
        }

    result = db._execute_write(_do)
    db.close()
    return {
        **result,
        "execution_id": obs.execution_id,
        "agent_identity": obs.agent_identity,
        "candidate_status": result.get("status", "candidate"),
    }


def mark_candidate_delivered(
    config: SelfReviewConfig,
    execution_id: str,
    result_hash: str,
    result_ref: Optional[str] = None,
) -> Dict[str, Any]:
    if not _string(execution_id):
        raise ValueError("execution_id is required")
    if not _string(result_hash):
        raise ValueError("result_hash is required")
    db = _db(config)
    now = _now()

    def _do(conn: sqlite3.Connection) -> Dict[str, Any]:
        row = conn.execute(
            "SELECT candidate_id, status, result_hash FROM self_review_candidates WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        if not row:
            return {"status": "missing", "execution_id": execution_id}
        if row["status"] in {"ineligible", "candidate_write_failed"}:
            return {"candidate_id": row["candidate_id"], "status": row["status"]}
        existing_hash = _string(row["result_hash"])
        if existing_hash and existing_hash != result_hash:
            conn.execute(
                "UPDATE self_review_candidates SET status='delivery_ref_mismatch', updated_at=?, last_error=? WHERE candidate_id=?",
                (now, "result hash mismatch for repeated delivery marker", row["candidate_id"]),
            )
            return {"candidate_id": row["candidate_id"], "status": "delivery_ref_mismatch"}
        conn.execute(
            """
            UPDATE self_review_candidates
            SET status=CASE WHEN status='candidate' THEN 'validated' ELSE status END,
                native_envelope_validated_at=COALESCE(native_envelope_validated_at, ?),
                delivered_at=COALESCE(delivered_at, ?),
                result_hash=?,
                result_ref=COALESCE(result_ref, ?),
                updated_at=?
            WHERE candidate_id=?
            """,
            (now, now, result_hash, result_ref or "", now, row["candidate_id"]),
        )
        return {"candidate_id": row["candidate_id"], "status": "validated"}

    result = db._execute_write(_do)
    db.close()
    return result


def mark_candidate_ineligible(config: SelfReviewConfig, execution_id: str, reason: str) -> Dict[str, Any]:
    db = _db(config)
    now = _now()

    def _do(conn: sqlite3.Connection) -> Dict[str, Any]:
        row = conn.execute(
            "SELECT candidate_id FROM self_review_candidates WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        if not row:
            return {"status": "missing", "execution_id": execution_id}
        conn.execute(
            "UPDATE self_review_candidates SET status='ineligible', updated_at=?, last_error=? WHERE candidate_id=?",
            (now, reason[:800], row["candidate_id"]),
        )
        return {"candidate_id": row["candidate_id"], "status": "ineligible"}

    result = db._execute_write(_do)
    db.close()
    return result


def _earliest_promotable(conn: sqlite3.Connection, agent_identity: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM self_review_candidates
        WHERE agent_identity=?
          AND status IN ('validated', 'pending_promote_retry')
          AND native_envelope_validated_at IS NOT NULL
          AND delivered_at IS NOT NULL
        ORDER BY created_at ASC, candidate_id ASC
        LIMIT 1
        """,
        (agent_identity,),
    ).fetchone()


def promote_review_candidate(config: SelfReviewConfig, candidate_id: int) -> Dict[str, Any]:
    db = _db(config)
    now = _now()

    def _do(conn: sqlite3.Connection) -> Dict[str, Any]:
        row = conn.execute("SELECT * FROM self_review_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if not row:
            return {"status": "missing", "candidate_id": candidate_id}
        if row["status"] == "enqueued":
            return {"status": "enqueued", "candidate_id": candidate_id}
        if row["status"] not in RETRYABLE_PROMOTION_STATUSES:
            return {"status": row["status"], "candidate_id": candidate_id}
        if not row["native_envelope_validated_at"] or not row["delivered_at"]:
            return {"status": "not_delivered", "candidate_id": candidate_id}
        earliest = _earliest_promotable(conn, row["agent_identity"])
        if earliest and int(earliest["candidate_id"]) != int(candidate_id):
            conn.execute(
                "UPDATE self_review_candidates SET status='pending_promote_retry', updated_at=? WHERE candidate_id=?",
                (now, candidate_id),
            )
            return {
                "status": "blocked_by_earlier_candidate",
                "candidate_id": candidate_id,
                "blocking_candidate_id": int(earliest["candidate_id"]),
            }
        counters = conn.execute(
            "SELECT memory_turns, skill_iterations FROM self_review_counters WHERE agent_identity=?",
            (row["agent_identity"],),
        ).fetchone()
        memory_turns = int(counters["memory_turns"]) if counters else 0
        skill_iterations = int(counters["skill_iterations"]) if counters else 0
        memory_turns += int(row["memory_turn_delta"] or 0)
        skill_iterations += int(row["skill_iteration_delta"] or 0)
        review_memory = bool(row["memory_eligible"]) and int(row["memory_nudge_interval"] or 0) > 0 and memory_turns >= int(row["memory_nudge_interval"] or 0)
        review_skills = bool(row["skill_eligible"]) and int(row["skill_nudge_interval"] or 0) > 0 and skill_iterations >= int(row["skill_nudge_interval"] or 0)
        if review_memory:
            memory_turns = 0
        if review_skills:
            skill_iterations = 0
        conn.execute(
            """
            INSERT INTO self_review_counters(agent_identity, memory_turns, skill_iterations, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(agent_identity) DO UPDATE SET
                memory_turns=excluded.memory_turns,
                skill_iterations=excluded.skill_iterations,
                updated_at=excluded.updated_at
            """,
            (row["agent_identity"], memory_turns, skill_iterations, now),
        )
        work_created: List[str] = []
        for kind, should_create in (("memory", review_memory), ("skill", review_skills)):
            if not should_create:
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO self_review_work_items(
                    candidate_id, execution_id, agent_identity, kind, status,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'pending', ?, ?)
                """,
                (row["candidate_id"], row["execution_id"], row["agent_identity"], kind, now, now),
            )
            work_created.append(kind)
        decision = {
            "review_memory": review_memory,
            "review_skills": review_skills,
            "memory_turns_after": memory_turns,
            "skill_iterations_after": skill_iterations,
            "work_created": work_created,
        }
        conn.execute(
            """
            UPDATE self_review_candidates
            SET status=?, updated_at=?, promoted_at=?, promotion_attempts=promotion_attempts + 1,
                trigger_decision_json=?
            WHERE candidate_id=?
            """,
            ("enqueued" if work_created else "skipped", now, now, _json_dumps(decision), row["candidate_id"]),
        )
        return {"status": "enqueued" if work_created else "skipped", "candidate_id": row["candidate_id"], **decision}

    result = db._execute_write(_do)
    db.close()
    return result


def _status_counts(conn: sqlite3.Connection, config: SelfReviewConfig, stale_recovered: int) -> Dict[str, Any]:
    candidate_rows = conn.execute("SELECT status, COUNT(*) AS c FROM self_review_candidates GROUP BY status").fetchall()
    work_rows = conn.execute("SELECT kind, status, COUNT(*) AS c FROM self_review_work_items GROUP BY kind, status").fetchall()
    promotable = conn.execute(
        """
        SELECT COUNT(*) FROM self_review_candidates
        WHERE status IN ('validated', 'pending_promote_retry')
          AND native_envelope_validated_at IS NOT NULL
          AND delivered_at IS NOT NULL
        """
    ).fetchone()[0]
    pending_running = conn.execute(
        "SELECT COUNT(*) FROM self_review_work_items WHERE status IN ('pending', 'running', 'interrupted')"
    ).fetchone()[0]
    skill_blocking = conn.execute(
        "SELECT COUNT(*) FROM self_review_work_items WHERE kind='skill' AND status IN ('pending', 'running', 'interrupted')"
    ).fetchone()[0]
    local_owned_pending = conn.execute(
        """
        SELECT COUNT(*) FROM self_review_work_items w
        JOIN self_review_candidates c ON c.candidate_id=w.candidate_id
        WHERE w.status IN ('pending', 'interrupted')
          AND (
            (w.review_owner_executor_instance_id=? AND w.review_owner_generation=?)
            OR (c.candidate_owner_executor_instance_id=? AND c.candidate_owner_generation=?)
          )
        """,
        (
            config.executor_instance_id,
            config.pod_generation,
            config.executor_instance_id,
            config.pod_generation,
        ),
    ).fetchone()[0]
    local_running = conn.execute(
        """
        SELECT COUNT(*) FROM self_review_work_items
        WHERE status='running'
          AND review_owner_executor_instance_id=?
          AND review_owner_generation=?
        """,
        (config.executor_instance_id, config.pod_generation),
    ).fetchone()[0]
    local_promotable = conn.execute(
        """
        SELECT COUNT(*) FROM self_review_candidates
        WHERE status IN ('validated', 'pending_promote_retry')
          AND native_envelope_validated_at IS NOT NULL
          AND delivered_at IS NOT NULL
          AND candidate_owner_executor_instance_id=?
          AND candidate_owner_generation=?
        """,
        (config.executor_instance_id, config.pod_generation),
    ).fetchone()[0]
    return {
        "ok": promotable == 0 and pending_running == 0,
        "promotable_candidate_count": int(promotable),
        "global_review_blocking_count": int(promotable) + int(pending_running),
        "global_skill_review_blocking_count": int(skill_blocking),
        "local_owned_pending_count": int(local_owned_pending),
        "local_owned_promotable_count": int(local_promotable),
        "local_running_review_count": int(local_running),
        "candidate_status_counts": {row["status"]: int(row["c"]) for row in candidate_rows},
        "work_status_counts": {f"{row['kind']}:{row['status']}": int(row["c"]) for row in work_rows},
        "stale_recovered_count": stale_recovered,
    }


def _reconcile_stale(conn: sqlite3.Connection, config: SelfReviewConfig, now: float) -> int:
    cutoff = now - max(1.0, float(config.stale_after_seconds))
    cur = conn.execute(
        """
        UPDATE self_review_work_items
        SET status='interrupted', updated_at=?, last_error='stale lease recovered'
        WHERE status='running'
          AND (lease_heartbeat_at IS NULL OR lease_heartbeat_at < ?)
        """,
        (now, cutoff),
    )
    return int(cur.rowcount or 0)


def _force_recover_running(config: SelfReviewConfig) -> int:
    db = _db(config)
    now = _now()

    def _do(conn: sqlite3.Connection) -> int:
        cur = conn.execute(
            """
            UPDATE self_review_work_items
            SET status='interrupted', updated_at=?, last_error='dead generation recovered by deployment drain'
            WHERE status='running'
            """,
            (now,),
        )
        return int(cur.rowcount or 0)

    recovered = db._execute_write(_do)
    db.close()
    return recovered


def review_queue_status(config: SelfReviewConfig, reconcile_stale: bool = True) -> Dict[str, Any]:
    db = _db(config)
    now = _now()
    if reconcile_stale:
        def _do(conn: sqlite3.Connection) -> Dict[str, Any]:
            stale_recovered = _reconcile_stale(conn, config, now)
            return _status_counts(conn, config, stale_recovered)

        result = db._execute_write(_do)
    else:
        with db._lock:
            result = _status_counts(db._conn, config, 0)
    db.close()
    return result


def candidate_status(config: SelfReviewConfig, execution_id: str) -> Dict[str, Any]:
    db = _db(config)
    with db._lock:
        row = db._conn.execute("SELECT * FROM self_review_candidates WHERE execution_id=?", (execution_id,)).fetchone()
        if not row:
            db.close()
            return {}
        work = db._conn.execute(
            "SELECT kind, status, result_summary, last_error FROM self_review_work_items WHERE candidate_id=? ORDER BY kind",
            (row["candidate_id"],),
        ).fetchall()
    db.close()
    return {
        "candidate_id": int(row["candidate_id"]),
        "self_review_candidate_status": row["status"],
        "self_review_status": "none" if not work else ",".join(f"{w['kind']}:{w['status']}" for w in work),
        "self_review_result_hash": row["result_hash"] or "",
        "self_review_snapshot_ref": row["snapshot_ref"] or "",
        "self_review_last_error": row["last_error"] or "",
        "self_review_work": [
            {"kind": w["kind"], "status": w["status"], "summary": w["result_summary"] or "", "error": w["last_error"] or ""}
            for w in work
        ],
    }


def _load_snapshot(row: sqlite3.Row | Dict[str, Any]) -> Dict[str, Any]:
    path = Path(row["snapshot_path"] or "")
    if not path.exists():
        raise FileNotFoundError("self-review snapshot is missing")
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("self-review snapshot must be an object")
    return parsed


def _mark_work(config: SelfReviewConfig, work_id: int, status: str, *, summary: str = "", error: str = "") -> None:
    db = _db(config)
    now = _now()

    def _do(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE self_review_work_items
            SET status=?, updated_at=?, last_progress_at=?, result_summary=?, last_error=?
            WHERE work_id=?
            """,
            (status, now, now, summary[:800], error[:800], work_id),
        )

    db._execute_write(_do)
    db.close()


def run_memory_self_review(config: SelfReviewConfig, candidate_id: int) -> Dict[str, Any]:
    db = _db(config)
    now = _now()

    def _claim(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            """
            SELECT w.*, c.snapshot_path, c.observation_json FROM self_review_work_items w
            JOIN self_review_candidates c ON c.candidate_id=w.candidate_id
            WHERE w.candidate_id=? AND w.kind='memory' AND w.status IN ('pending', 'interrupted')
            ORDER BY w.created_at, w.work_id LIMIT 1
            """,
            (candidate_id,),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            """
            UPDATE self_review_work_items
            SET status='running', updated_at=?, lease_heartbeat_at=?, last_progress_at=?,
                review_owner_executor_instance_id=?, review_owner_generation=?, attempt_count=attempt_count + 1
            WHERE work_id=?
            """,
            (now, now, now, config.executor_instance_id, config.pod_generation, row["work_id"]),
        )
        return dict(row)

    claimed = db._execute_write(_claim)
    db.close()
    if not claimed:
        return {"status": "skipped", "reason": "no_pending_memory_work", "candidate_id": candidate_id}
    try:
        snapshot = _load_snapshot(claimed)
        _run_review_agent(config, snapshot, review_memory=True, review_skills=False)
        _mark_work(config, int(claimed["work_id"]), "completed", summary="memory review completed")
        return {"status": "completed", "candidate_id": candidate_id, "kind": "memory"}
    except Exception as exc:
        _mark_work(config, int(claimed["work_id"]), "failed", error=_redact_error(exc))
        return {"status": "failed", "candidate_id": candidate_id, "kind": "memory", "error": _redact_error(exc)}


def _claim_skill_batch(config: SelfReviewConfig, *, local_only: bool = False) -> List[Dict[str, Any]]:
    db = _db(config)
    now = _now()
    batch_id = f"skill-{uuid.uuid4().hex}"

    def _do(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        params: List[Any] = [config.agent_identity]
        owner_clause = ""
        if local_only:
            owner_clause = " AND c.candidate_owner_executor_instance_id=? AND c.candidate_owner_generation=?"
            params.extend([config.executor_instance_id, config.pod_generation])
        candidate_rows = conn.execute(
            f"""
            SELECT w.*, c.snapshot_path, c.observation_json, c.snapshot_size FROM self_review_work_items w
            JOIN self_review_candidates c ON c.candidate_id=w.candidate_id
            WHERE w.agent_identity=?
              AND w.kind='skill'
              AND w.status IN ('pending', 'interrupted')
              {owner_clause}
            ORDER BY w.created_at ASC, w.work_id ASC
            LIMIT {max(1, int(config.max_batch_rows)) * 4}
            """,
            params,
        ).fetchall()
        rows: List[sqlite3.Row] = []
        estimated_tokens = 0
        for row in candidate_rows:
            row_tokens = max(1, int(row["snapshot_size"] or 0) // 3 + 1000)
            if rows and estimated_tokens + row_tokens > int(config.max_batch_tokens):
                break
            rows.append(row)
            estimated_tokens += row_tokens
            if len(rows) >= max(1, int(config.max_batch_rows)):
                break
        for row in rows:
            conn.execute(
                """
                UPDATE self_review_work_items
                SET status='running', updated_at=?, lease_heartbeat_at=?, last_progress_at=?,
                    review_owner_executor_instance_id=?, review_owner_generation=?, batch_id=?,
                    attempt_count=attempt_count + 1
                WHERE work_id=?
                """,
                (now, now, now, config.executor_instance_id, config.pod_generation, batch_id, row["work_id"]),
            )
        return [dict(row) for row in rows]

    result = db._execute_write(_do)
    db.close()
    return result


def run_skill_self_review_batch(config: SelfReviewConfig, batch_id: Optional[str] = None, *, local_only: bool = False) -> Dict[str, Any]:
    claimed = _claim_skill_batch(config, local_only=local_only)
    if not claimed:
        return {"status": "skipped", "reason": "no_pending_skill_work"}
    valid_rows: List[Dict[str, Any]] = []
    snapshots: List[Dict[str, Any]] = []
    failed = 0
    for row in claimed:
        try:
            snapshots.append(_load_snapshot(row))
            valid_rows.append(row)
        except Exception as exc:
            _mark_work(config, int(row["work_id"]), "failed", error=_redact_error(exc))
            failed += 1
    if not valid_rows:
        return {"status": "completed", "completed": 0, "failed": failed}
    try:
        _run_review_agent(config, _combine_skill_snapshots(snapshots), review_memory=False, review_skills=True)
    except Exception as exc:
        for row in valid_rows:
            _mark_work(config, int(row["work_id"]), "interrupted", error=_redact_error(exc))
        return {"status": "retry", "error": _redact_error(exc), "claimed": len(claimed)}
    for row in valid_rows:
        _mark_work(config, int(row["work_id"]), "completed", summary="skill review completed")
    return {"status": "completed", "completed": len(valid_rows), "failed": failed}


def _combine_skill_snapshots(snapshots: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = []
    first_observation: Dict[str, Any] = {}
    for index, snapshot in enumerate(snapshots, start=1):
        obs = snapshot.get("observation") if isinstance(snapshot.get("observation"), dict) else {}
        if not first_observation and obs:
            first_observation = obs
        messages.append(
            {
                "role": "user",
                "content": f"[Self-review snapshot {index}: session={obs.get('session_id', '')} execution={obs.get('execution_id', '')}]",
            }
        )
        for msg in snapshot.get("messages") or []:
            if isinstance(msg, dict):
                messages.append(msg)
    return {"messages": messages, "observation": first_observation}


def _run_review_agent(config: SelfReviewConfig, snapshot: Dict[str, Any], *, review_memory: bool, review_skills: bool) -> None:
    from run_agent import AIAgent

    observation = snapshot.get("observation") if isinstance(snapshot.get("observation"), dict) else {}
    memory_target = observation.get("memory_target") if isinstance(observation.get("memory_target"), dict) else {}
    prompt = AIAgent._COMBINED_REVIEW_PROMPT if review_memory and review_skills else (
        AIAgent._MEMORY_REVIEW_PROMPT if review_memory else AIAgent._SKILL_REVIEW_PROMPT
    )
    agent = AIAgent(
        model=config.model or None,
        provider=config.provider or "openrouter",
        base_url=config.base_url or None,
        api_mode=config.api_mode or None,
        max_iterations=8,
        quiet_mode=True,
        enabled_toolsets=["memory", "skills"],
        self_review_mode="disabled",
        skip_context_files=True,
        session_id=_string(observation.get("session_id")) or None,
        parent_session_id=_string(observation.get("parent_session_id")) or _string(memory_target.get("parent_session_id")) or None,
        platform=_string(observation.get("platform")) or "rsi",
        user_id=_string(observation.get("user_peer_id")) or _string(memory_target.get("user_peer_id")) or None,
        user_name=_string(observation.get("user_peer_name")) or None,
        chat_id=_string(observation.get("chat_id")) or _string(memory_target.get("chat_id")) or None,
        thread_id=_string(observation.get("thread_id")) or _string(memory_target.get("thread_id")) or None,
    )
    agent._memory_nudge_interval = 0
    agent._skill_nudge_interval = 0
    try:
        agent.run_conversation(
            user_message=prompt,
            conversation_history=[item for item in (snapshot.get("messages") or []) if isinstance(item, dict)],
        )
    finally:
        try:
            agent.shutdown_memory_provider()
        except Exception:
            pass
        try:
            agent.close()
        except Exception:
            pass


def drain_review_queue(
    config: SelfReviewConfig,
    timeout_seconds: float,
    max_attempts: int,
    force_dead_generations: bool = False,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_seconds))
    attempts = 0
    last_status: Dict[str, Any] = {}
    forced_recovered = _force_recover_running(config) if force_dead_generations else 0
    while time.monotonic() < deadline and attempts < max(1, int(max_attempts)):
        attempts += 1
        last_status = review_queue_status(config, reconcile_stale=True)
        if last_status.get("global_review_blocking_count") == 0:
            return {
                "ok": True,
                "attempts": attempts,
                "force_dead_generations": force_dead_generations,
                "forced_recovered_count": forced_recovered,
                **last_status,
            }
        _promote_next(config)
        _run_next_work(config)
        time.sleep(0.25)
    last_status = review_queue_status(config, reconcile_stale=True)
    return {"ok": False, "attempts": attempts, "force_dead_generations": force_dead_generations, "forced_recovered_count": forced_recovered, **last_status}


def advance_local_review_queue(config: SelfReviewConfig) -> Dict[str, Any]:
    promoted = _promote_next(config, local_only=True)
    ran = _run_next_work(config, local_only=True)
    return {"promoted": promoted, "ran": ran}


def _promote_next(config: SelfReviewConfig, *, local_only: bool = False) -> Dict[str, Any]:
    db = _db(config)
    with db._lock:
        owner_clause = ""
        params: List[Any] = []
        if local_only:
            owner_clause = " AND candidate_owner_executor_instance_id=? AND candidate_owner_generation=?"
            params.extend([config.executor_instance_id, config.pod_generation])
        row = db._conn.execute(
            f"""
            SELECT candidate_id FROM self_review_candidates
            WHERE status IN ('validated', 'pending_promote_retry')
              AND native_envelope_validated_at IS NOT NULL
              AND delivered_at IS NOT NULL
              {owner_clause}
            ORDER BY agent_identity, created_at, candidate_id
            LIMIT 1
            """,
            params,
        ).fetchone()
    db.close()
    if row:
        return promote_review_candidate(config, int(row["candidate_id"]))
    return {"status": "skipped", "reason": "no_promotable_candidate"}


def _run_next_work(config: SelfReviewConfig, *, local_only: bool = False) -> Dict[str, Any]:
    db = _db(config)
    with db._lock:
        owner_clause = ""
        params: List[Any] = []
        if local_only:
            owner_clause = (
                " AND (w.review_owner_executor_instance_id=? AND w.review_owner_generation=? "
                "OR c.candidate_owner_executor_instance_id=? AND c.candidate_owner_generation=?)"
            )
            params.extend([config.executor_instance_id, config.pod_generation, config.executor_instance_id, config.pod_generation])
        row = db._conn.execute(
            f"""
            SELECT w.candidate_id, w.kind FROM self_review_work_items w
            JOIN self_review_candidates c ON c.candidate_id=w.candidate_id
            WHERE w.status IN ('pending', 'interrupted')
              {owner_clause}
            ORDER BY w.created_at, w.work_id
            LIMIT 1
            """,
            params,
        ).fetchone()
    db.close()
    if not row:
        return {"status": "skipped", "reason": "no_pending_work"}
    if row["kind"] == "memory":
        return run_memory_self_review(config, int(row["candidate_id"]))
    return run_skill_self_review_batch(config, local_only=local_only)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hermes self-review queue helper")
    parser.add_argument("command", choices=["status", "drain"])
    parser.add_argument("--timeout-seconds", type=float, default=float(os.getenv("RSI_HERMES_SELF_REVIEW_DRAIN_TIMEOUT_SECONDS", "300")))
    parser.add_argument("--max-attempts", type=int, default=int(os.getenv("RSI_HERMES_SELF_REVIEW_DRAIN_MAX_ATTEMPTS", "20")))
    parser.add_argument("--force-dead-generations", action="store_true")
    args = parser.parse_args()
    config = SelfReviewConfig.from_env()
    payload = review_queue_status(config, reconcile_stale=True) if args.command == "status" else drain_review_queue(
        config,
        timeout_seconds=args.timeout_seconds,
        max_attempts=args.max_attempts,
        force_dead_generations=args.force_dead_generations,
    )
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
