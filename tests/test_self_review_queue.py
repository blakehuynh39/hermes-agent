from __future__ import annotations

import sqlite3

from self_review_contracts import SelfReviewObservationV1
from self_review_queue import (
    SelfReviewConfig,
    apply_turn_review_candidate,
    mark_candidate_delivered,
    promote_review_candidate,
    review_queue_status,
)


def _config(tmp_path, identity: str = "rsi:test:company") -> SelfReviewConfig:
    home = tmp_path / "hermes"
    return SelfReviewConfig(
        hermes_home=home,
        state_db_path=home / "state.db",
        snapshot_root=home / "self-review" / "snapshots",
        agent_identity=identity,
        executor_instance_id="executor-a",
        pod_generation="pod-a",
        stale_after_seconds=60,
        max_batch_rows=4,
        max_batch_tokens=12000,
    )


def _observation(execution_id: str, *, memory_delta: int = 1, skill_delta: int = 1) -> SelfReviewObservationV1:
    return SelfReviewObservationV1(
        execution_id=execution_id,
        session_id=f"session-{execution_id}",
        agent_identity="rsi:test:company",
        memory_turn_delta=memory_delta,
        skill_iteration_delta=skill_delta,
        memory_nudge_interval=2,
        skill_nudge_interval=2,
        memory_eligible=True,
        skill_eligible=True,
        completed=True,
        final_response_present=True,
        memory_target={"session_id": f"session-{execution_id}", "user_peer_id": "user-a"},
        skill_target={"agent_identity": "rsi:test:company"},
        messages=[
            {"role": "user", "content": "remember this"},
            {"role": "assistant", "content": "ok"},
        ],
        final_response="ok",
    )


def test_candidate_creation_does_not_advance_counters_until_promotion(tmp_path):
    config = _config(tmp_path)
    candidate = apply_turn_review_candidate(config, _observation("exec-1"))

    conn = sqlite3.connect(config.state_db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM self_review_counters").fetchone()[0] == 0
    finally:
        conn.close()

    mark_candidate_delivered(config, "exec-1", "hash-1", result_ref="status.json")
    promoted = promote_review_candidate(config, candidate["candidate_id"])
    assert promoted["status"] == "skipped"

    conn = sqlite3.connect(config.state_db_path)
    try:
        row = conn.execute(
            "SELECT memory_turns, skill_iterations FROM self_review_counters WHERE agent_identity=?",
            ("rsi:test:company",),
        ).fetchone()
        assert row == (1, 1)
    finally:
        conn.close()


def test_promotion_is_deterministic_by_candidate_order(tmp_path):
    config = _config(tmp_path)
    first = apply_turn_review_candidate(config, _observation("exec-1"))
    second = apply_turn_review_candidate(config, _observation("exec-2"))
    mark_candidate_delivered(config, "exec-2", "hash-2", result_ref="status-2.json")
    mark_candidate_delivered(config, "exec-1", "hash-1", result_ref="status-1.json")

    blocked = promote_review_candidate(config, second["candidate_id"])
    assert blocked["status"] == "blocked_by_earlier_candidate"
    assert blocked["blocking_candidate_id"] == first["candidate_id"]

    assert promote_review_candidate(config, first["candidate_id"])["status"] == "skipped"
    promoted = promote_review_candidate(config, second["candidate_id"])
    assert promoted["status"] == "enqueued"
    assert set(promoted["work_created"]) == {"memory", "skill"}

    status = review_queue_status(config, reconcile_stale=True)
    assert status["global_review_blocking_count"] == 2
