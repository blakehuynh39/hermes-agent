"""Typed contracts for Hermes self-review handoff."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


SELF_REVIEW_OBSERVATION_SCHEMA = "hermes.self_review_observation.v1"


def _json_object(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _json_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


@dataclass
class SelfReviewObservationV1:
    """Observed turn data used later for durable self-review accounting.

    This is deliberately an observation, not a trigger decision. Durable review
    cadence is applied only after the platform validates and persists the
    user-facing result.
    """

    schema: str = SELF_REVIEW_OBSERVATION_SCHEMA
    execution_id: str = ""
    session_id: str = ""
    parent_session_id: str = ""
    platform: str = ""
    user_peer_id: str = ""
    user_peer_name: str = ""
    chat_id: str = ""
    thread_id: str = ""
    gateway_session_key: str = ""
    agent_identity: str = ""
    cadence_scope_key: str = ""
    memory_turn_delta: int = 0
    skill_iteration_delta: int = 0
    memory_tool_used: bool = False
    skill_manage_used: bool = False
    skill_iteration_delta_after_last_skill_manage: int = 0
    memory_nudge_interval: int = 0
    skill_nudge_interval: int = 0
    memory_eligible: bool = False
    skill_eligible: bool = False
    completed: bool = False
    interrupted: bool = False
    final_response_present: bool = False
    api_calls: int = 0
    model: str = ""
    provider: str = ""
    base_url: str = ""
    memory_target: Dict[str, Any] = field(default_factory=dict)
    skill_target: Dict[str, Any] = field(default_factory=dict)
    messages: List[Dict[str, Any]] = field(default_factory=list)
    final_response: str = ""
    loaded_skills: List[str] = field(default_factory=list)
    tool_trace_summary: List[Dict[str, Any]] = field(default_factory=list)
    safe_review_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SelfReviewObservationV1":
        if not isinstance(payload, dict):
            raise ValueError("self-review observation must be an object")
        schema = str(payload.get("schema") or "").strip()
        if schema != SELF_REVIEW_OBSERVATION_SCHEMA:
            raise ValueError(f"unsupported self-review observation schema: {schema!r}")
        return cls(
            schema=schema,
            execution_id=str(payload.get("execution_id") or ""),
            session_id=str(payload.get("session_id") or ""),
            parent_session_id=str(payload.get("parent_session_id") or ""),
            platform=str(payload.get("platform") or ""),
            user_peer_id=str(payload.get("user_peer_id") or ""),
            user_peer_name=str(payload.get("user_peer_name") or ""),
            chat_id=str(payload.get("chat_id") or ""),
            thread_id=str(payload.get("thread_id") or ""),
            gateway_session_key=str(payload.get("gateway_session_key") or ""),
            agent_identity=str(payload.get("agent_identity") or ""),
            cadence_scope_key=str(payload.get("cadence_scope_key") or ""),
            memory_turn_delta=int(payload.get("memory_turn_delta") or 0),
            skill_iteration_delta=int(payload.get("skill_iteration_delta") or 0),
            memory_tool_used=bool(payload.get("memory_tool_used")),
            skill_manage_used=bool(payload.get("skill_manage_used")),
            skill_iteration_delta_after_last_skill_manage=int(payload.get("skill_iteration_delta_after_last_skill_manage") or 0),
            memory_nudge_interval=int(payload.get("memory_nudge_interval") or 0),
            skill_nudge_interval=int(payload.get("skill_nudge_interval") or 0),
            memory_eligible=bool(payload.get("memory_eligible")),
            skill_eligible=bool(payload.get("skill_eligible")),
            completed=bool(payload.get("completed")),
            interrupted=bool(payload.get("interrupted")),
            final_response_present=bool(payload.get("final_response_present")),
            api_calls=int(payload.get("api_calls") or 0),
            model=str(payload.get("model") or ""),
            provider=str(payload.get("provider") or ""),
            base_url=str(payload.get("base_url") or ""),
            memory_target=_json_object(payload.get("memory_target")),
            skill_target=_json_object(payload.get("skill_target")),
            messages=[item for item in _json_list(payload.get("messages")) if isinstance(item, dict)],
            final_response=str(payload.get("final_response") or ""),
            loaded_skills=[str(item) for item in _json_list(payload.get("loaded_skills")) if str(item or "").strip()],
            tool_trace_summary=[item for item in _json_list(payload.get("tool_trace_summary")) if isinstance(item, dict)],
            safe_review_context=_json_object(payload.get("safe_review_context")),
        )

    def validate(self) -> None:
        if self.schema != SELF_REVIEW_OBSERVATION_SCHEMA:
            raise ValueError(f"unsupported self-review observation schema: {self.schema!r}")
        if not self.execution_id:
            raise ValueError("self-review observation requires execution_id")
        if not self.session_id:
            raise ValueError("self-review observation requires session_id")
        if (
            self.memory_turn_delta < 0
            or self.skill_iteration_delta < 0
            or self.skill_iteration_delta_after_last_skill_manage < 0
        ):
            raise ValueError("self-review cadence deltas must be non-negative")
        if self.memory_eligible and not self.memory_target:
            raise ValueError("memory-eligible observations require memory_target")
        if self.skill_eligible and not self.skill_target:
            raise ValueError("skill-eligible observations require skill_target")
