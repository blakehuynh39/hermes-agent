"""External tool pause control flow.

Tools can raise :class:`ExternalToolPending` when a host-owned approval or
execution flow must complete outside the live Hermes process. The agent loop
must persist the unanswered assistant tool call and exit without appending a
tool result. A later resume can append the approved result for the same
``tool_call_id`` and continue from the original transcript.
"""

from __future__ import annotations

from typing import Any, Dict


class ExternalToolPending(Exception):
    """Signal that a tool call is pending an external approval/result."""

    def __init__(self, payload: Dict[str, Any] | None = None, message: str = ""):
        self.payload = dict(payload or {})
        super().__init__(
            message or str(self.payload.get("summary") or "external tool pending")
        )

    def to_dict(
        self,
        *,
        session_id: str = "",
        tool_call_id: str = "",
        tool_name: str = "",
    ) -> Dict[str, Any]:
        out = dict(self.payload)
        out["kind"] = "external_tool_pending"
        if session_id and not out.get("session_id"):
            out["session_id"] = session_id
        if tool_call_id and not out.get("tool_call_id"):
            out["tool_call_id"] = tool_call_id
        if tool_name and not out.get("tool_name"):
            out["tool_name"] = tool_name
        return out
