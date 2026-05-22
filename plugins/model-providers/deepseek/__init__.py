"""DeepSeek provider profile.

DeepSeek's V4 family (and the legacy ``deepseek-reasoner``) defaults to
thinking-mode ON when ``extra_body.thinking`` is unset. The API then returns
``reasoning_content`` and enforces that subsequent tool-call turns echo it
back.

This profile mirrors the OpenAI-compatible wire shape DeepSeek expects:

    {"reasoning_effort": "<low|medium|high|max>",
     "extra_body": {"thinking": {"type": "enabled" | "disabled"}}}

Non-thinking models (only ``deepseek-chat`` today, which is V3) are left as
no-ops so we do not perturb the V3 wire format.
"""

from __future__ import annotations

from typing import Any

from providers import register_provider
from providers.base import ProviderProfile


def _model_supports_thinking(model: str | None) -> bool:
    """DeepSeek thinking-capable model families."""
    m = (model or "").strip().lower()
    if not m:
        return False
    if m.startswith("deepseek-v") and not m.startswith("deepseek-v3"):
        return True
    if m == "deepseek-reasoner":
        return True
    return False


class DeepSeekProfile(ProviderProfile):
    """DeepSeek — extra_body.thinking + top-level reasoning_effort."""

    def build_api_kwargs_extras(
        self,
        *,
        reasoning_config: dict | None = None,
        model: str | None = None,
        **context: Any,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        extra_body: dict[str, Any] = {}
        top_level: dict[str, Any] = {}

        if not _model_supports_thinking(model):
            return extra_body, top_level

        enabled = True
        if isinstance(reasoning_config, dict) and reasoning_config.get("enabled") is False:
            enabled = False

        extra_body["thinking"] = {"type": "enabled" if enabled else "disabled"}
        if not enabled:
            return extra_body, top_level

        if isinstance(reasoning_config, dict):
            effort = (reasoning_config.get("effort") or "").strip().lower()
            if effort in {"xhigh", "max"}:
                top_level["reasoning_effort"] = "max"
            elif effort in {"low", "medium", "high"}:
                top_level["reasoning_effort"] = effort

        return extra_body, top_level


deepseek = DeepSeekProfile(
    name="deepseek",
    aliases=("deepseek-chat",),
    env_vars=("DEEPSEEK_API_KEY",),
    display_name="DeepSeek",
    description="DeepSeek — native DeepSeek API",
    signup_url="https://platform.deepseek.com/",
    fallback_models=(
        "deepseek-chat",
        "deepseek-reasoner",
    ),
    base_url="https://api.deepseek.com/v1",
    default_aux_model="deepseek-chat",
)

register_provider(deepseek)
