"""Unit tests for the DeepSeek provider profile's V4 thinking-mode wiring."""

from __future__ import annotations

import pytest


@pytest.fixture
def deepseek_profile():
    import model_tools  # noqa: F401 - triggers bundled provider discovery
    import providers

    profile = providers.get_provider_profile("deepseek")
    assert profile is not None
    return profile


class TestDeepSeekThinkingWireShape:
    def test_v4_pro_default_enables_thinking_without_effort(self, deepseek_profile):
        extra_body, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config=None,
            model="deepseek-v4-pro",
        )

        assert extra_body == {"thinking": {"type": "enabled"}}
        assert top_level == {}

    def test_v4_pro_enabled_with_high_effort(self, deepseek_profile):
        extra_body, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": True, "effort": "high"},
            model="deepseek-v4-pro",
        )

        assert extra_body == {"thinking": {"type": "enabled"}}
        assert top_level == {"reasoning_effort": "high"}

    @pytest.mark.parametrize("effort", ["low", "medium", "high"])
    def test_standard_efforts_pass_through(self, deepseek_profile, effort):
        _, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": True, "effort": effort},
            model="deepseek-v4-pro",
        )

        assert top_level == {"reasoning_effort": effort}

    @pytest.mark.parametrize("effort", ["xhigh", "max", "MAX", "  Max  "])
    def test_xhigh_and_max_normalize_to_max(self, deepseek_profile, effort):
        _, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": True, "effort": effort},
            model="deepseek-v4-pro",
        )

        assert top_level == {"reasoning_effort": "max"}

    def test_explicitly_disabled_sends_disabled_marker(self, deepseek_profile):
        extra_body, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": False},
            model="deepseek-v4-flash",
        )

        assert extra_body == {"thinking": {"type": "disabled"}}
        assert top_level == {}

    def test_disabled_ignores_effort_field(self, deepseek_profile):
        _, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": False, "effort": "high"},
            model="deepseek-v4-pro",
        )

        assert top_level == {}

    def test_unknown_effort_omits_top_level(self, deepseek_profile):
        _, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": True, "effort": "garbage"},
            model="deepseek-v4-pro",
        )

        assert top_level == {}


class TestDeepSeekModelGating:
    @pytest.mark.parametrize(
        "model",
        [
            "deepseek-v4-pro",
            "deepseek-v4-flash",
            "deepseek-v4-future-variant",
            "deepseek-reasoner",
            "DEEPSEEK-V4-PRO",
        ],
    )
    def test_thinking_capable_models_emit_thinking(self, deepseek_profile, model):
        extra_body, _ = deepseek_profile.build_api_kwargs_extras(
            reasoning_config=None,
            model=model,
        )

        assert extra_body == {"thinking": {"type": "enabled"}}

    @pytest.mark.parametrize(
        "model",
        ["deepseek-chat", "deepseek-v3-0324", "deepseek-v3.1", "", None, "deepseek-unknown"],
    )
    def test_non_thinking_models_emit_nothing(self, deepseek_profile, model):
        extra_body, top_level = deepseek_profile.build_api_kwargs_extras(
            reasoning_config={"enabled": True, "effort": "high"},
            model=model,
        )

        assert extra_body == {}
        assert top_level == {}


class TestDeepSeekFullKwargsIntegration:
    def test_full_kwargs_match_live_wire_shape(self, deepseek_profile):
        from agent.transports.chat_completions import ChatCompletionsTransport

        kwargs = ChatCompletionsTransport().build_kwargs(
            model="deepseek-v4-pro",
            messages=[{"role": "user", "content": "ping"}],
            tools=None,
            provider_profile=deepseek_profile,
            reasoning_config={"enabled": True, "effort": "high"},
            base_url="https://api.deepseek.com/v1",
            provider_name="deepseek",
        )

        assert kwargs["model"] == "deepseek-v4-pro"
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["extra_body"] == {"thinking": {"type": "enabled"}}

    def test_thinking_mode_omits_sampling_fields(self, deepseek_profile):
        from agent.transports.chat_completions import ChatCompletionsTransport

        kwargs = ChatCompletionsTransport().build_kwargs(
            model="deepseek-v4-pro",
            messages=[{"role": "user", "content": "ping"}],
            tools=None,
            provider_profile=deepseek_profile,
            reasoning_config={"enabled": True, "effort": "high"},
            base_url="https://api.deepseek.com/v1",
            provider_name="deepseek",
            temperature=0.2,
            request_overrides={
                "top_p": 0.9,
                "presence_penalty": 0.1,
                "frequency_penalty": 0.1,
            },
        )

        assert kwargs["extra_body"] == {"thinking": {"type": "enabled"}}
        assert "temperature" not in kwargs
        assert "top_p" not in kwargs
        assert "presence_penalty" not in kwargs
        assert "frequency_penalty" not in kwargs

    def test_disabled_thinking_keeps_no_reasoning_effort(self, deepseek_profile):
        from agent.transports.chat_completions import ChatCompletionsTransport

        kwargs = ChatCompletionsTransport().build_kwargs(
            model="deepseek-v4-flash",
            messages=[{"role": "user", "content": "ping"}],
            tools=None,
            provider_profile=deepseek_profile,
            reasoning_config={"enabled": False, "effort": "none"},
            base_url="https://api.deepseek.com/v1",
            provider_name="deepseek",
        )

        assert kwargs["extra_body"] == {"thinking": {"type": "disabled"}}
        assert "reasoning_effort" not in kwargs

    def test_v3_chat_full_kwargs_omit_thinking(self, deepseek_profile):
        from agent.transports.chat_completions import ChatCompletionsTransport

        kwargs = ChatCompletionsTransport().build_kwargs(
            model="deepseek-chat",
            messages=[{"role": "user", "content": "ping"}],
            tools=None,
            provider_profile=deepseek_profile,
            reasoning_config={"enabled": True, "effort": "high"},
            base_url="https://api.deepseek.com/v1",
            provider_name="deepseek",
        )

        assert "reasoning_effort" not in kwargs
        assert "extra_body" not in kwargs or "thinking" not in kwargs.get("extra_body", {})


class TestDeepSeekAuxModel:
    def test_profile_advertises_deepseek_chat(self, deepseek_profile):
        assert deepseek_profile.default_aux_model == "deepseek-chat"

    def test_consumer_api_returns_deepseek_chat(self):
        from agent.auxiliary_client import _get_aux_model_for_provider

        assert _get_aux_model_for_provider("deepseek") == "deepseek-chat"
