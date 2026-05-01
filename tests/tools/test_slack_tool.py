"""Tests for tools/slack_tool.py."""

import json

import pytest

from tools import slack_tool as slack_mod


def _decode(result: str) -> dict:
    return json.loads(result)


def test_read_thread_parses_permalink_and_fetches_replies(monkeypatch):
    calls = []

    async def fake_read_thread(channel_id, thread_ts, *, token=None, limit=50, cursor=None, inclusive=True):
        calls.append((channel_id, thread_ts, token, limit, cursor, inclusive))
        return {
            "ok": True,
            "messages": [
                {"ts": "1777650186.068179", "user": "U1", "text": "parent"},
                {"ts": "1777650190.000100", "user": "U2", "text": "reply"},
            ],
        }

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_read_thread", fake_read_thread)

    result = _decode(
        slack_mod.slack_tool(
            {
                "action": "read_thread",
                "permalink": "https://storyprotocol.slack.com/archives/C0AKH5SNGKH/p1777650186068179",
            }
        )
    )

    assert result["success"] is True
    assert result["channel_id"] == "C0AKH5SNGKH"
    assert result["thread_ts"] == "1777650186.068179"
    assert result["message_count"] == 2
    assert calls == [("C0AKH5SNGKH", "1777650186.068179", "xoxb-test", 50, None, True)]


def test_read_thread_accepts_slack_target(monkeypatch):
    async def fake_read_thread(_channel_id, _thread_ts, **_kwargs):
        return {"ok": True, "messages": []}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_read_thread", fake_read_thread)

    result = _decode(
        slack_mod.slack_tool(
            {
                "action": "read_thread",
                "target": "slack:C0AKH5SNGKH:1777650186.068179",
            }
        )
    )

    assert result["success"] is True
    assert result["channel_id"] == "C0AKH5SNGKH"
    assert result["thread_ts"] == "1777650186.068179"


def test_read_channel_fetches_history(monkeypatch):
    calls = []

    async def fake_read_channel(channel_id, *, token=None, limit=50, cursor=None, oldest=None, latest=None):
        calls.append((channel_id, token, limit, cursor, oldest, latest))
        return {"ok": True, "messages": [{"ts": "1.0", "text": "hello"}], "has_more": False}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_read_channel", fake_read_channel)

    result = _decode(slack_mod.slack_tool({"action": "read_channel", "target": "slack:C0AKH5SNGKH", "limit": 500}))

    assert result["success"] is True
    assert result["message_count"] == 1
    assert calls == [("C0AKH5SNGKH", "xoxb-test", 200, None, None, None)]


def test_missing_bot_token_fails_loudly(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)

    result = _decode(slack_mod.slack_tool({"action": "read_channel", "target": "slack:C0AKH5SNGKH"}))

    assert "SLACK_BOT_TOKEN is required" in result["error"]


def test_slack_api_error_includes_missing_scope(monkeypatch):
    async def fake_read_channel(_channel_id, **_kwargs):
        return {"ok": False, "error": "missing_scope", "needed": "channels:history"}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_read_channel", fake_read_channel)

    result = _decode(slack_mod.slack_tool({"action": "read_channel", "channel_id": "C0AKH5SNGKH"}))

    assert "missing_scope" in result["error"]
    assert result["slack_error"] == "missing_scope"


def test_channel_info_uses_native_gateway_helper(monkeypatch):
    async def fake_channel_info(channel_id, *, token=None):
        return {"ok": True, "channel": {"id": channel_id, "name": "team-tiger", "is_private": True}}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_channel_info", fake_channel_info)

    result = _decode(slack_mod.slack_tool({"action": "channel_info", "target": "slack:C0AKH5SNGKH"}))

    assert result["success"] is True
    assert result["channel"]["name"] == "team-tiger"


def test_list_channels_uses_native_gateway_helper(monkeypatch):
    async def fake_list_channels(**kwargs):
        return {"ok": True, "channels": [{"id": "C0AKH5SNGKH", "name": "team-tiger"}]}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr("gateway.platforms.slack.slack_list_channels", fake_list_channels)

    result = _decode(slack_mod.slack_tool({"action": "list_channels"}))

    assert result["success"] is True
    assert result["channel_count"] == 1


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://storyprotocol.slack.com/archives/C0AKH5SNGKH/p1777650186068179", ("C0AKH5SNGKH", "1777650186.068179")),
        ("https://storyprotocol.slack.com/archives/G1234567890/p1234567890", ("G1234567890", "1234567890.000000")),
    ],
)
def test_parse_permalink(raw, expected):
    assert slack_mod._parse_permalink(raw) == expected
