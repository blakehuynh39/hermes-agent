"""Native Slack tool for reading Slack context with the bot token.

This complements ``send_message`` in the ``messaging`` toolset. Gateway remains
the native Slack ingress, but CLI/API-server sessions also need a first-class
way to read Slack permalinks and thread context when Slack is not the ingress.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any

from agent.redact import redact_sensitive_text
from tools.registry import registry, tool_error, tool_result


_SLACK_TARGET_RE = re.compile(r"^\s*(?:slack:)?([CGD][A-Z0-9]{8,})(?::([0-9]{1,20}(?:\.[0-9]{1,20})?))?\s*$")
_SLACK_PERMALINK_RE = re.compile(r"https://[^/\s]+/archives/([CGD][A-Z0-9]{8,})/p([0-9]{10,20})")


SLACK_SCHEMA = {
    "name": "slack",
    "description": (
        "Read Slack context using the native Slack bot token. Use this when the "
        "user provides a Slack permalink, asks about a Slack thread/channel, or "
        "needs recent Slack messages before replying. To send a Slack reply, use "
        "send_message from this same messaging toolset."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["read_thread", "read_channel", "channel_info", "list_channels"],
                "description": (
                    "read_thread fetches replies for a Slack thread/permalink; "
                    "read_channel fetches recent channel history; channel_info "
                    "inspects one conversation; list_channels lists conversations visible to the bot."
                ),
            },
            "permalink": {
                "type": "string",
                "description": "Slack message permalink, e.g. https://workspace.slack.com/archives/C123/p1777650186068179.",
            },
            "target": {
                "type": "string",
                "description": "Slack target, either slack:<channel_id> or slack:<channel_id>:<thread_ts>.",
            },
            "channel_id": {
                "type": "string",
                "description": "Slack conversation ID, such as C..., G..., or D....",
            },
            "thread_ts": {
                "type": "string",
                "description": "Slack thread timestamp, such as 1777650186.068179.",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 200,
                "description": "Maximum messages or channels to return. Defaults to 50.",
            },
            "cursor": {
                "type": "string",
                "description": "Slack pagination cursor from a previous result.",
            },
            "oldest": {
                "type": "string",
                "description": "Optional oldest timestamp for read_channel.",
            },
            "latest": {
                "type": "string",
                "description": "Optional latest timestamp for read_channel.",
            },
            "types": {
                "type": "string",
                "description": "Conversation types for list_channels. Defaults to public_channel,private_channel,mpim,im.",
            },
        },
        "required": ["action"],
    },
}


def slack_tool(args: dict[str, Any], **_kw: Any) -> str:
    """Synchronous registry handler for native Slack reads."""
    try:
        return _run_async(_slack_tool_async(args or {}))
    except Exception as exc:
        return tool_error(f"slack tool failed: {_sanitize_error(exc)}")


async def _slack_tool_async(args: dict[str, Any]) -> str:
    action = str(args.get("action") or "").strip()
    token = _slack_bot_token()
    if not token:
        return tool_error("SLACK_BOT_TOKEN is required for the native Slack tool.")

    if action == "read_thread":
        return await _read_thread(token, args)
    if action == "read_channel":
        return await _read_channel(token, args)
    if action == "channel_info":
        return await _channel_info(token, args)
    if action == "list_channels":
        return await _list_channels(token, args)
    return tool_error("Unknown Slack action. Use: read_thread, read_channel, channel_info, list_channels.")


async def _read_thread(token: str, args: dict[str, Any]) -> str:
    channel_id, thread_ts = _resolve_channel_and_thread(args, require_thread=True)
    if not channel_id or not thread_ts:
        return tool_error("read_thread requires permalink, target=slack:<channel_id>:<thread_ts>, or channel_id + thread_ts.")

    from gateway.platforms.slack import slack_read_thread

    data = await slack_read_thread(
        channel_id,
        thread_ts,
        token=token,
        limit=_bounded_limit(args.get("limit"), default=50),
        cursor=str(args["cursor"]) if args.get("cursor") else None,
        inclusive=True,
    )
    if not data.get("ok"):
        return _slack_api_error("conversations.replies", data)

    messages = [_serialize_message(item) for item in data.get("messages", []) if isinstance(item, dict)]
    return tool_result(
        success=True,
        platform="slack",
        action="read_thread",
        channel_id=channel_id,
        thread_ts=thread_ts,
        messages=messages,
        message_count=len(messages),
        has_more=bool(data.get("has_more")),
        next_cursor=_next_cursor(data),
    )


async def _read_channel(token: str, args: dict[str, Any]) -> str:
    channel_id, _thread_ts = _resolve_channel_and_thread(args, require_thread=False)
    if not channel_id:
        return tool_error("read_channel requires target=slack:<channel_id> or channel_id.")

    from gateway.platforms.slack import slack_read_channel

    data = await slack_read_channel(
        channel_id,
        token=token,
        limit=_bounded_limit(args.get("limit"), default=50),
        cursor=str(args["cursor"]) if args.get("cursor") else None,
        oldest=str(args["oldest"]) if args.get("oldest") else None,
        latest=str(args["latest"]) if args.get("latest") else None,
    )
    if not data.get("ok"):
        return _slack_api_error("conversations.history", data)

    messages = [_serialize_message(item) for item in data.get("messages", []) if isinstance(item, dict)]
    return tool_result(
        success=True,
        platform="slack",
        action="read_channel",
        channel_id=channel_id,
        messages=messages,
        message_count=len(messages),
        has_more=bool(data.get("has_more")),
        next_cursor=_next_cursor(data),
    )


async def _channel_info(token: str, args: dict[str, Any]) -> str:
    channel_id, _thread_ts = _resolve_channel_and_thread(args, require_thread=False)
    if not channel_id:
        return tool_error("channel_info requires target=slack:<channel_id> or channel_id.")

    from gateway.platforms.slack import slack_channel_info

    data = await slack_channel_info(channel_id, token=token)
    if not data.get("ok"):
        return _slack_api_error("conversations.info", data)

    return tool_result(
        success=True,
        platform="slack",
        action="channel_info",
        channel=_serialize_conversation(data.get("channel") or {}),
    )


async def _list_channels(token: str, args: dict[str, Any]) -> str:
    from gateway.platforms.slack import slack_list_channels

    data = await slack_list_channels(
        token=token,
        limit=_bounded_limit(args.get("limit"), default=100),
        cursor=str(args["cursor"]) if args.get("cursor") else None,
        types=str(args.get("types") or "public_channel,private_channel,mpim,im"),
    )
    if not data.get("ok"):
        return _slack_api_error("conversations.list", data)

    channels = [_serialize_conversation(item) for item in data.get("channels", []) if isinstance(item, dict)]
    return tool_result(
        success=True,
        platform="slack",
        action="list_channels",
        channels=channels,
        channel_count=len(channels),
        next_cursor=_next_cursor(data),
    )


def _resolve_channel_and_thread(args: dict[str, Any], *, require_thread: bool) -> tuple[str | None, str | None]:
    if args.get("permalink"):
        parsed = _parse_permalink(str(args["permalink"]))
        if parsed:
            return parsed

    if args.get("target"):
        parsed = _parse_target(str(args["target"]))
        if parsed and (parsed[1] or not require_thread):
            return parsed

    channel_id = str(args.get("channel_id") or "").strip() or None
    thread_ts = str(args.get("thread_ts") or "").strip() or None
    if channel_id and (thread_ts or not require_thread):
        return channel_id, thread_ts
    return None, None


def _parse_target(target: str) -> tuple[str, str | None] | None:
    match = _SLACK_TARGET_RE.match(target or "")
    if not match:
        return None
    return match.group(1), match.group(2)


def _parse_permalink(permalink: str) -> tuple[str, str] | None:
    match = _SLACK_PERMALINK_RE.search(permalink or "")
    if not match:
        return None
    channel_id = match.group(1)
    raw_ts = match.group(2)
    seconds = raw_ts[:10]
    micros = (raw_ts[10:] or "0")[:6].ljust(6, "0")
    return channel_id, f"{seconds}.{micros}"


def _serialize_message(message: dict[str, Any]) -> dict[str, Any]:
    text = _message_text(message)
    files = []
    for file_obj in message.get("files") or []:
        if not isinstance(file_obj, dict):
            continue
        files.append(
            {
                "id": file_obj.get("id"),
                "name": file_obj.get("name"),
                "title": file_obj.get("title"),
                "mimetype": file_obj.get("mimetype"),
                "filetype": file_obj.get("filetype"),
                "size": file_obj.get("size"),
            }
        )

    result: dict[str, Any] = {
        "ts": message.get("ts"),
        "thread_ts": message.get("thread_ts"),
        "user": message.get("user"),
        "bot_id": message.get("bot_id"),
        "username": message.get("username"),
        "subtype": message.get("subtype"),
        "text": text,
    }
    if files:
        result["files"] = files
    return result


def _serialize_conversation(channel: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id",
        "name",
        "is_channel",
        "is_group",
        "is_im",
        "is_mpim",
        "is_private",
        "is_archived",
        "is_member",
        "num_members",
    )
    return {key: channel.get(key) for key in keys if key in channel}


def _message_text(message: dict[str, Any]) -> str:
    text = str(message.get("text") or "")
    if not text and message.get("blocks"):
        try:
            from gateway.platforms.slack import _extract_text_from_slack_blocks

            text = _extract_text_from_slack_blocks(message.get("blocks") or [])
        except Exception:
            text = ""
    text = redact_sensitive_text(text)
    if len(text) > 6000:
        return text[:6000] + "\n...[truncated]"
    return text


def _bounded_limit(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, 200))


def _next_cursor(data: dict[str, Any]) -> str:
    metadata = data.get("response_metadata")
    if isinstance(metadata, dict):
        return str(metadata.get("next_cursor") or "")
    return ""


def _slack_api_error(method: str, data: dict[str, Any]) -> str:
    code = data.get("error") or "unknown_error"
    needed = data.get("needed")
    provided = data.get("provided")
    detail = str(code)
    if needed:
        detail += f" (needed scope: {needed})"
    if provided:
        detail += f" (provided scopes: {provided})"
    return tool_error(f"Slack {method} failed: {_sanitize_error(detail)}", slack_error=code)


def _slack_bot_token() -> str:
    return (os.getenv("SLACK_BOT_TOKEN") or "").strip()


def _check_slack_tool() -> bool:
    return bool(_slack_bot_token())


def _sanitize_error(value: Any) -> str:
    return redact_sensitive_text(str(value))


def _run_async(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    return asyncio.run(coro)


registry.register(
    name="slack",
    toolset="messaging",
    schema=SLACK_SCHEMA,
    handler=slack_tool,
    check_fn=_check_slack_tool,
    emoji="💬",
    max_result_size_chars=80_000,
)
