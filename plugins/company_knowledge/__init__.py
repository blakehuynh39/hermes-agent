"""Native RSI company knowledge tools.

The Platform source ledger is the authority. These tools call the Platform
control-plane company-wiki API and never mutate Markdown files directly.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from tools.registry import tool_error, tool_result

PLUGIN_NAME = "company_knowledge"
TOOLSET = "company_knowledge"


def _base_url() -> str:
    return os.getenv("RSI_CONTROL_PLANE_BASE_URL", "").strip().rstrip("/")


def _check_available() -> bool:
    return bool(_base_url())


def _api_request(method: str, path: str, *, params: dict[str, Any] | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    base = _base_url()
    if not base:
        raise RuntimeError("RSI_CONTROL_PLANE_BASE_URL is required for company_knowledge tools")
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    body: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path + query, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310 - Platform URL is deployment config.
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"company wiki API returned HTTP {exc.code}: {_truncate(raw, 800)}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"company wiki API request failed: {exc.reason}") from exc
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _truncate(value: str, limit: int) -> str:
    value = str(value or "")
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def _as_int(raw: Any, *, default: int = 10, minimum: int = 1, maximum: int = 50) -> int:
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _as_citations(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("citations must be a list")
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each citation must be an object")
        out.append(dict(item))
    return out


def _handle_wiki_search(args: dict, **_kwargs) -> str:
    query = str(args.get("query") or "").strip()
    limit = _as_int(args.get("limit"), default=10)
    try:
        return tool_result(_api_request("GET", "/internal/company-wiki/search", params={"query": query, "limit": limit}))
    except Exception as exc:
        return tool_error(f"wiki_search failed: {exc}")


def _handle_wiki_page_get(args: dict, **_kwargs) -> str:
    page_ref = str(args.get("page_ref") or args.get("slug") or "").strip()
    if not page_ref:
        return tool_error("wiki_page_get requires page_ref")
    try:
        encoded = urllib.parse.quote(page_ref, safe="")
        return tool_result(_api_request("GET", f"/internal/company-wiki/pages/{encoded}"))
    except Exception as exc:
        return tool_error(f"wiki_page_get failed: {exc}")


def _handle_wiki_index_get(args: dict, **_kwargs) -> str:
    try:
        return tool_result(_api_request("GET", "/internal/company-wiki/index"))
    except Exception as exc:
        return tool_error(f"wiki_index_get failed: {exc}")


def _handle_wiki_log_get(args: dict, **_kwargs) -> str:
    limit = _as_int(args.get("limit"), default=20, maximum=100)
    try:
        return tool_result(_api_request("GET", "/internal/company-wiki/log", params={"limit": limit}))
    except Exception as exc:
        return tool_error(f"wiki_log_get failed: {exc}")


def _handle_wiki_edit_propose(args: dict, **_kwargs) -> str:
    try:
        payload = {
            "actor": str(args.get("actor") or "").strip(),
            "reason": str(args.get("reason") or "").strip(),
            "idempotency_key": str(args.get("idempotency_key") or "").strip(),
            "slug": str(args.get("slug") or "").strip(),
            "title": str(args.get("title") or "").strip(),
            "body": str(args.get("body") or ""),
            "citations": _as_citations(args.get("citations")),
            "metadata": args.get("metadata") if isinstance(args.get("metadata"), dict) else {},
        }
        return tool_result(_api_request("POST", "/internal/company-wiki/edits/propose", payload=payload))
    except Exception as exc:
        return tool_error(f"wiki_edit_propose failed: {exc}")


def _handle_wiki_edit_apply(args: dict, **_kwargs) -> str:
    try:
        payload = {
            "actor": str(args.get("actor") or "").strip(),
            "reason": str(args.get("reason") or "").strip(),
            "idempotency_key": str(args.get("idempotency_key") or "").strip(),
            "slug": str(args.get("slug") or "").strip(),
            "title": str(args.get("title") or "").strip(),
            "body": str(args.get("body") or ""),
            "citations": _as_citations(args.get("citations")),
            "metadata": args.get("metadata") if isinstance(args.get("metadata"), dict) else {},
        }
        return tool_result(_api_request("POST", "/internal/company-wiki/edits/apply", payload=payload))
    except Exception as exc:
        return tool_error(f"wiki_edit_apply failed: {exc}")


WIKI_SEARCH_SCHEMA = {
    "name": "wiki_search",
    "description": "Search the compiled RSI company wiki. Results are canonical Markdown pages backed by source citations.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Search query."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
        },
        "required": ["query"],
    },
}

WIKI_PAGE_GET_SCHEMA = {
    "name": "wiki_page_get",
    "description": "Read a compiled company wiki page by slug or page id, including citations and manifest metadata.",
    "parameters": {
        "type": "object",
        "properties": {
            "page_ref": {"type": "string", "description": "Wiki page id or slug."},
        },
        "required": ["page_ref"],
    },
}

WIKI_INDEX_GET_SCHEMA = {
    "name": "wiki_index_get",
    "description": "Read the generated company wiki index.md catalog before drilling into relevant canonical pages.",
    "parameters": {
        "type": "object",
        "properties": {},
    },
}

WIKI_LOG_GET_SCHEMA = {
    "name": "wiki_log_get",
    "description": "Read recent append-only company wiki log.md entries. Entries start with '## [' for simple Unix parsing.",
    "parameters": {
        "type": "object",
        "properties": {
            "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
        },
    },
}

_CITATION_SCHEMA = {
    "type": "object",
    "properties": {
        "claim_key": {"type": "string"},
        "source_document_id": {"type": "string"},
        "source_revision_id": {"type": "string"},
        "chunk_id": {"type": "string"},
        "native_locator": {"type": "string"},
        "quote": {"type": "string"},
    },
    "required": ["claim_key", "source_document_id", "source_revision_id", "chunk_id"],
}

_EDIT_COMMON_PROPERTIES = {
    "actor": {"type": "string", "description": "Actor applying or proposing the wiki change."},
    "reason": {"type": "string", "description": "Operational reason for the edit."},
    "idempotency_key": {"type": "string", "description": "Stable key for deduplicating the mutation."},
    "slug": {"type": "string", "description": "Wiki page slug."},
    "title": {"type": "string", "description": "Wiki page title."},
    "body": {"type": "string", "description": "Markdown body. Apply requires YAML frontmatter."},
    "citations": {"type": "array", "items": _CITATION_SCHEMA},
    "metadata": {"type": "object"},
}

WIKI_EDIT_PROPOSE_SCHEMA = {
    "name": "wiki_edit_propose",
    "description": "Create an audited draft/intention for a company wiki edit without publishing Markdown.",
    "parameters": {
        "type": "object",
        "properties": _EDIT_COMMON_PROPERTIES,
        "required": ["actor", "reason", "idempotency_key", "slug", "title"],
    },
}

WIKI_EDIT_APPLY_SCHEMA = {
    "name": "wiki_edit_apply",
    "description": "Publish a validated company wiki edit through Platform audit, citation, manifest, and atomic Markdown publish paths.",
    "parameters": {
        "type": "object",
        "properties": _EDIT_COMMON_PROPERTIES,
        "required": ["actor", "reason", "idempotency_key", "slug", "title", "body", "citations"],
    },
}


def register(ctx) -> None:
    ctx.register_tool(
        name="wiki_search",
        toolset=TOOLSET,
        schema=WIKI_SEARCH_SCHEMA,
        handler=_handle_wiki_search,
        check_fn=_check_available,
        description=WIKI_SEARCH_SCHEMA["description"],
    )
    ctx.register_tool(
        name="wiki_page_get",
        toolset=TOOLSET,
        schema=WIKI_PAGE_GET_SCHEMA,
        handler=_handle_wiki_page_get,
        check_fn=_check_available,
        description=WIKI_PAGE_GET_SCHEMA["description"],
    )
    ctx.register_tool(
        name="wiki_index_get",
        toolset=TOOLSET,
        schema=WIKI_INDEX_GET_SCHEMA,
        handler=_handle_wiki_index_get,
        check_fn=_check_available,
        description=WIKI_INDEX_GET_SCHEMA["description"],
    )
    ctx.register_tool(
        name="wiki_log_get",
        toolset=TOOLSET,
        schema=WIKI_LOG_GET_SCHEMA,
        handler=_handle_wiki_log_get,
        check_fn=_check_available,
        description=WIKI_LOG_GET_SCHEMA["description"],
    )
    ctx.register_tool(
        name="wiki_edit_propose",
        toolset=TOOLSET,
        schema=WIKI_EDIT_PROPOSE_SCHEMA,
        handler=_handle_wiki_edit_propose,
        check_fn=_check_available,
        description=WIKI_EDIT_PROPOSE_SCHEMA["description"],
    )
    ctx.register_tool(
        name="wiki_edit_apply",
        toolset=TOOLSET,
        schema=WIKI_EDIT_APPLY_SCHEMA,
        handler=_handle_wiki_edit_apply,
        check_fn=_check_available,
        description=WIKI_EDIT_APPLY_SCHEMA["description"],
    )
