"""Tests for the RSI company_knowledge plugin."""

import json

from plugins import company_knowledge


class _FakeContext:
    def __init__(self):
        self.tools = []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)


def test_company_knowledge_registers_expected_tools():
    ctx = _FakeContext()
    company_knowledge.register(ctx)
    names = {tool["name"] for tool in ctx.tools}
    assert names == {"wiki_search", "wiki_page_get", "wiki_edit_propose", "wiki_edit_apply"}
    assert {tool["toolset"] for tool in ctx.tools} == {"company_knowledge"}


def test_wiki_page_get_encodes_slug(monkeypatch):
    calls = []

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "path": path}

    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    out = json.loads(company_knowledge._handle_wiki_page_get({"page_ref": "runbooks/deploy v1"}))
    assert out["ok"] is True
    assert calls == [("GET", "/internal/company-wiki/pages/runbooks%2Fdeploy%20v1", None, None)]


def test_wiki_edit_apply_preserves_structured_citations(monkeypatch):
    calls = []

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "audit": {"status": "published"}}

    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    result = json.loads(
        company_knowledge._handle_wiki_edit_apply(
            {
                "actor": "hermes",
                "reason": "publish sourced correction",
                "idempotency_key": "idem-1",
                "slug": "runbooks/deploy",
                "title": "Deploy",
                "body": "---\ntitle: Deploy\n---\n# Deploy\n",
                "citations": [
                    {
                        "claim_key": "deploy",
                        "source_document_id": "srcdoc_1",
                        "source_revision_id": "srcrev_1",
                        "chunk_id": "srcchunk_1",
                    }
                ],
            }
        )
    )
    assert result["ok"] is True
    _, path, _, payload = calls[0]
    assert path == "/internal/company-wiki/edits/apply"
    assert payload["citations"][0]["chunk_id"] == "srcchunk_1"

