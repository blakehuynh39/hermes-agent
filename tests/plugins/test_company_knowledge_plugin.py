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
    assert names == {"wiki_search", "wiki_page_get", "wiki_index_get", "wiki_log_get", "wiki_edit_propose", "wiki_edit_apply"}
    assert {tool["toolset"] for tool in ctx.tools} == {"company_knowledge"}


def test_wiki_page_get_preserves_slug_slashes(monkeypatch):
    calls = []

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "path": path}

    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    out = json.loads(company_knowledge._handle_wiki_page_get({"page_ref": "runbooks/deploy v1"}))
    assert out["ok"] is True
    assert calls == [("GET", "/internal/company-wiki/pages/runbooks/deploy%20v1", None, None)]


def test_wiki_index_get_reads_local_generated_catalog(monkeypatch, tmp_path):
    calls = []
    wiki_root = tmp_path / "wiki"
    wiki_root.mkdir()
    (wiki_root / "index.md").write_text("# Company Wiki Index\n\n- Deploy\n", encoding="utf-8")

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "content": "api should not be called"}

    monkeypatch.setenv("RSI_COMPANY_WIKI_ROOT", str(wiki_root))
    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    out = json.loads(company_knowledge._handle_wiki_index_get({}))
    assert out["ok"] is True
    assert out["path"] == "index.md"
    assert "Deploy" in out["content"]
    assert calls == []


def test_wiki_index_get_requires_local_root(monkeypatch):
    calls = []
    monkeypatch.delenv("RSI_COMPANY_WIKI_ROOT", raising=False)

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "content": "# Company Wiki Index"}

    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    out = json.loads(company_knowledge._handle_wiki_index_get({}))
    assert "error" in out
    assert "RSI_COMPANY_WIKI_ROOT" in out["error"]
    assert calls == []


def test_wiki_log_get_reads_local_log_and_clamps_limit(monkeypatch, tmp_path):
    calls = []
    wiki_root = tmp_path / "wiki"
    wiki_root.mkdir()
    (wiki_root / "log.md").write_text(
        "# Company Wiki Log\n\n"
        "## [2026-05-01T00:00:00Z] ingest | One\n\n"
        "## [2026-05-02T00:00:00Z] ingest | Two\n\n"
        "## [2026-05-03T00:00:00Z] ingest | Three\n",
        encoding="utf-8",
    )

    def fake_request(method, path, *, params=None, payload=None):
        calls.append((method, path, params, payload))
        return {"ok": True, "content": "api should not be called"}

    monkeypatch.setenv("RSI_COMPANY_WIKI_ROOT", str(wiki_root))
    monkeypatch.setattr(company_knowledge, "_api_request", fake_request)
    out = json.loads(company_knowledge._handle_wiki_log_get({"limit": 2}))
    assert out["ok"] is True
    assert "One" not in out["content"]
    assert "Two" in out["content"]
    assert "Three" in out["content"]
    assert calls == []


def test_wiki_log_get_returns_empty_local_log_for_initialized_unpublished_root(monkeypatch, tmp_path):
    wiki_root = tmp_path / "wiki"
    wiki_root.mkdir()
    monkeypatch.setenv("RSI_COMPANY_WIKI_ROOT", str(wiki_root))
    out = json.loads(company_knowledge._handle_wiki_log_get({}))
    assert out["ok"] is True
    assert out["path"] == "log.md"
    assert "_No wiki log entries yet._" in out["content"]


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
