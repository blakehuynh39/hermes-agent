# Company Knowledge Wiki

Use this skill when a task asks for RSI company knowledge, operational history,
Notion/Slack-derived facts, runbooks, policies, or durable wiki updates.

The Platform source ledger is authoritative. Honcho is semantic retrieval. The
Markdown wiki is the published, agent-readable artifact.

## Read Path

1. Start with `wiki_search` for company facts, runbooks, decisions, and
   historical Slack/Notion evidence.
2. Use `wiki_page_get` on the best matching page before answering.
3. Cite source IDs from the page when stating company facts:
   `source_document_id`, `source_revision_id`, and `chunk_id`.
4. Surface conflicts when a page includes a conflict section or conflicting
   citations. Do not hide uncertainty.

Local filesystem search is allowed only for inspection, for example:

```bash
rg "deploy" "$RSI_COMPANY_WIKI_ROOT"
```

Do not edit wiki files with shell commands or generic file tools. Direct file
edits bypass audit and can be overwritten by the compiler.

## Write Path

Use `wiki_edit_propose` for drafts or uncertain edits.

Use `wiki_edit_apply` only when all of these are true:

- The edit includes YAML frontmatter.
- Every factual claim has citations with `source_document_id`,
  `source_revision_id`, and `chunk_id`.
- The request has a clear actor, reason, and idempotency key.
- The update is a structured correction or full-page replacement that preserves
  citation provenance.

If evidence is missing, search or ask for source material instead of publishing
an uncited wiki change.
