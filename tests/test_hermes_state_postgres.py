from hermes_state import SessionDB, SQLiteSessionDB
from hermes_state_postgres import PostgresSessionDB


def test_default_session_db_backend_remains_sqlite(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_STATE_BACKEND", raising=False)
    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        assert isinstance(db, SQLiteSessionDB)
        db.create_session("s1", "cli")
        assert db.get_session("s1")["source"] == "cli"
    finally:
        db.close()


def test_postgres_content_encoding_avoids_nul_sentinel():
    content = [{"type": "text", "text": "hello"}, {"type": "image_url", "image_url": {}}]
    encoded = PostgresSessionDB._encode_content(content)
    assert isinstance(encoded, str)
    assert "\x00" not in encoded
    assert encoded.startswith(PostgresSessionDB._PG_CONTENT_JSON_PREFIX)
    assert PostgresSessionDB._decode_content(encoded) == content


def test_postgres_content_decoder_accepts_legacy_sqlite_sentinel():
    legacy = SQLiteSessionDB._encode_content([{"type": "text", "text": "legacy"}])
    assert PostgresSessionDB._decode_content(legacy) == [{"type": "text", "text": "legacy"}]


def test_postgres_query_parser_preserves_common_fts5_subset():
    parsed = PostgresSessionDB._parse_compatible_query('"exact phrase" deploy* AND postgres NOT sqlite OR pgvector')
    assert "exact phrase" in parsed["positive"]
    assert "deploy" in parsed["positive"]
    assert "deploy" in parsed["prefix"]
    assert "sqlite" in parsed["negative"]
    assert parsed["has_or"] is True
    assert "*" not in parsed["websearch"]


def test_postgres_conninfo_normalizes_sqlalchemy_psycopg_scheme():
    assert (
        PostgresSessionDB._normalize_conninfo("postgresql+psycopg://u:p@host/db")
        == "postgresql://u:p@host/db"
    )


def test_postgres_vector_literal_is_pgvector_compatible():
    assert PostgresSessionDB._vector_literal([0, 1.25, -2]) == "[0,1.25,-2]"
