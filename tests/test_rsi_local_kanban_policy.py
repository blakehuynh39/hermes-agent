import argparse
import json

import pytest

from hermes_cli import kanban as kanban_cli
from hermes_cli import kanban_db
from tools import terminal_tool as terminal_tool_module


DISABLED_ENV = "RSI_DISABLE_HERMES_LOCAL_KANBAN"


def test_kanban_cli_is_disabled_in_rsi_mode(monkeypatch, capsys):
    monkeypatch.setenv(DISABLED_ENV, "true")

    code = kanban_cli.kanban_command(argparse.Namespace(kanban_action="list"))

    assert code == 2
    assert "Hermes local Kanban is disabled in RSI" in capsys.readouterr().err


def test_kanban_slash_is_disabled_in_rsi_mode(monkeypatch):
    monkeypatch.setenv(DISABLED_ENV, "true")

    out = kanban_cli.run_slash("list")

    assert "Hermes local Kanban is disabled in RSI" in out


def test_kanban_db_open_is_disabled_in_rsi_mode(monkeypatch, tmp_path):
    monkeypatch.setenv(DISABLED_ENV, "true")

    with pytest.raises(RuntimeError, match="Hermes local Kanban is disabled in RSI"):
        kanban_db.connect(tmp_path / "kanban.db")

    with pytest.raises(RuntimeError, match="Hermes local Kanban is disabled in RSI"):
        kanban_db.init_db(tmp_path / "kanban.db")


@pytest.mark.parametrize(
    "command",
    [
        "hermes kanban boards list",
        "/usr/local/bin/hermes kanban create 'x'",
        "sudo hermes kanban list",
        "env FOO=bar hermes kanban list",
        "python -m hermes_cli.kanban list",
        "python3 -m hermes_cli.main kanban list",
        "uv run hermes kanban list",
        "poetry run python -m hermes_cli.kanban list",
    ],
)
def test_terminal_policy_detects_local_kanban_commands(command):
    assert terminal_tool_module._command_invokes_local_hermes_kanban(command)


@pytest.mark.parametrize(
    "command",
    [
        "echo hermes kanban",
        "printf '%s\\n' 'python -m hermes_cli.kanban list'",
        "search_files 'hermes kanban'",
    ],
)
def test_terminal_policy_does_not_block_plain_text_mentions(command):
    assert not terminal_tool_module._command_invokes_local_hermes_kanban(command)


def test_terminal_tool_blocks_local_kanban_before_environment_setup(monkeypatch):
    monkeypatch.setenv(DISABLED_ENV, "true")

    result = json.loads(terminal_tool_module.terminal_tool("hermes kanban boards list"))

    assert result["status"] == "blocked"
    assert result["exit_code"] == -1
    assert "Hermes local Kanban is disabled in RSI" in result["error"]
