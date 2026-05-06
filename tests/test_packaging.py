from pathlib import Path
import tomllib


def test_postgres_state_module_is_packaged() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    modules = pyproject["tool"]["setuptools"]["py-modules"]

    assert "hermes_state_postgres" in modules
