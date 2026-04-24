from pathlib import Path
import importlib.util

import pytest

from tests.helpers.pytuck_compat import load_pytuck_symbols


def test_load_pytuck_symbols_raises_when_pytuck_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    original_exists = Path.exists
    original_find_spec = importlib.util.find_spec

    def patched_exists(path: Path) -> bool:
        if path.name == "pytuck" and path.parent == repo_root.parent:
            return False
        return original_exists(path)

    def patched_find_spec(name: str, package: str | None = None):
        if name == "pytuck":
            return None
        return original_find_spec(name, package)

    monkeypatch.setattr(Path, "exists", patched_exists)
    monkeypatch.setattr(importlib.util, "find_spec", patched_find_spec)

    with pytest.raises(AssertionError, match="Cannot import 'pytuck'"):
        load_pytuck_symbols(repo_root=repo_root)
