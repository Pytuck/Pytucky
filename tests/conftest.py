from pathlib import Path
import sys
import pytest


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.pytucky"
