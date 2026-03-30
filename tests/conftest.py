from pathlib import Path
import sys
import pytest

# Ensure tests package root is importable so tests.helpers can be imported as a top-level package
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.pytucky"
