from pathlib import Path
import pytest

# expose pytest-generated tmp_path to helpers if needed

@pytest.fixture
def tmp_file_path(tmp_path: Path) -> Path:
    return tmp_path / "testdb.pytuck"
