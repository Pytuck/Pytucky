from pathlib import Path

import pytest

from pytucky import Storage
from tests.helpers.factories import build_user_storage


@pytest.mark.feature
def test_ptk6_roundtrip_smoke(tmp_path: Path) -> None:
    db_path = tmp_path / "smoke.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=db_path, engine="pytucky")
    assert reopened.select("users", 1)["name"] == "Alice"
    reopened.close()
