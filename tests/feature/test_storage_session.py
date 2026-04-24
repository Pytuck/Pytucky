from pathlib import Path

import pytest

from pytucky import Storage
from tests.helpers.factories import build_user_storage


@pytest.mark.feature
@pytest.mark.parametrize(
    "payload",
    [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": None},
    ],
)
def test_storage_insert_select_roundtrip(tmp_path: Path, payload: dict) -> None:
    db = build_user_storage(tmp_path / "storage-basic.pytucky")
    try:
        db.insert("users", payload)
        db.flush()
    finally:
        # build_user_storage 返回的 db 在这里关闭
        db.close()

    reopened = Storage(file_path=tmp_path / "storage-basic.pytucky")
    try:
        row = reopened.select("users", payload["id"])
        assert row["name"] == payload["name"]
    finally:
        reopened.close()
