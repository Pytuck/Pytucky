from pathlib import Path
from typing import Type

import pytest

from pytucky import Column, Storage, declarative_base
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
    db.insert("users", payload)
    db.flush()
    db.close()

    reopened = Storage(file_path=tmp_path / "storage-basic.pytucky", engine="pytucky")
    row = reopened.select("users", payload["id"])
    assert row["name"] == payload["name"]
    reopened.close()
