from pathlib import Path

import pytest

from pytucky import Column, Storage
from pytucky.common.exceptions import RecordNotFoundError, ValidationError
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


@pytest.mark.feature
def test_bulk_insert_validation_failure_is_atomic() -> None:
    db = Storage(in_memory=True)
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False, index=True),
        ],
    )
    table = db.get_table("users")
    table.reset_dirty()
    db._dirty = False

    with pytest.raises(ValidationError, match="cannot be null"):
        db.bulk_insert("users", [{"name": "ok"}, {"name": None}])

    assert table.next_id == 1
    assert table.data == {}
    assert table.indexes["name"].lookup("ok") == set()
    assert not table.is_dirty
    assert db._dirty is False


@pytest.mark.feature
def test_bulk_update_validation_failure_is_atomic() -> None:
    db = Storage(in_memory=True)
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False, index=True),
        ],
    )
    db.bulk_insert("users", [{"name": "A"}, {"name": "B"}])
    table = db.get_table("users")
    table.reset_dirty()
    db._dirty = False

    with pytest.raises(ValidationError, match="cannot be null"):
        db.bulk_update("users", [(1, {"name": "A1"}), (2, {"name": None})])

    assert db.select("users", 1)["name"] == "A"
    assert db.select("users", 2)["name"] == "B"
    assert table.indexes["name"].lookup("A") == {1}
    assert table.indexes["name"].lookup("A1") == set()
    assert not table.is_dirty
    assert db._dirty is False

    with pytest.raises(RecordNotFoundError):
        db.bulk_update("users", [(1, {"name": "A1"}), (999, {"name": "missing"})])
    assert db.select("users", 1)["name"] == "A"
