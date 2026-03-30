from pathlib import Path
import pytest

from pytucky import Storage, Column


@pytest.mark.feature
def test_index_is_restored_after_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "index-reopen.pytucky"
    db = Storage(file_path=str(db_path))
    # create table and columns
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True),
        ],
    )
    db.insert("users", {"id": 1, "name": "Alice"})
    db.insert("users", {"id": 2, "name": "Alice"})
    db.flush()
    db.close()

    reopened = Storage(file_path=str(db_path))
    table = reopened.get_table("users")
    assert getattr(table, "_lazy_loaded", True) is True
    assert getattr(table, "data", {}) == {} or table.data is None

    idx = table.indexes.get("name")
    # index lookup may return list or set; normalize to set of ints
    found = set(idx.lookup("Alice")) if idx is not None else set()
    assert found == {1, 2}
    assert getattr(table, "data", {}) == {} or table.data is None
    reopened.close()
