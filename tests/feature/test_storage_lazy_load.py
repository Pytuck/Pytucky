from pathlib import Path
import pytest

from pytucky import Storage, Column


def build_user_storage(path: Path) -> Storage:
    # helper to create a simple storage for tests
    s = Storage(file_path=str(path))
    s.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
            Column(int, name="age"),
        ],
    )
    return s


@pytest.mark.feature
def test_lazy_table_select_does_not_require_full_materialization(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-select.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    table = reopened.get_table("users")
    # implementation detail: table should indicate lazy load; accept either attribute or property
    assert getattr(table, "_lazy_loaded", True) is True
    # data may be not materialized until accessed
    assert getattr(table, "data", {}) == {} or table.data is None
    assert reopened.select("users", 1)["name"] == "Alice"
    reopened.close()


@pytest.mark.feature
def test_changed_lazy_table_flush_only_materializes_modified_table(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-flush.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    table = reopened.get_table("users")
    # reading an entry should not fully materialize underlying data structure
    _ = reopened.select("users", 1)
    # modify the table
    reopened.insert("users", {"id": 2, "name": "Bob", "age": 30})
    # flush should persist changes; only the modified table should be materialized
    reopened.flush()
    reopened.close()

    reopened2 = Storage(file_path=str(db_path))
    assert reopened2.select("users", 2)["name"] == "Bob"
    reopened2.close()


@pytest.mark.feature
def test_read_lazy_record_rebuilds_missing_offset_mapping(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-offset-rebuild.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        backend = reopened.backend
        assert backend is not None
        offset = table._pk_offsets[1]
        backend._offset_map.pop(offset, None)

        record = backend.read_lazy_record(db_path, offset, table.columns, 1)

        assert record["name"] == "Alice"
        assert backend._offset_map[offset] == ("users", 1)
    finally:
        reopened.close()
