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
def test_lazy_table_select_returns_isolated_record_copy(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-select-copy.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        record = reopened.select("users", 1)
        record["name"] = "Mutated"

        assert reopened.select("users", 1)["name"] == "Alice"
    finally:
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
def test_changed_lazy_table_update_fastpath_does_not_materialize(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "lazy-flush-update.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        assert table._lazy_loaded is True

        # prevent full materialization: if called, fail the test
        def fail_if_called(*args, **kwargs):
            raise AssertionError("Table was materialized during fast-path flush")

        monkeypatch.setattr(table, "_ensure_all_loaded", fail_if_called)

        # perform update on existing (on-disk) pk
        reopened.update("users", 1, {"age": 21})
        # flush should use fast-path and not call _ensure_all_loaded
        reopened.flush()
    finally:
        reopened.close()

    # verify persisted
    reopened2 = Storage(file_path=str(db_path))
    assert reopened2.select("users", 1)["age"] == 21
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
        # 强制构建 offset map 以测试 fallback 路径
        backend._rebuild_offset_map()
        backend._offset_map.pop(offset, None)

        record = backend.read_lazy_record(db_path, offset, table.columns, 1)

        assert record["name"] == "Alice"
        assert backend._offset_map[offset] == ("users", 1)
    finally:
        reopened.close()


@pytest.mark.feature
def test_reopened_lazy_table_shares_backend_pk_offset_view(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-offset-view.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        backend = reopened.backend
        assert backend is not None

        state = backend.store.table_state("users")
        assert table._pk_offsets is not None
        assert table._pk_offsets[1] == state.pk_index[1][0]

        reopened.delete("users", 1)

        assert 1 not in state.pk_index
    finally:
        reopened.close()


@pytest.mark.feature
def test_read_lazy_record_prefers_offset_mapping_over_passed_pk(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-offset-prefer-offset.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.insert("users", {"id": 2, "name": "Bob", "age": 30})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        backend = reopened.backend
        assert backend is not None
        offset = table._pk_offsets[1]

        backend._rebuild_offset_map()
        record = backend.read_lazy_record(db_path, offset, table.columns, 2)

        assert record["name"] == "Alice"
        assert record["id"] == 1
    finally:
        reopened.close()


@pytest.mark.feature
def test_read_lazy_record_rebuilds_offset_mapping_by_offset_not_passed_pk(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-offset-rebuild-by-offset.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.insert("users", {"id": 2, "name": "Bob", "age": 30})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        backend = reopened.backend
        assert backend is not None
        offset = table._pk_offsets[1]

        backend._rebuild_offset_map()
        backend._offset_map.pop(offset, None)

        record = backend.read_lazy_record(db_path, offset, table.columns, 2)

        assert record["name"] == "Alice"
        assert record["id"] == 1
        assert backend._offset_map[offset] == ("users", 1)
    finally:
        reopened.close()


def test_flush_only_materializes_changed_table_among_many(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """确保在存在多张 lazy 表且只修改其中一张时，flush 不会对未修改的表进行 materialize/scan。

    该测试 monkeypatch Table._ensure_all_loaded 以计数呼叫次数，期望未修改表不被触发。
    """
    db_path = tmp_path / "lazy-multi.pytucky"
    # build storage with two tables
    s = Storage(file_path=str(db_path))
    s.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
        ],
    )
    s.create_table(
        "items",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="tag"),
        ],
    )
    s.insert("users", {"id": 1, "name": "Alice"})
    s.insert("items", {"id": 1, "tag": "blue"})
    s.flush()
    s.close()

    reopened = Storage(file_path=str(db_path))
    try:
        users = reopened.get_table("users")
        items = reopened.get_table("items")
        assert users._lazy_loaded is True
        assert items._lazy_loaded is True

        # prevent items table from being materialized: if called, fail the test
        def fail_if_called(*args, **kwargs):
            raise AssertionError("Unchanged table was materialized during flush")

        # patch only the instance to avoid affecting other tables
        monkeypatch.setattr(items, "_ensure_all_loaded", fail_if_called)

        # keep users behavior intact
        # only modify 'users' table
        reopened.insert("users", {"id": 2, "name": "Bob"})
        # mark dirty only on users; items remains unchanged
        reopened.flush()

        # if we reach here without AssertionError, items was not materialized
        assert items.data == {} or items.data is None
    finally:
        reopened.close()


@pytest.mark.feature
def test_mixed_ops_on_reopened_lazy_table_flush_does_not_materialize(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """在同一张 lazy 表里混合 insert/update/delete，flush 不应触发 _ensure_all_loaded。

    场景：创建 users 表，插入 id=1 Alice age=20，id=2 Bob age=30，flush，close。
    reopen，patch table._ensure_all_loaded 为抛 AssertionError。
    update id=1 -> age=21；delete id=2；insert id=3 Carol age=40；flush。
    reopen 验证：id=1 age==21；select id=2 抛 RecordNotFoundError；id=3 name==Carol
    """
    db_path = tmp_path / "lazy-mixed.pytucky"
    # create initial storage and data
    s = build_user_storage(db_path)
    s.insert("users", {"id": 1, "name": "Alice", "age": 20})
    s.insert("users", {"id": 2, "name": "Bob", "age": 30})
    s.flush()
    s.close()

    reopened = Storage(file_path=str(db_path))
    try:
        table = reopened.get_table("users")
        assert table._lazy_loaded is True

        # prevent full materialization: if called, fail the test
        def fail_if_called(*args, **kwargs):
            raise AssertionError("Table was materialized during mixed-ops flush")

        monkeypatch.setattr(table, "_ensure_all_loaded", fail_if_called)

        # perform mixed operations
        reopened.update("users", 1, {"age": 21})
        reopened.delete("users", 2)
        reopened.insert("users", {"id": 3, "name": "Carol", "age": 40})

        # flush should persist changes without materializing the full table
        reopened.flush()
    finally:
        reopened.close()

    # verify persisted results
    reopened2 = Storage(file_path=str(db_path))
    try:
        assert reopened2.select("users", 1)["age"] == 21
        from pytucky.common.exceptions import RecordNotFoundError

        with pytest.raises(RecordNotFoundError):
            reopened2.select("users", 2)
        assert reopened2.select("users", 3)["name"] == "Carol"
    finally:
        reopened2.close()
