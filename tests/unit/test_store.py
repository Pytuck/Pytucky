from __future__ import annotations

import io
from pathlib import Path

import pytest

from pytucky import Column
from pytucky.common.exceptions import RecordNotFoundError
from pytucky.backends import store as store_module
from pytucky.backends.format import PkDirEntry, TableBlockRef
from pytucky.backends.store import Store



def build_store(path: Path) -> Store:
    store = Store(path)
    store.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False, index=True),
            Column(int, name="age", nullable=True),
        ],
    )
    return store



def test_open_loads_only_directory_metadata(tmp_path: Path) -> None:
    file_path = tmp_path / "demo.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.flush()

    reopened = Store(file_path)
    state = reopened.table_state("users")
    assert state.overlay.inserted == {}
    assert state.overlay.updated == {}
    assert state.overlay.deleted == set()
    assert state.overlay.row_cache == {}

    row = reopened.select("users", 1)
    assert row["name"] == "Alice"
    assert 1 in reopened.table_state("users").overlay.row_cache



def test_repeated_select_reuses_single_reader_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_path = tmp_path / "reader-reuse.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.flush()

    reopened = Store(file_path)
    calls = {"count": 0}
    path_type = type(file_path)
    original_open = path_type.open

    def counting_open(self, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == file_path and mode == "rb":
            calls["count"] += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(path_type, "open", counting_open)

    assert reopened.select("users", 1)["name"] == "Alice"
    assert reopened.select("users", 2)["name"] == "Bob"
    assert calls["count"] == 1



def test_insert_is_visible_before_flush(tmp_path: Path) -> None:
    store = build_store(tmp_path / "overlay-insert.pytucky")

    pk = store.insert("users", {"name": "Alice", "age": 18})
    row = store.select("users", pk)

    assert pk == 1
    assert row == {"id": 1, "name": "Alice", "age": 18}



def test_update_overrides_disk_row_before_flush(tmp_path: Path) -> None:
    file_path = tmp_path / "overlay-update.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.flush()

    reopened = Store(file_path)
    reopened.update("users", 1, {"name": "Bob", "age": 19})

    row = reopened.select("users", 1)
    assert row == {"id": 1, "name": "Bob", "age": 19}



def test_delete_hides_disk_row_before_flush(tmp_path: Path) -> None:
    file_path = tmp_path / "overlay-delete.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.flush()

    reopened = Store(file_path)
    reopened.delete("users", 1)

    with pytest.raises(RecordNotFoundError):
        reopened.select("users", 1)


def test_flush_reopen_multiple_records_and_next_id(tmp_path: Path) -> None:
    """确保 flush 后重新打开能读取多条记录，且 next_id 在插入后继续递增"""
    file_path = tmp_path / "flush-roundtrip.pytucky"
    store = build_store(file_path)

    # 插入多条记录并 flush
    pk1 = store.insert("users", {"name": "Alice", "age": 18})
    pk2 = store.insert("users", {"name": "Bob", "age": 20})
    assert pk1 == 1
    assert pk2 == 2
    store.flush()

    # 重新打开并验证都能读取
    reopened = Store(file_path)
    row1 = reopened.select("users", 1)
    row2 = reopened.select("users", 2)
    assert row1["name"] == "Alice"
    assert row2["name"] == "Bob"

    # 在 reopened 上继续插入，检查 next_id 继续正确递增
    pk3 = reopened.insert("users", {"name": "Carol", "age": 30})
    assert pk3 == 3
    # 也能在原始 store 上插入并得到合适的 pk（原始 store 对象仍存在）
    pk4 = store.insert("users", {"name": "Dan", "age": 40})
    # 原始 store 的 next_id 也应已在 flush 时同步到文件并在 reopen 后继续使用；最小要求是 pk4 不冲突
    assert pk4 >= 3


def test_update_and_delete_persist_across_flush(tmp_path: Path) -> None:
    """验证 update/delete 经 flush 后能持久化到磁盘并在 reopen 后可见"""
    file_path = tmp_path / "flush-update-delete.pytucky"
    store = build_store(file_path)

    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.flush()

    reopened = Store(file_path)
    # update 并 flush
    reopened.update("users", 1, {"name": "AliceUpdated", "age": 19})
    reopened.flush()

    reopened2 = Store(file_path)
    row1 = reopened2.select("users", 1)
    assert row1["name"] == "AliceUpdated"

    # delete 并 flush
    reopened2.delete("users", 2)
    reopened2.flush()

    reopened3 = Store(file_path)
    with pytest.raises(RecordNotFoundError):
        reopened3.select("users", 2)


def test_flush_reuses_in_memory_state_without_reopen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    file_path = tmp_path / "flush-no-reopen.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})

    calls = {"count": 0}
    original_open = store.open

    def counting_open() -> None:
        calls["count"] += 1
        original_open()

    monkeypatch.setattr(store, "open", counting_open)

    store.flush()

    assert calls["count"] == 0
    assert store.select("users", 1)["name"] == "Alice"


def test_flush_materializes_live_records_once_per_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    file_path = tmp_path / "flush-materialize-once.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})

    calls = {"count": 0}
    original_materialize = store._materialize_records

    def counting_materialize(state):
        calls["count"] += 1
        return original_materialize(state)

    monkeypatch.setattr(store, "_materialize_records", counting_materialize)

    store.flush()

    assert calls["count"] == 1


def test_reopened_select_reuses_decode_layout_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_path = tmp_path / "reader-layout-cache.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.flush()

    reopened = Store(file_path)
    calls = {"payload_columns": 0, "codec": 0}
    original_payload_columns = store_module._payload_columns
    original_get_codec = store_module.TypeRegistry.get_codec

    def counting_payload_columns(columns, pk_name):
        calls["payload_columns"] += 1
        return original_payload_columns(columns, pk_name)

    def counting_get_codec(col_type):
        calls["codec"] += 1
        return original_get_codec(col_type)

    monkeypatch.setattr(store_module, "_payload_columns", counting_payload_columns)
    monkeypatch.setattr(store_module.TypeRegistry, "get_codec", counting_get_codec)

    assert reopened.select("users", 1)["name"] == "Alice"
    assert reopened.select("users", 2)["name"] == "Bob"
    assert calls["payload_columns"] == 1
    assert calls["codec"] == 2


def test_read_pk_dir_rebases_legacy_relative_offsets(tmp_path: Path) -> None:
    store = Store(tmp_path / "legacy-relative.pytucky")
    blob = b"".join(
        [
            PkDirEntry(pk=1, offset=0, length=12).pack_int(),
            PkDirEntry(pk=2, offset=12, length=16).pack_int(),
        ]
    )
    ref = TableBlockRef(
        name="users",
        record_count=2,
        next_id=3,
        data_offset=1024,
        data_size=28,
        pk_dir_offset=0,
        pk_dir_size=len(blob),
        index_meta_offset=0,
        index_meta_size=0,
        index_data_offset=0,
        index_data_size=0,
    )

    pk_index = store._read_pk_dir(io.BytesIO(blob), ref)

    assert pk_index == {1: (1024, 12), 2: (1036, 16)}
