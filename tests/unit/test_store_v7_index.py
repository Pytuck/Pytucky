from __future__ import annotations

from pathlib import Path

import pytest

from pytucky import Column
from pytucky.backends.store_v7 import StoreV7


def build_store(path: Path) -> StoreV7:
    store = StoreV7(path)
    store.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False, index=True),
            Column(int, name="age", nullable=True),
        ],
    )
    store.create_table(
        "items",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="tag", nullable=False, index=True),
            Column(int, name="value", nullable=True),
        ],
    )
    return store


def test_flush_reopen_index_allows_search(tmp_path: Path) -> None:
    file_path = tmp_path / "index.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.insert("users", {"name": "Alice", "age": 30})

    store.insert("items", {"tag": "blue", "value": 1})
    store.insert("items", {"tag": "red", "value": 2})
    store.insert("items", {"tag": "blue", "value": 3})

    store.flush()

    reopened = StoreV7(file_path)
    res = reopened.search_index("users", "name", "Alice")
    assert set(res) == {1, 3}
    res_items = reopened.search_index("items", "tag", "blue")
    assert set(res_items) == {1, 3}


def test_overlay_changes_visible_in_search_without_flush(tmp_path: Path) -> None:
    file_path = tmp_path / "index_overlay.pytucky"
    store = build_store(file_path)
    # initial persisted rows
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.flush()

    reopened = StoreV7(file_path)
    # insert new row (not flushed) with indexed name
    new_pk = reopened.insert("users", {"name": "Alice", "age": 40})
    # update existing row to change name to Alice
    reopened.update("users", 2, {"name": "Alice"})
    # delete the original Alice (pk=1)
    reopened.delete("users", 1)

    res = reopened.search_index("users", "name", "Alice")
    # Expect pk 2 (updated) and new_pk, but not 1
    assert set(res) == {2, new_pk}



def test_repeated_index_search_reuses_single_reader_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_path = tmp_path / "index-reader-reuse.pytucky"
    store = build_store(file_path)
    store.insert("users", {"name": "Alice", "age": 18})
    store.insert("users", {"name": "Bob", "age": 20})
    store.flush()

    reopened = StoreV7(file_path)
    calls = {"count": 0}
    path_type = type(file_path)
    original_open = path_type.open

    def counting_open(self, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == file_path and mode == "rb":
            calls["count"] += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(path_type, "open", counting_open)

    assert reopened.search_index("users", "name", "Alice") == [1]
    assert reopened.search_index("users", "name", "Bob") == [2]
    assert calls["count"] == 1
