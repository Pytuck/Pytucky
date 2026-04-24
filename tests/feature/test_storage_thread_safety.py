from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from pytucky import Column, Session, Storage, declarative_base
from pytucky import PureBaseModel


def _run_threads(worker_count: int, target: Any) -> list[BaseException]:
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def runner(idx: int) -> None:
        try:
            target(idx)
        except BaseException as exc:  # pragma: no cover - 仅用于线程内收集异常
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=runner, args=(idx,), daemon=True)
        for idx in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    alive_threads = [thread.name for thread in threads if thread.is_alive()]
    if alive_threads:
        raise AssertionError(f"Threads did not finish: {alive_threads}")
    return errors


class _ConcurrencyCheckedList(list[PureBaseModel]):
    def __init__(self) -> None:
        super().__init__()
        self._active = 0
        self._lock = threading.Lock()

    def append(self, item: PureBaseModel) -> None:
        with self._lock:
            self._active += 1
            current = self._active
        try:
            if current > 1:
                raise AssertionError("Session.add mutated _new_objects concurrently")
            time.sleep(0.02)
            super().append(item)
        finally:
            with self._lock:
                self._active -= 1


@pytest.mark.feature
def test_storage_serializes_concurrent_insert_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Storage(file_path=tmp_path / "thread-storage-insert.pytucky")
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
        ],
    )

    table = db.get_table("users")
    original_insert = table.insert
    active = 0
    active_lock = threading.Lock()

    def guarded_insert(record: dict[str, Any]) -> Any:
        nonlocal active
        with active_lock:
            active += 1
            current = active
        try:
            if current > 1:
                raise AssertionError("Storage.insert entered table.insert concurrently")
            time.sleep(0.02)
            return original_insert(record)
        finally:
            with active_lock:
                active -= 1

    monkeypatch.setattr(table, "insert", guarded_insert)

    try:
        errors = _run_threads(
            worker_count=8,
            target=lambda idx: db.insert("users", {"name": f"user-{idx}"}),
        )
        assert errors == []
        assert db.count_rows("users") == 8
    finally:
        db.close()


@pytest.mark.feature
def test_storage_auto_flush_serializes_backend_save_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Storage(file_path=tmp_path / "thread-storage-autoflush.pytucky", auto_flush=True)
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
        ],
    )

    backend = db.backend
    assert backend is not None
    original_save = backend.save
    active = 0
    active_lock = threading.Lock()

    def guarded_save(
        tables: dict[str, Any],
        *,
        changed_tables: set[str] | None = None,
    ) -> None:
        nonlocal active
        with active_lock:
            active += 1
            current = active
        try:
            if current > 1:
                raise AssertionError("Storage.flush called backend.save concurrently")
            time.sleep(0.02)
            original_save(tables, changed_tables=changed_tables)
        finally:
            with active_lock:
                active -= 1

    monkeypatch.setattr(backend, "save", guarded_save)

    try:
        errors = _run_threads(
            worker_count=6,
            target=lambda idx: db.insert("users", {"name": f"user-{idx}"}),
        )
        assert errors == []
    finally:
        db.close()

    reopened = Storage(file_path=tmp_path / "thread-storage-autoflush.pytucky")
    try:
        assert reopened.count_rows("users") == 6
    finally:
        reopened.close()


@pytest.mark.feature
def test_session_serializes_shared_add_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Storage(file_path=tmp_path / "thread-session-add.pytucky")
    Base: type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    guarded_new_objects = _ConcurrencyCheckedList()
    monkeypatch.setattr(session, "_new_objects", guarded_new_objects)

    try:
        errors = _run_threads(
            worker_count=8,
            target=lambda idx: session.add(User(name=f"user-{idx}")),
        )
        assert errors == []
        assert len(session._new_objects) == 8

        session.flush()
        session.commit()
        assert db.count_rows("users") == 8
    finally:
        try:
            session.close()
        finally:
            db.close()
