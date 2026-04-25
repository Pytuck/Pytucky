import pytest
from pathlib import Path
from pytucky import Storage, declarative_base, Session, Column, select


def _patch_store_select_calls(store, monkeypatch, calls) -> None:
    orig_store_select = store.select
    orig_store_select_raw = store.select_raw

    def store_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select(table_name, pk)

    def store_raw_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select_raw(table_name, pk)

    monkeypatch.setattr(store, 'select', store_wrapper)
    monkeypatch.setattr(store, 'select_raw', store_raw_wrapper)


@pytest.mark.feature
def test_select_filter_by_pk_uses_direct_storage_select(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "fastpath.pytucky"

    # create, insert, flush and close to force on-disk path
    db = Storage(file_path=db_path)
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)

    db.create_table("users", [Column(int, name='id', primary_key=True), Column(str, name='name')])
    pk = db.insert("users", {"name": "Alice"})
    db.flush()
    db.close()

    # reopen storage to ensure operations hit backend.store
    reopened = Storage(file_path=db_path)
    Base = declarative_base(reopened)
    session = Session(reopened)

    # patch only the low-level store.select
    assert hasattr(reopened, 'backend') and hasattr(reopened.backend, 'store')
    calls = {"count": 0}
    _patch_store_select_calls(reopened.backend.store, monkeypatch, calls)

    result = session.execute(select(User).filter_by(id=pk)).all()

    assert calls["count"] == 1
    assert result and result[0].name == "Alice"


@pytest.mark.feature
def test_session_get_uses_direct_storage_select(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "fastpath2.pytucky"

    # create, insert, flush and close to force on-disk path
    db = Storage(file_path=db_path)
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)

    db.create_table("users", [Column(int, name='id', primary_key=True), Column(str, name='name')])
    pk = db.insert("users", {"name": "Bob"})
    db.flush()
    db.close()

    # reopen storage to ensure operations hit backend.store
    reopened = Storage(file_path=db_path)
    Base = declarative_base(reopened)
    session = Session(reopened)

    # patch only the low-level store.select
    assert hasattr(reopened, 'backend') and hasattr(reopened.backend, 'store')
    calls = {"count": 0}
    _patch_store_select_calls(reopened.backend.store, monkeypatch, calls)

    user = session.get(User, pk)
    assert calls["count"] == 1
    assert user is not None and user.name == "Bob"


@pytest.mark.feature
def test_select_filter_by_pk_keeps_offset_slice_semantics(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "fastpath-pk-offset.pytucky"

    db = Storage(file_path=db_path)
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    db.create_table("users", [Column(int, name='id', primary_key=True), Column(str, name='name')])
    pk = db.insert("users", {"name": "Alice"})
    db.flush()
    db.close()

    reopened = Storage(file_path=db_path)
    session = Session(reopened)

    assert hasattr(reopened, 'backend') and hasattr(reopened.backend, 'store')
    calls = {"count": 0}
    _patch_store_select_calls(reopened.backend.store, monkeypatch, calls)

    rows = session.execute(select(User).filter_by(id=pk).offset(1)).all()

    assert rows == []
    assert calls["count"] == 1
    session.close()
    reopened.close()


@pytest.mark.feature
def test_storage_query_limit_offset_stops_after_requested_window(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "query-window-fastpath.pytucky"
    db = Storage(file_path=db_path)
    db.create_table(
        "users",
        [
            Column(int, name='id', primary_key=True),
            Column(str, name='name'),
        ],
    )
    for idx in range(10):
        db.insert("users", {"name": f"user-{idx}"})
    db.flush()
    db.close()

    reopened = Storage(file_path=db_path)
    assert hasattr(reopened, 'backend') and hasattr(reopened.backend, 'store')
    calls = {"count": 0}
    _patch_store_select_calls(reopened.backend.store, monkeypatch, calls)

    rows = reopened.query("users", [], limit=3, offset=2)

    assert len(rows) == 3
    assert calls["count"] == 5
    reopened.close()


@pytest.mark.feature
def test_select_limit_offset_uses_pushed_pagination_on_reopened_storage(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "select-window-fastpath.pytucky"
    db = Storage(file_path=db_path)
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    db.create_table("users", [Column(int, name='id', primary_key=True), Column(str, name='name')])
    for idx in range(10):
        db.insert("users", {"name": f"user-{idx}"})
    db.flush()
    db.close()

    reopened = Storage(file_path=db_path)
    Base = declarative_base(reopened)
    session = Session(reopened)

    assert hasattr(reopened, 'backend') and hasattr(reopened.backend, 'store')
    calls = {"count": 0}
    _patch_store_select_calls(reopened.backend.store, monkeypatch, calls)

    rows = session.execute(select(User).limit(3).offset(2)).all()

    assert len(rows) == 3
    assert calls["count"] == 5
    session.close()
    reopened.close()
