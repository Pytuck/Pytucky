import pytest
from pathlib import Path
from pytucky import Storage, declarative_base, Session, Column, select


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
    orig_store_select = reopened.backend.store.select

    def store_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select(table_name, pk)

    monkeypatch.setattr(reopened.backend.store, 'select', store_wrapper)

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
    orig_store_select = reopened.backend.store.select

    def store_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select(table_name, pk)

    monkeypatch.setattr(reopened.backend.store, 'select', store_wrapper)

    user = session.get(User, pk)
    assert calls["count"] == 1
    assert user is not None and user.name == "Bob"


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
    orig_store_select = reopened.backend.store.select

    def store_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select(table_name, pk)

    monkeypatch.setattr(reopened.backend.store, 'select', store_wrapper)

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
    orig_store_select = reopened.backend.store.select

    def store_wrapper(table_name, pk):
        calls["count"] += 1
        return orig_store_select(table_name, pk)

    monkeypatch.setattr(reopened.backend.store, 'select', store_wrapper)

    rows = session.execute(select(User).limit(3).offset(2)).all()

    assert len(rows) == 3
    assert calls["count"] == 5
    session.close()
    reopened.close()
