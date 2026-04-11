from pathlib import Path
from typing import Type

import pytest

from pytucky import Column, CRUDBaseModel, PureBaseModel, Session, Storage
from pytucky import declarative_base, insert, select


@pytest.mark.feature
def test_pure_model_roundtrip_after_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "contract.pytucky"
    db = Storage(file_path=db_path)
    Base: Type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False, index=True)

    session = Session(db)
    try:
        session.execute(insert(User).values(name="Alice"))
        session.commit()
        db.flush()

        # 验证通过高层 API flush 后文件头为 PTK7（高层应接入 PTK7 后端）
        with open(db_path, 'rb') as f:
            magic = f.read(4)
        assert magic == b"PTK7"
        # 进一步确保文件可以被底层 Store 直接读取
        from pytucky.backends.store import Store
        store = Store(db_path)
        assert store.select("users", 1)["name"] == "Alice"
    finally:
        session.close()
        db.close()

    reopened = Storage(file_path=db_path)
    reopened_session = Session(reopened)
    try:
        rows = reopened_session.execute(select(User).filter_by(id=1)).all()
        assert [row.name for row in rows] == ["Alice"]
    finally:
        reopened_session.close()
        reopened.close()


@pytest.mark.feature
def test_active_record_roundtrip_after_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "crud-contract.pytucky"
    db = Storage(file_path=db_path)
    Base: Type[CRUDBaseModel] = declarative_base(db, crud=True)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False)

    try:
        created = User.create(name="Bob")
        assert created.id == 1
        db.flush()
    finally:
        db.close()

    reopened = Storage(file_path=db_path)
    ReopenedBase: Type[CRUDBaseModel] = declarative_base(reopened, crud=True)

    class ReopenedUser(ReopenedBase):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False)

    try:
        user = ReopenedUser.get(1)
        assert user is not None
        assert user.name == "Bob"
    finally:
        reopened.close()


@pytest.mark.feature
def test_storage_open_requires_no_lazy_option(tmp_path: Path) -> None:
    db_path = tmp_path / "simple-open.pytucky"
    db = Storage(file_path=db_path)
    try:
        db.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name", nullable=False),
            ],
        )
        db.insert("users", {"name": "Alice"})
        db.flush()
    finally:
        db.close()

    reopened = Storage(file_path=db_path)
    try:
        assert reopened.select("users", 1)["name"] == "Alice"
    finally:
        reopened.close()


@pytest.mark.feature
def test_storage_flush_bulk_loads_store_without_row_by_row_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pytucky.backends.store import Store

    db_path = tmp_path / "bulk-flush.pytucky"
    db = Storage(file_path=db_path)
    try:
        db.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name", nullable=False, index=True),
            ],
        )
        db.insert("users", {"name": "Alice"})
        db.insert("users", {"name": "Bob"})

        calls = {"count": 0}
        original_insert = Store.insert

        def counting_insert(self, table_name, data):
            calls["count"] += 1
            return original_insert(self, table_name, data)

        monkeypatch.setattr(Store, "insert", counting_insert)

        db.flush()

        assert calls["count"] == 0
        assert db.select("users", 1)["name"] == "Alice"
        assert db.select("users", 2)["name"] == "Bob"
    finally:
        db.close()


@pytest.mark.feature
def test_storage_close_delegates_to_backend_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Storage(file_path=tmp_path / "close-contract.pytucky")
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False),
        ],
    )

    assert db.backend is not None
    calls = {"count": 0}
    original_close = db.backend.close

    def counting_close() -> None:
        calls["count"] += 1
        original_close()

    monkeypatch.setattr(db.backend, "close", counting_close)

    db.close()

    assert calls["count"] == 1
