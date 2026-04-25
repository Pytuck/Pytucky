from __future__ import annotations

from pathlib import Path
import re
import tomllib

import pytest

from pytucky import Column, CRUDBaseModel, PureBaseModel, Session, Storage
from pytucky import declarative_base, insert, select

@pytest.mark.feature
def test_pure_model_roundtrip_after_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "contract.pytucky"
    db = Storage(file_path=db_path)
    Base: type[PureBaseModel] = declarative_base(db)

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
    Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

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
    ReopenedBase: type[CRUDBaseModel] = declarative_base(reopened, crud=True)

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


@pytest.mark.feature
def test_version_metadata_uses_package_attr_source() -> None:
    import pytucky

    repo_root = Path(__file__).resolve().parents[2]
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))

    project = pyproject["project"]
    assert "version" not in project
    assert project["dynamic"] == ["version"]
    assert pyproject["tool"]["setuptools"]["dynamic"]["version"]["attr"] == "pytucky.__version__"

    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs/api/index.md").read_text(encoding="utf-8")

    assert not re.search(r"^\| 版本 \| \*\*\d+\.\d+\.\d+\*\* \|$", readme, re.MULTILINE)
    assert not re.search(r"^当前版本：\*\*\d+\.\d+\.\d+\*\*$", docs_index, re.MULTILINE)
    assert isinstance(pytucky.__version__, str)
    assert pytucky.__version__


@pytest.mark.feature
def test_root_package_exports_match_all_contract() -> None:
    import pytucky

    expected_common_exports = {
        "Storage",
        "Session",
        "Column",
        "Relationship",
        "declarative_base",
        "select",
        "insert",
        "update",
        "delete",
        "or_",
        "and_",
        "not_",
        "Result",
        "CursorResult",
        "SyncOptions",
        "SyncResult",
    }

    assert expected_common_exports.issubset(set(pytucky.__all__))
    assert [name for name in pytucky.__all__ if not hasattr(pytucky, name)] == []

    assert pytucky.Storage is Storage
    assert pytucky.Session is Session
    assert pytucky.Column is Column
    assert pytucky.select is select
    assert pytucky.insert is insert
    assert pytucky.declarative_base is declarative_base
    assert pytucky.PytuckException is pytucky.PytuckyException
    assert pytucky.PytuckIndexError is pytucky.PytuckyIndexError
