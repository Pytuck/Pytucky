from __future__ import annotations

from pathlib import Path
import shutil

import pytest

from pytucky import Column, Session, Storage, declarative_base, insert, select
from pytucky import PureBaseModel
from pytucky.common.options import PytuckBackendOptions

@pytest.mark.feature
def test_session_commit_persists_inserted_rows(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-basic.pytucky")
    Base: type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    try:
        session.execute(insert(User).values(name="Alice"))
        session.commit()

        rows = session.execute(select(User)).all()
        assert len(rows) == 1
        assert rows[0].name == "Alice"
    finally:
        # 确保在任何情况下都关闭会话和存储
        try:
            session.close()
        finally:
            db.close()

@pytest.mark.feature
def test_transaction_rollback_restores_original_data(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rollback.pytucky")
    Base: type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)
        balance = Column(int)

    session = Session(db)
    try:
        session.execute(insert(User).values(name="Alice", balance=100))
        session.commit()

        # 在事务内做一次真实写入，然后抛出异常以触发回滚
        with pytest.raises(ValueError):
            with session.begin():
                session.execute(insert(User).values(name="Bob", balance=50))
                # 确保写入发生（可在事务内查询到）
                rows_in_tx = session.execute(select(User)).all()
                assert any(r.name == "Bob" for r in rows_in_tx)
                raise ValueError("abort")

        # 事务结束后，回滚应恢复为只有 Alice
        rows = session.execute(select(User)).all()
        names = [r.name for r in rows]
        assert "Alice" in names
        assert "Bob" not in names
    finally:
        try:
            session.close()
        finally:
            db.close()


@pytest.mark.feature
def test_transaction_rollback_restores_table_membership_and_schema() -> None:
    db = Storage(in_memory=True)
    db.create_table("base", [Column(int, name="id", primary_key=True)])

    with pytest.raises(RuntimeError, match="rollback"):
        with db.transaction():
            db.add_column("base", Column(str, name="temporary"))
            db.drop_table("base")
            db.create_table("new", [Column(int, name="id", primary_key=True)])
            raise RuntimeError("rollback")

    assert set(db.tables) == {"base"}
    assert set(db.get_table("base").columns) == {"id"}


@pytest.mark.feature
def test_transaction_snapshot_supports_reopened_lazy_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "lazy-transaction.pytuck"
    db = Storage(db_path)
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True),
        ],
    )
    db.insert("users", {"name": "A"})
    db.flush()
    db.close()

    reopened = Storage(db_path)
    try:
        with pytest.raises(RuntimeError, match="rollback"):
            with reopened.transaction():
                reopened.update("users", 1, {"name": "B"})
                raise RuntimeError("rollback")

        assert reopened.select("users", 1)["name"] == "A"
        assert reopened.get_table("users").indexes["name"].lookup("A") == {1}
        assert reopened.get_table("users").indexes["name"].lookup("B") == set()
    finally:
        reopened.close()


@pytest.mark.feature
@pytest.mark.parametrize("encryption", [None, "high"])
def test_database_file_is_self_contained_after_copy(
    tmp_path: Path,
    encryption: str | None,
) -> None:
    source_dir = tmp_path / "source"
    destination_dir = tmp_path / "destination"
    source_dir.mkdir()
    destination_dir.mkdir()
    source_path = source_dir / "portable.pytuck"
    options = PytuckBackendOptions(
        encryption=encryption,
        password="portable-secret" if encryption else None,
    )

    database = Storage(source_path, backend_options=options)
    database.create_table(
        "items",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
        ],
    )
    database.insert("items", {"name": "portable"})
    database.close()

    destination_path = destination_dir / source_path.name
    shutil.copy2(source_path, destination_path)
    assert [path.name for path in destination_dir.iterdir()] == ["portable.pytuck"]

    reopen_options = PytuckBackendOptions(
        password="portable-secret" if encryption else None,
    )
    copied = Storage(destination_path, backend_options=reopen_options)
    try:
        assert copied.select("items", 1)["name"] == "portable"
    finally:
        copied.close()
