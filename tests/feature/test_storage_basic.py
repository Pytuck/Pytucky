from __future__ import annotations

from pathlib import Path

import pytest

from pytucky import Column, Session, Storage, declarative_base, insert, select
from pytucky import PureBaseModel

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
