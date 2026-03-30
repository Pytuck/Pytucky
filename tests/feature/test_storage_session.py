from pathlib import Path
from typing import Type

import pytest

from pytucky import Column, Session, Storage, declarative_base, insert, select
from pytucky import PureBaseModel


@pytest.mark.feature
def test_session_commit_persists_inserted_rows(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-basic.pytucky", engine="pytucky")
    Base: Type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    session.execute(insert(User).values(name="Alice"))
    session.commit()

    rows = session.execute(select(User)).all()
    assert len(rows) == 1
    assert rows[0].name == "Alice"


@pytest.mark.feature
def test_transaction_rollback_restores_original_data(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rollback.pytucky", engine="pytucky")
    Base: Type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)
        balance = Column(int)

    session = Session(db)
    session.execute(insert(User).values(name="Alice", balance=100))
    session.commit()

    with pytest.raises(ValueError):
        with session.begin():
            session.execute(select(User)).all()
            raise ValueError("abort")
