from pathlib import Path
from typing import Type

import pytest

from pytucky import Column, Storage, declarative_base
from pytucky import CRUDBaseModel


@pytest.mark.feature
def test_active_record_create_and_delete(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "active-record.pytucky", engine="pytucky")
    Base: Type[CRUDBaseModel] = declarative_base(db, crud=True)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    user = User.create(name="Alice")
    assert user.name == "Alice"
    user.delete()
    assert User.all() == []
