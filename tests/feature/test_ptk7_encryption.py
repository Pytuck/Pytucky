from pathlib import Path
from typing import Optional

import pytest

from pytucky import Column, Session, Storage, declarative_base, select
from pytucky.common.options import BinaryBackendOptions


@pytest.mark.feature
@pytest.mark.parametrize("level", [None, "low", "medium", "high"])
def test_encryption_roundtrip_levels(tmp_path: Path, level: Optional[str]) -> None:
    """在不同加密等级下，通过高层 Session 路径进行写入与重开读取。"""
    db_path = tmp_path / f"enc-{level or 'plain'}.pytuck"
    opts = BinaryBackendOptions(encryption=level, password=None if level is None else "secret123")
    db = Storage(file_path=str(db_path), backend_options=opts)
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, index=True)

    session = Session(db)
    try:
        session.add(User(id=1, name="Alice"))
        session.add(User(id=2, name="Alice"))
        session.commit()
        db.flush()
    finally:
        session.close()
        db.close()

    reopen_opts = BinaryBackendOptions(password=None if level is None else "secret123")
    reopened = Storage(file_path=str(db_path), backend_options=reopen_opts)
    reopened_session = Session(reopened)
    try:
        rows = reopened_session.execute(select(User).filter_by(id=1)).all()
        assert [row.name for row in rows] == ["Alice"]
    finally:
        reopened_session.close()
        reopened.close()




@pytest.mark.feature
def test_lazy_index_uses_store_decryption_on_reopen(tmp_path: Path) -> None:
    """加密文件 reopen 后，lazy index proxy 不直接读取磁盘明文，而应通过 Store 的解密接口。"""
    db_path = tmp_path / "lazy-index.pytuck"
    # create encrypted DB
    opts = BinaryBackendOptions(encryption='medium', password='secret123')
    db = Storage(file_path=str(db_path), backend_options=opts)
    try:
        db.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name", index=True),
            ],
        )
        db.insert("users", {"id": 1, "name": "Alice"})
        db.flush()
        db.close()

        # reopen providing only password (no explicit encryption level)
        reopened = Storage(file_path=str(db_path), backend_options=BinaryBackendOptions(password='secret123'))
        try:
            tbl = reopened.get_table("users")
            # table should be lazy
            assert getattr(tbl, "_lazy_loaded", True) is True
            idx = tbl.indexes.get("name")
            # lookup should succeed (and must have used Store._read_region internally)
            found = set(idx.lookup("Alice")) if idx is not None else set()
            assert found == {1}
        finally:
            reopened.close()
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_reopen_with_password_and_flush_preserves_encryption(tmp_path: Path) -> None:
    """只提供 password 重新打开并 flush 后，文件仍然是加密的（不应包含明文 name 值）。"""
    db_path = tmp_path / "preserve-enc.pytuck"
    opts = BinaryBackendOptions(encryption='high', password='secret123')
    db = Storage(file_path=str(db_path), backend_options=opts)
    try:
        db.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name", index=True),
            ],
        )
        db.insert("users", {"id": 1, "name": "ShouldNotAppear"})
        db.flush()
        db.close()

        # reopen with only password, modify and flush
        reopened = Storage(file_path=str(db_path), backend_options=BinaryBackendOptions(password='secret123'))
        try:
            reopened.update("users", 1, {"name": "StillSecret"})
            reopened.flush()
            reopened.close()
            # file should not contain the plaintext strings
            raw = db_path.read_bytes()
            assert b"ShouldNotAppear" not in raw
            assert b"StillSecret" not in raw
        finally:
            try:
                reopened.close()
            except Exception:
                pass
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_sorted_index_range_query_on_encrypted_reopen_uses_store_read(tmp_path: Path) -> None:
    """加密文件 reopen 后，SortedIndexProxy.range_query 的 blob 快速路径应能通过 Store 的解密接口正常工作。"""
    db_path = tmp_path / "sorted-reopen.pytuck"
    opts = BinaryBackendOptions(encryption='medium', password='secret123')
    db = Storage(file_path=str(db_path), backend_options=opts)
    try:
        db.create_table(
            "items",
            [
                Column(int, name="id", primary_key=True),
                Column(int, name="price", index='sorted'),
                Column(str, name="name"),
            ],
        )
        for p in [10, 25, 30, 45, 60, 50, 20]:
            db.insert("items", {"price": p, "name": f"item-{p}"})
        db.flush()
        db.close()

        # reopen providing only password
        reopened = Storage(file_path=str(db_path), backend_options=BinaryBackendOptions(password='secret123'))
        try:
            tbl = reopened.get_table("items")
            assert getattr(tbl, "_lazy_loaded", True) is True
            # perform a high-level range query via Storage.query which should
            # exercise the backend's sorted index fast-path (blob range search)
            from pytucky.query.builder import Condition
            c1 = Condition("price", ">", 20)
            c2 = Condition("price", "<=", 50)
            results = reopened.query("items", [c1, c2], order_by="price")
            prices = [r["price"] for r in results]
            assert prices == [25, 30, 45, 50]
        finally:
            reopened.close()
    finally:
        try:
            db.close()
        except Exception:
            pass
