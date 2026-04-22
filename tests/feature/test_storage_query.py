from pathlib import Path

import pytest

from pytucky import Storage, declarative_base, Session, Column, select
from pytucky.query.builder import Condition, or_


@pytest.mark.feature
def test_index_equality_query_acceleration(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "idx-eq.pytucky")
    Base = declarative_base(db)

    class Person(Base):
        __tablename__ = "persons"
        id = Column(int, primary_key=True)
        name = Column(str, index=True)
        age = Column(int)

    try:
        # 使用 Storage.insert 直接插入
        db.insert("persons", {"name": "Alice", "age": 30})
        db.insert("persons", {"name": "Bob", "age": 25})
        db.insert("persons", {"name": "Alice", "age": 40})
        db.insert("persons", {"name": "Charlie", "age": 20})
        db.insert("persons", {"name": "Bob", "age": 50})

        # 使用等值条件查询 name == 'Alice'
        cond = Condition("name", "=", "Alice")
        results = db.query("persons", [cond])
        names = [r["name"] for r in results]
        assert all(n == "Alice" for n in names)
        assert len(results) == 2
    finally:
        db.close()


@pytest.mark.feature
def test_sorted_index_range_query(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "sorted-range.pytucky")
    Base: Type = declarative_base(db)

    class Item(Base):
        __tablename__ = "items"
        id = Column(int, primary_key=True)
        price = Column(int, index='sorted')
        name = Column(str)

    try:
        # 插入多条记录
        for p in [10, 25, 30, 45, 60, 50, 20]:
            db.insert("items", {"price": p, "name": f"item-{p}"})

        # 查询 price > 20 and price <= 50
        c1 = Condition("price", ">", 20)
        c2 = Condition("price", "<=", 50)
        results = db.query("items", [c1, c2], order_by="price")
        prices = [r["price"] for r in results]
        assert all(20 < v <= 50 for v in prices)
        assert sorted(prices) == prices  # order_by 生效
    finally:
        db.close()


@pytest.mark.feature
def test_none_value_ordering_ascending_and_descending(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "none-order.pytucky")
    Base: Type = declarative_base(db)

    class Thing(Base):
        __tablename__ = "things"
        id = Column(int, primary_key=True)
        score = Column(int, index='sorted')
        note = Column(str)

    try:
        # 插入包含 None 的记录
        db.insert("things", {"score": 10, "note": "a"})
        db.insert("things", {"score": None, "note": "none1"})
        db.insert("things", {"score": 30, "note": "b"})
        db.insert("things", {"score": None, "note": "none2"})
        db.insert("things", {"score": 20, "note": "c"})

        # 升序：None 应该在最后
        asc = db.query("things", [], order_by="score", order_desc=False)
        asc_scores = [r.get("score") for r in asc]
        # None 出现在末尾
        assert asc_scores[-2:] == [None, None]
        # 非 None 部分升序
        non_none = [s for s in asc_scores if s is not None]
        assert non_none == sorted(non_none)

        # 降序：根据实现，使用索引排序时 None 会排在开头（降序时被当作更大/更高优先级）
        desc = db.query("things", [], order_by="score", order_desc=True)
        desc_scores = [r.get("score") for r in desc]
        # None 在开头
        assert desc_scores[:2] == [None, None]
        non_none_desc = [s for s in desc_scores if s is not None]
        assert non_none_desc == sorted(non_none_desc, reverse=True)
    finally:
        db.close()


@pytest.mark.feature
def test_multi_condition_intersection(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "multi-cond.pytucky")
    Base: Type = declarative_base(db)

    class Record(Base):
        __tablename__ = "records"
        id = Column(int, primary_key=True)
        a = Column(int, index=True)
        b = Column(int, index=True)

    try:
        # 插入
        for i in range(10):
            db.insert("records", {"a": i % 3, "b": i % 5})

        # a == 1 AND b == 2
        c1 = Condition("a", "=", 1)
        c2 = Condition("b", "=", 2)
        results = db.query("records", [c1, c2])
        for r in results:
            assert r["a"] == 1 and r["b"] == 2
    finally:
        db.close()


@pytest.mark.feature
def test_offset_limit_pagination(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "pagination.pytucky")
    Base: Type = declarative_base(db)

    class P(Base):
        __tablename__ = "pages"
        id = Column(int, primary_key=True)
        val = Column(int, index='sorted')

    try:
        for i in range(10):
            db.insert("pages", {"val": i})

        results = db.query("pages", [], order_by="val")
        assert len(results) == 10

        page = db.query("pages", [], order_by="val", offset=3, limit=3)
        vals = [r["val"] for r in page]
        assert vals == [3, 4, 5]
    finally:
        db.close()


@pytest.mark.feature
def test_query_with_or_composite_condition(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "or-cond.pytucky")
    Base: Type = declarative_base(db)

    class User(Base):
        __tablename__ = "users_or"
        id = Column(int, primary_key=True)
        role = Column(str, index=True)
        active = Column(int)

    try:
        db.insert("users_or", {"role": "admin", "active": 1})
        db.insert("users_or", {"role": "member", "active": 1})
        db.insert("users_or", {"role": "guest", "active": 0})

        # OR: role == 'admin' OR active == 0
        cond = or_(User.role == "admin", User.active == 0).to_condition()
        results = db.query("users_or", [cond])
        roles = {r["role"] for r in results}
        assert "admin" in roles
        assert "guest" in roles
    finally:
        db.close()
