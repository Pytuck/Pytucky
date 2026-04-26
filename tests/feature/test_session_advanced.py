from pathlib import Path

import pytest

from pytucky import Column, Session, Storage, declarative_base, insert, select
from pytucky.core.event import event
from pytucky.common.exceptions import TransactionError


@pytest.mark.feature
def test_flush_triggers_before_after_insert(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-evt.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    fired = []

    def before_insert(u):
        fired.append(('before', u.name))

    def after_insert(u):
        fired.append(('after', u.name))

    try:
        # 注册监听器到模型级别
        event.listen(User, 'before_insert', before_insert)
        event.listen(User, 'after_insert', after_insert)

        u = User(name="Alice")
        session.add(u)
        session.flush()

        # flush 应该先触发 before_insert 然后 after_insert
        assert fired == [('before', 'Alice'), ('after', 'Alice')]

        # 并且实例应被分配主键并注册到 identity map
        assert getattr(u, 'id', None) is not None
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_flush_handles_dirty_update(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-dirty.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    try:
        # insert
        session.execute(insert(User).values(name="Bob"))
        session.commit()

        # load, modify, flush
        u = session.get(User, 1)
        assert u is not None
        u.name = "Bobby"
        # 标记为 dirty 是由属性赋值触发的（session 已关联实例）
        session.flush()

        # 从数据库重新读取确认修改已持久化
        fresh = session.get(User, 1)
        assert fresh.name == "Bobby"
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_flush_dirty_updates_do_not_call_storage_select(tmp_path: Path, monkeypatch) -> None:
    db = Storage(file_path=tmp_path / "session-dirty-no-readback.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    try:
        session.execute(insert(User).values(name="A"))
        session.execute(insert(User).values(name="B"))
        session.commit()

        u1 = session.get(User, 1)
        u2 = session.get(User, 2)
        assert u1 is not None and u2 is not None

        def fail_select(table_name, pk):
            raise AssertionError("dirty flush should not call storage.select()")

        monkeypatch.setattr(db, 'select', fail_select)

        u1.name = "A1"
        u2.name = "B1"
        session.flush()

        rows = session.execute(select(User)).all()
        names = {row.id: row.name for row in rows}
        assert names == {1: "A1", 2: "B1"}
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_flush_dirty_update_after_event_sees_validated_values(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-dirty-events.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        age = Column(int)

    session = Session(db)
    fired = []

    def before_update(user):
        user.__dict__['age'] = "12"
        fired.append(("before", type(user.age).__name__, user.age))

    def after_update(user):
        fired.append(("after", type(user.age).__name__, user.age))

    try:
        event.listen(User, 'before_update', before_update)
        event.listen(User, 'after_update', after_update)

        session.execute(insert(User).values(age=1))
        session.commit()

        user = session.get(User, 1)
        assert user is not None

        user.age = 2
        session.flush()

        assert fired == [
            ("before", "str", "12"),
            ("after", "int", 12),
        ]
        assert user.age == 12

        fresh = session.execute(select(User)).first()
        assert fresh is not None
        assert fresh.age == 12
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_merge_new_object(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-merge-new.pytucky")
    Base = declarative_base(db)

    class Item(Base):
        __tablename__ = "items"
        id = Column(int, primary_key=True)
        title = Column(str)

    session = Session(db)
    try:
        # merge 一个没有主键的新对象 -> 应当作为新对象添加到 session
        detached = Item(title="New")
        managed = session.merge(detached)
        # merge 返回的对象在无主键时就是传入对象
        assert managed is detached

        session.commit()

        saved = session.execute(select(Item)).all()
        assert len(saved) == 1
        assert saved[0].title == "New"
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_merge_existing_object_updates_identity_map(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-merge-existing.pytucky")
    Base = declarative_base(db)

    class Item(Base):
        __tablename__ = "items"
        id = Column(int, primary_key=True)
        title = Column(str)

    session = Session(db)
    try:
        session.execute(insert(Item).values(title="Orig"))
        session.commit()

        # 从 session 获取实例，确保在 identity map 中
        inst = session.get(Item, 1)
        assert inst is not None
        assert inst.title == "Orig"

        # 构造一个 detached 实例表示更新
        detached = Item(id=1, title="Updated")
        merged = session.merge(detached)

        # merge 应返回 identity map 中的实例，并更新属性
        assert merged is inst
        assert merged.title == "Updated"

        session.commit()

        fresh = session.get(Item, 1)
        assert fresh.title == "Updated"
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_bulk_insert_and_bulk_update(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-bulk.pytucky")
    Base = declarative_base(db)

    class Product(Base):
        __tablename__ = "products"
        id = Column(int, primary_key=True)
        name = Column(str)
        price = Column(int)

    session = Session(db)
    try:
        p1 = Product(name="A", price=10)
        p2 = Product(name="B", price=20)
        p3 = Product(name="C", price=30)

        pks = session.bulk_insert([p1, p2, p3])
        assert len(pks) == 3
        assert getattr(p1, 'id', None) is not None

        # 修改并 bulk_update
        p1.price = 11
        p2.price = 22
        count = session.bulk_update([p1, p2])
        assert count == 2

        # 验证持久化结果
        rows = session.execute(select(Product)).all()
        prices = {r.name: r.price for r in rows}
        assert prices["A"] == 11
        assert prices["B"] == 22
        assert prices["C"] == 30
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_begin_rollback_and_nesting(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "session-tx.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    try:
        # 初始插入
        session.execute(insert(User).values(name="X"))
        session.commit()

        # 在事务中插入然后抛出，确认回滚
        with pytest.raises(ValueError):
            with session.begin():
                session.execute(insert(User).values(name="Y"))
                # 在事务内可见
                names_in_tx = [r.name for r in session.execute(select(User)).all()]
                assert "Y" in names_in_tx
                raise ValueError("abort")

        names_after = [r.name for r in session.execute(select(User)).all()]
        assert "Y" not in names_after

        # 嵌套事务不被允许
        with session.begin():
            with pytest.raises(TransactionError):
                with session.begin():
                    pass
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()


@pytest.mark.feature
def test_get_behaviour_for_missing_and_error_propagation(tmp_path: Path, monkeypatch) -> None:
    db = Storage(file_path=tmp_path / "session-get.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)
    try:
        # 空库时 get 返回 None
        assert session.get(User, 999) is None

        # 强制让 storage.select 抛出非 RecordNotFoundError 的异常，验证会向上抛出
        def raise_runtime(table_name, pk):
            raise RuntimeError("unexpected")

        monkeypatch.setattr(db, 'select', raise_runtime)

        with pytest.raises(RuntimeError):
            session.get(User, 1)
    finally:
        try:
            session.close()
        finally:
            db.close()
            event.clear()
