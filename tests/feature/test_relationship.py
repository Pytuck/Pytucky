from pathlib import Path
from typing import Type

import pytest

from pytucky import Storage, declarative_base, Session, Column, select
from pytucky.core.orm import Relationship
from pytucky.core.prefetch import prefetch, PrefetchOption


@pytest.mark.feature
def test_one_to_many_and_many_to_one_lazy_loading(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-one-many.pytucky")
    Base = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship('books', foreign_key='author_id')

    class Book(Base):
        __tablename__ = 'books'
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)
        author = Relationship('authors', foreign_key='author_id')

    try:
        # 使用 CRUD helper 快速创建
        a = Author.create(name='Alice')
        b1 = Book.create(title='Book1', author_id=a.id)
        b2 = Book.create(title='Book2', author_id=a.id)

        # 多对一：book.author 懒加载
        assert b1.author is not None
        assert b1.author.id == a.id
        assert b2.author.id == a.id

        # 一对多：author.books 懒加载，返回列表且每项的 author_id 正确
        books = a.books
        assert isinstance(books, list)
        assert len(books) == 2
        assert all(getattr(b, 'author_id') == a.id for b in books)
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_relationship_caching(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-cache.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship('books', foreign_key='author_id')

    class Book(Base):
        __tablename__ = 'books'
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)

    try:
        a = Author.create(name='Bob')
        Book.create(title='B1', author_id=a.id)

        first = a.books
        second = a.books
        # 缓存返回同一对象（is）
        assert first is second
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_prefetch_option_and_immediate_mode(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-prefetch.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship('books', foreign_key='author_id')

    class Book(Base):
        __tablename__ = 'books'
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)

    try:
        a1 = Author.create(name='C1')
        a2 = Author.create(name='C2')
        Book.create(title='C1-B1', author_id=a1.id)
        Book.create(title='C1-B2', author_id=a1.id)
        # a2 没有书

        # prefetch(...) 返回 PrefetchOption 当作为 options 使用
        opt = prefetch('books')
        assert isinstance(opt, PrefetchOption)

        # immediate 模式：为实例批量预取
        authors = [a1, a2]
        # 预取前不应存在缓存属性
        assert not hasattr(a1, '_cached_books')
        assert not hasattr(a2, '_cached_books')

        prefetch(authors, 'books')

        # 预取后每个实例应有 _cached_books
        assert hasattr(a1, '_cached_books')
        assert hasattr(a2, '_cached_books')

        assert isinstance(a1._cached_books, list)
        assert len(a1._cached_books) == 2
        assert a2._cached_books == []
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_empty_relationship_returns_empty_list(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-empty.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship('books', foreign_key='author_id')

    class Book(Base):
        __tablename__ = 'books'
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)

    try:
        a = Author.create(name='Empty')
        assert a.books == []
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_relationship_rejects_removed_lazy_argument(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-no-lazy.pytucky")
    Base = declarative_base(db, crud=True)

    try:
        with pytest.raises(TypeError):
            class Author(Base):
                __tablename__ = "authors"
                id = Column(int, primary_key=True)
                books = Relationship("books", foreign_key="author_id", lazy=True)  # type: ignore
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_cross_storage_many_to_one_relationship_reads_from_explicit_storage(
    tmp_path: Path,
) -> None:
    base_db = Storage(file_path=tmp_path / "base-many-to-one.pytucky")
    user_db = Storage(file_path=tmp_path / "user-many-to-one.pytucky")
    BaseBase = declarative_base(base_db, crud=True)
    BaseUser = declarative_base(user_db, crud=True)

    class BaseItem(BaseBase):
        __tablename__ = "base_items"
        id = Column(int, primary_key=True)
        name = Column(str)

    class UserItem(BaseUser):
        __tablename__ = "user_items"
        id = Column(int, primary_key=True)
        base_item_id = Column(int)
        nickname = Column(str)
        base_item = Relationship(
            "base_items",
            foreign_key="base_item_id",
            storage=base_db,
        )  # type: ignore

    try:
        base = BaseItem.create(name="Sword")
        item = UserItem.create(base_item_id=base.id, nickname="starter")

        assert item.base_item is not None
        assert item.base_item.id == base.id
        assert item.base_item.name == "Sword"
    finally:
        try:
            user_db.close()
        finally:
            base_db.close()


@pytest.mark.feature
def test_cross_storage_one_to_many_relationship_reads_from_explicit_storage(
    tmp_path: Path,
) -> None:
    base_db = Storage(file_path=tmp_path / "base-one-to-many.pytucky")
    user_db = Storage(file_path=tmp_path / "user-one-to-many.pytucky")
    BaseBase = declarative_base(base_db, crud=True)
    BaseUser = declarative_base(user_db, crud=True)

    class UserItem(BaseUser):
        __tablename__ = "user_items"
        id = Column(int, primary_key=True)
        base_item_id = Column(int)
        nickname = Column(str)

    class BaseItem(BaseBase):
        __tablename__ = "base_items"
        id = Column(int, primary_key=True)
        name = Column(str)
        user_items = Relationship(
            "user_items",
            foreign_key="base_item_id",
            storage=user_db,
        )  # type: ignore

    try:
        base = BaseItem.create(name="Shield")
        UserItem.create(base_item_id=base.id, nickname="alpha")
        UserItem.create(base_item_id=base.id, nickname="beta")

        related = base.user_items
        assert isinstance(related, list)
        assert sorted(item.nickname for item in related) == ["alpha", "beta"]
    finally:
        try:
            user_db.close()
        finally:
            base_db.close()


@pytest.mark.feature
def test_cross_storage_prefetch_many_to_one_uses_relationship_storage(
    tmp_path: Path,
) -> None:
    base_db = Storage(file_path=tmp_path / "base-prefetch-many-to-one.pytucky")
    user_db = Storage(file_path=tmp_path / "user-prefetch-many-to-one.pytucky")
    BaseBase = declarative_base(base_db, crud=True)
    BaseUser = declarative_base(user_db, crud=True)

    class BaseItem(BaseBase):
        __tablename__ = "base_items"
        id = Column(int, primary_key=True)
        name = Column(str)

    class UserItem(BaseUser):
        __tablename__ = "user_items"
        id = Column(int, primary_key=True)
        base_item_id = Column(int)
        nickname = Column(str)
        base_item = Relationship(
            "base_items",
            foreign_key="base_item_id",
            storage=base_db,
        )  # type: ignore

    try:
        base = BaseItem.create(name="Potion")
        item = UserItem.create(base_item_id=base.id, nickname="healer")

        prefetch([item], "base_item")

        assert hasattr(item, "_cached_base_item")
        assert item._cached_base_item is not None
        assert item._cached_base_item.name == "Potion"
    finally:
        try:
            user_db.close()
        finally:
            base_db.close()


@pytest.mark.feature
def test_cross_storage_prefetch_one_to_many_uses_relationship_storage(
    tmp_path: Path,
) -> None:
    base_db = Storage(file_path=tmp_path / "base-prefetch-one-to-many.pytucky")
    user_db = Storage(file_path=tmp_path / "user-prefetch-one-to-many.pytucky")
    BaseBase = declarative_base(base_db, crud=True)
    BaseUser = declarative_base(user_db, crud=True)

    class UserItem(BaseUser):
        __tablename__ = "user_items"
        id = Column(int, primary_key=True)
        base_item_id = Column(int)
        nickname = Column(str)

    class BaseItem(BaseBase):
        __tablename__ = "base_items"
        id = Column(int, primary_key=True)
        name = Column(str)
        user_items = Relationship(
            "user_items",
            foreign_key="base_item_id",
            storage=user_db,
        )  # type: ignore

    try:
        base = BaseItem.create(name="Bow")
        UserItem.create(base_item_id=base.id, nickname="ranged-a")
        UserItem.create(base_item_id=base.id, nickname="ranged-b")

        prefetch([base], "user_items")

        assert hasattr(base, "_cached_user_items")
        assert sorted(item.nickname for item in base._cached_user_items) == ["ranged-a", "ranged-b"]
    finally:
        try:
            user_db.close()
        finally:
            base_db.close()


@pytest.mark.feature
def test_select_options_prefetch_supports_cross_storage_relationship(
    tmp_path: Path,
) -> None:
    base_db = Storage(file_path=tmp_path / "base-select-prefetch.pytucky")
    user_db = Storage(file_path=tmp_path / "user-select-prefetch.pytucky")
    BaseBase = declarative_base(base_db)
    BaseUser = declarative_base(user_db)

    class BaseItem(BaseBase):
        __tablename__ = "base_items"
        id = Column(int, primary_key=True)
        name = Column(str)

    class UserItem(BaseUser):
        __tablename__ = "user_items"
        id = Column(int, primary_key=True)
        base_item_id = Column(int)
        nickname = Column(str)
        base_item = Relationship(
            "base_items",
            foreign_key="base_item_id",
            storage=base_db,
        )  # type: ignore

    session = Session(user_db)
    try:
        base_db.insert("base_items", {"name": "Hammer"})
        user_db.insert("user_items", {"base_item_id": 1, "nickname": "builder"})

        stmt = select(UserItem).options(prefetch("base_item"))
        items = session.execute(stmt).all()

        assert len(items) == 1
        assert items[0].base_item is not None
        assert items[0].base_item.name == "Hammer"
    finally:
        try:
            session.close()
        finally:
            try:
                user_db.close()
            finally:
                base_db.close()
