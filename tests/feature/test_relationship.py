import importlib
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
def test_back_populates_populates_reverse_cache_on_many_to_one(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-back-populates-many-to-one.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = "authors"
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship("books", foreign_key="author_id", back_populates="author")

    class Book(Base):
        __tablename__ = "books"
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)
        author = Relationship("authors", foreign_key="author_id", back_populates="books")

    try:
        author = Author.create(name="Alice")
        book = Book.create(title="Book1", author_id=author.id)

        loaded_book = Book.get(book.id)
        assert loaded_book is not None
        loaded_author = loaded_book.author

        assert loaded_author is not None
        assert hasattr(loaded_author, "_cached_books")
        assert len(loaded_author._cached_books) == 1
        assert loaded_author._cached_books[0] is loaded_book
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_back_populates_populates_reverse_cache_on_one_to_many_prefetch(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-back-populates-prefetch.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = "authors"
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship("books", foreign_key="author_id", back_populates="author")

    class Book(Base):
        __tablename__ = "books"
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)
        author = Relationship("authors", foreign_key="author_id", back_populates="books")

    try:
        author = Author.create(name="Bob")
        Book.create(title="B1", author_id=author.id)
        Book.create(title="B2", author_id=author.id)

        loaded_author = Author.get(author.id)
        assert loaded_author is not None

        prefetch([loaded_author], "books")

        assert hasattr(loaded_author, "_cached_books")
        assert len(loaded_author._cached_books) == 2
        for loaded_book in loaded_author._cached_books:
            assert hasattr(loaded_book, "_cached_author")
            assert loaded_book._cached_author is loaded_author
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_back_populates_requires_symmetric_definition(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-back-populates-invalid.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class Author(Base):
        __tablename__ = "authors"
        id = Column(int, primary_key=True)
        name = Column(str)
        books = Relationship("books", foreign_key="author_id", back_populates="writer")

    class Book(Base):
        __tablename__ = "books"
        id = Column(int, primary_key=True)
        title = Column(str)
        author_id = Column(int)
        author = Relationship("authors", foreign_key="author_id", back_populates="books")

    try:
        author = Author.create(name="Carol")
        Book.create(title="Broken", author_id=author.id)
        loaded_author = Author.get(author.id)
        assert loaded_author is not None

        with pytest.raises(Exception, match="back_populates|writer|books"):
            _ = loaded_author.books
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
def test_prefetch_passes_hash_container_to_in_conditions(tmp_path: Path, monkeypatch) -> None:
    db = Storage(file_path=tmp_path / "rel-prefetch-in-container.pytucky")
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
        author = Relationship('authors', foreign_key='author_id')

    captured_values = []
    prefetch_module = importlib.import_module("pytucky.core.prefetch")
    original_condition = prefetch_module.Condition

    def capture_condition(field, operator, value):
        if operator == 'IN':
            captured_values.append(value)
        return original_condition(field, operator, value)

    monkeypatch.setattr(prefetch_module, 'Condition', capture_condition)

    try:
        author = Author.create(name='Hashy')
        book = Book.create(title='B1', author_id=author.id)

        loaded_author = Author.get(author.id)
        loaded_book = Book.get(book.id)
        assert loaded_author is not None
        assert loaded_book is not None

        prefetch([loaded_author], 'books')
        prefetch([loaded_book], 'author')

        assert len(captured_values) == 2
        assert all(isinstance(value, frozenset) for value in captured_values)
        assert len(loaded_author._cached_books) == 1
        assert loaded_book._cached_author is not None
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.feature
def test_relationship_string_target_is_table_name_only(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "rel-string-table-only.pytucky")
    Base: Type = declarative_base(db, crud=True)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)

    class Order(Base):
        __tablename__ = "orders"
        id = Column(int, primary_key=True)
        user_id = Column(int)
        user = Relationship("User", foreign_key="user_id")  # type: ignore[arg-type]

    try:
        user = User.create(name="WrongString")
        order = Order.create(user_id=user.id)
        loaded_order = Order.get(order.id)
        assert loaded_order is not None

        with pytest.raises(Exception, match="table name|String targets|User"):
            _ = loaded_order.user
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
