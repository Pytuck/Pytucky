from pathlib import Path

import pytest

from pytucky import Storage, declarative_base, Session, Column
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
