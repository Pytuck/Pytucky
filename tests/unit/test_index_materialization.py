"""索引物化缓存回归测试

验证 proxy 索引在首次 lookup 后缓存解码结果，不再重复全量 decode。
"""
from __future__ import annotations

from pathlib import Path
from typing import Type

import pytest

from pytucky import Column, Storage, declarative_base, Session, select, insert
from pytucky import PureBaseModel
from pytucky.backends.store import Store


def _build_store_with_data(path: Path, n: int = 50) -> Store:
    """构建带索引数据的 Store 并 flush"""
    store = Store(path)
    store.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", nullable=False, index=True),
            Column(int, name="age", nullable=True, index="sorted"),
        ],
    )
    for i in range(1, n + 1):
        store.insert("users", {"name": f"user_{i}", "age": 20 + (i % 30)})
    store.flush()
    return store


# ---------- P0 核心测试：重复 lookup 不重复 decode ----------


def test_hash_index_proxy_decodes_once_on_repeated_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """重复等值索引查询不应每次都完整解码索引 blob"""
    file_path = tmp_path / "mat.pytucky"
    _build_store_with_data(file_path, n=100)

    # 通过 PytuckyBackend.load() 重建 proxy
    from pytucky.backends.backend_pytucky import PytuckyBackend
    from pytucky.common.options import PytuckBackendOptions

    backend = PytuckyBackend(file_path, PytuckBackendOptions())
    tables = backend.load()
    table = tables["users"]
    name_index = table.indexes.get("name")
    assert name_index is not None, "name 列应有索引"

    # monkeypatch decode_sorted_pairs 计数
    import pytucky.backends.index as idx_mod

    original_decode = idx_mod.decode_sorted_pairs
    decode_calls = {"count": 0}

    def counting_decode(blob, column):
        decode_calls["count"] += 1
        return original_decode(blob, column)

    monkeypatch.setattr(idx_mod, "decode_sorted_pairs", counting_decode)

    # 连续 10 次 lookup
    for i in range(1, 11):
        result = name_index.lookup(f"user_{i}")
        assert i in result, f"user_{i} 应在索引结果中"

    # 核心断言：decode 应只被调用 1 次（首次物化），而非 10 次
    assert decode_calls["count"] == 1, (
        f"decode_sorted_pairs 被调用了 {decode_calls['count']} 次，"
        f"期望 1 次（首次物化后缓存）"
    )


def test_sorted_index_proxy_decodes_once_on_repeated_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SortedIndexProxy 重复 lookup 也不应重复 decode"""
    file_path = tmp_path / "mat_sorted.pytucky"
    _build_store_with_data(file_path, n=100)

    from pytucky.backends.backend_pytucky import PytuckyBackend
    from pytucky.common.options import PytuckBackendOptions

    backend = PytuckyBackend(file_path, PytuckBackendOptions())
    tables = backend.load()
    table = tables["users"]
    age_index = table.indexes.get("age")
    assert age_index is not None, "age 列应有索引"

    import pytucky.backends.index as idx_mod

    original_decode = idx_mod.decode_sorted_pairs
    decode_calls = {"count": 0}

    def counting_decode(blob, column):
        decode_calls["count"] += 1
        return original_decode(blob, column)

    monkeypatch.setattr(idx_mod, "decode_sorted_pairs", counting_decode)

    # 连续 10 次 lookup
    for age in range(20, 30):
        age_index.lookup(age)

    assert decode_calls["count"] == 1, (
        f"decode_sorted_pairs 被调用了 {decode_calls['count']} 次，期望 1 次"
    )


# ---------- overlay 合并测试 ----------


def test_proxy_insert_before_materialize_visible_after_lookup(
    tmp_path: Path,
) -> None:
    """物化前通过 proxy.insert() 添加的记录，物化后仍可查到"""
    file_path = tmp_path / "overlay.pytucky"
    _build_store_with_data(file_path, n=10)

    from pytucky.backends.backend_pytucky import PytuckyBackend
    from pytucky.common.options import PytuckBackendOptions

    backend = PytuckyBackend(file_path, PytuckBackendOptions())
    tables = backend.load()
    table = tables["users"]
    name_index = table.indexes["name"]

    # 在物化前通过 overlay 插入一条新记录
    name_index.insert("phantom_user", 999)

    # 首次 lookup 触发物化（如果实现了的话），overlay 应合并
    result = name_index.lookup("phantom_user")
    assert 999 in result, "overlay 插入的记录应在物化后可见"


def test_proxy_remove_before_materialize_respected(
    tmp_path: Path,
) -> None:
    """物化前通过 proxy.remove() 删除的记录，物化后不应出现"""
    file_path = tmp_path / "overlay_rm.pytucky"
    _build_store_with_data(file_path, n=10)

    from pytucky.backends.backend_pytucky import PytuckyBackend
    from pytucky.common.options import PytuckBackendOptions

    backend = PytuckyBackend(file_path, PytuckBackendOptions())
    tables = backend.load()
    table = tables["users"]
    name_index = table.indexes["name"]

    # pk=1 对应 name="user_1"，先 remove 再 lookup
    name_index.remove("user_1", 1)
    result = name_index.lookup("user_1")
    assert 1 not in result, "被 remove 的记录不应出现在 lookup 结果中"


# ---------- range_query 正确性 ----------
# 注意：当前 Store schema 反序列化会把 index='sorted' 丢失为 index=True
# (store.py:543)，导致 reopen 后所有索引都创建为 HashIndexProxy。
# range_query 功能在高层通过 Storage.query() 的 blob-based 路径实现，
# 不依赖 proxy 的 range_query。这里只验证 lookup 后不影响正常查询。


def test_materialize_does_not_break_subsequent_store_search(
    tmp_path: Path,
) -> None:
    """物化后 Store.search_index() 仍然可以独立使用（用于 range 等路径）"""
    file_path = tmp_path / "range.pytucky"
    _build_store_with_data(file_path, n=100)

    from pytucky.backends.backend_pytucky import PytuckyBackend
    from pytucky.common.options import PytuckBackendOptions

    backend = PytuckyBackend(file_path, PytuckBackendOptions())
    tables = backend.load()
    table = tables["users"]
    age_index = table.indexes["age"]

    # 先通过 lookup 触发物化
    result_25 = age_index.lookup(25)

    # 物化后 Store.search_index() 仍然能独立工作
    store_results = backend.store.search_index("users", "age", 25)
    assert set(store_results) == result_25


# ---------- 高层 ORM 集成测试 ----------


def test_orm_repeated_filter_by_indexed_column(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """通过 ORM select().filter_by() 反复查询索引列，decode 只发生一次"""
    db = Storage(file_path=tmp_path / "orm_mat.pytucky")
    Base: Type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False, index=True)

    session = Session(db)
    for i in range(1, 51):
        session.execute(insert(User).values(name=f"user_{i}"))
    session.commit()
    db.flush()

    # 重新打开
    db2 = Storage(file_path=tmp_path / "orm_mat.pytucky")
    Base2: Type[PureBaseModel] = declarative_base(db2)

    class User2(Base2):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False, index=True)

    session2 = Session(db2)

    import pytucky.backends.index as idx_mod

    original_decode = idx_mod.decode_sorted_pairs
    decode_calls = {"count": 0}

    def counting_decode(blob, column):
        decode_calls["count"] += 1
        return original_decode(blob, column)

    monkeypatch.setattr(idx_mod, "decode_sorted_pairs", counting_decode)

    # 10 次查询
    for i in range(1, 11):
        rows = session2.execute(select(User2).filter_by(name=f"user_{i}")).all()
        assert len(rows) == 1
        assert rows[0].name == f"user_{i}"

    assert decode_calls["count"] <= 1, (
        f"ORM filter_by 查询触发了 {decode_calls['count']} 次 decode，期望 <= 1"
    )
