# 测试 PTK7 索引块原语（TDD）
# 中文注释，英文标识符

from pytucky.backends.index_v7 import (
    build_sorted_pairs,
    encode_sorted_pairs,
    decode_sorted_pairs,
    search_sorted_pairs,
    range_search_sorted_pairs,
)
from pytucky.core.orm import Column
from pytucky.common.exceptions import SerializationError


def test_build_skips_none_and_sorts() -> None:
    col = Column(int, name='age')
    records = [
        (1, {'age': 30}),
        (2, {'age': None}),
        (3, {'age': 20}),
        (4, {'age': 25}),
    ]
    pairs = build_sorted_pairs(records, col)
    # 应跳过 None，并按 value 升序
    assert pairs == [(20, 3), (25, 4), (30, 1)]


def test_encode_decode_roundtrip() -> None:
    col = Column(str, name='name')
    pairs = [("bob", 1), ("alice", 2)]
    blob = encode_sorted_pairs(pairs, col)
    decoded = decode_sorted_pairs(blob, col)
    assert decoded == pairs


def test_search_equals() -> None:
    col = Column(int, name='score')
    pairs = [(10, 1), (20, 2), (10, 3), (30, 4)]
    blob = encode_sorted_pairs(pairs, col)
    found = search_sorted_pairs(blob, 10, col)
    assert set(found) == {1, 3}
    found2 = search_sorted_pairs(blob, 25, col)
    assert found2 == []


def test_range_search_inclusive_exclusive() -> None:
    col = Column(int, name='v')
    pairs = [(1, 1), (2, 2), (3, 3), (4, 4)]
    blob = encode_sorted_pairs(pairs, col)
    # inclusive range 2..3 -> pks 2,3
    r = range_search_sorted_pairs(blob, col, 2, 3, True, True)
    assert set(r) == {2, 3}
    # exclusive min -> 2.. -> 3,4 when min=2 exclusive and max None
    r2 = range_search_sorted_pairs(blob, col, 2, None, False, True)
    assert set(r2) == {3, 4}
    # max exclusive 1..3 (include min True, exclude max) -> should include 1 and 2
    r3 = range_search_sorted_pairs(blob, col, 1, 3, True, False)
    assert set(r3) == {1, 2}


def test_decode_truncated_header_raises() -> None:
    col = Column(int, name='x')
    # empty blob and too-short header should raise SerializationError
    import pytest
    with pytest.raises(SerializationError):
        decode_sorted_pairs(b'', col)
    with pytest.raises(SerializationError):
        decode_sorted_pairs(b'\x01\x00', col)


def test_decode_truncated_pk_raises() -> None:
    col = Column(int, name='x')
    pairs = [(1, 1)]
    blob = encode_sorted_pairs(pairs, col)
    # truncate the blob so pk bytes are missing
    truncated = blob[:-2]
    import pytest
    with pytest.raises(SerializationError):
        decode_sorted_pairs(truncated, col)
