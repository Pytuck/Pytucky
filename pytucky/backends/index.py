# PTK7 索引块低层原语，最小实现：构建、编码、解码、查找、范围查找
# 中文注释，英文标识符
from __future__ import annotations

from typing import Any
import struct
from pytucky.core.types import TypeRegistry
from pytucky.core.orm import Column

# 格式说明（简单版）
# [1 byte type_code][4 bytes entry_count][entry...]
# 每个 entry: [value encoded by codec][8 bytes pk (int64)]

HEADER_STRUCT = struct.Struct('<B I')
PK_STRUCT = struct.Struct('<q')

def build_sorted_pairs(records: list[tuple[int, dict[str, Any]]], column: Column) -> list[tuple[Any, int]]:
    """从 records 构建 (value, pk) 列表，跳过 None 并按 value 升序排序"""
    out: list[tuple[Any, int]] = []
    for pk, record in records:
        value = record.get(column.name)
        if value is None:
            continue
        out.append((value, pk))
    out.sort(key=lambda t: (t[0], t[1]))
    return out

def encode_sorted_pairs(pairs: list[tuple[Any, int]], column: Column) -> bytes:
    """将已排序的 pairs 编码为 bytes"""
    # 获取类型编码与 codec
    type_code, codec = TypeRegistry.get_codec(column.col_type)
    buf = bytearray()
    buf.extend(HEADER_STRUCT.pack(int(type_code), len(pairs)))
    for value, pk in pairs:
        # encode value
        buf.extend(codec.encode(value))
        # encode pk as int64
        buf.extend(PK_STRUCT.pack(int(pk)))
    return bytes(buf)

def decode_sorted_pairs(blob: bytes, column: Column) -> list[tuple[Any, int]]:
    """从 blob 解码为 pairs 列表

    出现任何损坏或截断时统一抛出 SerializationError，而不是 ValueError 或静默返回。
    """
    from pytucky.common.exceptions import SerializationError

    if len(blob) < HEADER_STRUCT.size:
        raise SerializationError('header too short')
    try:
        type_code, count = HEADER_STRUCT.unpack(blob[: HEADER_STRUCT.size])
    except struct.error as e:
        raise SerializationError('invalid header') from e
    _, codec = TypeRegistry.get_codec_by_code(type_code)
    offset = HEADER_STRUCT.size
    out: list[tuple[Any, int]] = []
    for _ in range(count):
        # decode value using codec which returns (value, consumed)
        try:
            value, consumed = codec.decode(blob[offset:])
        except Exception as e:
            raise SerializationError('value decode failed') from e
        offset += consumed
        if offset + PK_STRUCT.size > len(blob):
            raise SerializationError('truncated pk')
        try:
            pk = PK_STRUCT.unpack(blob[offset: offset + PK_STRUCT.size])[0]
        except struct.error as e:
            raise SerializationError('invalid pk') from e
        offset += PK_STRUCT.size
        out.append((value, pk))
    return out

def search_sorted_pairs(blob: bytes, value: Any, column: Column) -> list[int]:
    """等值查找，返回匹配的 pk 列表"""
    pairs = decode_sorted_pairs(blob, column)
    # linear scan acceptable for now; but pairs are sorted so we can binary search
    result: list[int] = []
    for v, pk in pairs:
        if v == value:
            result.append(pk)
    return result

def range_search_sorted_pairs(
    blob: bytes,
    column: Column,
    min_value: Any | None = None,
    max_value: Any | None = None,
    include_min: bool = True,
    include_max: bool = True,
) -> list[int]:
    """范围查找，基于解码后列表的二分逻辑实现"""
    pairs = decode_sorted_pairs(blob, column)
    values = [v for v, _ in pairs]
    # perform binary search using bisect on values
    from bisect import bisect_left, bisect_right
    if min_value is None:
        left = 0
    else:
        left = bisect_left(values, min_value) if include_min else bisect_right(values, min_value)
    if max_value is None:
        right = len(values)
    else:
        right = bisect_right(values, max_value) if include_max else bisect_left(values, max_value)
    result: list[int] = [pk for _, pk in pairs[left:right]]
    return result
