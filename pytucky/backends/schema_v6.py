"""
PTK6 schema 页编码与解码。

当前职责：
- 将表元数据编码到 schema 页 cell
- 从 schema 页恢复表元数据
- 复用现有 Column / TypeRegistry 定义
"""

import struct
from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING

from ..common.exceptions import SerializationError
from ..core.orm import Column
from ..core.types import TypeCode, TypeRegistry
from .pager import PageType, build_slotted_page, iter_slotted_page_cells

if TYPE_CHECKING:
    from ..core.storage import Table


@dataclass
class TableSchemaEntry:
    """单个表的 schema 条目。"""

    name: str
    primary_key: Optional[str]
    comment: Optional[str]
    next_id: int
    root_page: int
    columns: List[Column]


def serialize_table_schema(table: 'Table', root_page: int) -> bytes:
    """序列化单个表的 schema。"""
    buf = bytearray()

    table_name_bytes = table.name.encode('utf-8')
    buf += struct.pack('<H', len(table_name_bytes))
    buf += table_name_bytes

    primary_key_bytes = (table.primary_key or '').encode('utf-8')
    buf += struct.pack('<H', len(primary_key_bytes))
    buf += primary_key_bytes

    comment_bytes = (table.comment or '').encode('utf-8')
    buf += struct.pack('<H', len(comment_bytes))
    buf += comment_bytes

    buf += struct.pack('<I', root_page)
    buf += struct.pack('<Q', table.next_id)
    buf += struct.pack('<H', len(table.columns))

    for column in table.columns.values():
        assert column.name is not None, 'Column name must be set'
        column_name_bytes = column.name.encode('utf-8')
        type_code, _ = TypeRegistry.get_codec(column.col_type)

        flags = 0
        if column.nullable:
            flags |= 0x01
        if column.primary_key:
            flags |= 0x02
        if column.index:
            flags |= 0x04
        if column.index == 'sorted':
            flags |= 0x08

        column_comment_bytes = (column.comment or '').encode('utf-8')

        buf += struct.pack('<H', len(column_name_bytes))
        buf += column_name_bytes
        buf += struct.pack('B', int(type_code))
        buf += struct.pack('B', flags)
        buf += struct.pack('<H', len(column_comment_bytes))
        buf += column_comment_bytes
        buf += struct.pack('<I', 0)  # index_root_page 预留，V1 固定为 0

    return bytes(buf)


def deserialize_table_schema(data: bytes) -> TableSchemaEntry:
    """反序列化单个表的 schema。"""
    offset = 0

    def read_chunk(size: int) -> bytes:
        nonlocal offset
        end = offset + size
        if end > len(data):
            raise SerializationError('PTK6 schema cell is truncated')
        chunk = data[offset:end]
        offset = end
        return chunk

    table_name_length = struct.unpack('<H', read_chunk(2))[0]
    table_name = read_chunk(table_name_length).decode('utf-8')

    primary_key_length = struct.unpack('<H', read_chunk(2))[0]
    primary_key_raw = read_chunk(primary_key_length).decode('utf-8')
    primary_key = primary_key_raw or None

    comment_length = struct.unpack('<H', read_chunk(2))[0]
    comment_raw = read_chunk(comment_length).decode('utf-8')
    comment = comment_raw or None

    root_page = struct.unpack('<I', read_chunk(4))[0]
    next_id = struct.unpack('<Q', read_chunk(8))[0]
    column_count = struct.unpack('<H', read_chunk(2))[0]

    columns: List[Column] = []
    for _ in range(column_count):
        column_name_length = struct.unpack('<H', read_chunk(2))[0]
        column_name = read_chunk(column_name_length).decode('utf-8')

        type_code = TypeCode(read_chunk(1)[0])
        flags = read_chunk(1)[0]

        column_comment_length = struct.unpack('<H', read_chunk(2))[0]
        column_comment_raw = read_chunk(column_comment_length).decode('utf-8')
        column_comment = column_comment_raw or None

        read_chunk(4)  # index_root_page 预留，当前忽略

        column = Column(
            TypeRegistry.get_type_from_code(type_code),
            name=column_name,
            nullable=bool(flags & 0x01),
            primary_key=bool(flags & 0x02),
            index='sorted' if (flags & 0x08) else bool(flags & 0x04),
            comment=column_comment,
        )
        columns.append(column)

    if offset != len(data):
        raise SerializationError('PTK6 schema cell contains trailing bytes')

    return TableSchemaEntry(
        name=table_name,
        primary_key=primary_key,
        comment=comment,
        next_id=next_id,
        root_page=root_page,
        columns=columns,
    )


def build_schema_page(entries: List[TableSchemaEntry]) -> bytes:
    """构建 schema 页。"""
    cells = [serialize_entry(entry) for entry in entries]
    return build_slotted_page(PageType.SCHEMA, cells)


def parse_schema_page(page_data: bytes) -> List[TableSchemaEntry]:
    """解析 schema 页。"""
    return [deserialize_table_schema(cell) for _, cell in iter_slotted_page_cells(page_data)]


def serialize_entry(entry: TableSchemaEntry) -> bytes:
    """序列化条目对象。"""
    class TableProxy:
        def __init__(self) -> None:
            self.name = entry.name
            self.primary_key = entry.primary_key
            self.comment = entry.comment
            self.next_id = entry.next_id
            self.columns = {column.name: column for column in entry.columns if column.name is not None}

    return serialize_table_schema(TableProxy(), entry.root_page)
