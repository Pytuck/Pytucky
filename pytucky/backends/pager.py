"""
PTK6 / pytucky 页管理器。

当前实现提供：
- 文件头页（Page 0）读写
- 通用 4KB 页头定义
- 槽式页（slotted page）构建与解析
- 基础页读写能力

说明：
- V1 先把页格式和页工具稳定下来，后续可在此基础上继续引入真正的
  B+Tree 内部页分裂、空闲页链表和 rollback journal。
- 当前不实现加密，但文件头保留盐值和 key check 字段。
"""

import os
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import List, Sequence, Tuple

from ..common.exceptions import SerializationError


PAGE_SIZE = 4096
MAGIC = b'PTKY'
FORMAT_VERSION = 6

_FILE_HEADER_PREFIX_STRUCT = struct.Struct('<4sHHIIIQI16s4s')
_FILE_HEADER_CRC_STRUCT = struct.Struct('<I')
_PAGE_HEADER_STRUCT = struct.Struct('<BBHHHII')
_CELL_POINTER_STRUCT = struct.Struct('<H')
_CELL_LENGTH_STRUCT = struct.Struct('<H')


class PageType(IntEnum):
    """PTK6 页类型。"""

    LEAF = 1
    INTERNAL = 2
    OVERFLOW = 3
    FREE = 4
    SCHEMA = 5


@dataclass
class FileHeader:
    """Page 0 文件头。"""

    page_count: int
    free_list_head: int = 0
    schema_root_page: int = 1
    generation: int = 1
    flags: int = 0
    salt: bytes = b'\x00' * 16
    key_check: bytes = b'\x00' * 4
    version: int = FORMAT_VERSION
    page_size: int = PAGE_SIZE
    magic: bytes = MAGIC

    def pack(self) -> bytes:
        """打包为完整 4KB 页。"""
        prefix = _FILE_HEADER_PREFIX_STRUCT.pack(
            self.magic,
            self.version,
            self.page_size,
            self.page_count,
            self.free_list_head,
            self.schema_root_page,
            self.generation,
            self.flags,
            self.salt,
            self.key_check,
        )
        crc32 = zlib.crc32(prefix) & 0xFFFFFFFF
        page = bytearray(PAGE_SIZE)
        page[:len(prefix)] = prefix
        page[len(prefix):len(prefix) + _FILE_HEADER_CRC_STRUCT.size] = _FILE_HEADER_CRC_STRUCT.pack(crc32)
        return bytes(page)

    @classmethod
    def unpack(cls, data: bytes) -> 'FileHeader':
        """从完整 4KB 页解包文件头。"""
        if len(data) < PAGE_SIZE:
            raise SerializationError('PTK6 header page is truncated')

        prefix_size = _FILE_HEADER_PREFIX_STRUCT.size
        prefix = data[:prefix_size]
        stored_crc = _FILE_HEADER_CRC_STRUCT.unpack(
            data[prefix_size:prefix_size + _FILE_HEADER_CRC_STRUCT.size]
        )[0]
        actual_crc = zlib.crc32(prefix) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise SerializationError('PTK6 header CRC mismatch')

        (
            magic,
            version,
            page_size,
            page_count,
            free_list_head,
            schema_root_page,
            generation,
            flags,
            salt,
            key_check,
        ) = _FILE_HEADER_PREFIX_STRUCT.unpack(prefix)

        if magic != MAGIC:
            raise SerializationError(f'Invalid PTK6 magic: {magic!r}')
        if page_size != PAGE_SIZE:
            raise SerializationError(f'Unsupported PTK6 page size: {page_size}')

        return cls(
            magic=magic,
            version=version,
            page_size=page_size,
            page_count=page_count,
            free_list_head=free_list_head,
            schema_root_page=schema_root_page,
            generation=generation,
            flags=flags,
            salt=salt,
            key_check=key_check,
        )


@dataclass
class PageHeader:
    """通用页头（所有非头页共用）。"""

    page_type: int
    flags: int = 0
    cell_count: int = 0
    cell_content_offset: int = PAGE_SIZE
    free_block_offset: int = 0
    right_pointer: int = 0
    reserved: int = 0

    def pack(self) -> bytes:
        return _PAGE_HEADER_STRUCT.pack(
            self.page_type,
            self.flags,
            self.cell_count,
            self.cell_content_offset,
            self.free_block_offset,
            self.right_pointer,
            self.reserved,
        )

    @classmethod
    def unpack(cls, data: bytes) -> 'PageHeader':
        if len(data) < _PAGE_HEADER_STRUCT.size:
            raise SerializationError('PTK6 page header is truncated')
        return cls(*_PAGE_HEADER_STRUCT.unpack(data[:_PAGE_HEADER_STRUCT.size]))


class Pager:
    """基础页读写器。"""

    def __init__(self, file_path: Path):
        self.file_path = Path(file_path).expanduser()

    @staticmethod
    def page_offset(page_no: int) -> int:
        """页号转文件偏移。"""
        return page_no * PAGE_SIZE

    def exists(self) -> bool:
        return self.file_path.exists()

    def read_file_header(self) -> FileHeader:
        with open(self.file_path, 'rb') as handle:
            return FileHeader.unpack(handle.read(PAGE_SIZE))

    def read_page(self, page_no: int) -> bytes:
        with open(self.file_path, 'rb') as handle:
            handle.seek(self.page_offset(page_no))
            data = handle.read(PAGE_SIZE)
        if len(data) != PAGE_SIZE:
            raise SerializationError(f'PTK6 page {page_no} is truncated')
        return data

    def write_pages(self, pages: Sequence[bytes]) -> None:
        with open(self.file_path, 'wb') as handle:
            for page in pages:
                if len(page) != PAGE_SIZE:
                    raise SerializationError('PTK6 page must be exactly 4096 bytes')
                handle.write(page)
            handle.flush()
            os.fsync(handle.fileno())


def estimate_slotted_page_size(cells: Sequence[bytes]) -> int:
    """估算槽式页大小。"""
    return _PAGE_HEADER_STRUCT.size + len(cells) * _CELL_POINTER_STRUCT.size + sum(
        _CELL_LENGTH_STRUCT.size + len(cell) for cell in cells
    )


def build_slotted_page(
    page_type: int,
    cells: Sequence[bytes],
    *,
    flags: int = 0,
    right_pointer: int = 0,
) -> bytes:
    """构建槽式页。

    每个 cell 实际落盘格式：
    - cell_length: uint16
    - cell_payload: bytes
    """
    if estimate_slotted_page_size(cells) > PAGE_SIZE:
        raise SerializationError('PTK6 page overflow while building slotted page')

    page = bytearray(PAGE_SIZE)
    content_offset = PAGE_SIZE
    pointers: List[int] = []

    for cell in cells:
        packed_cell = _CELL_LENGTH_STRUCT.pack(len(cell)) + cell
        content_offset -= len(packed_cell)
        if content_offset < _PAGE_HEADER_STRUCT.size + len(cells) * _CELL_POINTER_STRUCT.size:
            raise SerializationError('PTK6 page overflow while placing cell')
        page[content_offset:content_offset + len(packed_cell)] = packed_cell
        pointers.append(content_offset)

    header = PageHeader(
        page_type=page_type,
        flags=flags,
        cell_count=len(cells),
        cell_content_offset=content_offset,
        free_block_offset=0,
        right_pointer=right_pointer,
        reserved=0,
    )
    page[:_PAGE_HEADER_STRUCT.size] = header.pack()

    offset = _PAGE_HEADER_STRUCT.size
    for pointer in pointers:
        page[offset:offset + _CELL_POINTER_STRUCT.size] = _CELL_POINTER_STRUCT.pack(pointer)
        offset += _CELL_POINTER_STRUCT.size

    return bytes(page)


def iter_slotted_page_cells(page_data: bytes) -> List[Tuple[int, bytes]]:
    """解析槽式页，返回 [(cell_offset, cell_payload), ...]。"""
    header = PageHeader.unpack(page_data)
    offset = _PAGE_HEADER_STRUCT.size
    result: List[Tuple[int, bytes]] = []
    min_cell_offset = _PAGE_HEADER_STRUCT.size + header.cell_count * _CELL_POINTER_STRUCT.size

    for _ in range(header.cell_count):
        pointer = _CELL_POINTER_STRUCT.unpack(
            page_data[offset:offset + _CELL_POINTER_STRUCT.size]
        )[0]
        offset += _CELL_POINTER_STRUCT.size

        if pointer < min_cell_offset or pointer + _CELL_LENGTH_STRUCT.size > PAGE_SIZE:
            raise SerializationError('PTK6 cell pointer out of bounds')

        cell_length = _CELL_LENGTH_STRUCT.unpack(
            page_data[pointer:pointer + _CELL_LENGTH_STRUCT.size]
        )[0]
        cell_start = pointer + _CELL_LENGTH_STRUCT.size
        cell_end = cell_start + cell_length
        if cell_end > PAGE_SIZE:
            raise SerializationError('PTK6 cell exceeds page boundary')

        result.append((pointer, page_data[cell_start:cell_end]))

    return result
