import struct
from pathlib import Path

import pytest

from pytucky.backends.pager import (
    PAGE_SIZE,
    FileHeader,
    PageHeader,
    PageType,
    Pager,
    build_slotted_page,
    iter_slotted_page_cells,
)
from pytucky.common.exceptions import SerializationError

pytestmark = pytest.mark.unit


def test_file_header_pack_unpack_roundtrip() -> None:
    header = FileHeader(
        page_count=5,
        free_list_head=3,
        schema_root_page=2,
        generation=7,
        flags=9,
        salt=b"a" * 16,
        key_check=b"KEY!",
    )

    restored = FileHeader.unpack(header.pack())

    assert restored == header


def test_file_header_unpack_rejects_truncated_page() -> None:
    with pytest.raises(SerializationError, match="truncated"):
        FileHeader.unpack(b"\x00" * (PAGE_SIZE - 1))


def test_file_header_unpack_rejects_bad_crc() -> None:
    page = bytearray(FileHeader(page_count=2).pack())
    page[8] ^= 0x01

    with pytest.raises(SerializationError, match="CRC mismatch"):
        FileHeader.unpack(bytes(page))


def test_file_header_unpack_rejects_bad_magic() -> None:
    page = FileHeader(page_count=2, magic=b"NOPE").pack()

    with pytest.raises(SerializationError, match="Invalid PTK6 magic"):
        FileHeader.unpack(page)


def test_file_header_unpack_rejects_invalid_page_size() -> None:
    page = FileHeader(page_count=2, page_size=2048).pack()

    with pytest.raises(SerializationError, match="Unsupported PTK6 page size"):
        FileHeader.unpack(page)


def test_pager_write_and_read_roundtrip(tmp_path: Path) -> None:
    file_path = tmp_path / "pager-roundtrip.pytucky"
    pager = Pager(file_path)
    schema_page = build_slotted_page(PageType.SCHEMA, [b"users"])

    pager.write_pages([FileHeader(page_count=2).pack(), schema_page])

    restored_header = pager.read_file_header()
    restored_page = pager.read_page(1)
    cells = iter_slotted_page_cells(restored_page)

    assert restored_header.page_count == 2
    assert [cell for _, cell in cells] == [b"users"]


def test_build_and_iter_slotted_page_roundtrip() -> None:
    page = build_slotted_page(PageType.LEAF, [b"alpha", b"beta"], right_pointer=9)

    header = PageHeader.unpack(page)
    cells = iter_slotted_page_cells(page)

    assert header.page_type == PageType.LEAF
    assert header.right_pointer == 9
    assert [cell for _, cell in cells] == [b"alpha", b"beta"]


def test_build_slotted_page_rejects_overflow() -> None:
    with pytest.raises(SerializationError, match="overflow"):
        build_slotted_page(PageType.LEAF, [b"x" * PAGE_SIZE])


def test_iter_slotted_page_cells_rejects_out_of_bounds_pointer() -> None:
    header_size = len(PageHeader(page_type=PageType.LEAF).pack())
    page = bytearray(build_slotted_page(PageType.LEAF, [b"alpha"]))
    page[header_size:header_size + 2] = struct.pack("<H", 1)

    with pytest.raises(SerializationError, match="out of bounds"):
        iter_slotted_page_cells(bytes(page))


def test_iter_slotted_page_cells_rejects_cell_past_page_boundary() -> None:
    header_size = len(PageHeader(page_type=PageType.LEAF).pack())
    page = bytearray(build_slotted_page(PageType.LEAF, [b"alpha"]))
    page[header_size:header_size + 2] = struct.pack("<H", PAGE_SIZE - 2)
    page[PAGE_SIZE - 2:PAGE_SIZE] = struct.pack("<H", 2)

    with pytest.raises(SerializationError, match="page boundary"):
        iter_slotted_page_cells(bytes(page))
