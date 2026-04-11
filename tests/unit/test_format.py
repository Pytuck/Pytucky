from __future__ import annotations

import pytest

from pytucky import Column
from pytucky.common.exceptions import SerializationError
from pytucky.backends.format import (
    FileHeader,
    PkDirEntry,
    TableBlockRef,
    decode_row,
    encode_row,
)


def test_header_roundtrip() -> None:
    header = FileHeader(
        version=7,
        flags=3,
        table_count=2,
        schema_offset=64,
        schema_size=128,
        table_ref_offset=192,
        table_ref_size=96,
        file_size=4096,
        checksum=12345,
    )
    blob = header.pack()

    assert len(blob) == 64
    assert FileHeader.unpack(blob) == header



def test_table_block_ref_roundtrip() -> None:
    ref = TableBlockRef(
        name="users",
        record_count=10,
        next_id=11,
        data_offset=256,
        data_size=1024,
        pk_dir_offset=1280,
        pk_dir_size=160,
        index_meta_offset=1440,
        index_meta_size=96,
        index_data_offset=1536,
        index_data_size=512,
    )
    blob = ref.pack()

    decoded, consumed = TableBlockRef.unpack(blob)
    assert consumed == len(blob)
    assert decoded == ref



def test_int_pk_dir_entry_roundtrip() -> None:
    entry = PkDirEntry(pk=42, offset=1024, length=37)
    blob = entry.pack_int()

    decoded = PkDirEntry.unpack_int(blob)
    assert decoded == entry



def test_header_unpack_rejects_wrong_magic() -> None:
    header = FileHeader().pack()
    bad = b"NOPE" + header[4:]

    with pytest.raises(SerializationError):
        FileHeader.unpack(bad)



def test_table_block_ref_pack_rejects_overlong_name() -> None:
    ref = TableBlockRef(
        name="u" * 70000,
        record_count=0,
        next_id=1,
        data_offset=0,
        data_size=0,
        pk_dir_offset=0,
        pk_dir_size=0,
        index_meta_offset=0,
        index_meta_size=0,
        index_data_offset=0,
        index_data_size=0,
    )

    with pytest.raises(SerializationError):
        ref.pack()



def test_table_block_ref_unpack_rejects_invalid_utf8_name() -> None:
    blob = b"\x01\x00" + b"\xff" + (b"\x00" * 80)

    with pytest.raises(SerializationError):
        TableBlockRef.unpack(blob)



def test_decode_row_rejects_truncated_payload() -> None:
    columns = [
        Column(int, name="id", primary_key=True),
        Column(str, name="name", nullable=False),
        Column(int, name="age", nullable=True),
    ]
    record = {"id": 7, "name": "Alice", "age": 18}

    payload = encode_row(columns, record, pk_name="id")
    with pytest.raises(SerializationError):
        decode_row(columns, payload[:-1], pk_name="id")



def test_decode_row_wraps_invalid_string_payload() -> None:
    columns = [
        Column(int, name="id", primary_key=True),
        Column(str, name="name", nullable=False),
    ]
    payload = b"\x00\x00\x00\x00" + b"\x01\x00" + b"\xff"

    with pytest.raises(SerializationError):
        decode_row(columns, payload, pk_name="id")



def test_encode_row_skips_primary_key_payload() -> None:
    columns = [
        Column(int, name="id", primary_key=True),
        Column(str, name="name", nullable=False),
        Column(int, name="age", nullable=True),
    ]
    record = {"id": 7, "name": "Alice", "age": 18}

    payload = encode_row(columns, record, pk_name="id")
    decoded = decode_row(columns, payload, pk_name="id")

    assert decoded == {"name": "Alice", "age": 18}



def test_column_index_meta_roundtrip() -> None:
    from pytucky.backends.format import ColumnIndexMeta

    cim = ColumnIndexMeta(column_name="name", offset=1234, size=56, entry_count=3, type_code=7)
    packed = cim.pack()
    cim2, consumed = ColumnIndexMeta.unpack(packed)
    assert consumed == len(packed)
    assert cim2.column_name == cim.column_name
    assert cim2.offset == cim.offset
    assert cim2.size == cim.size
    assert cim2.entry_count == cim.entry_count
    assert cim2.type_code == cim.type_code
