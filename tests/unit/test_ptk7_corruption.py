from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import random

import pytest

from pytucky import Column
from pytucky.backends.format import FileHeader, HEADER_STRUCT
from pytucky.backends.store import Store
from pytucky.common.exceptions import PytuckyException, SerializationError


def test_random_single_file_never_leaks_low_level_decode_errors(tmp_path: Path) -> None:
    file_path = tmp_path / "random.pytuck"
    generator = random.Random(20260714)

    for _ in range(200):
        data = generator.randbytes(generator.randint(1, 512))
        file_path.write_bytes(data)
        try:
            store = Store(file_path)
        except PytuckyException:
            continue
        store.close()


def test_open_rejects_header_file_size_mismatch(tmp_path: Path) -> None:
    file_path = tmp_path / "size-mismatch.pytuck"
    store = Store(file_path, open_existing=False)
    store.create_table("users", [Column(int, name="id", primary_key=True)])
    store.insert("users", {})
    store.flush()
    store.close()

    raw = file_path.read_bytes()
    header = FileHeader.unpack(raw[:HEADER_STRUCT.size])
    file_path.write_bytes(replace(header, file_size=header.file_size + 1).pack() + raw[HEADER_STRUCT.size:])

    with pytest.raises(SerializationError, match="truncated"):
        Store(file_path)


def test_every_truncated_prefix_of_valid_file_is_rejected(tmp_path: Path) -> None:
    file_path = tmp_path / "truncated.pytuck"
    store = Store(file_path, open_existing=False)
    store.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True),
        ],
    )
    store.insert("users", {"name": "Alice"})
    store.flush()
    store.close()
    complete = file_path.read_bytes()

    for length in range(1, len(complete)):
        truncated_path = tmp_path / f"truncated-{length}.pytuck"
        truncated_path.write_bytes(complete[:length])
        with pytest.raises(PytuckyException):
            Store(truncated_path)


def test_open_rejects_overlapping_table_regions(tmp_path: Path) -> None:
    file_path = tmp_path / "overlap.pytuck"
    store = Store(file_path, open_existing=False)
    store.create_table("users", [Column(int, name="id", primary_key=True)])
    store.insert("users", {})
    store.flush()
    store.close()
    raw = bytearray(file_path.read_bytes())
    header = FileHeader.unpack(raw[:HEADER_STRUCT.size])

    table_ref_start = header.table_ref_offset
    name_length = int.from_bytes(raw[table_ref_start:table_ref_start + 2], "little")
    body_start = table_ref_start + 2 + name_length
    data_offset = int.from_bytes(raw[body_start + 16:body_start + 24], "little")
    raw[body_start + 32:body_start + 40] = data_offset.to_bytes(8, "little")
    file_path.write_bytes(raw)

    with pytest.raises(SerializationError, match="overlaps"):
        Store(file_path)


def test_schema_decoder_rejects_malformed_json_entry(tmp_path: Path) -> None:
    store = Store(tmp_path / "schema.pytuck", open_existing=False)

    with pytest.raises(SerializationError, match="table schema entry"):
        store._decode_schema_catalog(
            b'{"tables":[{"name":"users","columns":"invalid"}]}',
            1,
        )
