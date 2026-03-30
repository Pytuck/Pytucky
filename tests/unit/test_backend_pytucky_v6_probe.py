from pathlib import Path
from typing import List

import pytest

from pytucky import Column
from pytucky.backends.backend_pytucky_v6 import PytuckyBackend
from pytucky.backends.pager import FileHeader, PAGE_SIZE, Pager
from pytucky.backends.schema_v6 import TableSchemaEntry, build_schema_page

pytestmark = pytest.mark.unit


def make_schema_entries() -> List[TableSchemaEntry]:
    return [
        TableSchemaEntry(
            name="users",
            primary_key="id",
            comment="probe table",
            next_id=2,
            root_page=2,
            columns=[
                Column(int, name="id", primary_key=True),
                Column(str, name="name", index=True),
            ],
        )
    ]


def write_probe_file(
    file_path: Path,
    *,
    page_count: int,
    entries: List[TableSchemaEntry],
) -> None:
    Pager(file_path).write_pages([
        FileHeader(page_count=page_count).pack(),
        build_schema_page(entries),
    ])


def test_probe_returns_false_for_missing_file(tmp_path: Path) -> None:
    matched, info = PytuckyBackend.probe(tmp_path / "missing.pytucky")

    assert matched is False
    assert info is None


def test_probe_returns_false_for_non_ptky_magic(tmp_path: Path) -> None:
    file_path = tmp_path / "wrong-magic.pytucky"
    file_path.write_bytes(b"NOPE" + (b"\x00" * (PAGE_SIZE - 4)))

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is False
    assert info is None


def test_probe_returns_false_for_page_count_mismatch(tmp_path: Path) -> None:
    file_path = tmp_path / "mismatch.pytucky"
    write_probe_file(file_path, page_count=3, entries=[])

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is False
    assert info is None


def test_probe_returns_true_for_valid_file(tmp_path: Path) -> None:
    file_path = tmp_path / "valid.pytucky"
    write_probe_file(file_path, page_count=2, entries=make_schema_entries())

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is True
    assert info is not None
    assert info["engine"] == "pytucky"
    assert info["format_version"] == "6"
    assert info["page_size"] == PAGE_SIZE
    assert info["page_count"] == 2
    assert info["table_count"] == 1
    assert info["confidence"] == "high"
