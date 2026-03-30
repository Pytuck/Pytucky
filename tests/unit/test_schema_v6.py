from pathlib import Path
from typing import Dict, List, Optional

import pytest

from pytucky import Column
from pytucky.backends.schema_v6 import (
    TableSchemaEntry,
    build_schema_page,
    deserialize_table_schema,
    parse_schema_page,
    serialize_entry,
    serialize_table_schema,
)
from pytucky.common.exceptions import SerializationError

pytestmark = pytest.mark.unit


class TableStub:
    def __init__(self) -> None:
        self.name: str = "users"
        self.primary_key: Optional[str] = "id"
        self.comment: Optional[str] = "user table"
        self.next_id: int = 42
        columns = [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True, comment="display name"),
            Column(int, name="score", nullable=True, index="sorted"),
        ]
        self.columns: Dict[str, Column] = {
            column.name: column for column in columns if column.name is not None
        }


def make_entry(*, name: str = "users", root_page: int = 7) -> TableSchemaEntry:
    columns = [
        Column(int, name="id", primary_key=True),
        Column(str, name="name", index=True, comment="display name"),
        Column(int, name="score", nullable=True, index="sorted"),
    ]
    return TableSchemaEntry(
        name=name,
        primary_key="id",
        comment="user table",
        next_id=42,
        root_page=root_page,
        columns=columns,
    )


def test_serialize_deserialize_table_schema_roundtrip() -> None:
    payload = serialize_table_schema(TableStub(), root_page=7)

    entry = deserialize_table_schema(payload)

    assert entry.name == "users"
    assert entry.primary_key == "id"
    assert entry.comment == "user table"
    assert entry.next_id == 42
    assert entry.root_page == 7
    assert [column.name for column in entry.columns] == ["id", "name", "score"]
    assert entry.columns[0].primary_key is True
    assert entry.columns[1].index is True
    assert entry.columns[1].comment == "display name"
    assert entry.columns[2].nullable is True
    assert entry.columns[2].index == "sorted"


def test_deserialize_table_schema_rejects_truncated_payload() -> None:
    payload = serialize_entry(make_entry())[:-1]

    with pytest.raises(SerializationError, match="truncated"):
        deserialize_table_schema(payload)


def test_deserialize_table_schema_rejects_trailing_bytes() -> None:
    payload = serialize_entry(make_entry()) + b"\x00"

    with pytest.raises(SerializationError, match="trailing bytes"):
        deserialize_table_schema(payload)


def test_build_and_parse_schema_page_roundtrip() -> None:
    entries = [make_entry(name="users", root_page=7), make_entry(name="profiles", root_page=9)]

    page = build_schema_page(entries)
    restored_entries = parse_schema_page(page)

    assert [entry.name for entry in restored_entries] == ["users", "profiles"]
    assert [entry.root_page for entry in restored_entries] == [7, 9]
    assert restored_entries[0].columns[1].index is True
    assert restored_entries[1].columns[2].index == "sorted"


def test_parse_schema_page_accepts_empty_schema() -> None:
    restored_entries = parse_schema_page(build_schema_page([]))

    assert restored_entries == []
