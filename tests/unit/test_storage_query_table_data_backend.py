from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pytucky import Column, Storage
from pytucky.backends.base import StorageBackend
from pytucky.common.options import PytuckBackendOptions


class _BackendPaginationStub(StorageBackend):
    def __init__(self, file_path: str | Path, *, should_raise: bool = False) -> None:
        super().__init__(file_path, PytuckBackendOptions())
        self.should_raise = should_raise
        self.calls: list[dict[str, Any]] = []

    def save(self, tables: dict[str, Any], *, changed_tables: set[str] | None = None) -> None:
        del tables, changed_tables

    def load(self) -> dict[str, Any]:
        return {}

    def exists(self) -> bool:
        return False

    def delete(self) -> None:
        return None

    def supports_server_side_pagination(self) -> bool:
        return True

    def query_with_pagination(
        self,
        *,
        table_name: str,
        conditions: list[dict[str, Any]],
        limit: int | None,
        offset: int,
        order_by: str | None,
        order_desc: bool,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "table_name": table_name,
                "conditions": conditions,
                "limit": limit,
                "offset": offset,
                "order_by": order_by,
                "order_desc": order_desc,
            }
        )
        if self.should_raise:
            raise NotImplementedError
        return {
            "records": [{"id": 99, "name": "stub", "age": 88}],
            "total_count": 123,
            "has_more": True,
        }


pytestmark = pytest.mark.unit


def _build_storage(tmp_path: Path) -> Storage:
    db = Storage(file_path=tmp_path / "backend-pagination.pytucky")
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True),
            Column(int, name="age", index="sorted"),
        ],
    )
    db.insert("users", {"id": 1, "name": "alice", "age": 20})
    db.insert("users", {"id": 2, "name": "bob", "age": 30})
    db.insert("users", {"id": 3, "name": "anna", "age": 40})
    db._dirty = False
    return db


def test_query_table_data_uses_backend_pagination_when_supported(tmp_path: Path) -> None:
    db = _build_storage(tmp_path)
    backend = _BackendPaginationStub(tmp_path / "backend-stub.pytucky")
    db.backend = backend

    try:
        payload = db.query_table_data(
            "users",
            limit=1,
            offset=2,
            order_by="age",
            order_desc=True,
            filters=[
                {"field": "name", "operator": "STARTSWITH", "value": "a"},
                {"field": "missing", "operator": "=", "value": "ignored"},
            ],
        )

        assert payload["records"] == [{"id": 99, "name": "stub", "age": 88}]
        assert payload["total_count"] == 123
        assert payload["has_more"] is True
        assert [column["name"] for column in payload["schema"]] == ["id", "name", "age"]
        assert backend.calls == [
            {
                "table_name": "users",
                "conditions": [{"field": "name", "operator": "STARTSWITH", "value": "a"}],
                "limit": 1,
                "offset": 2,
                "order_by": "age",
                "order_desc": True,
            }
        ]
    finally:
        db.close()


def test_query_table_data_falls_back_when_backend_pagination_not_implemented(tmp_path: Path) -> None:
    db = _build_storage(tmp_path)
    backend = _BackendPaginationStub(tmp_path / "backend-fallback.pytucky", should_raise=True)
    db.backend = backend

    try:
        payload = db.query_table_data(
            "users",
            limit=1,
                offset=0,
            order_by="age",
            filters={"name": "anna"},
        )

        assert payload["total_count"] == 1
        assert payload["has_more"] is False
        assert payload["records"] == [{"id": 3, "name": "anna", "age": 40}]
        assert len(backend.calls) == 1
    finally:
        db.close()
