from pathlib import Path

import pytest

from pytucky import Column, Storage, declarative_base


@pytest.mark.feature
def test_query_table_data_returns_paginated_records_and_schema(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "query-table-data.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users_qtd"
        id = Column(int, primary_key=True)
        name = Column(str, index=True)
        age = Column(int, index="sorted")

    try:
        for idx, (name, age) in enumerate(
            [
                ("alice", 20),
                ("bob", 25),
                ("alice", 30),
                ("charlie", 35),
                ("david", 40),
            ],
            start=1,
        ):
            db.insert("users_qtd", {"id": idx, "name": name, "age": age})

        payload = db.query_table_data(
            "users_qtd",
            limit=2,
            offset=1,
            order_by="age",
            filters={"name": "alice"},
        )

        assert payload["total_count"] == 2
        assert payload["has_more"] is False
        assert [row["age"] for row in payload["records"]] == [30]
        assert [column["name"] for column in payload["schema"]] == ["id", "name", "age"]
    finally:
        db.close()


@pytest.mark.feature
def test_query_table_data_accepts_operator_filter_list(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "query-table-data-ops.pytucky")
    Base = declarative_base(db)

    class User(Base):
        __tablename__ = "users_qtd_ops"
        id = Column(int, primary_key=True)
        name = Column(str, index=True)
        age = Column(int, index="sorted")

    try:
        for idx, (name, age) in enumerate(
            [
                ("amy", 18),
                ("alice", 22),
                ("albert", 27),
                ("bob", 35),
                ("anna", 42),
            ],
            start=1,
        ):
            db.insert("users_qtd_ops", {"id": idx, "name": name, "age": age})

        payload = db.query_table_data(
            "users_qtd_ops",
            limit=2,
            offset=0,
            order_by="age",
            order_desc=True,
            filters=[
                {"field": "name", "operator": "STARTSWITH", "value": "a"},
                {"field": "age", "operator": ">=", "value": 20},
            ],
        )

        assert payload["total_count"] == 3
        assert payload["has_more"] is True
        assert [row["name"] for row in payload["records"]] == ["anna", "albert"]
        assert [row["age"] for row in payload["records"]] == [42, 27]
    finally:
        db.close()
