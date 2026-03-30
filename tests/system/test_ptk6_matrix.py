from pathlib import Path
from typing import Dict, List, cast

import pytest

from pytucky import Column, Storage
from pytucky.backends.backend_pytucky_v6 import PytuckyBackend
from pytucky.common.exceptions import RecordNotFoundError, SchemaError
from pytucky.common.options import BinaryBackendOptions
from pytucky.core.storage import Table
from tests.helpers.factories import build_user_storage, insert_users


def open_pytucky_storage(file_path: Path, *, lazy_load: bool) -> Storage:
    return Storage(
        file_path=file_path,
        backend_options=BinaryBackendOptions(lazy_load=lazy_load),
    )


def build_sorted_user_storage(file_path: Path, *, lazy_load: bool) -> Storage:
    storage = open_pytucky_storage(file_path, lazy_load=lazy_load)
    storage.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
            Column(int, name="age", nullable=True, index="sorted"),
        ],
    )
    return storage


def make_user_rows(
    count: int,
    *,
    large_payload: bool = False,
    start_id: int = 1,
) -> List[Dict[str, object]]:
    name_suffix = "x" * (120 if large_payload else 8)
    return [
        {
            "id": start_id + index,
            "name": f"user-{start_id + index:04d}-{name_suffix}",
            "age": (start_id + index) % 100,
        }
        for index in range(count)
    ]


def make_profile_rows(count: int, *, start_id: int = 1) -> List[Dict[str, object]]:
    bio_suffix = "b" * 240
    return [
        {
            "id": start_id + index,
            "bio": f"profile-{start_id + index:04d}-{bio_suffix}",
        }
        for index in range(count)
    ]


def make_ranked_user_rows() -> List[Dict[str, object]]:
    return [
        {"id": 1, "name": "alice", "age": 20},
        {"id": 2, "name": "bravo", "age": 30},
        {"id": 3, "name": "charlie", "age": 40},
        {"id": 4, "name": "delta", "age": 50},
        {"id": 5, "name": "echo", "age": 60},
    ]


def assert_lazy_unmaterialized(table: Table) -> None:
    assert table._lazy_loaded is True
    assert table.data == {}


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
@pytest.mark.parametrize(
    ("row_count", "large_payload", "expect_multi_page"),
    [
        (10, False, False),
        (400, True, True),
    ],
)
def test_ptk6_roundtrip_matrix(
    tmp_path: Path,
    lazy_load: bool,
    row_count: int,
    large_payload: bool,
    expect_multi_page: bool,
) -> None:
    db_path = tmp_path / f"roundtrip-{lazy_load}-{row_count}.pytucky"
    rows = make_user_rows(row_count, large_payload=large_payload)
    storage = build_user_storage(db_path, lazy_load=lazy_load)
    try:
        insert_users(storage, rows)
        storage.flush()
        backend = cast(PytuckyBackend, storage.backend)
        header = backend.pager.read_file_header()
        if expect_multi_page:
            assert header.page_count > 3
        else:
            assert header.page_count >= 3
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        table = reopened.get_table("users")
        if lazy_load:
            assert_lazy_unmaterialized(table)
        assert reopened.count_rows("users") == row_count

        sample_ids = sorted({1, max(1, row_count // 2), row_count})
        for sample_id in sample_ids:
            row = reopened.select("users", sample_id)
            expected = rows[sample_id - 1]
            assert row["name"] == expected["name"]
            assert row["age"] == expected["age"]
    finally:
        reopened.close()


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
def test_ptk6_incremental_save_matrix(tmp_path: Path, lazy_load: bool) -> None:
    db_path = tmp_path / f"incremental-{lazy_load}.pytucky"
    storage = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        storage.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name"),
                Column(int, name="age", nullable=True),
            ],
        )
        storage.create_table(
            "profiles",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="bio"),
            ],
        )

        initial_user_rows = make_user_rows(80, large_payload=True)
        additional_user_rows = make_user_rows(200, large_payload=True, start_id=81)
        profile_rows = make_profile_rows(120)

        insert_users(storage, initial_user_rows)
        for row in profile_rows:
            storage.insert("profiles", row)

        storage.flush()
        backend = cast(PytuckyBackend, storage.backend)
        header_before = backend.pager.read_file_header()
        page_count_before = header_before.page_count
        generation_before = header_before.generation

        insert_users(storage, additional_user_rows)
        storage.flush()

        header_after = backend.pager.read_file_header()
        assert header_after.generation == generation_before + 1
        assert header_after.page_count > page_count_before
        journal_path = db_path.with_name(f".{db_path.name}.journal")
        assert not journal_path.exists()
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        profiles = reopened.get_table("profiles")
        if lazy_load:
            assert_lazy_unmaterialized(profiles)

        assert reopened.count_rows("users") == 280
        assert reopened.count_rows("profiles") == 120

        first_user = reopened.select("users", 1)
        assert first_user["name"] == initial_user_rows[0]["name"]
        assert first_user["age"] == initial_user_rows[0]["age"]

        last_user = reopened.select("users", 280)
        assert last_user["name"] == additional_user_rows[-1]["name"]
        assert last_user["age"] == additional_user_rows[-1]["age"]

        last_profile = reopened.select("profiles", 120)
        assert last_profile["bio"] == profile_rows[-1]["bio"]

        if lazy_load:
            assert profiles.data == {}
    finally:
        reopened.close()


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
def test_ptk6_lazy_write_matrix(tmp_path: Path, lazy_load: bool) -> None:
    db_path = tmp_path / f"lazy-write-{lazy_load}.pytucky"
    storage = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        storage.create_table(
            "users",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name"),
                Column(int, name="age", nullable=True),
            ],
        )
        storage.create_table(
            "profiles",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="bio"),
            ],
        )

        initial_user_rows = make_user_rows(60, large_payload=True)
        initial_profile_rows = make_profile_rows(80)
        additional_profile_rows = make_profile_rows(80, start_id=81)

        insert_users(storage, initial_user_rows)
        for row in initial_profile_rows:
            storage.insert("profiles", row)
        storage.flush()
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened.get_table("users")
        profiles_table = reopened.get_table("profiles")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)
            assert_lazy_unmaterialized(profiles_table)

        assert reopened.select("users", 1)["name"] == initial_user_rows[0]["name"]
        if lazy_load:
            assert users_table.data == {}

        backend = cast(PytuckyBackend, reopened.backend)
        header_before = backend.pager.read_file_header()
        page_count_before = header_before.page_count
        generation_before = header_before.generation

        for row in additional_profile_rows:
            reopened.insert("profiles", row)

        if lazy_load:
            assert users_table.data == {}

        reopened.flush()

        header_after = backend.pager.read_file_header()
        assert header_after.generation == generation_before + 1
        assert header_after.page_count > page_count_before
        journal_path = db_path.with_name(f".{db_path.name}.journal")
        assert not journal_path.exists()
        if lazy_load:
            assert users_table.data == {}
    finally:
        reopened.close()

    reopened_again = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened_again.get_table("users")
        profiles_table = reopened_again.get_table("profiles")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)
            assert_lazy_unmaterialized(profiles_table)

        assert reopened_again.count_rows("users") == 60
        assert reopened_again.count_rows("profiles") == 160
        assert reopened_again.select("users", 1)["name"] == initial_user_rows[0]["name"]
        assert reopened_again.select("profiles", 160)["bio"] == additional_profile_rows[-1]["bio"]

        if lazy_load:
            assert users_table.data == {}
            assert profiles_table.data == {}
    finally:
        reopened_again.close()


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
def test_ptk6_schema_evolution_matrix(tmp_path: Path, lazy_load: bool) -> None:
    db_path = tmp_path / f"schema-evolution-{lazy_load}.pytucky"
    storage = build_user_storage(db_path, lazy_load=lazy_load)
    initial_rows = [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": None},
    ]
    try:
        insert_users(storage, initial_rows)
        storage.flush()
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened.get_table("users")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)

        with pytest.raises(SchemaError, match="Cannot add non-nullable column 'status'"):
            reopened.add_column("users", Column(str, name="status", nullable=False))

        reopened.add_column(
            "users",
            Column(str, name="status", nullable=False),
            default_value="active",
        )
        reopened.alter_column("users", "age", nullable=False, default=0)
        reopened.flush()
    finally:
        reopened.close()

    reopened_again = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened_again.get_table("users")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)

        assert users_table.columns["status"].nullable is False
        assert users_table.columns["age"].nullable is False

        first_user = reopened_again.select("users", 1)
        second_user = reopened_again.select("users", 2)
        assert first_user["status"] == "active"
        assert second_user["status"] == "active"
        assert first_user["age"] == 20
        assert second_user["age"] == 0
    finally:
        reopened_again.close()


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
@pytest.mark.parametrize(
    ("order_desc", "expected_ages"),
    [
        (False, [40, 50]),
        (True, [50, 40]),
    ],
)
def test_ptk6_query_matrix(
    tmp_path: Path,
    lazy_load: bool,
    order_desc: bool,
    expected_ages: List[int],
) -> None:
    db_path = tmp_path / f"query-{lazy_load}-{order_desc}.pytucky"
    storage = build_sorted_user_storage(db_path, lazy_load=lazy_load)
    try:
        insert_users(storage, make_ranked_user_rows())
        storage.flush()
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened.get_table("users")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)

        result = reopened.query_table_data(
            "users",
            limit=2,
            offset=1,
            order_by="age",
            order_desc=order_desc,
            filters=[{"field": "age", "operator": ">=", "value": 30}],
        )

        assert result["total_count"] == 4
        assert result["has_more"] is True
        assert [record["age"] for record in result["records"]] == expected_ages
        assert [column["name"] for column in result["schema"]] == ["id", "name", "age"]

        if lazy_load:
            assert users_table.data == {}
    finally:
        reopened.close()


@pytest.mark.system
@pytest.mark.parametrize("lazy_load", [False, True])
def test_ptk6_transaction_rollback_matrix(tmp_path: Path, lazy_load: bool) -> None:
    db_path = tmp_path / f"transaction-{lazy_load}.pytucky"
    storage = build_user_storage(db_path, lazy_load=lazy_load)
    initial_rows = [
        {"id": 1, "name": "Alice", "age": 20},
        {"id": 2, "name": "Bob", "age": 30},
    ]
    try:
        insert_users(storage, initial_rows)
        storage.flush()
    finally:
        storage.close()

    reopened = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        users_table = reopened.get_table("users")
        if lazy_load:
            assert_lazy_unmaterialized(users_table)

        with pytest.raises(RuntimeError, match="abort transaction"):
            with reopened.transaction():
                reopened.insert("users", {"id": 3, "name": "Carol", "age": 40})
                assert reopened.count_rows("users") == 3
                raise RuntimeError("abort transaction")

        assert reopened.count_rows("users") == 2
        assert reopened.select("users", 1)["name"] == "Alice"
        with pytest.raises(RecordNotFoundError):
            reopened.select("users", 3)

        if lazy_load:
            assert users_table._lazy_loaded is True
            assert users_table.data == {}

        journal_path = db_path.with_name(f".{db_path.name}.journal")
        assert not journal_path.exists()
    finally:
        reopened.close()

    reopened_again = open_pytucky_storage(db_path, lazy_load=lazy_load)
    try:
        assert reopened_again.count_rows("users") == 2
        with pytest.raises(RecordNotFoundError):
            reopened_again.select("users", 3)
    finally:
        reopened_again.close()
