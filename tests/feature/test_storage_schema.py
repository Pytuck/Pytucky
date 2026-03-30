from pathlib import Path
import pytest

from pytucky import Storage, Column


def build_user_storage(path: Path) -> Storage:
    s = Storage(file_path=str(path))
    s.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
            Column(int, name="age"),
        ],
    )
    return s


@pytest.mark.feature
def test_add_nullable_column_preserves_existing_rows(tmp_path: Path) -> None:
    db = build_user_storage(tmp_path / "schema-add-column.pytucky")
    db.insert("users", {"id": 1, "name": "Alice", "age": 20})
    table = db.get_table("users")
    # add nullable column
    table.add_column(Column(str, name="nickname", nullable=True))
    row = table.get(1)
    # existing rows should have None for new nullable column
    assert row.get("nickname") is None
    db.close()
