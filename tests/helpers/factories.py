from pathlib import Path
from typing import Iterable, Dict, Any
from pytucky import Column, Storage
from pytucky.common.options import BinaryBackendOptions


def build_user_storage(file_path: Path, *, lazy_load: bool = True) -> Storage:
    storage = Storage(
        file_path=file_path,
        backend_options=BinaryBackendOptions(),
    )
    storage.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name"),
            Column(int, name="age", nullable=True),
        ],
    )
    return storage


def insert_users(storage: Storage, rows: Iterable[Dict[str, Any]]) -> None:
    for row in rows:
        storage.insert("users", dict(row))
