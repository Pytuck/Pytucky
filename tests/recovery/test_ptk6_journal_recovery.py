from pathlib import Path

import pytest

from pytucky import Storage
from pytucky.backends.backend_pytucky_v6 import PytuckyBackend
from pytucky.common.options import BinaryBackendOptions
from tests.helpers.factories import build_user_storage
from tests.helpers.recovery import overwrite_file_bytes, write_journal_backup


@pytest.mark.recovery
def test_ptk6_recovers_from_stale_journal(tmp_path: Path) -> None:
    db_path = tmp_path / "recoverable.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    journal_path = write_journal_backup(
        backend,
        [0, header.schema_root_page],
        header.page_count,
    )

    overwrite_file_bytes(db_path, 0, b"\x00" * 4096)

    recovered = Storage(file_path=db_path)
    assert recovered.select("users", 1)["name"] == "Alice"
    recovered.close()
    assert not journal_path.exists()
