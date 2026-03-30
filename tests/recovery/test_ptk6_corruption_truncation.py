from pathlib import Path

import pytest

from pytucky import Storage
from pytucky.backends.backend_pytucky_v6 import PytuckyBackend
from pytucky.common.exceptions import SerializationError
from pytucky.common.options import BinaryBackendOptions
from tests.helpers.factories import build_user_storage
from tests.helpers.recovery import (
    flip_file_byte,
    overwrite_file_bytes,
    truncate_file,
    write_journal_backup,
)


@pytest.mark.recovery
def test_journal_exists_but_database_deleted_triggers_serialization_error(tmp_path: Path) -> None:
    db_path = tmp_path / "missing-db.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Alice", "age": 20})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    write_journal_backup(backend, [0, header.schema_root_page], header.page_count)

    # delete the main database file to simulate crash that removed it
    db_path.unlink()

    with pytest.raises(SerializationError, match="database file is missing"):
        backend._recover_from_journal()


@pytest.mark.recovery
def test_tampered_journal_magic_raises_serialization_error(tmp_path: Path) -> None:
    db_path = tmp_path / "tampered-journal.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Bob", "age": 30})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    journal_path = write_journal_backup(
        backend,
        [0, header.schema_root_page],
        header.page_count,
    )

    overwrite_file_bytes(journal_path, 0, b"BAD!")

    with pytest.raises(SerializationError, match="journal magic"):
        Storage(file_path=db_path)


@pytest.mark.recovery
def test_truncated_journal_backup_raises_serialization_error(tmp_path: Path) -> None:
    db_path = tmp_path / "truncated-journal.pytucky"
    storage = build_user_storage(db_path)
    # add a few rows to ensure multiple pages possibly
    for i in range(1, 6):
        storage.insert("users", {"id": i, "name": f"u{i}", "age": 20 + i})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    journal_path = write_journal_backup(
        backend,
        [0, header.schema_root_page],
        header.page_count,
    )

    size = journal_path.stat().st_size
    truncated_size = max(0, size - 100)
    truncate_file(journal_path, truncated_size)

    with pytest.raises(SerializationError, match="page backup is truncated"):
        Storage(file_path=db_path)


@pytest.mark.recovery
def test_header_crc_mismatch_without_journal_raises_serialization_error(tmp_path: Path) -> None:
    db_path = tmp_path / "crc-mismatch.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Carol", "age": 25})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    # ensure no journal exists
    journal_path = backend._journal_path()
    if journal_path.exists():
        journal_path.unlink()

    flip_file_byte(db_path, 8)

    with pytest.raises(SerializationError, match="header CRC mismatch"):
        Storage(file_path=db_path)


@pytest.mark.recovery
def test_valid_journal_recovers_truncated_database_file(tmp_path: Path) -> None:
    db_path = tmp_path / "recover-truncated-db.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Dana", "age": 28})
    storage.insert("users", {"id": 2, "name": "Eli", "age": 31})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    journal_path = write_journal_backup(
        backend,
        list(range(header.page_count)),
        header.page_count,
    )

    original_size = db_path.stat().st_size
    truncate_file(db_path, original_size - 128)

    recovered = Storage(file_path=db_path)
    try:
        assert recovered.select("users", 1)["name"] == "Dana"
        assert recovered.select("users", 2)["name"] == "Eli"
    finally:
        recovered.close()

    assert db_path.stat().st_size == original_size
    assert not journal_path.exists()


@pytest.mark.recovery
def test_valid_journal_allows_recovery_even_with_corrupted_header(tmp_path: Path) -> None:
    db_path = tmp_path / "recover-with-journal.pytucky"
    storage = build_user_storage(db_path)
    storage.insert("users", {"id": 1, "name": "Dana", "age": 28})
    storage.flush()
    storage.close()

    backend = PytuckyBackend(db_path, BinaryBackendOptions(lazy_load=True))
    header = backend.pager.read_file_header()
    journal_path = write_journal_backup(
        backend,
        [0, header.schema_root_page],
        header.page_count,
    )

    flip_file_byte(db_path, 0)

    recovered = Storage(file_path=db_path)
    try:
        assert recovered.select("users", 1)["name"] == "Dana"
    finally:
        recovered.close()
    assert not journal_path.exists()
