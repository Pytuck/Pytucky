from pathlib import Path

import pytest

from pytucky import Column, Storage
from pytucky.backends.backend_pytucky import PytuckyBackend
from pytucky.common.options import BinaryBackendOptions

pytestmark = pytest.mark.unit


def test_backend_defaults_empty_suffix_to_pytuck(tmp_path: Path) -> None:
    backend = PytuckyBackend(tmp_path / "users", BinaryBackendOptions())

    assert backend.file_path.suffix == ".pytuck"


def test_backend_preserves_explicit_pytuck_suffix(tmp_path: Path) -> None:
    backend = PytuckyBackend(tmp_path / "users.pytuck", BinaryBackendOptions())

    assert backend.file_path.suffix == ".pytuck"


def test_backend_preserves_explicit_pytucky_suffix(tmp_path: Path) -> None:
    backend = PytuckyBackend(tmp_path / "users.pytucky", BinaryBackendOptions())

    assert backend.file_path.suffix == ".pytucky"


def test_probe_returns_false_for_missing_file(tmp_path: Path) -> None:
    matched, info = PytuckyBackend.probe(tmp_path / "missing.pytuck")

    assert matched is False
    assert info is None


def test_probe_returns_false_for_wrong_magic(tmp_path: Path) -> None:
    file_path = tmp_path / "wrong-magic.pytuck"
    file_path.write_bytes(b"NOPE")

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is False
    assert info is None


def test_probe_accepts_encrypted_ptk7_file_without_password(tmp_path: Path) -> None:
    db_path = tmp_path / "encrypted-probe.pytuck"
    db = Storage(
        file_path=db_path,
        backend_options=BinaryBackendOptions(encryption="high", password="secret123"),
    )
    try:
        db.create_table("users", [Column(int, name="id", primary_key=True), Column(str, name="name")])
        db.insert("users", {"name": "Alice"})
        db.flush()
    finally:
        db.close()

    matched, info = PytuckyBackend.probe(db_path)

    assert matched is True
    assert info == {"engine": "pytucky", "version": 7}


def test_storage_file_path_tracks_backend_suffix_normalization(tmp_path: Path) -> None:
    db = Storage(file_path=tmp_path / "normalized")
    try:
        assert db.file_path is not None
        assert db.file_path.suffix == ".pytuck"
    finally:
        db.close()
