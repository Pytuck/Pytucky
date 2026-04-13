from pathlib import Path

import pytest

from pytucky import Column, Storage
from pytucky.backends.backend_pytucky import PytuckyBackend
from pytucky.common.options import BinaryBackendOptions

pytestmark = pytest.mark.unit


def test_get_backend_returns_v7_backend(tmp_path: Path) -> None:
    backend = PytuckyBackend(tmp_path / 'probe.pytucky', BinaryBackendOptions())

    assert isinstance(backend, PytuckyBackend)


def test_probe_returns_false_for_missing_file(tmp_path: Path) -> None:
    matched, info = PytuckyBackend.probe(tmp_path / 'missing.pytucky')

    assert matched is False
    assert info is None


def test_probe_returns_false_for_wrong_magic(tmp_path: Path) -> None:
    file_path = tmp_path / 'wrong-magic.pytucky'
    file_path.write_bytes(b'NOPE')

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is False
    assert info is None


def test_probe_and_registry_match_valid_ptk7_file(tmp_path: Path) -> None:
    file_path = tmp_path / 'valid.pytucky'
    db = Storage(file_path=file_path)
    try:
        db.create_table(
            'users',
            [
                Column(int, name='id', primary_key=True),
                Column(str, name='name', index=True),
            ],
        )
        db.insert('users', {'name': 'Alice'})
        db.flush()
    finally:
        db.close()

    matched, info = PytuckyBackend.probe(file_path)

    assert matched is True
    assert info == {'engine': 'pytucky', 'version': 7}
