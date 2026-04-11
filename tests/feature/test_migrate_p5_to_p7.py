from pathlib import Path

import pytest

from pytucky import Column, Storage, migrate_p5_to_p7, migrate_pytuck_to_pytucky

pytestmark = pytest.mark.feature


def build_pytuck_source(path: Path) -> None:
    db = Storage(file_path=path, engine='pytuck')
    try:
        db.create_table(
            'users',
            [
                Column(int, name='id', primary_key=True),
                Column(str, name='name', index=True),
            ],
        )
        db.insert('users', {'id': 1, 'name': 'Alice'})
        db.insert('users', {'id': 2, 'name': 'Bob'})
        db.flush()
    finally:
        db.close()


def test_migrate_pytuck_to_pytucky_writes_ptk7_file(tmp_path: Path) -> None:
    source_path = tmp_path / 'source.pytuck'
    target_path = tmp_path / 'target.pytucky'
    build_pytuck_source(source_path)

    migrated_path = migrate_pytuck_to_pytucky(source_path, target_path)

    assert migrated_path == target_path
    assert target_path.exists()
    assert target_path.read_bytes()[:4] == b'PTK7'

    reopened = Storage(file_path=target_path)
    try:
        assert reopened.select('users', 1)['name'] == 'Alice'
        assert reopened.select('users', 2)['name'] == 'Bob'
    finally:
        reopened.close()


def test_migrate_p5_to_p7_alias(tmp_path: Path) -> None:
    source_path = tmp_path / 'alias-source.pytuck'
    target_path = tmp_path / 'alias-target.pytucky'
    build_pytuck_source(source_path)

    migrated_path = migrate_p5_to_p7(source_path, target_path)

    assert migrated_path == target_path
    reopened = Storage(file_path=target_path)
    try:
        assert reopened.count_rows('users') == 2
    finally:
        reopened.close()
