from pathlib import Path

from pytucky import Column, Storage, migrate_pytuck_to_pytucky


def build_user_storage(file_path: Path, engine: str = 'pytucky') -> Storage:
    storage = Storage(file_path=file_path, engine=engine)
    storage.create_table(
        'users',
        [
            Column(int, name='id', primary_key=True),
            Column(str, name='name'),
            Column(int, name='age', nullable=True),
        ],
    )
    return storage


def test_pytucky_backend_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / 'users.pytucky'

    storage = build_user_storage(db_path)
    storage.insert('users', {'id': 1, 'name': 'Alice', 'age': 20})
    storage.insert('users', {'id': 2, 'name': 'Bob', 'age': None})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=db_path, engine='pytucky')
    table = reopened.get_table('users')

    assert table._lazy_loaded is True

    alice = reopened.select('users', 1)
    bob = reopened.select('users', 2)

    assert alice['id'] == 1
    assert alice['name'] == 'Alice'
    assert alice['age'] == 20
    assert bob['id'] == 2
    assert bob['name'] == 'Bob'
    assert bob['age'] is None

    scanned = list(table.scan())
    assert [pk for pk, _ in scanned] == [1, 2]
    assert scanned[0][1]['name'] == 'Alice'
    assert scanned[1][1]['name'] == 'Bob'

    reopened.close()


def test_pytucky_engine_is_inferred_from_suffix(tmp_path: Path) -> None:
    db_path = tmp_path / 'auto-detect.pytucky'

    storage = build_user_storage(db_path, engine='pytuck')
    assert storage.engine_name == 'pytucky'

    storage.insert('users', {'id': 1, 'name': 'Alice', 'age': 18})
    storage.flush()
    storage.close()

    reopened = Storage(file_path=db_path)
    assert reopened.engine_name == 'pytucky'
    assert reopened.select('users', 1)['name'] == 'Alice'
    reopened.close()


def test_pytucky_backend_handles_multi_page_tables(tmp_path: Path) -> None:
    db_path = tmp_path / 'multi-page.pytucky'

    storage = build_user_storage(db_path)
    for user_id in range(1, 151):
        storage.insert(
            'users',
            {
                'id': user_id,
                'name': f'user-{user_id}-' + ('x' * 64),
                'age': 20 + (user_id % 10),
            },
        )
    storage.flush()
    storage.close()

    reopened = Storage(file_path=db_path, engine='pytucky')
    assert reopened.select('users', 150)['name'].startswith('user-150-')
    assert reopened.count_rows('users') == 150
    assert len(list(reopened.get_table('users').scan())) == 150
    reopened.close()


def test_can_migrate_pytuck_file_to_pytucky(tmp_path: Path) -> None:
    source_path = tmp_path / 'legacy.pytuck'
    target_path = tmp_path / 'migrated.pytucky'

    legacy_storage = build_user_storage(source_path, engine='pytuck')
    legacy_storage.insert('users', {'id': 1, 'name': 'Alice', 'age': 20})
    legacy_storage.insert('users', {'id': 2, 'name': 'Bob', 'age': 21})
    legacy_storage.flush()
    legacy_storage.close()

    migrated_path = migrate_pytuck_to_pytucky(source_path, target_path)
    assert migrated_path.suffix == '.pytucky'
    assert migrated_path.exists()

    reopened = Storage(file_path=migrated_path, engine='pytucky')
    assert reopened.select('users', 1)['name'] == 'Alice'
    assert reopened.select('users', 2)['name'] == 'Bob'
    assert reopened.count_rows('users') == 2
    reopened.close()
