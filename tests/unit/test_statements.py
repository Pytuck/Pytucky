from __future__ import annotations

from pathlib import Path
from pytucky import Storage, declarative_base, Session, Column, select, insert, update, delete

def make_storage(tmp_path: Path) -> Storage:
    dbfile = tmp_path / "test_statements.pytucky"
    return Storage(file_path=str(dbfile))

def make_user_model(storage: Storage):
    Base = declarative_base(storage)

    class User(Base):
        __tablename__ = "users"
        id = Column(int, primary_key=True)
        name = Column(str)
        age = Column(int, nullable=True)

    return User

def seed_users(session: Session, User) -> list[int]:
    # convenience: insert three users
    r1 = session.execute(insert(User).values(name='Alice', age=30))
    r2 = session.execute(insert(User).values(name='Bob', age=25))
    r3 = session.execute(insert(User).values(name='Charlie', age=35))
    # commit to ensure persistence
    session.commit()
    return [r1.inserted_primary_key, r2.inserted_primary_key, r3.inserted_primary_key]

def test_select_with_pk_fast_path(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    pks = seed_users(session, User)
    target_pk = pks[0]

    result = session.execute(select(User).where(User.id == target_pk))
    user = result.first()
    assert user is not None
    assert user.id == target_pk
    assert user.name == 'Alice'

def test_select_filter_by(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    seed_users(session, User)

    result = session.execute(select(User).filter_by(name='Alice'))
    users = result.all()
    assert len(users) == 1
    assert users[0].name == 'Alice'

def test_select_order_by_single_column(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    seed_users(session, User)

    # ascending by age
    res = session.execute(select(User).order_by('age', desc=False))
    users = res.all()
    ages = [u.age for u in users]
    assert ages == sorted(ages)

    # descending by age
    res = session.execute(select(User).order_by('age', desc=True))
    users = res.all()
    ages = [u.age for u in users]
    assert ages == sorted(ages, reverse=True)

def test_select_limit_offset(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    seed_users(session, User)

    res = session.execute(select(User).order_by('age').limit(2).offset(1))
    users = res.all()
    assert [user.age for user in users] == [30, 35]


def test_select_pushes_limit_offset_to_storage_query(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    captured = {}

    def fake_query(table_name, conditions, limit=None, offset=0, order_by=None, order_desc=False):
        captured.update(
            {
                'table_name': table_name,
                'conditions': conditions,
                'limit': limit,
                'offset': offset,
                'order_by': order_by,
                'order_desc': order_desc,
            }
        )
        return [{'id': 1, 'name': 'Alice', 'age': 30}]

    storage.query = fake_query  # type: ignore[method-assign]

    users = session.execute(select(User).order_by('age', desc=True).limit(2).offset(1)).all()

    assert len(users) == 1
    assert captured == {
        'table_name': 'users',
        'conditions': [],
        'limit': 2,
        'offset': 1,
        'order_by': 'age',
        'order_desc': True,
    }

def test_select_returning_empty(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    seed_users(session, User)

    res = session.execute(select(User).filter_by(name='NoSuch'))
    assert res.rowcount() == 0
    assert res.first() is None

def test_insert_single_and_batch(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    # single insert
    r = session.execute(insert(User).values(name='David', age=40))
    assert r.rowcount() == 1
    new_pk = r.inserted_primary_key
    assert new_pk is not None

    # batch insert
    r2 = session.execute(insert(User).values_list([
        {'name': 'Eve', 'age': 22},
        {'name': 'Frank', 'age': 28},
    ]))
    assert r2.rowcount() == 2
    # inserted_primary_key should be first pk
    assert r2.inserted_primary_key is not None

    session.commit()

    # verify persisted
    res = session.execute(select(User).filter_by(name='Eve'))
    assert res.rowcount() == 1

def test_update_with_pk_fast_path_and_condition(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    pks = seed_users(session, User)
    pk = pks[1]

    # PK fast-path update
    r = session.execute(update(User).where(User.id == pk).values(age=99))
    assert r.rowcount() == 1

    # verify
    res = session.execute(select(User).where(User.id == pk))
    user = res.first()
    assert user.age == 99

    # condition update (non-pk)
    r2 = session.execute(update(User).where(User.age > 30).values(age=1))
    # Charlie had age 35 -> should be updated
    assert r2.rowcount() >= 1

def test_delete_with_pk_fast_path_and_missing(tmp_path):
    storage = make_storage(tmp_path)
    User = make_user_model(storage)
    session = Session(storage)

    pks = seed_users(session, User)
    pk = pks[2]

    # delete existing
    r = session.execute(delete(User).where(User.id == pk))
    assert r.rowcount() == 1

    # delete missing
    r2 = session.execute(delete(User).where(User.id == 999999))
    assert r2.rowcount() == 0
