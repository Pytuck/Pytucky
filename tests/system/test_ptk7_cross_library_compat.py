from pathlib import Path

import pytest

from tests.helpers.pytuck_compat import load_pytuck_symbols

LEVELS = [None, 'low', 'medium', 'high']


def _make_options(level):
    if level is None:
        return None
    return {'encryption': level, 'password': 'secret123'}


@pytest.mark.parametrize('level', LEVELS)
def test_pytucky_write_then_pytuck_rewrite_then_pytucky_read(tmp_path: Path, level):
    syms = load_pytuck_symbols()
    PytuckStorage = syms['Storage']
    PytuckColumn = syms['Column']
    PytuckBinaryBackendOptions = syms['BinaryBackendOptions']
    PytuckyStorage = syms['PytuckyStorage']
    PytuckyColumn = syms['PytuckyColumn']
    PytuckyBinaryBackendOptions = syms['PytuckyBinaryBackendOptions']

    file_path = tmp_path / f'db_ptk7_{level or "none"}.pytuck'

    opts = _make_options(level)
    if opts is None:
        py_opts = None
    else:
        py_opts = PytuckyBinaryBackendOptions(encryption=opts['encryption'], password=opts['password'])

    # create with pytucky
    db1 = PytuckyStorage(file_path=str(file_path), backend_options=py_opts)
    db1.create_table('users', [PytuckyColumn(int, name='id', primary_key=True), PytuckyColumn(str, name='name')])
    db1.insert('users', {'name': 'alice'})
    db1.flush()
    db1.close()

    # open with pytuck, read/update/insert
    if opts is None:
        pytuck_opts = None
    else:
        pytuck_opts = PytuckBinaryBackendOptions(encryption=opts['encryption'], password=opts['password'])
    db2 = PytuckStorage(file_path=str(file_path), engine='pytuck', backend_options=pytuck_opts)
    # use simple Storage API
    rows = []
    try:
        # read pk 1
        r = db2.select('users', 1)
        assert r['name'] == 'alice'
        db2.update('users', 1, {'name': 'alice-updated'})
    finally:
        db2.insert('users', {'name': 'bob'})
        db2.flush()
        db2.close()

    # reopen with pytucky and verify
    db3 = PytuckyStorage(file_path=str(file_path), backend_options=py_opts)
    r1 = db3.select('users', 1)
    assert r1['name'] == 'alice-updated'
    r2 = db3.select('users', 2)
    assert r2['name'] == 'bob'
    db3.close()


@pytest.mark.parametrize('level', LEVELS)
def test_pytuck_write_then_pytucky_rewrite_then_pytuck_read(tmp_path: Path, level):
    syms = load_pytuck_symbols()
    PytuckStorage = syms['Storage']
    PytuckColumn = syms['Column']
    PytuckBinaryBackendOptions = syms['BinaryBackendOptions']
    PytuckyStorage = syms['PytuckyStorage']
    PytuckyColumn = syms['PytuckyColumn']
    PytuckyBinaryBackendOptions = syms['PytuckyBinaryBackendOptions']

    file_path = tmp_path / f'db_ptk7_rev_{level or "none"}.pytuck'

    opts = _make_options(level)
    if opts is None:
        pytuck_opts = None
    else:
        pytuck_opts = PytuckBinaryBackendOptions(encryption=opts['encryption'], password=opts['password'])

    db1 = PytuckStorage(file_path=str(file_path), engine='pytuck', backend_options=pytuck_opts)
    db1.create_table('users', [PytuckColumn(int, name='id', primary_key=True), PytuckColumn(str, name='name')])
    db1.insert('users', {'name': 'alice'})
    db1.flush()
    db1.close()

    # open with pytucky
    if opts is None:
        py_opts = None
    else:
        py_opts = PytuckyBinaryBackendOptions(encryption=opts['encryption'], password=opts['password'])
    db2 = PytuckyStorage(file_path=str(file_path), backend_options=py_opts)
    r = db2.select('users', 1)
    assert r['name'] == 'alice'
    db2.update('users', 1, {'name': 'alice-updated'})
    db2.insert('users', {'name': 'bob'})
    db2.flush()
    db2.close()

    # reopen with pytuck and verify
    db3 = PytuckStorage(file_path=str(file_path), engine='pytuck', backend_options=pytuck_opts)
    r1 = db3.select('users', 1)
    assert r1['name'] == 'alice-updated'
    r2 = db3.select('users', 2)
    assert r2['name'] == 'bob'
    db3.close()
