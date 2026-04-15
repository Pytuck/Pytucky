import pytest

from pytucky.common import options
from pytucky.common.options import BinaryBackendOptions


def test_get_default_backend_options_returns_binary_for_known_and_unknown_engines():
    known = options.get_default_backend_options('pytuck')
    current = options.get_default_backend_options('pytucky')
    unknown = options.get_default_backend_options('json')
    fallback = options.get_default_backend_options('anything_else')

    for value in (known, current, unknown, fallback):
        assert isinstance(value, BinaryBackendOptions)


def test_binary_backend_options_have_encryption_fields_and_defaults():
    opts = BinaryBackendOptions()
    # 默认无加密
    assert hasattr(opts, 'encryption')
    assert opts.encryption is None
    # 默认无密码
    assert hasattr(opts, 'password')
    assert opts.password is None
