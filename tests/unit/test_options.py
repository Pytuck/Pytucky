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
        assert value.lazy_load is True
        assert value.sidecar_wal is False
        assert value.encryption is None
        assert value.password is None
