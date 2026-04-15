from pytucky.common.options import PytuckBackendOptions


def test_pytuck_backend_options_have_encryption_fields_and_defaults():
    opts = PytuckBackendOptions()
    # 默认无加密
    assert hasattr(opts, 'encryption')
    assert opts.encryption is None
    # 默认无密码
    assert hasattr(opts, 'password')
    assert opts.password is None


def test_pytuck_backend_options_accept_explicit_encryption_config():
    opts = PytuckBackendOptions(encryption='high', password='secret123')

    assert opts.encryption == 'high'
    assert opts.password == 'secret123'
