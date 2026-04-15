from pathlib import Path
from typing import Optional

import pytest

from pytucky import Column
from pytucky.backends.store import Store
from pytucky.common.exceptions import ConfigurationError, EncryptionError
from pytucky.common.options import PytuckBackendOptions


_SECRET_VALUE = "Alice-PTK7-Secret"


@pytest.mark.parametrize(
    ("encryption", "password", "plaintext_visible"),
    [
        (None, None, True),
        ("low", "secret123", False),
        ("medium", "secret123", False),
        ("high", "secret123", False),
    ],
)
def test_store_roundtrip_with_supported_encryption_modes(
    tmp_path: Path,
    encryption: Optional[str],
    password: Optional[str],
    plaintext_visible: bool,
) -> None:
    file_path = tmp_path / f"{encryption or 'plain'}-store.pytuck"
    store = Store(
        file_path,
        options=PytuckBackendOptions(encryption=encryption, password=password),
        open_existing=False,
    )
    store.create_table("users", [Column(int, name="id", primary_key=True), Column(str, name="name")])
    store.insert("users", {"name": _SECRET_VALUE})
    store.flush()
    store.close()

    raw_bytes = file_path.read_bytes()
    if plaintext_visible:
        assert _SECRET_VALUE.encode("utf-8") in raw_bytes
    else:
        assert _SECRET_VALUE.encode("utf-8") not in raw_bytes

    reopen_options = PytuckBackendOptions(password=password) if encryption else PytuckBackendOptions()
    reopened = Store(file_path, options=reopen_options)
    assert reopened.select("users", 1)["name"] == _SECRET_VALUE


@pytest.mark.parametrize("level", ["low", "medium", "high"])
def test_store_rejects_missing_or_wrong_password(tmp_path: Path, level: str) -> None:
    file_path = tmp_path / f"{level}-password-check.pytuck"
    writer = Store(
        file_path,
        options=PytuckBackendOptions(encryption=level, password="secret123"),
        open_existing=False,
    )
    writer.create_table("users", [Column(int, name="id", primary_key=True), Column(str, name="name")])
    writer.insert("users", {"name": "Alice"})
    writer.flush()
    writer.close()

    with pytest.raises(EncryptionError, match="需要提供密码"):
        Store(file_path, options=PytuckBackendOptions())

    with pytest.raises(EncryptionError, match="密码错误"):
        Store(file_path, options=PytuckBackendOptions(password="wrong-password"))


def test_store_flush_preserves_loaded_encryption_level_when_only_password_is_provided(tmp_path: Path) -> None:
    file_path = tmp_path / "preserve-medium.pytuck"
    writer = Store(
        file_path,
        options=PytuckBackendOptions(encryption="medium", password="secret123"),
        open_existing=False,
    )
    writer.create_table("users", [Column(int, name="id", primary_key=True), Column(str, name="name")])
    writer.insert("users", {"name": "Alice"})
    writer.flush()
    writer.close()

    reopened = Store(file_path, options=PytuckBackendOptions(password="secret123"))
    reopened.update("users", 1, {"name": "Bob"})
    reopened.flush()
    reopened.close()

    assert b"Bob" not in file_path.read_bytes()
    verified = Store(file_path, options=PytuckBackendOptions(password="secret123"))
    assert verified.select("users", 1)["name"] == "Bob"


def test_store_rejects_invalid_encryption_level_before_writing(tmp_path: Path) -> None:
    store = Store(
        tmp_path / "invalid-level.pytuck",
        options=PytuckBackendOptions(encryption="legacy", password="secret123"),
        open_existing=False,
    )
    store.create_table("users", [Column(int, name="id", primary_key=True), Column(str, name="name")])
    store.insert("users", {"name": "Alice"})
    with pytest.raises(ConfigurationError, match="无效的加密等级"):
        store.flush()
