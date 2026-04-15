import pytest

from pytucky.backends.format import FileHeader, CryptoMetadataV7, CRYPTO_META_STRUCT


def test_crypto_metadata_v7_roundtrip():
    # Ensure struct size matches spec: 16 bytes salt + 4 bytes key_check == 20
    assert CRYPTO_META_STRUCT.size == 20
    meta = CryptoMetadataV7(salt=b"s" * 16, key_check=b"k" * 4)
    packed = meta.pack()
    assert len(packed) == CRYPTO_META_STRUCT.size
    unpacked = CryptoMetadataV7.unpack(packed)
    assert unpacked.salt == b"s" * 16
    assert unpacked.key_check == b"k" * 4


def test_fileheader_encryption_flags_roundtrip():
    header = FileHeader()
    # initially not encrypted
    assert not header.is_encrypted()
    assert header.get_encryption_level() is None

    h_low = header.set_encryption('low')
    assert h_low.is_encrypted()
    assert h_low.get_encryption_level() == 'low'

    h_med = h_low.set_encryption('medium')
    assert h_med.is_encrypted()
    assert h_med.get_encryption_level() == 'medium'

    h_high = h_med.set_encryption('high')
    assert h_high.is_encrypted()
    assert h_high.get_encryption_level() == 'high'

    h_none = h_high.set_encryption(None)
    assert not h_none.is_encrypted()
    assert h_none.get_encryption_level() is None
