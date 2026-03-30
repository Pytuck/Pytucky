import os
from pathlib import Path
from typing import Sequence

from pytucky.backends.backend_pytucky_v6 import PytuckyBackend


def write_journal_backup(
    backend: PytuckyBackend,
    page_numbers: Sequence[int],
    original_page_count: int,
) -> Path:
    backend._write_journal(list(page_numbers), original_page_count)
    return backend._journal_path()


def overwrite_file_bytes(file_path: Path, offset: int, data: bytes) -> None:
    with file_path.open("r+b") as handle:
        handle.seek(offset)
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def flip_file_byte(file_path: Path, offset: int) -> None:
    with file_path.open("r+b") as handle:
        handle.seek(offset)
        original = handle.read(1)
        if len(original) != 1:
            raise AssertionError(f"Expected one byte at offset {offset} in {file_path}")
        handle.seek(offset)
        handle.write(bytes([original[0] ^ 0xFF]))
        handle.flush()
        os.fsync(handle.fileno())


def truncate_file(file_path: Path, size: int) -> None:
    with file_path.open("r+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())
