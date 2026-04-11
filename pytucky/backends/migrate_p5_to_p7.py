"""
PTK5 → PTK7 迁移工具。

当前策略：
- 明确调用，不做自动迁移
- 先用现有 PTK5 后端完整读取数据
- 再写入新的 PTK7 / .pytucky 文件
"""

from pathlib import Path
from typing import Union

from ..common.options import BinaryBackendOptions
from .backend_binary import BinaryBackend
from .backend_pytucky import PytuckyBackend


def migrate_pytuck_to_pytucky(
    source_path: Union[str, Path],
    target_path: Union[str, Path],
) -> Path:
    """将 PTK5 / .pytuck 文件迁移为 PTK7 / .pytucky 文件。"""
    source = Path(source_path).expanduser()
    target = Path(target_path).expanduser()

    source_backend = BinaryBackend(source, BinaryBackendOptions(lazy_load=True))
    tables = source_backend.load()
    if source_backend.supports_lazy_loading():
        source_backend.populate_tables_with_data(tables)

    target_backend = PytuckyBackend(target, BinaryBackendOptions())
    target_backend.save(tables)
    return target_backend.file_path


def migrate_p5_to_p7(
    source_path: Union[str, Path],
    target_path: Union[str, Path],
) -> Path:
    """PTK5 → PTK7 迁移别名。"""
    return migrate_pytuck_to_pytucky(source_path, target_path)
