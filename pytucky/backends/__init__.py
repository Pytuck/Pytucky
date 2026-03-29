"""
Pytuck 后端模块

提供引擎注册、发现和实例化功能
"""

from .base import StorageBackend
from .registry import (
    BackendRegistry,
    get_backend,
    is_valid_pytuck_database,
    get_database_info,
    is_valid_pytuck_database_engine,
    get_available_engines,
    print_available_engines,
)

# 导入内置后端模块，触发 __init_subclass__ 自动注册
# 后端模块的外部依赖使用延迟导入（TYPE_CHECKING + 方法内导入）
# 所以这些导入不会因缺少依赖而失败
from . import backend_binary   # noqa: F401
# 粗裁剪：已移除其他后端（json, jsonl, csv, sqlite, duckdb, excel, xml）
# TODO: 后续重构应移除 registry 和多引擎支持，直接使用 BinaryBackend

__all__ = [
    'StorageBackend',
    'BackendRegistry',
    'get_backend',
    'print_available_engines',
    'get_available_engines',
    'is_valid_pytuck_database',
    'get_database_info',
    'is_valid_pytuck_database_engine',
]
