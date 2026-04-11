"""
pytucky.backends — 后端包（简化文案）

本包包含项目保留的原生单文件存储引擎模块。当前仓库仅维护
基于 PTK 二进制格式的后端实现（backend_binary 和兼容适配器）。

注意：保留 registry/get_backend 等工厂和注册器机制以供 Storage 和
测试使用，但多后端文案已被精简以反映当前项目定位。
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
from . import backend_binary   # noqa: F401
from . import backend_pytucky   # noqa: F401

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
