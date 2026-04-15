"""
pytucky.backends — PTK7 单文件存储引擎
"""

from .base import StorageBackend
from .backend_pytucky import PytuckyBackend

__all__ = [
    'StorageBackend',
    'PytuckyBackend',
]
