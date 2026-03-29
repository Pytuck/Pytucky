"""
Pytuck 工具函数
"""

import hashlib
import re
from typing import Any

from .exceptions import ValidationError


# SQL 标识符安全字符正则：字母、数字、下划线、中文
_SQL_IDENTIFIER_PATTERN = re.compile(r'^[\w\u4e00-\u9fff]+$')


def validate_sql_identifier(identifier: str) -> str:
    """
    验证 SQL 标识符是否安全

    确保标识符只包含安全字符（字母、数字、下划线、中文），
    防止 SQL 注入攻击。

    Args:
        identifier: 表名或列名

    Returns:
        验证通过的标识符（原样返回）

    Raises:
        ValidationError: 如果标识符包含不安全字符
    """
    if not identifier:
        raise ValidationError("SQL identifier cannot be empty")

    if not _SQL_IDENTIFIER_PATTERN.match(identifier):
        raise ValidationError(
            f"Invalid SQL identifier '{identifier}': "
            "only alphanumeric, underscore, and Chinese characters are allowed"
        )

    return identifier


def compute_hash(value: Any) -> int:
    """计算值的哈希值（用于索引）"""
    if value is None:
        return 0

    if isinstance(value, (int, float, str, bool)):
        return hash(value)
    elif isinstance(value, bytes):
        return int(hashlib.md5(value).hexdigest()[:16], 16)
    else:
        return hash(str(value))


def compute_checksum(data: bytes) -> int:
    """计算数据的校验和（CRC32）"""
    import zlib
    return zlib.crc32(data) & 0xffffffff


def pad_bytes(data: bytes, length: int, pad_char: bytes = b'\x00') -> bytes:
    """填充字节到指定长度"""
    if len(data) >= length:
        return data[:length]
    return data + pad_char * (length - len(data))


def unpad_bytes(data: bytes, pad_char: bytes = b'\x00') -> bytes:
    """移除填充字节"""
    return data.rstrip(pad_char)
