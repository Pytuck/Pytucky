"""
存储引擎格式版本定义
"""
from typing import Dict

ENGINE_FORMAT_VERSIONS: Dict[str, int] = {
    'pytucky': 7,  # v7: PTK7 单文件目录 + 按需回表读取
}


def get_format_version(engine_name: str) -> int:
    return ENGINE_FORMAT_VERSIONS.get(engine_name, 1)
