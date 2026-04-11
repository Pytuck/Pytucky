"""
后端注册器和工厂（轻微文案清理）

该模块负责管理已注册的存储后端，并提供工厂函数用于实例化后端。
保持原有功能不变，仅清理关于多后端示例的陈旧注释和文档字符串。
"""

from typing import Any, Dict, List, Type, Optional, Tuple, Union
from pathlib import Path
from .base import StorageBackend
from ..common.options import BackendOptions
from ..common.exceptions import ConfigurationError


class BackendRegistry:
    """
    后端注册器（单例风格）

    负责管理所有可用的存储引擎类的注册与查询。
    """

    _backends: Dict[str, Type[StorageBackend]] = {}

    @classmethod
    def register(cls, backend_class: Type[StorageBackend]) -> None:
        """
        注册后端类
        """
        if not issubclass(backend_class, StorageBackend):
            raise ConfigurationError(f"{backend_class} must be a subclass of StorageBackend")

        if backend_class.ENGINE_NAME is None:
            raise ConfigurationError(f"{backend_class} must define ENGINE_NAME")

        cls._backends[backend_class.ENGINE_NAME] = backend_class

    @classmethod
    def get(cls, engine_name: str) -> Optional[Type[StorageBackend]]:
        """获取后端类（若未注册返回 None）"""
        return cls._backends.get(engine_name)

    @classmethod
    def available_engines(cls) -> Dict[str, bool]:
        """返回已注册引擎及其可用性"""
        return {
            name: backend.is_available()
            for name, backend in cls._backends.items()
        }

    @classmethod
    def list_engines(cls) -> List[str]:
        """列出所有已注册的引擎名称"""
        return list(cls._backends.keys())


def get_backend(engine: str, file_path: Union[str, Path], options: BackendOptions) -> StorageBackend:
    """
    获取后端实例（工厂函数）。

    参数 engine 为引擎名称，函数会在注册表中查找对应的后端类并实例化。
    """
    backend_class = BackendRegistry.get(engine)

    if backend_class is None:
        available = BackendRegistry.list_engines()
        raise ConfigurationError(
            f"Backend '{engine}' not found. "
            f"Available backends: {available}"
        )

    if not backend_class.is_available():
        deps = ', '.join(backend_class.REQUIRED_DEPENDENCIES) if backend_class.REQUIRED_DEPENDENCIES else 'none'
        raise ConfigurationError(
            f"Backend '{engine}' is not available. "
            f"Required dependencies: {deps}. "
            f"Install with: pip install pytucky[{engine}]"
        )

    return backend_class(file_path, options)


def _get_all_available_engines() -> List[str]:
    """获取所有可用引擎列表，按注册顺序返回"""
    result: List[str] = []
    for name in BackendRegistry.list_engines():
        backend_cls = BackendRegistry.get(name)
        if backend_cls is not None and backend_cls.is_available():
            result.append(name)
    return result


def is_valid_pytuck_database(file_path: Union[str, Path]) -> Tuple[bool, Optional[str]]:
    """
    检验是否是合法的 Pytuck 数据库文件并识别引擎

    通过调用各引擎的 probe 方法来识别数据库文件格式（基于文件内容特征），
    不依赖文件扩展名。
    """
    file_path = Path(file_path).expanduser()

    if not file_path.exists():
        return False, None

    # 获取所有可用引擎，不依赖文件后缀名
    available_engines = _get_all_available_engines()

    # 按注册顺序尝试各引擎
    for engine_name in available_engines:
        backend_class = BackendRegistry.get(engine_name)
        if backend_class is None:
            continue

        try:
            is_match, info = backend_class.probe(file_path)
            if is_match:
                return True, engine_name
        except Exception:
            # probe 方法应自行处理异常，若抛出则忽略并继续尝试其他引擎
            continue

    return False, None


def get_database_info(file_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    获取 Pytuck 数据库文件的详细信息

    返回包括引擎名称、版本、文件大小等信息的字典；若无法识别则返回 None。
    """
    file_path = Path(file_path).expanduser()

    if not file_path.exists():
        return None

    # 获取所有可用引擎
    available_engines = _get_all_available_engines()

    # 尝试各引擎
    for engine_name in available_engines:
        backend_class = BackendRegistry.get(engine_name)
        if backend_class is None:
            continue

        try:
            is_match, info = backend_class.probe(file_path)
            if is_match and info:
                file_stat = file_path.stat()
                info.setdefault('file_size', file_stat.st_size)
                info.setdefault('modified', file_stat.st_mtime)
                return info
        except Exception:
            continue

    return None


def is_valid_pytuck_database_engine(file_path: Union[str, Path], engine_name: str) -> bool:
    """
    检验文件是否为指定引擎的 Pytuck 数据库（仅验证指定引擎）。
    """
    backend_class = BackendRegistry.get(engine_name)
    if backend_class is None:
        available_engines = BackendRegistry.list_engines()
        raise ConfigurationError(
            f"Engine '{engine_name}' not found. "
            f"Available engines: {available_engines}"
        )

    if not backend_class.is_available():
        deps = ', '.join(backend_class.REQUIRED_DEPENDENCIES) if backend_class.REQUIRED_DEPENDENCIES else 'none'
        raise ConfigurationError(
            f"Engine '{engine_name}' is not available. "
            f"Required dependencies: {deps}. "
            f"Install with: pip install pytucky[{engine_name}]"
        )

    try:
        is_match, _ = backend_class.probe(file_path)
        return is_match
    except Exception:
        return False


def get_available_engines() -> Dict[str, Dict[str, Any]]:
    """获取所有已注册引擎的详细信息（结构化）"""
    engines_info: Dict[str, Dict[str, Any]] = {}

    for engine_name in BackendRegistry.list_engines():
        backend_class = BackendRegistry.get(engine_name)
        if backend_class is None:
            continue

        description = backend_class.__doc__ or f'{engine_name.title()} storage engine'
        if description:
            description = description.strip().split('\n')[0]

        engines_info[engine_name] = {
            'name': engine_name,
            'available': backend_class.is_available(),
            'dependencies': getattr(backend_class, 'REQUIRED_DEPENDENCIES', []),
            'description': description,
            'format_version': getattr(backend_class, 'FORMAT_VERSION', None)
        }

    return engines_info


def print_available_engines() -> None:
    """打印所有已注册引擎的信息"""
    engines_info = get_available_engines()

    print("Available Pytuck Storage Engines:")
    print("=" * 40)

    for engine_name, info in engines_info.items():
        status = "✅" if info['available'] else "❌"
        print(f"{status} {engine_name.upper()}")

        if not info['available'] and info['dependencies']:
            print(f"   Missing dependencies: {', '.join(info['dependencies'])}")

        if info['format_version']:
            print(f"   Format version: {info['format_version']}")

        print()  # 空行分隔
