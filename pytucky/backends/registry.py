"""
Pytuck 后端注册器和工厂

提供引擎注册、发现和实例化功能
"""

from typing import Any, Dict, List, Type, Optional, Tuple, Union
from pathlib import Path
from .base import StorageBackend
from ..common.options import BackendOptions
from ..common.exceptions import ConfigurationError


class BackendRegistry:
    """
    后端注册器（单例模式）

    负责管理所有可用的存储引擎
    """

    _backends: Dict[str, Type[StorageBackend]] = {}

    @classmethod
    def register(cls, backend_class: Type[StorageBackend]) -> None:
        """
        注册后端

        Args:
            backend_class: 后端类（必须是 StorageBackend 的子类）

        示例:
            BackendRegistry.register(BinaryBackend)
        """
        if not issubclass(backend_class, StorageBackend):
            raise ConfigurationError(f"{backend_class} must be a subclass of StorageBackend")

        if backend_class.ENGINE_NAME is None:
            raise ConfigurationError(f"{backend_class} must define ENGINE_NAME")

        cls._backends[backend_class.ENGINE_NAME] = backend_class

    @classmethod
    def get(cls, engine_name: str) -> Optional[Type[StorageBackend]]:
        """
        获取后端类

        Args:
            engine_name: 引擎名称

        Returns:
            后端类，如果不存在则返回 None
        """
        return cls._backends.get(engine_name)

    @classmethod
    def available_engines(cls) -> Dict[str, bool]:
        """
        获取所有引擎及其可用性

        Returns:
            字典 {engine_name: is_available}

        示例:
            {
                'pytuck': True,
                'json': True,
                'csv': True,
                'sqlite': True,
                'excel': False,  # 未安装 openpyxl
                'xml': False,    # 未安装 lxml
            }
        """
        return {
            name: backend.is_available()
            for name, backend in cls._backends.items()
        }

    @classmethod
    def list_engines(cls) -> List[str]:
        """
        列出所有已注册的引擎名称

        Returns:
            引擎名称列表
        """
        return list(cls._backends.keys())


def get_backend(engine: str, file_path: Union[str, Path], options: BackendOptions) -> StorageBackend:
    """
    获取后端实例（工厂函数）

    Args:
        engine: 引擎名称（'pytuck', 'json', 'jsonl', 'csv', 'sqlite', 'duckdb', 'excel', 'xml'）
        file_path: 文件路径（字符串或 Path 对象）
        options: 强类型的后端配置选项对象

    Returns:
        后端实例

    Raises:
        ValueError: 引擎不存在或不可用

    示例:
        from pytuck.common.options import JsonBackendOptions
        opts = JsonBackendOptions(indent=2)
        backend = get_backend('json', 'data.json', opts)
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
            f"Install with: pip install pytuck[{engine}]"
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

    通过调用各引擎的 probe 方法来识别数据库文件格式。
    不依赖文件扩展名，基于文件内容特征进行判断。

    Args:
        file_path: 数据库文件路径

    Returns:
        Tuple[bool, Optional[str]]: (是否有效, 引擎名称或None)

    示例:
        >>> is_valid_pytuck_database('data.pytuck')
        (True, 'pytuck')
        >>> is_valid_pytuck_database('invalid.txt')
        (False, None)
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
            # probe 方法应该捕获异常，但以防万一
            continue

    return False, None


def get_database_info(file_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    获取 Pytuck 数据库文件的详细信息

    提供比 is_valid_pytuck_database 更详细的信息，包括版本、表数量等。

    Args:
        file_path: 数据库文件路径

    Returns:
        Optional[Dict]: 包含引擎名称、版本、表数量等信息，如果不是有效数据库则返回 None

    返回字典结构:
        {
            'engine': str,                    # 引擎名称
            'format_version': str,            # 格式版本
            'file_size': int,                 # 文件大小
            'modified': float,                # 修改时间戳
            'table_count': int,               # 表数量（如果可获取）
            'confidence': str,                # 识别可信度: 'high', 'medium', 'low'
            'error': str                      # 错误信息（如果有）
        }

    示例:
        >>> info = get_database_info('data.pytuck')
        >>> if info:
        ...     print(f"引擎：{info['engine']}")
        ...     print(f"表数量：{info.get('table_count', 'unknown')}")
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
                # 添加文件基本信息
                file_stat = file_path.stat()
                info.setdefault('file_size', file_stat.st_size)
                info.setdefault('modified', file_stat.st_mtime)
                return info
        except Exception:
            continue

    return None


def is_valid_pytuck_database_engine(file_path: Union[str, Path], engine_name: str) -> bool:
    """
    检验文件是否为指定引擎的 Pytuck 数据库

    只检验指定的引擎，不会尝试其他引擎。

    Args:
        file_path: 数据库文件路径
        engine_name: 引擎名称（`pytuck`, `json`, `jsonl`, `csv`, `sqlite`, `duckdb`, `excel`, `xml`）

    Returns:
        bool: 是否为指定引擎的有效数据库

    Raises:
        ValueError: 引擎名称不存在或不可用

    示例:
        >>> is_valid_pytuck_database_engine('data.json', 'json')
        True
        >>> is_valid_pytuck_database_engine('data.json', 'pytuck')
        False
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
            f"Install with: pip install pytuck[{engine_name}]"
        )

    try:
        is_match, _ = backend_class.probe(file_path)
        return is_match
    except Exception:
        return False


def get_available_engines() -> Dict[str, Dict[str, Any]]:
    """
    获取所有可用引擎的详细信息

    替代 print_available_engines，返回结构化数据而非直接打印。

    Returns:
        Dict[str, Dict]: 引擎名称到引擎信息的映射

    返回结构:
        {
            'pytuck': {
                'name': 'pytuck',
                'available': True,
                'dependencies': [],
                'description': 'Pytuck storage engine',
                'format_version': '5'
            },
            'json': {
                'name': 'json',
                'available': True,
                'dependencies': [],
                'description': 'JSON storage engine',
                'format_version': '1'
            },
            'excel': {
                'name': 'excel',
                'available': False,  # 如果 openpyxl 缺失
                'dependencies': ['openpyxl'],
                'description': 'Excel storage engine',
                'format_version': '1'
            }
        }

    示例:
        >>> engines = get_available_engines()
        >>> for name, info in engines.items():
        ...     status = "✅" if info['available'] else "❌"
        ...     print(f"{status} {name}")
    """
    engines_info: Dict[str, Dict[str, Any]] = {}

    for engine_name in BackendRegistry.list_engines():
        backend_class = BackendRegistry.get(engine_name)
        if backend_class is None:
            continue

        # 提取描述
        description = backend_class.__doc__ or f'{engine_name.title()} storage engine'
        if description:
            description = description.strip().split('\n')[0]  # 只取第一行

        engines_info[engine_name] = {
            'name': engine_name,
            'available': backend_class.is_available(),
            'dependencies': getattr(backend_class, 'REQUIRED_DEPENDENCIES', []),
            'description': description,
            'format_version': getattr(backend_class, 'FORMAT_VERSION', None)
        }

    return engines_info


def print_available_engines() -> None:
    """
    打印所有可用引擎的信息

    使用 get_available_engines() 获取数据并格式化输出。

    输出格式：
        Available Pytuck Storage Engines:
        ========================================
        ✅ PYTUCK
           Format version: 5

        ❌ EXCEL
           Missing dependencies: openpyxl
           Format version: 1
    """
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
