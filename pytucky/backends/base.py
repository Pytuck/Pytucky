"""
Pytuck 存储后端抽象基类

定义所有持久化引擎必须实现的接口
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Union, TYPE_CHECKING, Tuple

from ..common.options import BackendOptions
from ..common.exceptions import ConfigurationError

if TYPE_CHECKING:
    from ..core.storage import Table


class StorageBackend(ABC):
    """
    存储后端抽象基类

    所有持久化引擎必须实现此接口，提供统一的 save/load API
    """

    # 引擎标识符（用于注册和选择）
    ENGINE_NAME: str  # type: ignore

    # 所需的外部依赖列表（用于检查可用性）
    REQUIRED_DEPENDENCIES: List[str] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        子类定义时自动注册到 BackendRegistry

        只有定义了 ENGINE_NAME 的具体后端类才会被注册。
        抽象基类或中间类不会被注册。
        """
        super().__init_subclass__(**kwargs)
        # 必须设置 ENGINE_NAME，且不能重复
        if getattr(cls, 'ENGINE_NAME', None) is None:
            raise ConfigurationError('ENGINE_NAME must be set')

        from .registry import BackendRegistry
        if cls.ENGINE_NAME in BackendRegistry.list_engines():
            raise ConfigurationError(f'The engine name is already registered: "{cls.ENGINE_NAME}"')
        BackendRegistry.register(cls)

    def __init__(self, file_path: Union[str, Path], options: BackendOptions):
        """
        初始化后端

        Args:
            file_path: 数据文件路径，接受字符串或 Path 对象（不同引擎解释不同）
                - pytuck: 单个 .pytuck 文件
                - json: 单个 .json 文件
                - csv: ZIP 文件路径
                - sqlite: 单个 .sqlite 文件
                - excel: 单个 .xlsx 文件
                - xml: 单个 .xml 文件
            options: 强类型的后端配置选项对象
        """
        # 输入兼容性处理：统一转为 Path 对象
        self.file_path: Path = Path(file_path).expanduser()
        self.options = options

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file_path='{self.file_path}')"

    @abstractmethod
    def save(self, tables: Dict[str, 'Table'], *, changed_tables: Optional[Set[str]] = None) -> None:
        """
        保存所有表数据到持久化存储

        Args:
            tables: 表字典 {table_name: Table对象}
            changed_tables: 可选，发生变更的表名集合。
                            后端可利用此信息进行增量保存优化。
                            如果为 None，应执行全量保存（向后兼容）。

        Table 对象结构：
            - table.name: str - 表名
            - table.columns: Dict[str, Column] - 列定义
            - table.primary_key: str - 主键字段名
            - table.data: Dict[pk, record_dict] - 数据 {主键: 记录字典}
            - table.indexes: Dict[str, HashIndex] - 索引
            - table.next_id: int - 下一个自增ID

        实现要点：
            1. 序列化表结构（列定义、主键、next_id）
            2. 序列化所有记录数据
            3. 可选：持久化索引数据（也可以在加载时重建）
            4. 确保原子性写入（先写临时文件，再重命名）
            5. 可选：利用 changed_tables 进行增量保存优化
        """
        pass

    @abstractmethod
    def load(self) -> Dict[str, 'Table']:
        """
        从持久化存储加载所有表数据

        Returns:
            表字典 {table_name: Table对象}

        实现要点：
            1. 反序列化表结构
            2. 反序列化所有记录
            3. 重建 Table 对象
            4. 重建索引（如果未持久化）
            5. 恢复 next_id 状态

        Raises:
            SerializationError: 反序列化失败
            FileNotFoundError: 数据文件不存在
        """
        pass

    @abstractmethod
    def exists(self) -> bool:
        """
        检查数据文件是否存在

        Returns:
            是否存在

        用于判断是加载现有数据还是创建新数据库
        """
        pass

    @abstractmethod
    def delete(self) -> None:
        """
        删除数据文件（用于清理）

        实现要点：
            - 删除所有相关文件（数据、索引、元数据等）
            - 如果是目录（如CSV），删除整个目录
        """
        pass

    @classmethod
    def is_available(cls) -> bool:
        """
        检查引擎是否可用（依赖是否满足）

        Returns:
            是否可用

        实现逻辑：
            尝试导入所有 REQUIRED_DEPENDENCIES，全部成功则可用
        """
        for dep in cls.REQUIRED_DEPENDENCIES:
            try:
                __import__(dep)
            except ImportError:
                return False
        return True

    def get_metadata(self) -> Dict[str, Any]:
        """
        获取后端元数据（可选实现）

        Returns:
            元数据字典（版本、大小、修改时间等）

        示例返回：
            {
                'version': '0.1.0',
                'size': 12345,  # 字节
                'modified': '2026-01-05T10:00:00',
                'table_count': 5,
                'record_count': 1000,
            }
        """
        return {}

    def supports_server_side_pagination(self) -> bool:
        """
        检查后端是否支持服务端分页

        Returns:
            True 如果支持服务端分页，False 使用内存分页
        """
        return False

    def supports_lazy_loading(self) -> bool:
        """
        检查后端是否支持延迟加载（即 load() 只加载 schema，数据按需加载）

        Returns:
            True 如果当前配置下 load() 只加载 schema，数据需要按需查询或填充
            False 如果 load() 会加载所有数据到内存

        用途：
            - 迁移工具需要知道是否需要调用 populate_tables_with_data()
            - 数据库类后端（如 SQLite 原生模式）可能只加载 schema

        Note:
            返回值可能依赖于当前的配置选项，而非静态属性。
            例如 SQLite 后端在 use_native_sql=True 时返回 True。

        后端实现说明：
            - BinaryBackend（ENGINE_NAME='pytuck', lazy_load=True）: 通过 _pk_offsets 遍历主键，逐条读取文件
            - SQLiteBackend (use_native_sql=True): 通过 SQL 查询获取全部数据
            - 其他数据库后端: 根据具体实现决定
        """
        return False

    def populate_tables_with_data(self, tables: Dict[str, 'Table']) -> None:
        """
        从持久化存储填充表数据（用于延迟加载模式）

        当 supports_lazy_loading() 返回 True 时，load() 只加载 schema，
        此方法用于在需要时（如迁移）填充实际数据。

        Args:
            tables: 需要填充数据的表字典（由 load() 返回）

        Note:
            - 如果 supports_lazy_loading() 返回 False，此方法通常什么都不做
            - 子类应检查 table.data 是否已有数据，避免重复加载
            - 此方法应该是幂等的（多次调用结果相同）
        """
        pass  # 默认实现：什么都不做

    def save_full(self, tables: Dict[str, 'Table']) -> None:
        """
        全量保存所有表数据（用于迁移场景）

        当 supports_lazy_loading() 返回 True 时，默认的 save() 可能只保存 schema，
        此方法强制保存所有数据（包括 table.data 中的内容）。

        Args:
            tables: 要保存的表字典

        Note:
            - 默认实现直接调用 save()，对于非延迟加载后端足够
            - 延迟加载后端（如 SQLite 原生模式）应覆盖此方法
        """
        self.save(tables)  # 默认实现：直接调用 save()

    def query_with_pagination(
            self,
            table_name: str,
            conditions: List[Dict[str, Any]],
            limit: Optional[int] = None,
            offset: int = 0,
            order_by: Optional[str] = None,
            order_desc: bool = False
    ) -> Dict[str, Any]:
        """
        带分页的查询（可选实现，用于数据库后端优化）

        Args:
            table_name: 表名
            conditions: 查询条件列表（简化格式）
            limit: 限制返回记录数
            offset: 跳过的记录数
            order_by: 排序字段名
            order_desc: 是否降序排列

        Returns:
            {
                'records': List[Dict[str, Any]],  # 查询结果
                'total_count': int,               # 总记录数（可选）
                'has_more': bool,                 # 是否还有更多数据（可选）
            }

        Note:
            - 默认实现抛出 NotImplementedError
            - 只有支持的后端才需要实现此方法
            - SQLite、DuckDB 等数据库后端可以实现真正的服务端分页
            - JSON、CSV 等文件后端通常不实现此方法
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support server-side pagination. "
            f"Use storage.query() with memory-based pagination instead."
        )

    @classmethod
    def probe(cls, file_path: Union[str, Path]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        轻量探测文件是否为本引擎格式

        此方法用于动态识别数据库文件格式，而不需要完整加载文件。
        各引擎应该覆盖此方法提供特定的探测逻辑。

        Args:
            file_path: 要探测的文件路径

        Returns:
            Tuple[bool, Optional[Dict]]: (是否匹配此引擎格式, 可选的元数据信息)

        返回元数据示例：
            {
                'engine': 'pytuck',
                'format_version': '1',
                'confidence': 'high',    # 'high', 'medium', 'low'
                'file_size': 12345,
                'modified': 1641234567.0,
                'table_count': 3,        # 如果可以轻量获取
                'error': 'error msg'     # 如果探测失败的原因
            }

        实现要点：
            1. 只读取必要的文件头部或元数据，避免完整解析
            2. 必须捕获所有异常，不得抛出到调用方
            3. 基于内容特征判断，不依赖文件扩展名
            4. 返回高置信度的识别结果或明确的拒绝
            5. 轻量级：Pytuck 读 64 字节头，JSON 读 32KB，XML 读 8KB 等

        注意：
            - 默认实现返回 False（不匹配），各引擎需要覆盖
            - 探测失败不等于文件无效，可能是其他格式
            - 此方法为类方法，不需要实例化后端对象
        """
        try:
            file_path = Path(file_path).expanduser()
            if not file_path.exists():
                return False, {'error': 'file_not_found'}

            # 默认实现：基于 ENGINE_NAME 和文件扩展名的简单匹配
            if cls.ENGINE_NAME and file_path.suffix.lower() in ['.pytuck', '.db', '.json', '.xml', '.xlsx', '.zip', '.sqlite']:
                return False, {'error': 'default_probe_not_implemented'}

            return False, None
        except Exception as e:
            return False, {'error': f'probe_exception: {str(e)}'}
