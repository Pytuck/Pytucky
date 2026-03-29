"""
Pytuck 配置选项 dataclass 定义

该模块定义了所有后端和连接器的配置选项，替代原有的 **kwargs 参数。
"""
import re
from dataclasses import dataclass, field
from typing import Any, Optional, Union, Dict, Literal, List

from .exceptions import ValidationError

# ASCII 可打印字符正则（排除空格 0x20，包含 0x21-0x7E）
_VALID_ZIP_PASSWORD_PATTERN = re.compile(r'^[\x21-\x7e]+$')


def _validate_zip_password(password: Optional[str]) -> None:
    """校验 ZIP 密码格式

    Args:
        password: 要校验的密码

    Raises:
        ValidationError: 密码包含非 ASCII 可打印字符时抛出
    """
    if password is not None and password != '':
        if not _VALID_ZIP_PASSWORD_PATTERN.match(password):
            raise ValidationError(
                "ZIP password can only contain ASCII printable characters "
                "(letters, digits, and symbols like !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~). "
                "Chinese, Japanese, spaces, and other non-ASCII characters are not allowed."
            )


@dataclass
class SqliteConnectorOptions:
    """SQLite 连接器配置选项"""
    check_same_thread: bool = True  # 检查同一线程
    timeout: Optional[float] = None  # 连接超时时间
    isolation_level: Optional[str] = None  # 事务隔离级别


@dataclass
class DuckdbConnectorOptions:
    """DuckDB 连接器配置选项"""
    read_only: bool = False  # 只读模式
    threads: Optional[int] = None  # 线程数（None 表示自动）
    schema: str = 'main'  # 默认 schema 名称


# Connector 选项联合类型
ConnectorOptions = Union[SqliteConnectorOptions, DuckdbConnectorOptions]


@dataclass
class JsonBackendOptions:
    """JSON 后端配置选项"""
    indent: Optional[int] = None  # 缩进空格数
    ensure_ascii: bool = False  # 是否强制 ASCII 编码
    impl: Optional[str] = None  # 指定JSON库名：'orjson', 'ujson', 'json' 等


@dataclass
class JsonlBackendOptions:
    """JSONL 后端配置选项"""
    ensure_ascii: bool = False  # 是否强制 ASCII 编码
    impl: Optional[str] = None  # 指定JSON库名：'orjson', 'ujson', 'json' 等
    password: Optional[str] = None  # ZIP 密码（仅允许 ASCII 字符）

    def __setattr__(self, name: str, value: Any) -> None:
        """拦截属性赋值，校验 password 字段"""
        if name == 'password':
            _validate_zip_password(value)
        object.__setattr__(self, name, value)


@dataclass
class CsvBackendOptions:
    """CSV 后端配置选项"""
    encoding: str = 'utf-8-sig'  # 字符编码（默认带 BOM，兼容 Excel）
    delimiter: str = ','  # 字段分隔符
    indent: Optional[int] = None  # json元数据缩进空格数（无缩进时为 None）
    password: Optional[str] = None  # ZIP 解压密码（仅允许 ASCII 字符）
    field_size_limit: Optional[int] = None  # CSV 字段大小上限（bytes），None 表示使用 csv 模块默认限制（131072）

    def __setattr__(self, name: str, value: Any) -> None:
        """拦截属性赋值，校验 password 和 field_size_limit 字段"""
        if name == 'password':
            _validate_zip_password(value)
        if name == 'field_size_limit' and value is not None:
            if not isinstance(value, int) or value <= 0:
                raise ValidationError("field_size_limit must be a positive integer or None")
        object.__setattr__(self, name, value)


@dataclass
class SqliteBackendOptions(SqliteConnectorOptions):
    """SQLite 后端配置选项"""
    use_native_sql: bool = True  # 使用原生 SQL 模式，直接执行 SQL 而非全量加载/保存


@dataclass
class DuckdbBackendOptions(DuckdbConnectorOptions):
    """DuckDB 后端配置选项"""
    use_native_sql: bool = True  # 使用原生 SQL 模式，直接执行 SQL 而非全量加载/保存


@dataclass
class ExcelBackendOptions:
    """Excel 后端配置选项"""
    read_only: bool = False  # 只读，只读情况下显著提升读取性能，但不可修改数据
    hide_metadata_sheets: bool = True  # 是否隐藏元数据工作表（_metadata 和 _pytuck_tables），默认隐藏


@dataclass
class XmlBackendOptions:
    """XML 后端配置选项"""
    encoding: str = 'utf-8'  # 字符编码
    pretty_print: bool = True  # 是否格式化输出


@dataclass
class BinaryBackendOptions:
    """Pytuck 引擎后端配置选项"""
    lazy_load: bool = True  # 是否懒加载（只加载 schema 和索引，按需读取数据）
    sidecar_wal: bool = False  # 是否将 WAL 写入独立 sidecar 文件（.<文件名>.wal）

    # 加密选项
    encryption: Optional[Literal['low', 'medium', 'high']] = None  # 加密等级: 'low' | 'medium' | 'high' | None
    password: Optional[str] = None    # 加密密码（仅 encryption 非 None 时生效）


# Backend 选项联合类型
BackendOptions = Union[
    JsonBackendOptions,
    JsonlBackendOptions,
    CsvBackendOptions,
    SqliteBackendOptions,
    DuckdbBackendOptions,
    ExcelBackendOptions,
    XmlBackendOptions,
    BinaryBackendOptions
]


# 默认选项获取函数
def get_default_backend_options(engine: str) -> BackendOptions:
    """根据引擎类型返回默认选项"""
    defaults: Dict[str, BackendOptions] = {
        'json': JsonBackendOptions(),
        'jsonl': JsonlBackendOptions(),
        'csv': CsvBackendOptions(),
        'sqlite': SqliteBackendOptions(),
        'duckdb': DuckdbBackendOptions(),
        'excel': ExcelBackendOptions(),
        'xml': XmlBackendOptions(),
        'pytuck': BinaryBackendOptions()
    }
    return defaults.get(engine, BinaryBackendOptions())


def get_default_connector_options(db_type: str) -> ConnectorOptions:
    """根据连接器类型返回默认选项"""
    defaults: Dict[str, ConnectorOptions] = {
        'sqlite': SqliteConnectorOptions(),
        'duckdb': DuckdbConnectorOptions()
    }
    return defaults.get(db_type, SqliteConnectorOptions())


# ========== Schema 同步选项 ==========


@dataclass
class SyncOptions:
    """Schema 同步选项

    控制 sync_table_schema 和 declarative_base(sync_schema=True) 的行为。
    """
    sync_table_comment: bool = True       # 是否同步表备注
    sync_column_comments: bool = True     # 是否同步列备注
    add_new_columns: bool = True          # 是否添加新列
    # 以下为安全选项，默认不启用
    drop_missing_columns: bool = False    # 是否删除模型中不存在的列（危险）
    update_column_types: bool = False     # 是否更新列类型（危险，暂未实现）


@dataclass
class SyncResult:
    """Schema 同步结果

    记录 sync_table_schema 执行后的变更详情。
    """
    table_name: str
    table_comment_updated: bool = False
    columns_added: List[str] = field(default_factory=list)
    columns_dropped: List[str] = field(default_factory=list)
    column_comments_updated: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """是否有任何变更"""
        return (
            self.table_comment_updated or
            bool(self.columns_added) or
            bool(self.columns_dropped) or
            bool(self.column_comments_updated)
        )
