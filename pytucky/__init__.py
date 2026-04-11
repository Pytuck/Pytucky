"""
Pytucky - 单文件、高性能、纯 Python 文档数据库

Pytucky 是一个轻量级、单文件格式的文档数据库，专注于提供高性能的读写
以及与原始 Pytuck API 兼容的基础 ORM 用法。当前项目专注于单一二进制
后端（PTK 格式），并保留与原有 pytuck 接口的兼容层，方便用户从 pytuck
迁移到 pytucky：只需将 import pytuck -> import pytucky。

主要特性：
- 单文件存储（PTK 二进制格式）
- 基于声明式模型的 ORM（declarative_base, Session, Column）
- 兼容 Pytuck 的基础查询与迁移工具
- 围绕当前单文件主路径持续优化性能

示例用法（简短）：
    from typing import Type
    from pytucky import Storage, declarative_base, Session, Column

    db = Storage(file_path='mydb.pytucky')
    Base: Type = declarative_base(db)

    class User(Base):
        __tablename__ = 'users'
        id = Column(int, primary_key=True)
        name = Column(str)

    session = Session(db)

请参考项目的 tests/ 目录获取更多使用示例。
"""

from .core import (
    Column,
    Relationship,
    declarative_base,
    PureBaseModel,
    CRUDBaseModel,
)
from .core import Storage
from .core import Session
from .core import event
from .core import prefetch
from .query import Query, BinaryExpression
from .query import select, insert, update, delete
from .query import or_, and_, not_
from .query import Result, CursorResult
from .common.exceptions import (
    PytuckyException,
    PytuckException,
    TableNotFoundError,
    RecordNotFoundError,
    DuplicateKeyError,
    ColumnNotFoundError,
    TransactionError,
    SerializationError,
    EncryptionError,
    ValidationError,
    TypeConversionError,
    ConfigurationError,
    SchemaError,
    QueryError,
    DatabaseConnectionError,
    UnsupportedOperationError,
    MigrationError,
    PytuckyIndexError,
    PytuckIndexError,
)
from .common.options import SyncOptions, SyncResult
from .backends.migrate_p5_to_p7 import migrate_pytuck_to_pytucky, migrate_p5_to_p7

__version__ = '1.0.0'
__all__ = [
    # ==================== 推荐 API ====================

    # SQLAlchemy 2.0 风格语句构建器
    'select',      # SELECT 查询
    'insert',      # INSERT 插入
    'update',      # UPDATE 更新
    'delete',      # DELETE 删除

    # 逻辑组合函数
    'or_',         # OR 条件组合
    'and_',        # AND 条件组合
    'not_',        # NOT 条件取反

    # 核心组件
    'Storage',            # 存储引擎
    'declarative_base',   # 声明式基类工厂
    'Session',            # 会话管理
    'Column',             # 列定义
    'Relationship',       # 关系定义

    # 事件系统
    'event',              # 事件管理器

    # 关系预取
    'prefetch',           # 批量预取关联数据

    # 类型定义（用于类型注解）
    'PureBaseModel',      # 纯模型基类类型
    'CRUDBaseModel',      # Active Record 基类类型

    # Schema 同步
    'SyncOptions',        # 同步选项
    'SyncResult',         # 同步结果

    # 迁移工具
    'migrate_pytuck_to_pytucky',  # PTK5 -> PTK7 显式迁移
    'migrate_p5_to_p7',           # 迁移别名

    # 查询结果
    'Result',        # 查询结果包装器
    'CursorResult',  # CUD 操作结果

    # 高级用法
    'BinaryExpression',  # 查询表达式（用于构建复杂查询）
    'Query',             # 查询构建器（内部使用）

    # ==================== 异常 ====================

    # 基类
    'PytuckyException',
    'PytuckException',  # 兼容旧名称

    # 表和记录级异常
    'TableNotFoundError',
    'RecordNotFoundError',
    'DuplicateKeyError',
    'ColumnNotFoundError',

    # 验证和类型异常
    'ValidationError',
    'TypeConversionError',

    # 配置异常
    'ConfigurationError',
    'SchemaError',

    # 查询异常
    'QueryError',

    # 连接和事务异常
    'DatabaseConnectionError',
    'TransactionError',

    # 操作异常
    'UnsupportedOperationError',
    'SerializationError',
    'EncryptionError',
    'MigrationError',
    'PytuckyIndexError',
    'PytuckIndexError',  # 兼容旧名称
]
