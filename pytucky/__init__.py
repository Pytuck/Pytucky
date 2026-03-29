"""
Pytuck - 轻量级 Python 文档数据库

基于对象模型的数据库系统，无需编写SQL，支持：
- SQLAlchemy 2.0 风格 API
- Pythonic 查询表达式
- 多存储引擎（Pytuck, JSON, CSV, SQLite, DuckDB, Excel, XML）
- 索引优化
- 事务支持
- 类型安全

两种使用模式：

1. 纯模型模式（默认，推荐）- 通过 Session 操作数据：
    from typing import Type
    from pytuck import Storage, declarative_base, Session, Column
    from pytuck import PureBaseModel, select, insert, update, delete

    db = Storage(file_path='mydb.pytuck')
    Base: Type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = 'users'
        id = Column(int, primary_key=True)
        name = Column(str)
        age = Column(int)

    session = Session(db)

    # 插入
    stmt = insert(User).values(name='Alice', age=20)
    session.execute(stmt)
    session.commit()

    # 查询
    stmt = select(User).where(User.age >= 18)
    result = session.execute(stmt)
    users = result.all()

2. Active Record 模式 - 模型自带 CRUD 方法：
    from typing import Type
    from pytuck import Storage, declarative_base, Column
    from pytuck import CRUDBaseModel

    db = Storage(file_path='mydb.pytuck')
    Base: Type[CRUDBaseModel] = declarative_base(db, crud=True)

    class User(Base):
        __tablename__ = 'users'
        id = Column(int, primary_key=True)
        name = Column(str)

    # 直接在模型上操作
    user = User.create(name='Alice')
    user.name = 'Bob'
    user.save()
    user.delete()
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
    PytuckIndexError,
)
from .common.options import SyncOptions, SyncResult
from .backends.migrate_p5_to_p6 import migrate_pytuck_to_pytucky, migrate_p5_to_p6

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
    'migrate_pytuck_to_pytucky',  # PTK5 -> PTK6 显式迁移
    'migrate_p5_to_p6',           # 迁移别名

    # 查询结果
    'Result',        # 查询结果包装器
    'CursorResult',  # CUD 操作结果

    # 高级用法
    'BinaryExpression',  # 查询表达式（用于构建复杂查询）
    'Query',             # 查询构建器（内部使用）

    # ==================== 异常 ====================

    # 基类
    'PytuckException',

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
    'PytuckIndexError',
]
