# API 参考

Pytucky 的完整 API 参考文档。

## 导入

```python
# 核心组件
from pytucky import Storage, Session, Column, Relationship
from pytucky import declarative_base, PureBaseModel, CRUDBaseModel

# SQLAlchemy 2.0 风格语句
from pytucky import select, insert, update, delete

# 逻辑组合
from pytucky import or_, and_, not_

# 查询结果
from pytucky import Result, CursorResult

# 事件系统
from pytucky import event

# 关系预取
from pytucky import prefetch

# Schema 同步
from pytucky import SyncOptions, SyncResult

# 异常
from pytucky import (
    PytuckyException,
    TableNotFoundError,
    RecordNotFoundError,
    DuplicateKeyError,
    ColumnNotFoundError,
    ValidationError,
    TypeConversionError,
    QueryError,
    TransactionError,
    SerializationError,
    SchemaError,
    ConfigurationError,
    UnsupportedOperationError,
    PytuckyIndexError,
)
```

## 文档索引

| 文档 | 内容 |
|------|------|
| [models.md](models.md) | Column、PureBaseModel、CRUDBaseModel、declarative_base、Relationship |
| [storage.md](storage.md) | Storage CRUD、表管理、事务、持久化 |
| [session.md](session.md) | Session 对象管理、批量操作、Schema 操作 |
| [query.md](query.md) | select/insert/update/delete、Result、逻辑操作符 |
| [exceptions.md](exceptions.md) | 异常层次与触发场景 |
| [best-practices.md](best-practices.md) | 持久化策略、性能优化、使用约束 |

## 版本

当前版本：**1.0.0**

- Python 要求：>= 3.7
- 运行时依赖：无
- 数据格式：PTK7（默认 `.pytuck`，显式 `.pytucky` 兼容）
