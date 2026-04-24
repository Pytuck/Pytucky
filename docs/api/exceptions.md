# 异常

所有异常继承自 `PytuckyException`，提供统一的结构化字段。

```python
from pytucky import PytuckyException
```

## 异常基类

### PytuckyException

```python
PytuckyException(
    message: str,
    *,
    table_name: str | None = None,
    column_name: str | None = None,
    pk: Any = None,
    details: dict[str, Any] | None = None,
)
```

| 属性 | 说明 |
|------|------|
| `message` | 错误消息 |
| `table_name` | 相关表名 |
| `column_name` | 相关列名 |
| `pk` | 相关主键值 |
| `details` | 额外详情字典 |
| `to_dict()` | 转换为字典（便于日志记录） |

---

## 异常层次

```
PytuckyException
├── TableNotFoundError          # 表不存在
├── RecordNotFoundError         # 记录不存在
├── DuplicateKeyError           # 主键重复
├── ColumnNotFoundError         # 列不存在
├── ValidationError             # 数据验证失败
│   └── TypeConversionError     # 类型转换失败
├── ConfigurationError          # 配置错误
│   └── SchemaError             # Schema 定义错误
├── QueryError                  # 查询构建/执行错误
├── TransactionError            # 事务操作错误
├── SerializationError          # 序列化/反序列化错误
├── UnsupportedOperationError   # 不支持的操作
├── PytuckyIndexError           # 索引操作错误
├── DatabaseConnectionError     # 数据库连接错误（兼容保留）
├── EncryptionError             # 加密错误（兼容保留）
└── MigrationError              # 迁移错误（兼容保留）
```

---

## 常见异常与触发场景

### TableNotFoundError

```python
db.select('nonexistent', 1)       # 查询不存在的表
db.drop_table('nonexistent')      # 删除不存在的表
```

### RecordNotFoundError

```python
db.select('users', 999)           # 查询不存在的记录
db.update('users', 999, {...})    # 更新不存在的记录
db.delete('users', 999)           # 删除不存在的记录
```

### DuplicateKeyError

```python
db.insert('users', {'id': 1, 'name': 'Alice'})
db.insert('users', {'id': 1, 'name': 'Bob'})  # 主键重复
```

### ColumnNotFoundError

```python
db.drop_column('users', 'nonexistent')
```

### ValidationError

```python
# 非空列赋 None
Column(str, nullable=False)  # 传入 None 时抛出

# bool 赋值给 int 列
Column(int)  # 传入 True/False 时抛出

# 严格模式类型不匹配
Column(int, strict=True)  # 传入 '123' 时抛出

# 自定义校验器失败
Column(int, validator=lambda x: x > 0)  # 传入 -1 时抛出
```

### TypeConversionError

ValidationError 的子类。类型转换失败时抛出。

```python
Column(int)  # 传入 'abc'（无法转换为 int）
```

额外属性：`value`（原始值）、`target_type`（目标类型名）。

### SchemaError

```python
# 定义多个主键
class Bad(Base):
    id = Column(int, primary_key=True)
    code = Column(str, primary_key=True)  # 不支持复合主键

# 非空列无默认值
db.add_column('users', Column(str, name='x', nullable=False))  # 表中已有数据时

# 删除主键列
db.drop_column('users', 'id')
```

### QueryError

```python
# 无主键模型使用 session.get()
session.get(NoKeyModel, 1)

# 不支持的语句类型
session.execute(unknown_stmt)
```

### TransactionError

```python
# 嵌套事务
with db.transaction():
    with db.transaction():  # 抛出 TransactionError
        pass
```

### SerializationError

PTK7 格式编解码错误。通常表示文件损坏或版本不兼容。

---

## 捕获示例

```python
from pytucky import (
    PytuckyException,
    RecordNotFoundError,
    DuplicateKeyError,
    ValidationError,
)

# 捕获特定异常
try:
    db.insert('users', {'id': 1, 'name': 'Alice'})
except DuplicateKeyError as e:
    print(f"主键 {e.pk} 已存在于表 {e.table_name}")

# 捕获所有 pytucky 异常
try:
    db.select('users', 999)
except PytuckyException as e:
    print(e.to_dict())

# 捕获所有验证错误（包括 TypeConversionError）
try:
    user = User(age='abc')
except ValidationError as e:
    print(e.message)
```

---

## 兼容别名

以下别名保留用于从 pytuck 迁移：

| 别名 | 实际类型 |
|------|----------|
| `PytuckException` | `PytuckyException` |
| `PytuckIndexError` | `PytuckyIndexError` |
