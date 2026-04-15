# Storage

`Storage` 是 Pytucky 的核心存储引擎，管理所有表和数据的生命周期。

```python
from pytucky import Storage
```

## 构造函数

```python
Storage(
    file_path: str | Path | None = None,
    in_memory: bool = False,
    auto_flush: bool = False,
    backend_options: PytuckBackendOptions | None = None,
)
```

| 参数 | 说明 |
|------|------|
| `file_path` | 数据文件路径（None 表示纯内存） |
| `in_memory` | 是否纯内存模式 |
| `auto_flush` | 每次写操作后是否自动刷盘 |
| `backend_options` | PTK7 后端配置对象，使用 `PytuckBackendOptions` 指定加密参数 |

### 示例

```python
# 文件模式
db = Storage(file_path='mydb.pytuck')

# 纯内存
db = Storage(in_memory=True)

# 自动刷盘
db = Storage(file_path='mydb.pytuck', auto_flush=True)

# 加密配置
from pytucky.common.options import PytuckBackendOptions

db = Storage(
    file_path='secure.pytuck',
    backend_options=PytuckBackendOptions(encryption='high', password='secret123'),
)
```

如果 `file_path` 指向已存在的文件，构造时自动加载数据。

PTK7 文件默认后缀为 `.pytuck`；项目包名虽然是 `pytucky`，但如果显式传入 `.pytucky` 文件名也仍然兼容。

---

## CRUD 操作

### insert

```python
def insert(self, table_name: str, data: Dict[str, Any]) -> Any
```

插入一条记录，返回主键值。

```python
pk = db.insert('users', {'name': 'Alice', 'age': 20})
```

### select

```python
def select(self, table_name: str, pk: Any) -> Dict[str, Any]
```

通过主键查询单条记录。

```python
record = db.select('users', 1)
# {'id': 1, 'name': 'Alice', 'age': 20}
```

### update

```python
def update(self, table_name: str, pk: Any, data: Dict[str, Any]) -> None
```

更新一条记录。

```python
db.update('users', 1, {'age': 21})
```

### delete

```python
def delete(self, table_name: str, pk: Any) -> None
```

删除一条记录。

```python
db.delete('users', 1)
```

### bulk_insert

```python
def bulk_insert(self, table_name: str, records: List[Dict[str, Any]]) -> List[Any]
```

批量插入记录，返回主键列表。

```python
records = [{'name': f'user_{i}', 'age': 20+i} for i in range(1000)]
pks = db.bulk_insert('users', records)
```

### bulk_update

```python
def bulk_update(self, table_name: str, updates: List[Tuple[Any, Dict[str, Any]]]) -> int
```

批量更新记录，返回更新数。

```python
updates = [(1, {'age': 25}), (2, {'age': 30})]
count = db.bulk_update('users', updates)
```

### query

```python
def query(
    self,
    table_name: str,
    conditions: Sequence[Condition],
    limit: int | None = None,
    offset: int = 0,
    order_by: str | None = None,
    order_desc: bool = False,
) -> List[Dict[str, Any]]
```

条件查询多条记录。自动利用索引优化等值和范围查询。

### count_rows

```python
def count_rows(self, table_name: str) -> int
```

返回表的记录数。

---

## 表管理

### create_table

```python
def create_table(
    self,
    name: str,
    columns: List[Column],
    comment: str | None = None,
) -> None
```

创建表。如果表已存在则跳过。

```python
from pytucky import Column

db.create_table('users', [
    Column(int, name='id', primary_key=True),
    Column(str, name='name', index=True),
    Column(int, name='age'),
], comment='用户表')
```

### drop_table

```python
def drop_table(self, table_name: str) -> None
```

### rename_table

```python
def rename_table(self, old_name: str, new_name: str) -> None
```

### get_table

```python
def get_table(self, name: str) -> Table
```

获取 Table 对象。表不存在时抛出 `TableNotFoundError`。

### tables 属性

```python
db.tables  # Dict[str, Table]
```

---

## Schema 操作

### add_column

```python
def add_column(self, table_name: str, column: Column, default_value: Any = None) -> None
```

向表添加列。非空列必须提供 default_value 或 Column.default。

### drop_column

```python
def drop_column(self, table_name: str, column_name: str) -> None
```

删除列。不可删除主键列。

### alter_column

```python
def alter_column(
    self,
    table_name: str,
    column_name: str,
    *,
    col_type: Any = ...,
    nullable: Any = ...,
    default: Any = ...,
) -> None
```

修改列属性（类型、可空性、默认值）。`...` 表示不修改。

### set_primary_key

```python
def set_primary_key(self, table_name: str, column_name: str) -> None
```

修改表的主键。会验证唯一性和非空性。

### reorder_columns

```python
def reorder_columns(self, table_name: str, new_order: List[str]) -> None
```

### sync_table_schema

```python
def sync_table_schema(
    self,
    table_name: str,
    columns: List[Column],
    comment: str | None = None,
    options: SyncOptions | None = None,
) -> SyncResult
```

同步表结构（轻量迁移）。自动添加新列、同步备注等。

---

## 事务

```python
with db.transaction():
    db.insert('users', {'name': 'Alice'})
    db.insert('users', {'name': 'Bob'})
    # 异常时自动回滚
```

- 不支持嵌套事务
- 事务期间自动禁用 auto_flush
- 基于内存快照实现回滚

---

## 持久化

### flush

```python
def flush(self) -> None
```

将内存中的变更写入磁盘。只写入有变更的表（增量 flush）。

### close

```python
def close(self) -> None
```

关闭数据库。先执行 flush，再释放底层资源。

**重要**：无论何种场景，都应显式调用 `close()`。

---

## 事件

Storage 的 flush 操作会触发以下事件：

| 事件 | 时机 |
|------|------|
| `before_flush` | flush 执行前 |
| `after_flush` | flush 完成后 |

```python
from pytucky import event

@event.on_storage('before_flush')
def on_flush(storage):
    print('即将写盘')
```
