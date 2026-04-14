# Session

`Session` 是类似 SQLAlchemy 的会话管理器，统一管理对象状态追踪和数据库操作。

```python
from pytucky import Session
```

## 构造函数

```python
Session(storage: Storage, autocommit: bool = False)
```

| 参数 | 说明 |
|------|------|
| `storage` | Storage 实例 |
| `autocommit` | 每次 add/delete 后是否自动 commit |

---

## 对象管理

### add / add_all

```python
session.add(instance)          # 标记为待插入
session.add_all([i1, i2, i3]) # 批量标记
```

### delete

```python
session.delete(instance)  # 标记为待删除
```

### flush

```python
session.flush()
```

将待处理的修改写入 Storage 内存：

1. **插入**：按模型类分组 → `bulk_insert` → 触发 `before_insert` / `after_insert`
2. **更新**：逐条更新 → 触发 `before_update` / `after_update`
3. **删除**：逐条删除 → 触发 `before_delete` / `after_delete`

### commit

```python
session.commit()
```

调用 `flush()` 后，如果 Storage 开启了 `auto_flush`，还会将数据写入磁盘。

### rollback

```python
session.rollback()
```

清空所有待处理的修改和 identity map。

---

## 查询

### get

```python
def get(self, model_class: Type[T], pk: Any) -> Optional[T]
```

通过主键获取对象。优先从 identity map 返回缓存实例。

```python
user = session.get(User, 1)
```

### refresh

```python
session.refresh(instance)
```

从数据库重新加载实例的所有字段。

### query（旧风格）

```python
users = session.query(User).filter(User.age >= 18).all()
```

返回 Query 对象，支持链式调用。不推荐，但会持续支持。

### execute（推荐）

```python
from pytucky import select, insert, update, delete

# SELECT
stmt = select(User).where(User.age >= 18)
result = session.execute(stmt)
users = result.all()

# INSERT
stmt = insert(User).values(name='Alice', age=20)
result = session.execute(stmt)
session.commit()

# UPDATE
stmt = update(User).where(User.id == 1).set(age=21)
result = session.execute(stmt)
session.commit()

# DELETE
stmt = delete(User).where(User.age < 18)
result = session.execute(stmt)
session.commit()
```

---

## 批量操作

### bulk_insert

```python
def bulk_insert(self, instances: List[PureBaseModel]) -> List[Any]
```

立即批量写入 Storage 内存（不经过 flush 队列）。触发 `before_bulk_insert` / `after_bulk_insert`。

```python
users = [User(name=f'u{i}') for i in range(1000)]
pks = session.bulk_insert(users)
session.commit()  # 持久化到磁盘
```

**与 add_all 的区别**：

| | `add_all` + `commit` | `bulk_insert` |
|---|---|---|
| 写入时机 | commit 时 | 立即 |
| 事件语义 | 逐条 before/after_insert | bulk before/after_bulk_insert |
| 性能 | 内部已优化为批量（分组 bulk_insert） | 直接批量 |

### bulk_update

```python
def bulk_update(self, instances: List[PureBaseModel]) -> int
```

立即批量更新。实例必须已有主键。

---

## merge

```python
managed = session.merge(detached_instance)
```

合并外部实例到会话：
- identity map 中已存在 → 更新现有实例
- 数据库中已存在 → 加载并更新
- 都不存在 → 作为新对象 add

---

## 事务

```python
with session.begin():
    session.add(User(name='Alice'))
    session.add(User(name='Bob'))
    # 异常时自动回滚
```

不支持嵌套事务。

### 上下文管理器

```python
with Session(db) as session:
    session.add(user)
    # 正常退出时自动 commit
    # 异常时自动 rollback
```

---

## Schema 操作

Session 提供面向模型的 Schema 操作，接受模型类或表名字符串：

```python
# 同步 schema
result = session.sync_schema(User)

# 添加列
session.add_column(User, Column(int, nullable=True, name='age'))

# 删除列
session.drop_column(User, 'old_field')

# 修改列
session.alter_column(User, 'age', col_type=str)

# 修改主键
session.set_primary_key(User, 'email')

# 重排列
session.reorder_columns(User, ['id', 'email', 'name'])

# 删除表
session.drop_table(User)

# 重命名表
session.rename_table(User, 'user_accounts')

# 更新备注
session.update_table_comment(User, '用户信息表')
session.update_column(User, 'name', comment='用户名')
```

---

## close

```python
session.close()
```

关闭会话，清理所有状态。等效于 `rollback()`。
