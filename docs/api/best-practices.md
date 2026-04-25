# 最佳实践

## 持久化策略

### flush 与 close

```python
db = Storage(file_path='mydb.pytuck')
try:
    # ... 操作 ...
    db.flush()  # 显式写盘（可选，close 会自动 flush）
finally:
    db.close()  # 必须调用：flush + 释放资源
```

**规则**：无论脚本、测试还是服务，都应显式调用 `close()`。

### auto_flush

```python
db = Storage(file_path='mydb.pytuck', auto_flush=True)
```

每次写操作（insert/update/delete）后自动刷盘。适合数据安全性要求高但写入量不大的场景。

**注意**：频繁写入时 auto_flush 会显著影响性能，建议批量操作后手动 flush。

### Session 的 commit 与 flush

```python
session = Session(db)

# add/add_all 只是标记，不写入 Storage
session.add(user)

# flush 将修改写入 Storage 内存
session.flush()

# commit = flush + auto_flush 时写盘
session.commit()
```

---

## 性能优化

### 批量插入

**推荐**：使用 `session.bulk_insert()` 或 `add_all()` + `commit()`。

```python
# 方式 1：bulk_insert（直接写入，触发 bulk 事件）
users = [User(name=f'u{i}') for i in range(10000)]
session.bulk_insert(users)
session.commit()

# 方式 2：add_all + commit（内部自动优化为批量）
users = [User(name=f'u{i}') for i in range(10000)]
session.add_all(users)
session.commit()
```

**避免**：循环中逐条 `add` + `commit`。

```python
# 不推荐
for i in range(10000):
    session.add(User(name=f'u{i}'))
    session.commit()  # 每次都 flush，性能差
```

### 索引

对频繁查询的列建立索引：

```python
name = Column(str, index=True)        # 哈希索引，等值查询 O(1)
age = Column(int, index='sorted')     # 有序索引，支持范围查询
```

- 等值查询（`==`）→ hash 或 sorted
- 范围查询（`>`, `>=`, `<`, `<=`）→ sorted
- 排序（`order_by`）→ sorted（可避免内存排序）

### 懒加载

PTK7 默认按需读取。打开文件时只加载：

- 文件头 + Schema
- 每表 PK 目录（pk → offset 映射）
- 索引元数据

记录内容在查询时按需解码，无需配置 `lazy_load`。

### flush 只写变更表

`Storage.flush()` 只物化和写入有变更的表，未修改的表完全跳过。

---

## 使用约束

### 单写者

当前不保证共享实例的线程安全。`Storage` / `Store` 面向单实例、单写者、显式生命周期的嵌入式场景。

多线程或多进程共享同一个数据库文件时，请在外部做同步控制。

### 主键

- 支持单列主键或无主键
- 不支持复合主键
- int 类型主键支持自动递增
- 非 int 类型主键必须手动指定

### 类型转换

默认模式下，Column 会尝试自动类型转换：

```python
Column(int)  # '123' → 123，'abc' → ValidationError
Column(bool) # 1 → True，'true' → True
```

严格模式下不做转换：

```python
Column(int, strict=True)  # '123' → ValidationError
```

**特殊规则**：`bool` 值不会自动转换为 `int`（防止逻辑错误）。

### 无主键模型

无主键模型使用内部 rowid，有以下限制：

- 不能使用 `session.get(Model, pk)`，需使用 `select()` 查询
- `CRUDBaseModel.get()` 返回 None

---

## 事件系统

### 模型事件

```python
from pytucky import event

@event.on_model(User, 'before_insert')
def before_insert(instance):
    instance.created_at = datetime.now()

@event.on_model(User, 'after_insert')
def after_insert(instance):
    print(f'Created user: {instance.name}')
```

支持的事件：`before_insert`, `after_insert`, `before_update`, `after_update`, `before_delete`, `after_delete`, `before_bulk_insert`, `after_bulk_insert`, `before_bulk_update`, `after_bulk_update`。

### Storage 事件

```python
@event.on_storage('before_flush')
def on_flush(storage):
    print('即将写盘')
```

支持的事件：`before_flush`, `after_flush`。

---

## 从 pytuck 迁移

Pytucky 保留了 pytuck 的核心 ORM API。基础用法只需更改 import：

```python
# 之前
from pytuck import Storage, declarative_base, Session, Column

# 之后
from pytucky import Storage, declarative_base, Session, Column
```

### 主要差异

| 特性 | pytuck | pytucky |
|------|--------|---------|
| 引擎 | 8 种（json, csv, sqlite 等） | PTK7 单引擎 |
| 文件格式 | 多种 | 默认 `.pytuck`（PTK7），显式 `.pytucky` 兼容 |
| Storage 参数 | engine, backend_options 等 | file_path, in_memory, auto_flush |
| 迁移工具 | tools.py | 无（单格式无需迁移） |
| Native SQL | 支持 | 不支持 |
| 懒加载配置 | lazy_load 参数 | 默认启用，无需配置 |

### 最小迁移步骤

- 把 `from pytuck ...` 改为 `from pytucky ...`
- 保持原有 PTK7 文件即可继续使用，无需数据迁移
- 若原项目显式传入 `.pytucky` 后缀，可继续保留；新项目默认推荐 `.pytuck`

### 加密兼容说明

Pytucky 与 pytuck 共享 PTK7 格式，以下加密等级已验证可双向互读互写：

- `None`
- `low`
- `medium`
- `high`

使用加密文件时，双方需要保持相同密码；只要密码正确，便可以在两个库之间继续 reopen、查询和 flush。
