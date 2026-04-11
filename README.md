# Pytucky

Pytucky 是一个**纯 Python、零第三方运行时依赖、单文件**的嵌入式文档数据库。

它保留了 Pytuck 的基础 ORM 用法（`Column`、`declarative_base`、`Session`、`select/insert/update/delete`、`CRUDBaseModel`），但把实现收敛到了单一的 **PTK7 / `.pytucky`** 路径，优先优化：

- 打开数据库（`open/load/reopen`）
- 持久化（`flush/save`）
- 主键查询
- 索引查询

如果你想要的是：**像 SQLite 一样直接打开就用，不想再为懒加载/WAL/多引擎配置分心，同时又希望保留一套轻量 ORM 用法**，那 Pytucky 就是这个方向。

## 当前状态

- 当前格式：**PTK7 / `.pytucky`**
- 运行时依赖：**无**
- Python 要求：**>= 3.7**
- 项目状态：**Pre-Alpha**
- 许可证：**MIT**

## 核心特性

- **纯 Python**：适合受限运行环境
- **零第三方运行时依赖**：安装简单，部署轻量
- **单文件数据库**：一个 `.pytucky` 文件即可承载完整数据
- **保留基础 ORM 用法**：迁移已有调用方式更平滑
- **默认按需读取**：不需要额外的 `lazy_load` 配置
- **主键直达读取**：`pk -> (offset, length)` 直接定位记录
- **索引按需读取**：索引元数据在打开时恢复，索引块在查询时再读取
- **显式迁移工具**：支持从 PTK5 / `.pytuck` 迁移到 PTK7

## 安装

### 从源码开发

```bash
uv sync
```

或：

```bash
uv pip install -e ".[dev]"
```

### 作为普通包安装

```bash
pip install pytucky
```

## 快速开始

### 1. 直接使用 `Storage`

```python
from pytucky import Storage, Column


db = Storage(file_path="demo.pytucky")

try:
    db.create_table(
        "users",
        [
            Column(int, name="id", primary_key=True),
            Column(str, name="name", index=True),
            Column(int, name="age"),
        ],
    )

    alice_id = db.insert("users", {"name": "Alice", "age": 20})
    bob_id = db.insert("users", {"name": "Bob", "age": 24})

    print(alice_id)                   # 1
    print(db.select("users", bob_id))

    db.flush()
finally:
    db.close()
```

### 2. `Session` + `PureBaseModel`

```python
from typing import Type

from pytucky import Column, PureBaseModel, Session, Storage
from pytucky import declarative_base, insert, select


db = Storage(file_path="orm-demo.pytucky")
Base: Type[PureBaseModel] = declarative_base(db)


class User(Base):
    __tablename__ = "users"
    id = Column(int, primary_key=True)
    name = Column(str, nullable=False, index=True)
    age = Column(int)


session = Session(db)
try:
    session.execute(insert(User).values(name="Alice", age=20))
    session.commit()

    rows = session.execute(select(User).filter_by(id=1)).all()
    print([row.name for row in rows])  # ['Alice']
finally:
    session.close()
    db.close()
```

### 3. `CRUDBaseModel`（Active Record）

```python
from typing import Type

from pytucky import Column, CRUDBaseModel, Storage, declarative_base


db = Storage(file_path="crud-demo.pytucky")
Base: Type[CRUDBaseModel] = declarative_base(db, crud=True)


class User(Base):
    __tablename__ = "users"
    id = Column(int, primary_key=True)
    name = Column(str, nullable=False)


try:
    user = User.create(name="Alice")
    user.name = "Bob"
    user.save()

    loaded = User.get(1)
    print(loaded.name)  # Bob
finally:
    db.close()
```

## 性能快照

以下数字来自当前仓库 2026-04-11 的 benchmark 快照（100000 条记录）：

| 指标 | 结果 |
|------|------|
| `insert` | 646.36ms |
| `save` | 492.17ms |
| `query_pk` | 1.45ms / 100 次 |
| `query_indexed` | 1.42ms / 100 次 |
| `load` | 140.72ms |
| `reopen` | 139.03ms |
| `reopen_first_query` | 77.91μs |
| `file_size` | 5.98MB |

相对于 `pytuck` 文档中的 100000 条基线，当前 PTK7 快照大致表现为：

- 插入快约 **22.5%**
- 索引查询快约 **24.7%**
- 保存快约 **12.4%**
- 加载快约 **58.3%**
- reopen 快约 **56.8%**
- 文件体积小约 **1.8%**

更完整的说明和复现命令见：[`docs/guide/benchmark.md`](docs/guide/benchmark.md)

## 迁移

### 从 `.pytuck` 迁移到 `.pytucky`

```python
from pytucky import migrate_pytuck_to_pytucky

migrate_pytuck_to_pytucky("old-data.pytuck", "new-data.pytucky")
```

也可以使用别名：

```python
from pytucky import migrate_p5_to_p7

migrate_p5_to_p7("old-data.pytuck", "new-data.pytucky")
```

## 使用约束与建议

### 1. `flush()` 与 `close()`

- `db.flush()`：把当前内存中的变更写回磁盘
- `db.close()`：在关闭前会先做 `flush()`，并释放底层 backend 资源

建议：**无论是脚本、测试还是服务中的短生命周期实例，都显式调用 `close()`**。

### 2. 默认就是按需读取

PTK7 打开文件时只恢复：

- 文件头
- schema catalog
- 每表 PK 目录
- 索引元数据

记录内容不会在打开时全部解码进内存，因此不再需要用户配置 `lazy_load`。

### 3. 当前不保证共享实例的线程安全

当前 `Storage` / `Store` 主要面向单实例、单写者、显式生命周期管理的嵌入式使用场景。若你要在多线程或多进程里共享同一个实例或同一个数据库文件，请在外部自己做同步控制。

### 4. 这是单格式项目，不再维护多引擎分发

Pytucky 的目标不是做一个“大而全的多后端 ORM”。当前主线就是：

- 一个单文件格式（PTK7）
- 一套轻量 ORM 用法
- 尽量把主要性能压在读写主路径上

## 测试

运行全部测试：

```bash
uv run pytest tests/ -v
```

当前仓库这轮验证结果：

- **61 passed**

## 运行 benchmark

```bash
uv run python tests/benchmark/benchmark.py -n 1000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-1000.json
uv run python tests/benchmark/benchmark.py -n 100000 --extended --output-json tests/benchmark/benchmark_output/pytucky-v7-100000.json
```

## 项目目标

Pytucky 的方向很明确：

1. **纯 Python**
2. **零第三方运行时依赖**
3. **单文件**
4. **性能优先**
5. **尽量保留基础 ORM 用法**

如果某个历史实现、兼容层或复杂配置会妨碍这几个目标，那么它就不应该留在主路径里。
