# Pytucky

Pytucky 是一个**纯 Python、零第三方运行时依赖、单文件**的嵌入式文档数据库。

基于 [Pytuck](https://github.com/go9sky/pytuck) 的核心 ORM API（`Column`、`declarative_base`、`Session`、`select/insert/update/delete`），收敛为 **PTK7 单引擎**实现。专为受限 Python 环境（如 Ren'Py）设计，提供类似 SQLAlchemy 的声明式 ORM 体验。

|  |  |
|--|--|
| 格式 | **PTK7** / `.pytucky` |
| 运行时依赖 | **无** |
| Python | **>= 3.7** |
| 版本 | **1.0.0** |
| 许可证 | **MIT** |

## 核心特性

- **纯 Python**：适合受限运行环境（如 Ren'Py）
- **零第三方运行时依赖**：安装简单，部署轻量
- **单文件数据库**：一个 `.pytucky` 文件即可承载完整数据
- **类似 SQLAlchemy 的 ORM**：声明式模型、Session、Statement API
- **默认按需读取**：打开时只加载目录和索引元数据，记录按需解码
- **主键直达读取**：`pk -> (offset, length)` 直接定位记录
- **索引物化缓存**：首次查询时物化，后续零解码开销
- **增量 flush**：只写入有变更的表

## 安装

```bash
pip install pytucky
```

### 开发安装

```bash
uv pip install -e ".[dev]"
```

## 快速开始

### 1. 直接使用 Storage

```python
from pytucky import Storage, Column

db = Storage(file_path="demo.pytucky")
try:
    db.create_table("users", [
        Column(int, name="id", primary_key=True),
        Column(str, name="name", index=True),
        Column(int, name="age"),
    ])

    alice_id = db.insert("users", {"name": "Alice", "age": 20})
    bob_id = db.insert("users", {"name": "Bob", "age": 24})

    print(db.select("users", bob_id))
    db.flush()
finally:
    db.close()
```

### 2. Session + PureBaseModel

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

    rows = session.execute(select(User).where(User.age >= 18)).all()
    print([row.name for row in rows])  # ['Alice']
finally:
    session.close()
    db.close()
```

### 3. CRUDBaseModel（Active Record）

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

## 性能（100,000 条记录）

与 Pytuck 1.2.1 同机对比（Linux / Python 3.12.3）：

| 指标 | Pytucky 1.0.0 | Pytuck 1.2.1 | 变化 |
|------|---------------|--------------|------|
| insert | 628.03ms | 808.43ms | **-22%** |
| save | 463.71ms | 609.01ms | **-24%** |
| query_pk (×100) | 1.51ms | 1.63ms | **-7%** |
| query_indexed (×100) | 1.52ms | 1.81ms | **-16%** |
| load | 137.92ms | 126.26ms | +9% |
| reopen | 129.22ms | 128.92ms | ≈ |
| file_size | 5.98MB | 9.51MB | **-37%** |

详见 [docs/guide/benchmark.md](docs/guide/benchmark.md)。

## 文档

| 文档 | 内容 |
|------|------|
| **API 参考** | |
| [docs/api/index.md](docs/api/index.md) | API 总览、导入参考 |
| [docs/api/models.md](docs/api/models.md) | Column、Model、declarative_base、Relationship |
| [docs/api/storage.md](docs/api/storage.md) | Storage CRUD、表管理、事务、持久化 |
| [docs/api/session.md](docs/api/session.md) | Session 对象管理、批量操作、Schema 操作 |
| [docs/api/query.md](docs/api/query.md) | select/insert/update/delete、Result、逻辑操作符 |
| [docs/api/exceptions.md](docs/api/exceptions.md) | 异常层次与触发场景 |
| [docs/api/best-practices.md](docs/api/best-practices.md) | 持久化策略、性能优化、使用约束 |
| **指南** | |
| [docs/guide/benchmark.md](docs/guide/benchmark.md) | 性能基准报告 |
| [docs/guide/development.md](docs/guide/development.md) | 开发指南 |
| [CHANGELOG.md](CHANGELOG.md) | 版本记录 |

## 从 pytuck 迁移

基础用法只需更改 import：

```python
# 之前
from pytuck import Storage, declarative_base, Session, Column

# 之后
from pytucky import Storage, declarative_base, Session, Column
```

详见 [docs/api/best-practices.md](docs/api/best-practices.md#从-pytuck-迁移)。

## 测试

```bash
uv run pytest tests/ -v
```

当前：**65 passed**

## 项目目标

1. **纯 Python**
2. **零第三方运行时依赖**
3. **单文件**
4. **性能优先**
5. **保留基础 ORM 用法**

## 许可证

MIT
