# Pytucky

<div align="center">
  <img src="https://raw.githubusercontent.com/Pytuck/Pytucky/main/logo.svg" width="120" alt="Pytucky logo">
</div>

[![Gitee](https://img.shields.io/badge/Gitee-Pytuck%2FPytucky-red)](https://gitee.com/Pytuck/Pytucky)
[![GitHub](https://img.shields.io/badge/GitHub-Pytuck%2FPytucky-blue)](https://github.com/Pytuck/Pytucky)

[![PyPI version](https://badge.fury.io/py/pytucky.svg)](https://badge.fury.io/py/pytucky)
[![Python Versions](https://img.shields.io/pypi/pyversions/pytucky.svg)](https://pypi.org/project/pytucky/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pytucky 是一个**纯 Python、零第三方运行时依赖、单文件**的嵌入式文档数据库。

基于 [Pytuck](https://github.com/go9sky/pytuck) 的核心 ORM API（`Column`、`declarative_base`、`Session`、`select/insert/update/delete`），收敛为 **PTK7 单引擎**实现。专为受限 Python 环境（如 Ren'Py）设计，提供类似 SQLAlchemy 的声明式 ORM 体验。

|  |  |
|--|--|
| 格式 | **PTK7** / 默认 `.pytuck`（显式 `.pytucky` 兼容） |
| 运行时依赖 | **无** |
| Python | **>= 3.10** |
| 许可证 | **MIT** |

## 核心特性

- **纯 Python**：适合受限运行环境（如 Ren'Py）
- **零第三方运行时依赖**：安装简单，部署轻量
- **单文件数据库**：一个 `.pytuck` 文件即可承载完整数据（显式 `.pytucky` 仍兼容）
- **类似 SQLAlchemy 的 ORM**：声明式模型、Session、Statement API
- **跨 Storage relationship / prefetch**：支持把基础库与用户库通过 relationship 关联起来
- **与 pytuck 共享 PTK7 格式**：已验证 `None / low / medium / high` 四档可双向互读互写
- **默认按需读取**：打开时只加载目录和索引元数据，记录按需解码
- **主键直达读取**：`pk -> (offset, length)` 直接定位记录
- **索引物化缓存**：首次查询时物化，后续零解码开销
- **增量 flush**：只写入有变更的表

## 安装

```bash
pip install pytucky
```

## 开发与发布

更完整的开发、测试与发布说明见 `docs/guide/development.md`。

如果你是克隆仓库后准备参与开发，不要使用 editable install 方式把项目本身装进当前环境，而是直接同步项目开发环境：

```bash
git clone <repo-url>
cd pytucky
uv sync --extra dev
```

## 快速开始

### 1. 直接使用 Storage

```python
from pytucky import Storage, Column

db = Storage(file_path="demo.pytuck")
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
from pytucky import Column, PureBaseModel, Session, Storage
from pytucky import declarative_base, insert, select

db = Storage(file_path="orm-demo.pytuck")
Base: type[PureBaseModel] = declarative_base(db)

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
from pytucky import Column, CRUDBaseModel, Storage, declarative_base

db = Storage(file_path="crud-demo.pytuck")
Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

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

### 4. 跨 Storage relationship

```python
from pytucky import Column, Relationship, Storage, declarative_base

base_db = Storage(file_path="base.pytuck")
user_db = Storage(file_path="user.pytuck")

BaseBase = declarative_base(base_db, crud=True)
BaseUser = declarative_base(user_db, crud=True)

class BaseItem(BaseBase):
    __tablename__ = "base_items"
    id = Column(int, primary_key=True)
    name = Column(str)

class UserItem(BaseUser):
    __tablename__ = "user_items"
    id = Column(int, primary_key=True)
    base_item_id = Column(int)
    nickname = Column(str)
    base_item = Relationship(
        "base_items",
        foreign_key="base_item_id",
        storage=base_db,
    )  # type: ignore

try:
    sword = BaseItem.create(name="Sword")
    starter = UserItem.create(base_item_id=sword.id, nickname="starter")
    print(starter.base_item.name)  # Sword
finally:
    user_db.close()
    base_db.close()
```

说明：
- `Relationship("base_items", ...)` 里的字符串表示**表名**
- 字符串目标省略 `storage` 时默认同库；跨库读取时再显式传 `storage=...`
- 配置 `back_populates` 后，懒加载和 `prefetch()` 都会自动回填反向缓存

## 性能（10,000 条记录）

当前基准环境：Darwin 25.4.0 / Python 3.13.11。对 `pytucky 1.1.2` 与 `pytuck 1.3.0` 使用**相同 schema、相同数据量、相同测试流程**连续运行 **3 轮均值**，唯一变量是库实现：

| 指标 | Pytucky 1.1.2 | Pytuck 1.3.0 | 变化 |
|------|---------------|--------------|------|
| insert | 35.2ms | 30.1ms | +17.2% |
| save | 25.2ms | 22.2ms | +13.1% |
| query_pk (×100) | 0.75ms | 0.69ms | +9.4% |
| query_indexed (×100) | 0.70ms | 0.64ms | +8.8% |
| load | 4.71ms | 4.74ms | -0.6% |
| reopen | 4.77ms | 4.72ms | +1.0% |
| reopen_first_query | 32.6μs | 35.6μs | -8.2% |
| file_size | 0.92MB | 0.92MB | 0% |

两个库共享 PTK7 格式。更完整的基准说明见 [docs/guide/benchmark.md](docs/guide/benchmark.md)。

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

当前：**204 passed**

## 项目目标

1. **纯 Python**
2. **零第三方运行时依赖**
3. **单文件**
4. **性能优先**
5. **保留基础 ORM 用法**

## 许可证

MIT
