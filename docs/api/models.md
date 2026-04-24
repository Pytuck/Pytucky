# 模型定义

## Column

列定义描述符。用于在模型类中声明字段结构。

```python
from pytucky import Column
```

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `col_type` | type | 必填 | Python 类型（见支持的类型） |
| `name` | str \| None | None | 列名（默认取变量名） |
| `nullable` | bool | True | 是否可空 |
| `primary_key` | bool | False | 是否为主键 |
| `index` | bool \| str | False | 索引设置：False / True / `'hash'` / `'sorted'` |
| `default` | Any | None | 静态默认值 |
| `default_factory` | callable \| None | None | 默认值工厂函数（与 default 互斥） |
| `foreign_key` | tuple \| None | None | 外键关系 `(table_name, column_name)` |
| `comment` | str \| None | None | 列备注 |
| `strict` | bool | False | 严格模式（不进行类型转换） |
| `validator` | callable \| list | None | 自定义校验函数或列表 |

### 支持的类型

| Python 类型 | 说明 |
|-------------|------|
| `int` | 整数 |
| `float` | 浮点数 |
| `str` | 字符串 |
| `bool` | 布尔值 |
| `bytes` | 字节串 |
| `datetime` | 日期时间 |
| `date` | 日期 |
| `timedelta` | 时间间隔 |
| `list` | 列表（JSON 序列化） |
| `dict` | 字典（JSON 序列化） |

### 索引类型

- `False`：不建索引
- `True` 或 `'hash'`：哈希索引，等值查询 O(1)
- `'sorted'`：有序索引，支持范围查询和排序

### 示例

```python
from datetime import datetime
from pytucky import Column

id = Column(int, primary_key=True)
name = Column(str, nullable=False, index=True)
age = Column(int, nullable=True)
score = Column(float, index='sorted')
created_at = Column(datetime, default_factory=datetime.now)
email = Column(str, name='user_email')  # 列名与变量名不同
status = Column(str, strict=True)  # 严格模式，不做类型转换
level = Column(int, validator=lambda x: 1 <= x <= 100)
```

### 查询表达式

Column 支持通过魔术方法构建查询表达式：

```python
User.age == 20         # 等于
User.age != 20         # 不等于
User.age > 18          # 大于
User.age >= 18         # 大于等于
User.age < 30          # 小于
User.age <= 30         # 小于等于
User.age.in_([18, 19]) # IN
User.name.contains('li')     # 包含（大小写不敏感）
User.name.startswith('Al')   # 前缀匹配
User.name.endswith('ce')     # 后缀匹配
```

---

## declarative_base

声明式基类工厂函数。返回绑定到指定 Storage 的基类。

```python
from pytucky import declarative_base
```

### 签名

```python
def declarative_base(
    storage: Storage,
    *,
    crud: bool = False,
    sync_schema: bool = False,
    sync_options: SyncOptions | None = None
) -> type[PureBaseModel] | type[CRUDBaseModel]
```

### 参数

| 参数 | 说明 |
|------|------|
| `storage` | Storage 实例 |
| `crud` | False → PureBaseModel（通过 Session 操作）；True → CRUDBaseModel（Active Record） |
| `sync_schema` | 表已存在时是否自动同步 schema |
| `sync_options` | 同步选项（仅 sync_schema=True 时生效） |

### 示例

```python
from pytucky import Storage, Column, PureBaseModel, CRUDBaseModel, declarative_base

db = Storage(file_path='mydb.pytuck')

# 纯模型
Base: type[PureBaseModel] = declarative_base(db)

# Active Record
Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

# 自动同步 schema
Base = declarative_base(db, sync_schema=True)
```

---

## PureBaseModel

纯模型基类。定义数据结构，通过 Session 进行所有数据库操作。

### 类属性

| 属性 | 说明 |
|------|------|
| `__tablename__` | 表名（必须定义） |
| `__table_comment__` | 表备注（可选） |
| `__abstract__` | 是否为抽象类（设为 True 则不创建表） |
| `__columns__` | 列定义字典（自动收集） |
| `__primary_key__` | 主键名（自动识别） |
| `__relationships__` | 关系定义字典（自动收集） |

### 实例方法

#### `to_dict()`

```python
def to_dict(
    self,
    use_column_names: bool = False,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
    depth: int = 0,
) -> dict[str, Any]
```

转换为字典。`depth > 0` 时展开 Relationship。

#### `to_json()`

```python
def to_json(
    self,
    use_column_names: bool = False,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
    depth: int = 0,
    ensure_ascii: bool = False,
    indent: int | None = None,
) -> str
```

转换为 JSON 字符串。自动处理 datetime/date/timedelta/bytes 等类型。

### 示例

```python
class User(Base):
    __tablename__ = 'users'
    id = Column(int, primary_key=True)
    name = Column(str, nullable=False)
    age = Column(int)

user = User(name='Alice', age=20)
isinstance(user, PureBaseModel)  # True
user.to_dict()  # {'id': None, 'name': 'Alice', 'age': 20}
```

### 无主键模型

不定义 `primary_key=True` 的列时，系统自动使用内部 rowid：

```python
class Log(Base):
    __tablename__ = 'logs'
    message = Column(str)
    level = Column(str)
```

### 模型继承

```python
class TimestampMixin(Base):
    __abstract__ = True
    created_at = Column(datetime, default_factory=datetime.now)
    updated_at = Column(datetime, default_factory=datetime.now)

class User(TimestampMixin):
    __tablename__ = 'users'
    id = Column(int, primary_key=True)
    name = Column(str)
```

---

## CRUDBaseModel

Active Record 基类。模型自带 CRUD 方法，继承自 PureBaseModel。

### 实例方法

| 方法 | 说明 |
|------|------|
| `save()` | 保存（自动判断 INSERT 或 UPDATE） |
| `delete()` | 删除当前记录 |
| `refresh()` | 从数据库刷新当前实例 |

### 类方法

| 方法 | 说明 |
|------|------|
| `create(**kwargs)` | 创建并保存新记录 |
| `get(pk)` | 根据主键获取记录 |
| `filter(*expressions)` | 条件查询（返回 Query） |
| `filter_by(**kwargs)` | 等值查询（返回 Query） |
| `all()` | 获取所有记录 |
| `bulk_insert(instances)` | 批量插入 |
| `bulk_update(instances)` | 批量更新 |

### 示例

```python
Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

class User(Base):
    __tablename__ = 'users'
    id = Column(int, primary_key=True)
    name = Column(str, nullable=False)

# 创建
user = User.create(name='Alice')

# 更新
user.name = 'Bob'
user.save()

# 查询
loaded = User.get(1)
users = User.filter(User.name == 'Alice').all()
users = User.filter_by(name='Alice').all()
all_users = User.all()

# 删除
user.delete()

# 批量操作
users = [User(name=f'user_{i}') for i in range(100)]
pks = User.bulk_insert(users)
```

---

## Relationship

关联关系描述符。支持一对多、多对一，以及显式跨 `Storage` 的延迟读取。

```python
from pytucky import Relationship
```

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `target_model` | str \| Type | 必填 | 目标模型类，或目标表名字符串 |
| `foreign_key` | str | 必填 | 外键字段名 |
| `storage` | Storage \| None | None | 仅在字符串目标跨库时需要；省略时默认使用当前模型同库的 `Storage` |
| `back_populates` | str \| None | None | 反向关联属性名；启用双向定义校验与反向缓存回填 |
| `uselist` | bool \| None | None | 强制列表/单个（None 自动判断） |

### 示例

```python
class Order(Base):
    __tablename__ = 'orders'
    id = Column(int, primary_key=True)
    user_id = Column(int, foreign_key=('users', 'id'))
    amount = Column(float)
    # 多对一
    user: User | None = Relationship('users', foreign_key='user_id')  # type: ignore

class User(Base):
    __tablename__ = 'users'
    id = Column(int, primary_key=True)
    name = Column(str)
    # 一对多
    orders: list[Order] = Relationship('orders', foreign_key='user_id')  # type: ignore
```

### 跨 Storage 示例

```python
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
    base_item: BaseItem | None = Relationship(
        "base_items",
        foreign_key="base_item_id",
        storage=base_db,
    )  # type: ignore
```

这里 `UserItem` 仍然写入 `user_db`，但 `user_item.base_item` 会从 `base_db` 读取。

### 注意事项

- `Relationship` 仍然默认惰性加载，不再提供 `lazy` 参数
- 字符串目标只表示**表名**，不再回退为类名字符串解析；若要按类绑定，请直接传模型类
- 字符串目标省略 `storage` 时默认同库；跨库时再显式传 `storage=...`
- 配置 `back_populates` 后，会校验双向定义是否对称，并在懒加载 / `prefetch()` 后自动回填反向缓存
- 跨 `Storage` relationship 仅支持读取与 `prefetch()` 预取
- 仍然不支持 join

### 判断规则

- 外键在**当前模型**中 → 多对一（返回单个对象）
- 外键在**目标模型**中 → 一对多（返回列表）
- 自引用关系需显式指定 `uselist`

---

## SyncOptions / SyncResult

Schema 同步选项和结果。

```python
from pytucky import SyncOptions, SyncResult
```

### SyncOptions

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `add_new_columns` | True | 添加模型中新增的列 |
| `drop_missing_columns` | False | 删除模型中不存在的列（危险） |
| `sync_table_comment` | True | 同步表备注 |
| `sync_column_comments` | True | 同步列备注 |

### SyncResult

| 属性 | 说明 |
|------|------|
| `table_name` | 表名 |
| `columns_added` | 新增的列名列表 |
| `columns_dropped` | 删除的列名列表 |
| `column_comments_updated` | 更新了备注的列名列表 |
| `table_comment_updated` | 表备注是否更新 |
| `has_changes` | 是否有变更 |
