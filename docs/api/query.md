# 查询

## Statement API（推荐）

SQLAlchemy 2.0 风格的语句构建器，通过 `session.execute()` 执行。

```python
from pytucky import select, insert, update, delete
```

### select

```python
stmt = select(User)
stmt = select(User).where(User.age >= 18)
stmt = select(User).where(User.age >= 18, User.name != 'admin')
stmt = select(User).order_by('age', desc=True)
stmt = select(User).limit(10).offset(20)
```

#### 方法链

| 方法 | 说明 |
|------|------|
| `.where(*expressions)` | 添加 WHERE 条件 |
| `.filter_by(**kwargs)` | 等值查询简写 |
| `.order_by(field, desc=False)` | 排序（可多次调用设置多列排序） |
| `.limit(n)` | 限制返回数量 |
| `.offset(n)` | 跳过记录数 |
| `.options(*opts)` | 查询选项（如 prefetch） |

#### 执行结果：Result

```python
result = session.execute(select(User).where(User.age >= 18))

users = result.all()         # list[User]
user = result.first()        # User | None
user = result.one()          # User（无结果或多结果抛异常）
user = result.one_or_none()  # User | None（多结果抛异常）
count = result.count()       # int
```

### insert

```python
# 单条插入
stmt = insert(User).values(name='Alice', age=20)
result = session.execute(stmt)
session.commit()

# 批量插入
stmt = insert(User).values_list([
    {'name': 'Alice', 'age': 20},
    {'name': 'Bob', 'age': 25},
])
result = session.execute(stmt)
session.commit()
```

#### 执行结果：CursorResult

```python
result.rowcount       # 影响的行数
result.inserted_pk    # 插入的主键（仅 insert）
```

### update

```python
stmt = update(User).where(User.id == 1).set(age=21)
result = session.execute(stmt)
session.commit()
```

### delete

```python
stmt = delete(User).where(User.age < 18)
result = session.execute(stmt)
session.commit()
```

---

## 逻辑组合

```python
from pytucky import or_, and_, not_
```

### or_

```python
stmt = select(User).where(or_(User.age < 18, User.age > 60))
```

### and_

```python
stmt = select(User).where(and_(User.age >= 18, User.name != 'admin'))
```

### not_

```python
stmt = select(User).where(not_(User.name == 'admin'))
```

### 组合嵌套

```python
stmt = select(User).where(
    or_(
        and_(User.age >= 18, User.name.startswith('A')),
        User.vip == True
    )
)
```

---

## Query API（旧风格）

通过 `session.query()` 或 `CRUDBaseModel.filter()` 使用。不推荐，但会持续支持。

```python
# 通过 Session
users = session.query(User).filter(User.age >= 18).all()

# 通过 CRUDBaseModel
users = User.filter(User.age >= 18).all()
users = User.filter_by(name='Alice').all()
```

### Query 方法

| 方法 | 说明 |
|------|------|
| `.filter(*expressions)` | 表达式条件 |
| `.filter_by(**kwargs)` | 等值条件 |
| `.order_by(field, desc=False)` | 排序 |
| `.limit(n)` | 限制数量 |
| `.offset(n)` | 跳过记录 |
| `.all()` | 返回所有结果 |
| `.first()` | 返回第一条 |
| `.one()` | 返回唯一一条 |
| `.count()` | 返回数量 |

---

## Result / CursorResult

```python
from pytucky import Result, CursorResult
```

### Result（SELECT 结果）

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `all()` | `list[T]` | 所有记录 |
| `first()` | `T | None` | 第一条 |
| `one()` | `T` | 唯一一条（否则抛异常） |
| `one_or_none()` | `T | None` | 唯一一条或 None |
| `count()` | `int` | 记录数 |

### CursorResult（INSERT/UPDATE/DELETE 结果）

| 属性 | 类型 | 说明 |
|------|------|------|
| `rowcount` | `int` | 影响的行数 |
| `inserted_pk` | `Any` | 插入的主键（仅 INSERT） |

---

## 索引优化

查询引擎自动利用索引优化：

- **等值查询**（`==`）：使用 HashIndex / SortedIndex，O(1)
- **范围查询**（`>`, `>=`, `<`, `<=`）：使用 SortedIndex
- **排序**：使用 SortedIndex 时可避免内存排序

多个索引条件会取交集缩小候选集。

---

## 关系预取

```python
from pytucky import prefetch, select

stmt = select(User).options(prefetch(User.orders))
result = session.execute(stmt)
users = result.all()
# users[0].orders 已预加载，不会触发延迟查询
```
