"""
Pytuck 查询构建器

提供链式查询API
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Generic, TYPE_CHECKING, Union

from ..common.typing import T
from ..common.exceptions import QueryError
from ..core.orm import PSEUDO_PK_NAME

if TYPE_CHECKING:
    from ..core.orm import Column
    from ..core.storage import Storage


_OPERATOR_EVAL: Dict[str, Callable[[Any, Any], bool]] = {
    '=': lambda x, y: x == y,
    '>': lambda x, y: x > y,
    '<': lambda x, y: x < y,
    '>=': lambda x, y: x >= y,
    '<=': lambda x, y: x <= y,
    '!=': lambda x, y: x != y,
    'IN': lambda x, y: x in y,
    'LIKE': lambda x, y: isinstance(x, str) and isinstance(y, str) and y.lower() in x.lower(),
    'STARTSWITH': lambda x, y: isinstance(x, str) and isinstance(y, str) and x.lower().startswith(y.lower()),
    'ENDSWITH': lambda x, y: isinstance(x, str) and isinstance(y, str) and x.lower().endswith(y.lower()),
}


class Condition:
    """查询条件"""

    def __init__(self, field: str, operator: str, value: Any):
        """
        初始化条件

        Args:
            field: 字段名
            operator: 操作符 ('=', '>', '<', '>=', '<=', '!=', 'IN', 'LIKE', 'STARTSWITH', 'ENDSWITH')
            value: 比较值

        Raises:
            QueryError: 如果操作符不被支持
        """
        if operator not in _OPERATOR_EVAL:
            raise QueryError(f"Unsupported operator: {operator}")
        self.field = field
        self.operator = operator
        self.value = value

    def evaluate(self, record: dict) -> bool:
        """
        评估条件是否满足

        Args:
            record: 记录字典

        Returns:
            条件是否满足
        """
        if self.field not in record:
            return False
        field_value = record[self.field]
        return bool(_OPERATOR_EVAL[self.operator](field_value, self.value))

    def __repr__(self) -> str:
        return f"Condition({self.field} {self.operator} {self.value})"


class BinaryExpression:
    """
    二元表达式：表示 Column 和值之间的比较操作

    由 Column 的魔术方法返回，用于构建查询条件。
    例如：Student.age >= 18 会创建 BinaryExpression(Student.age, '>=', 18)
    """

    def __init__(self, column: 'Column', operator: str, value: Any):
        """
        初始化二元表达式

        Args:
            column: Column 对象
            operator: 操作符 ('=', '>', '<', '>=', '<=', '!=', 'IN', 'LIKE', 'STARTSWITH', 'ENDSWITH')
            value: 比较值
        """
        self.column = column
        self.operator = operator
        self.value = value

    def to_condition(self) -> Condition:
        """转换为 Condition 对象"""
        assert self.column.name is not None, "Column name must be set"
        return Condition(self.column.name, self.operator, self.value)

    def __repr__(self) -> str:
        return f"BinaryExpression({self.column.name} {self.operator} {self.value})"


# 表达式类型：BinaryExpression 或 LogicalExpression
ExpressionType = Union['BinaryExpression', 'LogicalExpression']


class CompositeCondition:
    """
    组合条件：用于 AND/OR/NOT 逻辑评估

    支持递归嵌套，可以表示任意复杂的布尔逻辑组合。
    """

    def __init__(self, operator: str, conditions: List[Union[Condition, 'CompositeCondition']]):
        """
        初始化组合条件

        Args:
            operator: 逻辑操作符 ('AND' | 'OR' | 'NOT')
            conditions: 子条件列表（NOT 时只有一个元素）
        """
        self.operator = operator
        self.conditions = conditions

    def evaluate(self, record: dict) -> bool:
        """
        递归评估条件是否满足

        Args:
            record: 记录字典

        Returns:
            条件是否满足
        """
        if self.operator == 'AND':
            return all(cond.evaluate(record) for cond in self.conditions)
        elif self.operator == 'OR':
            return any(cond.evaluate(record) for cond in self.conditions)
        elif self.operator == 'NOT':
            return not self.conditions[0].evaluate(record)
        else:
            raise QueryError(f"Unsupported logical operator: {self.operator}")

    def __repr__(self) -> str:
        if self.operator == 'NOT':
            return f"NOT({self.conditions[0]})"
        sep = f" {self.operator} "
        return f"({sep.join(repr(c) for c in self.conditions)})"


class LogicalExpression:
    """
    逻辑组合表达式：表示 AND/OR/NOT 组合

    由 or_(), and_(), not_() 函数创建，用于构建复杂查询条件。

    Example:
        # OR 组合
        or_(User.age >= 18, User.vip == True)

        # AND 组合
        and_(User.active == True, User.verified == True)

        # NOT 取反
        not_(User.banned == True)

        # 嵌套组合
        or_(User.role == 'admin', and_(User.age >= 21, User.verified == True))
    """

    def __init__(self, operator: str, expressions: List[ExpressionType]):
        """
        初始化逻辑表达式

        Args:
            operator: 逻辑操作符 ('AND' | 'OR' | 'NOT')
            expressions: 子表达式列表（NOT 时只有一个元素）
        """
        self.operator = operator
        self.expressions = expressions

    def to_condition(self) -> CompositeCondition:
        """
        转换为 CompositeCondition 对象

        递归转换所有子表达式。

        Returns:
            CompositeCondition 对象
        """
        conditions: List[Union[Condition, CompositeCondition]] = []
        for expr in self.expressions:
            if isinstance(expr, BinaryExpression):
                conditions.append(expr.to_condition())
            elif isinstance(expr, LogicalExpression):
                conditions.append(expr.to_condition())
            else:
                raise QueryError(f"Unexpected expression type: {type(expr).__name__}")
        return CompositeCondition(self.operator, conditions)

    def __repr__(self) -> str:
        if self.operator == 'NOT':
            return f"not_({self.expressions[0]})"
        func_name = 'or_' if self.operator == 'OR' else 'and_'
        args = ', '.join(repr(e) for e in self.expressions)
        return f"{func_name}({args})"


def or_(*expressions: ExpressionType) -> LogicalExpression:
    """
    创建 OR 组合表达式

    将多个条件以 OR 逻辑组合，只要其中一个条件满足即返回 True。

    Args:
        *expressions: 要进行 OR 组合的表达式（至少 2 个）

    Returns:
        LogicalExpression 对象

    Raises:
        QueryError: 如果表达式少于 2 个

    Example:
        # 查询 age >= 18 或者 vip == True 的用户
        stmt = select(User).where(or_(User.age >= 18, User.vip == True))

        # 多个条件的 OR
        stmt = select(User).where(or_(
            User.role == 'admin',
            User.role == 'moderator',
            User.role == 'editor'
        ))
    """
    if len(expressions) < 2:
        raise QueryError("or_() requires at least 2 expressions")
    return LogicalExpression('OR', list(expressions))


def and_(*expressions: ExpressionType) -> LogicalExpression:
    """
    创建 AND 组合表达式

    将多个条件以 AND 逻辑组合，所有条件都必须满足才返回 True。

    注意：多参数 where() 调用默认就是 AND 语义，此函数主要用于与 or_() 嵌套组合。

    Args:
        *expressions: 要进行 AND 组合的表达式（至少 2 个）

    Returns:
        LogicalExpression 对象

    Raises:
        QueryError: 如果表达式少于 2 个

    Example:
        # 与 or_() 嵌套使用
        stmt = select(User).where(or_(
            User.role == 'admin',
            and_(User.age >= 21, User.verified == True)
        ))
    """
    if len(expressions) < 2:
        raise QueryError("and_() requires at least 2 expressions")
    return LogicalExpression('AND', list(expressions))


def not_(expression: ExpressionType) -> LogicalExpression:
    """
    创建 NOT 表达式

    对条件取反，条件不满足时返回 True。

    Args:
        expression: 要取反的表达式

    Returns:
        LogicalExpression 对象

    Example:
        # 查询未被封禁的用户
        stmt = select(User).where(not_(User.banned == True))

        # 与 or_() 组合
        stmt = select(User).where(not_(or_(User.banned == True, User.deleted == True)))
    """
    return LogicalExpression('NOT', [expression])


# 条件类型：Condition 或 CompositeCondition
ConditionType = Union[Condition, CompositeCondition]


class Query(Generic[T]):
    """查询构建器（支持链式调用）"""

    def __init__(self, model_class: Type[T], storage: Optional['Storage'] = None) -> None:
        """
        初始化查询构建器

        Args:
            model_class: 模型类
            storage: Storage 实例（新 API 需要，旧 API 兼容）
        """
        self.model_class = model_class
        self.storage = storage  # 新 API：通过参数传入
        self._conditions: List[ConditionType] = []
        self._order_by_fields: List[Tuple[str, bool]] = []  # [(field, desc), ...]
        self._limit_value: Optional[int] = None
        self._offset_value: int = 0

    def filter(self, *expressions: ExpressionType) -> 'Query[T]':
        """
        添加过滤条件（支持表达式和逻辑组合）

        用法：
            query.filter(Student.age >= 20, Student.name == 'Alice')
            query.filter(or_(Student.age >= 20, Student.vip == True))

        Args:
            *expressions: BinaryExpression 或 LogicalExpression 对象

        Returns:
            Query 对象（链式调用）

        Example:
            # 单条件
            query.filter(Student.age >= 20)

            # 多条件（AND）
            query.filter(Student.age >= 20, Student.name == 'Alice')

            # OR 条件
            query.filter(or_(Student.age >= 20, Student.vip == True))

            # 链式调用
            query.filter(Student.age >= 20).filter(Student.score > 85).all()
        """
        for expr in expressions:
            if isinstance(expr, BinaryExpression):
                self._conditions.append(expr.to_condition())
            elif isinstance(expr, LogicalExpression):
                self._conditions.append(expr.to_condition())
            else:
                raise QueryError(
                    f"Expected BinaryExpression or LogicalExpression, got {type(expr).__name__}. "
                    f"Use Model.column >= value syntax or or_(), and_(), not_() functions."
                )

        return self

    def filter_by(self, **kwargs) -> 'Query[T]':
        """
        添加过滤条件（简单等值查询，SQLAlchemy 风格）

        用于简单的等值匹配，语法更简洁。对于复杂查询，使用 filter() 配合表达式。

        Args:
            **kwargs: 字段名=值 的等值条件

        Returns:
            Query 对象（链式调用）

        Example:
            # 简单等值查询（推荐 filter_by）
            query.filter_by(name='Bob', active=True)

            # 复杂表达式查询（使用 filter）
            query.filter(Student.age >= 20, Student.name != 'Alice')
        """
        for attr_name, value in kwargs.items():
            # 将属性名转换为 Column.name（存储列名）
            if hasattr(self.model_class, '__columns__') and attr_name in self.model_class.__columns__:
                column = self.model_class.__columns__[attr_name]
                db_col_name = column.name if column.name else attr_name
            else:
                db_col_name = attr_name
            # 创建等值条件
            condition = Condition(db_col_name, '=', value)
            self._conditions.append(condition)
        return self

    def order_by(self, field: str, desc: bool = False) -> 'Query[T]':
        """
        添加排序字段

        支持多列排序，多次调用时按调用顺序确定排序优先级。

        Args:
            field: 排序字段
            desc: 是否降序，默认 False（升序）

        Returns:
            查询构建器（链式调用）

        Example:
            # 单列排序
            query.order_by('age')

            # 多列排序（先按 age 降序，再按 name 升序）
            query.order_by('age', desc=True).order_by('name')
        """
        self._order_by_fields.append((field, desc))
        return self

    def limit(self, n: int) -> 'Query[T]':
        """
        限制返回数量

        Args:
            n: 限制数量

        Returns:
            查询构建器（链式调用）
        """
        self._limit_value = n
        return self

    def offset(self, n: int) -> 'Query[T]':
        """
        偏移

        Args:
            n: 偏移量

        Returns:
            查询构建器（链式调用）
        """
        self._offset_value = n
        return self

    def first(self) -> Optional[T]:
        """
        返回第一条记录

        Returns:
            模型实例或None
        """
        original_limit = self._limit_value
        self._limit_value = 1

        results = self.all()

        self._limit_value = original_limit

        return results[0] if results else None

    def all(self) -> List[T]:
        """
        执行查询并返回所有结果

        Returns:
            模型实例列表
        """
        records = self._execute()

        # 转换为模型实例
        instances = []
        for record in records:
            # 将 Column.name 映射为模型属性名
            mapped = {}
            for db_col_name, value in record.items():
                if db_col_name == PSEUDO_PK_NAME:
                    continue  # 跳过内部 rowid
                # 使用模型的 _column_to_attr_name 方法转换
                attr_name = self.model_class._column_to_attr_name(db_col_name) or db_col_name
                mapped[attr_name] = value

            instance = self.model_class(**mapped)

            # 兼容旧 API 的属性
            if hasattr(instance, '_loaded_from_db'):
                instance._loaded_from_db = True

            instances.append(instance)

        return instances

    def count(self) -> int:
        """
        返回满足条件的记录数

        Returns:
            记录数
        """
        records = self._execute()
        return len(records)

    def _execute(self) -> List[dict]:
        """
        执行查询（内部方法）

        Returns:
            记录字典列表
        """
        # 获取 storage 实例（新 API 优先，兼容旧 API）
        storage: Optional['Storage'] = (
            self.storage or
            getattr(self.model_class, '__storage__', None) or
            getattr(self.model_class, '_db', None)
        )

        if not storage:
            raise QueryError(f"No database configured for {self.model_class.__name__}")

        # 获取表名（支持新旧两种风格）
        table_name: Optional[str] = (
            getattr(self.model_class, '__tablename__', None) or
            getattr(self.model_class, '_table_name', None)
        )

        if not table_name:
            raise QueryError(f"No table name defined for {self.model_class.__name__}")

        # 从存储引擎查询
        if len(self._order_by_fields) == 1:
            # 单列排序：下推给 Storage.query（可利用 SortedIndex 优化）
            field, desc = self._order_by_fields[0]
            records: List[dict] = storage.query(
                table_name, self._conditions,
                order_by=field, order_desc=desc
            )
        else:
            records = storage.query(table_name, self._conditions)

        # 多列排序（从后往前排序，确保优先级正确）
        if len(self._order_by_fields) > 1:
            # 反向遍历，先按低优先级排序，再按高优先级排序
            # 利用 Python 排序的稳定性，最终实现多列排序
            for field, desc in reversed(self._order_by_fields):
                # 使用工厂函数 make_sort_key 来正确捕获循环变量 field
                # 这避免了闭包中常见的"后期绑定"问题
                def make_sort_key(f: str) -> Any:
                    def sort_key(r: dict) -> Any:
                        return r.get(f) if r.get(f) is not None else ''
                    return sort_key
                records.sort(key=make_sort_key(field), reverse=desc)

        # 偏移和限制
        if self._offset_value > 0:
            records = records[self._offset_value:]

        if self._limit_value is not None:
            records = records[:self._limit_value]

        return records

    def __repr__(self) -> str:
        return f"Query({self.model_class.__name__}, conditions={len(self._conditions)})"
