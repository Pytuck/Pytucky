"""
SQLAlchemy 2.0 风格的 Statement API

提供 select, insert, update, delete 语句构建器
"""

from __future__ import annotations

from typing import Any, Generic, TYPE_CHECKING
from abc import ABC, abstractmethod

from ..common.typing import T
from ..common.exceptions import QueryError
from ..core.orm import PSEUDO_PK_NAME

if TYPE_CHECKING:
    from .builder import BinaryExpression, LogicalExpression, ExpressionType
    from ..core.storage import Storage

class Statement(Generic[T], ABC):
    """
    Statement abstract base class.

    All SQL-style statement builders (Select, Insert, Update, Delete) inherit from this class.
    Statements are executed through Session.execute() method.

    Attributes:
        model_class: The model class this statement operates on
    """

    def __init__(self, model_class: type[T]) -> None:
        self.model_class = model_class

    @abstractmethod
    def _execute(self, storage: 'Storage') -> Any:
        """执行语句（由 Session.execute 调用）"""
        pass

class Select(Statement[T]):
    """
    SELECT statement builder for querying records.

    Supports method chaining for building complex queries with conditions,
    ordering, and pagination.

    Example:
        stmt = select(User).where(User.age >= 18).order_by('name').limit(10)
        result = session.execute(stmt)
        users = result.all()

        # Multi-column ordering (priority: first to last)
        stmt = select(User).order_by('age', desc=True).order_by('name')

        # OR conditions
        stmt = select(User).where(or_(User.age >= 18, User.vip == True))

    Attributes:
        model_class: The model class to query
        _where_clauses: List of query conditions (BinaryExpression or LogicalExpression)
        _order_by_fields: List of (field_name, desc) tuples for multi-column ordering
        _limit_value: Maximum number of records to return
        _offset_value: Number of records to skip
        _options: List of query options (e.g., PrefetchOption)
    """

    def __init__(self, model_class: type[T]) -> None:
        super().__init__(model_class)
        self._where_clauses: list['ExpressionType'] = []
        self._order_by_fields: list[tuple[str, bool]] = []  # [(field, desc), ...]
        self._limit_value: int | None = None
        self._offset_value: int = 0
        self._options: list[Any] = []

    def where(self, *expressions: 'ExpressionType') -> 'Select[T]':
        """
        添加 WHERE 条件（支持表达式和逻辑组合）

        Args:
            *expressions: BinaryExpression 或 LogicalExpression 对象

        Returns:
            Select 对象（链式调用）

        Example:
            # 简单条件
            stmt = select(User).where(User.age >= 18)

            # OR 条件
            stmt = select(User).where(or_(User.age >= 18, User.vip == True))

            # 组合条件
            stmt = select(User).where(
                User.active == True,
                or_(User.role == 'admin', User.role == 'moderator')
            )
        """
        self._where_clauses.extend(expressions)
        return self

    def filter_by(self, **kwargs: Any) -> 'Select[T]':
        """
        添加 WHERE 条件（简单等值查询，SQLAlchemy 风格）

        用于简单的等值匹配，语法更简洁。对于复杂查询，使用 where() 配合表达式。

        Args:
            **kwargs: 字段名=值 的等值条件

        Returns:
            Select 对象（链式调用）

        Example:
            # 简单等值查询（推荐 filter_by）
            stmt = select(User).filter_by(name='Bob', active=True)

            # 复杂表达式查询（使用 where）
            stmt = select(User).where(User.age >= 20, User.name != 'Alice')
        """
        from .builder import BinaryExpression

        # 为每个 kwargs 创建等值表达式
        for field_name, value in kwargs.items():
            # 获取 Column 对象
            if field_name in self.model_class.__columns__:
                column = self.model_class.__columns__[field_name]
                # 创建等值表达式
                expr = BinaryExpression(column, '=', value)
                self._where_clauses.append(expr)
            else:
                raise QueryError(
                    f"Column '{field_name}' not found in {self.model_class.__name__}",
                    column_name=field_name
                )

        return self

    def order_by(self, field: str, desc: bool = False) -> 'Select[T]':
        """
        添加排序字段

        支持多列排序，多次调用时按调用顺序确定排序优先级。

        Args:
            field: 排序字段名
            desc: 是否降序，默认 False（升序）

        Returns:
            Select 对象（链式调用）

        Example:
            # 单列排序
            stmt = select(User).order_by('age')

            # 多列排序（先按 age 降序，再按 name 升序）
            stmt = select(User).order_by('age', desc=True).order_by('name')
        """
        self._order_by_fields.append((field, desc))
        return self

    def limit(self, n: int) -> 'Select[T]':
        """限制返回数量"""
        self._limit_value = n
        return self

    def offset(self, n: int) -> 'Select[T]':
        """偏移"""
        self._offset_value = n
        return self

    def options(self, *opts: Any) -> 'Select[T]':
        """
        添加查询选项（如 prefetch）

        Args:
            *opts: 查询选项对象（如 prefetch('orders')）

        Returns:
            Select 对象（链式调用）

        Example:
            from pytucky import prefetch

            stmt = select(User).options(prefetch('orders'))
            result = session.execute(stmt)
            users = result.all()  # orders 已批量加载
        """
        self._options.extend(opts)
        return self

    def _execute(self, storage: 'Storage') -> list[dict[str, Any]]:
        """执行查询，返回记录字典列表"""
        from .builder import BinaryExpression, LogicalExpression, ConditionType

        table_name = self.model_class.__tablename__
        assert table_name is not None, f"Model {self.model_class.__name__} must have __tablename__ defined"

        can_push_pagination = (
            self._offset_value >= 0
            and (self._limit_value is None or self._limit_value >= 0)
        )

        # 快速路径：如果是单一主键等值查询，直接下推到 storage.select
        pk_name = self.model_class.__primary_key__
        if pk_name and len(self._where_clauses) == 1:
            expr = self._where_clauses[0]
            if isinstance(expr, BinaryExpression) and expr.column._attr_name == pk_name and expr.operator in ('=', '=='):
                from ..common.exceptions import RecordNotFoundError
                try:
                    records = [storage.select(table_name, expr.value)]
                except RecordNotFoundError:
                    records = []
                if self._offset_value != 0:
                    records = records[self._offset_value:]
                if self._limit_value is not None:
                    records = records[:self._limit_value]
                return records

        # 转换 Expression 为 Condition（支持 BinaryExpression 和 LogicalExpression）
        conditions: list[ConditionType] = []
        for expr in self._where_clauses:
            if isinstance(expr, (BinaryExpression, LogicalExpression)):
                conditions.append(expr.to_condition())
            else:
                raise QueryError(
                    f"Unexpected expression type: {type(expr).__name__}",
                    details={'expression': repr(expr)}
                )

        if len(self._order_by_fields) == 1:
            field, desc = self._order_by_fields[0]
            if can_push_pagination:
                records = storage.query(
                    table_name,
                    conditions,
                    limit=self._limit_value,
                    offset=self._offset_value,
                    order_by=field,
                    order_desc=desc,
                )
            else:
                records = storage.query(table_name, conditions, order_by=field, order_desc=desc)
        elif len(self._order_by_fields) == 0 and can_push_pagination:
            records = storage.query(
                table_name,
                conditions,
                limit=self._limit_value,
                offset=self._offset_value,
            )
        else:
            records = storage.query(table_name, conditions)

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

        if len(self._order_by_fields) > 1 or not can_push_pagination:
            if self._offset_value != 0:
                records = records[self._offset_value:]
            if self._limit_value is not None:
                records = records[:self._limit_value]

        return records

class Insert(Statement[T]):
    """
    INSERT statement builder for creating new records.

    Example:
        stmt = insert(User).values(name='Alice', age=20)
        result = session.execute(stmt)
        new_id = result.inserted_primary_key

    Attributes:
        model_class: The model class to insert into
        _values: Dictionary of column names to values
    """

    def __init__(self, model_class: type[T]) -> None:
        super().__init__(model_class)
        self._values: dict[str, Any] = {}
        self._values_list: list[dict[str, Any]] | None = None

    def values(self, **kwargs: Any) -> 'Insert[T]':
        """设置要插入的值（单条）"""
        self._values.update(kwargs)
        return self

    def values_list(self, records: list[dict[str, Any]]) -> 'Insert[T]':
        """
        设置要批量插入的值

        Args:
            records: 数据字典列表

        Example:
            stmt = insert(User).values_list([
                {'name': 'Alice', 'age': 20},
                {'name': 'Bob', 'age': 21},
            ])
            result = session.execute(stmt)
        """
        self._values_list = records
        return self

    def _execute(self, storage: 'Storage') -> Any:
        """执行插入，返回插入的主键（或主键列表）"""
        table_name = self.model_class.__tablename__
        assert table_name is not None, f"Model {self.model_class.__name__} must have __tablename__ defined"

        # 批量插入
        if self._values_list is not None:
            validated_records: list[dict[str, Any]] = []
            for record in self._values_list:
                validated_data: dict[str, Any] = {}
                for attr_name, column in self.model_class.__columns__.items():
                    db_col_name = column.name if column.name else attr_name
                    if attr_name in record:
                        validated_data[db_col_name] = column.validate(record[attr_name])
                    elif column.has_default():
                        validated_data[db_col_name] = column.resolve_default()
                validated_records.append(validated_data)
            pks = storage.bulk_insert(table_name, validated_records)
            return pks

        # 单条插入
        # 验证和转换值（使用 Column.name 作为存储键）
        validated_data_single: dict[str, Any] = {}
        for attr_name, column in self.model_class.__columns__.items():
            db_col_name = column.name if column.name else attr_name
            if attr_name in self._values:
                validated_data_single[db_col_name] = column.validate(self._values[attr_name])
            elif column.has_default():
                validated_data_single[db_col_name] = column.resolve_default()

        # 插入
        pk = storage.insert(table_name, validated_data_single)
        return pk

class Update(Statement[T]):
    """
    UPDATE statement builder for modifying existing records.

    Example:
        stmt = update(User).where(User.id == 1).values(age=21)
        result = session.execute(stmt)
        affected = result.rowcount()

        # OR conditions
        stmt = update(User).where(or_(User.role == 'guest', User.expired == True)).values(active=False)

    Attributes:
        model_class: The model class to update
        _where_clauses: List of conditions to match records
        _values: Dictionary of column names to new values
    """

    def __init__(self, model_class: type[T]) -> None:
        super().__init__(model_class)
        self._where_clauses: list['ExpressionType'] = []
        self._values: dict[str, Any] = {}

    def where(self, *expressions: 'ExpressionType') -> 'Update[T]':
        """
        添加 WHERE 条件（支持表达式和逻辑组合）

        Args:
            *expressions: BinaryExpression 或 LogicalExpression 对象

        Returns:
            Update 对象（链式调用）
        """
        self._where_clauses.extend(expressions)
        return self

    def values(self, **kwargs: Any) -> 'Update[T]':
        """设置要更新的值"""
        self._values.update(kwargs)
        return self

    def _execute(self, storage: 'Storage') -> int:
        """执行更新，返回受影响的行数"""
        from .builder import BinaryExpression, LogicalExpression, ConditionType

        table_name = self.model_class.__tablename__
        assert table_name is not None, f"Model {self.model_class.__name__} must have __tablename__ defined"
        pk_name = self.model_class.__primary_key__

        # 优化：检测主键等于查询，直接访问而非全表扫描
        # 仅适用于单个 BinaryExpression 且是主键等值条件
        pk_value = None
        if pk_name and len(self._where_clauses) == 1:
            expr = self._where_clauses[0]
            if isinstance(expr, BinaryExpression):
                # 比较属性名（_attr_name）而非 Column.name
                if expr.column._attr_name == pk_name and expr.operator in ('=', '=='):
                    pk_value = expr.value

        # 验证值（使用 Column.name 作为存储键）
        validated_values: dict[str, Any] = {}
        for attr_name, value in self._values.items():
            if attr_name in self.model_class.__columns__:
                column = self.model_class.__columns__[attr_name]
                db_col_name = column.name if column.name else attr_name
                validated_values[db_col_name] = column.validate(value)

        if pk_value is not None:
            # 主键直接查询（O(1)）
            from ..common.exceptions import RecordNotFoundError
            try:
                storage.update(table_name, pk_value, validated_values)
                return 1
            except RecordNotFoundError:
                # 记录不存在，返回 0（符合 SQL UPDATE 语义）
                return 0
            # 其他异常（如数据库错误）向上传播
        else:
            # 条件查询
            conditions: list[ConditionType] = []
            for expr in self._where_clauses:
                if isinstance(expr, (BinaryExpression, LogicalExpression)):
                    conditions.append(expr.to_condition())
                else:
                    raise QueryError(
                        f"Unexpected expression type: {type(expr).__name__}",
                        details={'expression': repr(expr)}
                    )
            records = storage.query(table_name, conditions)

            count = 0
            for record in records:
                # 获取主键或 rowid
                if pk_name:
                    # 使用主键列的 Column.name 访问 record
                    pk_column = self.model_class.__columns__[pk_name]
                    pk_db_col_name = pk_column.name if pk_column.name else pk_name
                    pk = record[pk_db_col_name]
                else:
                    # 无主键模型：使用内部 rowid
                    pk = record.get(PSEUDO_PK_NAME)
                    if pk is None:
                        raise QueryError(
                            "Cannot update record without primary key or rowid",
                            details={'table': table_name}
                        )
                storage.update(table_name, pk, validated_values)
                count += 1

            return count

class Delete(Statement[T]):
    """
    DELETE statement builder for removing records.

    Example:
        stmt = delete(User).where(User.id == 1)
        result = session.execute(stmt)
        affected = result.rowcount()

        # OR conditions
        stmt = delete(User).where(or_(User.expired == True, User.banned == True))

    Attributes:
        model_class: The model class to delete from
        _where_clauses: List of conditions to match records for deletion
    """

    def __init__(self, model_class: type[T]) -> None:
        super().__init__(model_class)
        self._where_clauses: list['ExpressionType'] = []

    def where(self, *expressions: 'ExpressionType') -> 'Delete[T]':
        """
        添加 WHERE 条件（支持表达式和逻辑组合）

        Args:
            *expressions: BinaryExpression 或 LogicalExpression 对象

        Returns:
            Delete 对象（链式调用）
        """
        self._where_clauses.extend(expressions)
        return self

    def _execute(self, storage: 'Storage') -> int:
        """执行删除，返回受影响的行数"""
        from .builder import BinaryExpression, LogicalExpression, ConditionType

        table_name = self.model_class.__tablename__
        assert table_name is not None, f"Model {self.model_class.__name__} must have __tablename__ defined"
        pk_name = self.model_class.__primary_key__

        # 优化：检测主键等于查询，直接访问而非全表扫描
        # 仅适用于单个 BinaryExpression 且是主键等值条件
        pk_value = None
        if pk_name and len(self._where_clauses) == 1:
            expr = self._where_clauses[0]
            if isinstance(expr, BinaryExpression):
                # 比较属性名（_attr_name）而非 Column.name
                if expr.column._attr_name == pk_name and expr.operator in ('=', '=='):
                    pk_value = expr.value

        if pk_value is not None:
            # 主键直接删除（O(1)）
            from ..common.exceptions import RecordNotFoundError
            try:
                storage.delete(table_name, pk_value)
                return 1
            except RecordNotFoundError:
                # 记录不存在，返回 0（符合 SQL DELETE 语义）
                return 0
            # 其他异常（如数据库错误）向上传播
        else:
            # 条件查询
            conditions: list[ConditionType] = []
            for expr in self._where_clauses:
                if isinstance(expr, (BinaryExpression, LogicalExpression)):
                    conditions.append(expr.to_condition())
                else:
                    raise QueryError(
                        f"Unexpected expression type: {type(expr).__name__}",
                        details={'expression': repr(expr)}
                    )
            records = storage.query(table_name, conditions)

            count = 0
            for record in records:
                # 获取主键或 rowid
                if pk_name:
                    # 使用主键列的 Column.name 访问 record
                    pk_column = self.model_class.__columns__[pk_name]
                    pk_db_col_name = pk_column.name if pk_column.name else pk_name
                    pk = record[pk_db_col_name]
                else:
                    # 无主键模型：使用内部 rowid
                    pk = record.get(PSEUDO_PK_NAME)
                    if pk is None:
                        raise QueryError(
                            "Cannot delete record without primary key or rowid",
                            details={'table': table_name}
                        )
                storage.delete(table_name, pk)
                count += 1

            return count

# ==================== 顶层工厂函数 ====================

def select(model_class: type[T]) -> Select[T]:
    """创建 SELECT 语句"""
    return Select(model_class)

def insert(model_class: type[T]) -> Insert[T]:
    """创建 INSERT 语句"""
    return Insert(model_class)

def update(model_class: type[T]) -> Update[T]:
    """创建 UPDATE 语句"""
    return Update(model_class)

def delete(model_class: type[T]) -> Delete[T]:
    """创建 DELETE 语句"""
    return Delete(model_class)
