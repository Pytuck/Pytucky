"""
SQL 语句编译器

将 Statement 对象编译为原生 SQL 语句和参数
"""

import json
from datetime import datetime, date, timedelta
from typing import Any, List, NamedTuple, Optional, Tuple, Type, TYPE_CHECKING

from ..common.exceptions import QueryError
from ..core.types import TypeRegistry

if TYPE_CHECKING:
    from .statements import Statement, Select, Insert, Update, Delete
    from .builder import BinaryExpression, LogicalExpression


class CompiledQuery(NamedTuple):
    """
    编译后的查询结果

    Attributes:
        sql: 生成的 SQL 语句
        params: 参数元组（用于参数化查询）
        statement_type: 语句类型 ('select', 'insert', 'update', 'delete')
        table_name: 表名
    """
    sql: str
    params: Tuple[Any, ...]
    statement_type: str
    table_name: str


class SQLDialect:
    """
    SQL 方言

    定义不同数据库的 SQL 语法差异，如参数占位符和标识符引号。
    可扩展支持不同数据库（SQLite, DuckDB 等）。
    """

    # 参数占位符（SQLite / DuckDB 使用 ?）
    param_style: str = '?'

    # 标识符引号：使用标准 SQL 双引号，SQLite 与 DuckDB 都支持
    identifier_quote: str = '"'

    def quote_identifier(self, name: str) -> str:
        """引用标识符（表名、列名）"""
        escaped = name.replace(self.identifier_quote, self.identifier_quote * 2)
        return f'{self.identifier_quote}{escaped}{self.identifier_quote}'


class QueryCompiler:
    """
    查询编译器

    将 Statement 对象（Select, Insert, Update, Delete）编译为
    原生 SQL 语句和参数化的参数列表。

    Example:
        compiler = QueryCompiler()
        if compiler.can_compile(statement):
            result = compiler.compile(statement)
            # result.sql = 'SELECT * FROM "users" WHERE "age" > ?'
            # result.params = (18,)
    """

    def __init__(self, dialect: Optional[SQLDialect] = None) -> None:
        """
        初始化编译器

        Args:
            dialect: SQL 方言，默认使用 SQLDialect（SQLite 兼容）
        """
        self.dialect = dialect or SQLDialect()

    def can_compile(self, statement: 'Statement') -> bool:
        """
        检查语句是否可以编译为 SQL

        目前支持：
        - 简单的二元表达式
        - 支持的操作符：=, !=, <, <=, >, >=, IN
        - AND/OR/NOT 逻辑组合

        不支持（回退到内存执行）：
        - 子查询
        - 函数调用

        Args:
            statement: Statement 对象

        Returns:
            是否可以编译
        """
        # 检查 where 条件是否都可以编译
        if hasattr(statement, '_where_clauses'):
            for clause in statement._where_clauses:
                if not self._is_compilable_expression(clause):
                    return False
        return True

    def compile(self, statement: 'Statement') -> CompiledQuery:
        """
        编译语句为 SQL

        Args:
            statement: Statement 对象

        Returns:
            CompiledQuery 包含 SQL、参数、语句类型和表名

        Raises:
            QueryError: 如果语句类型未知
        """
        stmt_type = type(statement).__name__.lower()

        if stmt_type == 'select':
            return self._compile_select(statement)  # type: ignore
        elif stmt_type == 'insert':
            return self._compile_insert(statement)  # type: ignore
        elif stmt_type == 'update':
            return self._compile_update(statement)  # type: ignore
        elif stmt_type == 'delete':
            return self._compile_delete(statement)  # type: ignore
        else:
            raise QueryError(
                message=f"Unknown statement type: {stmt_type}",
                details={"statement_type": stmt_type}
            )

    def _compile_select(self, stmt: 'Select') -> CompiledQuery:
        """编译 SELECT 语句"""
        table = self._quote(stmt.model_class.__tablename__)
        sql = f'SELECT * FROM {table}'

        params: List[Any] = []

        # WHERE 子句
        where_sql, where_params = self._compile_where(stmt)
        if where_sql:
            sql += f' WHERE {where_sql}'
            params.extend(where_params)

        # ORDER BY（支持多列）
        if stmt._order_by_fields:
            order_parts: List[str] = []
            for field, desc in stmt._order_by_fields:
                order_expr = self._quote(field)
                if desc:
                    order_expr += ' DESC'
                order_parts.append(order_expr)
            sql += f' ORDER BY {", ".join(order_parts)}'

        # LIMIT / OFFSET
        if stmt._limit_value is not None:
            sql += f' LIMIT {stmt._limit_value}'
        if stmt._offset_value > 0:
            sql += f' OFFSET {stmt._offset_value}'

        return CompiledQuery(
            sql=sql,
            params=tuple(params),
            statement_type='select',
            table_name=stmt.model_class.__tablename__
        )

    def _compile_insert(self, stmt: 'Insert') -> CompiledQuery:
        """编译 INSERT 语句"""
        table = self._quote(stmt.model_class.__tablename__)
        columns = list(stmt._values.keys())
        cols_sql = ', '.join([self._quote(c) for c in columns])
        placeholders = ', '.join(['?' for _ in columns])

        sql = f'INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})'

        params = tuple(
            self._serialize_param(v, stmt.model_class, k)
            for k, v in stmt._values.items()
        )

        return CompiledQuery(
            sql=sql,
            params=params,
            statement_type='insert',
            table_name=stmt.model_class.__tablename__
        )

    def _compile_update(self, stmt: 'Update') -> CompiledQuery:
        """编译 UPDATE 语句"""
        table = self._quote(stmt.model_class.__tablename__)

        set_parts: List[str] = []
        params: List[Any] = []

        for col, val in stmt._values.items():
            set_parts.append(f'{self._quote(col)} = ?')
            params.append(self._serialize_param(val, stmt.model_class, col))

        sql = f'UPDATE {table} SET {", ".join(set_parts)}'

        # WHERE 子句
        where_sql, where_params = self._compile_where(stmt)
        if where_sql:
            sql += f' WHERE {where_sql}'
            params.extend(where_params)

        return CompiledQuery(
            sql=sql,
            params=tuple(params),
            statement_type='update',
            table_name=stmt.model_class.__tablename__
        )

    def _compile_delete(self, stmt: 'Delete') -> CompiledQuery:
        """编译 DELETE 语句"""
        table = self._quote(stmt.model_class.__tablename__)
        sql = f'DELETE FROM {table}'

        params: List[Any] = []

        # WHERE 子句
        where_sql, where_params = self._compile_where(stmt)
        if where_sql:
            sql += f' WHERE {where_sql}'
            params.extend(where_params)

        return CompiledQuery(
            sql=sql,
            params=tuple(params),
            statement_type='delete',
            table_name=stmt.model_class.__tablename__
        )

    def _compile_where(self, stmt: Any) -> Tuple[str, List[Any]]:
        """
        编译 WHERE 条件

        Args:
            stmt: Statement 对象

        Returns:
            (WHERE 子句字符串, 参数列表)
        """
        from .builder import BinaryExpression, LogicalExpression

        if not hasattr(stmt, '_where_clauses') or not stmt._where_clauses:
            return '', []

        parts: List[str] = []
        params: List[Any] = []

        for expr in stmt._where_clauses:
            if isinstance(expr, LogicalExpression):
                # 编译逻辑表达式
                sql_part, expr_params = self._compile_logical_expression(expr, stmt.model_class)
                parts.append(f'({sql_part})')
                params.extend(expr_params)
            elif isinstance(expr, BinaryExpression):
                # 编译二元表达式
                sql_part, expr_params = self._compile_binary_expression(expr, stmt.model_class)
                parts.append(sql_part)
                params.extend(expr_params)
            else:
                raise QueryError(
                    message=f"Unknown expression type: {type(expr).__name__}",
                    details={"expression": repr(expr)}
                )

        return ' AND '.join(parts), params

    def _compile_binary_expression(
        self,
        expr: 'BinaryExpression',
        model_class: Type
    ) -> Tuple[str, List[Any]]:
        """
        编译二元表达式

        Args:
            expr: BinaryExpression 对象
            model_class: 模型类

        Returns:
            (SQL 片段, 参数列表)
        """
        col_name = expr.column.name
        if col_name is None:
            raise QueryError(
                message="Column name is not set",
                details={"column": repr(expr.column)}
            )
        op = self._convert_op(expr.operator)
        params: List[Any] = []

        if expr.operator == 'IN':
            # IN 操作符需要特殊处理
            if isinstance(expr.value, (list, tuple)):
                placeholders = ', '.join(['?' for _ in expr.value])
                sql = f'{self._quote(col_name)} IN ({placeholders})'
                for v in expr.value:
                    params.append(self._serialize_param(v, model_class, col_name))
            else:
                raise QueryError(
                    message="IN operator requires a list or tuple value",
                    column_name=col_name
                )
        elif expr.value is None:
            # NULL 值需要特殊处理：使用 IS NULL 或 IS NOT NULL
            if op in ('=', '=='):
                sql = f'{self._quote(col_name)} IS NULL'
            elif op in ('!=', '<>'):
                sql = f'{self._quote(col_name)} IS NOT NULL'
            else:
                # 其他操作符与 NULL 比较返回空结果
                sql = '1 = 0'  # 永假条件
        else:
            sql = f'{self._quote(col_name)} {op} ?'
            params.append(self._serialize_param(expr.value, model_class, col_name))

        return sql, params

    def _compile_logical_expression(
        self,
        expr: 'LogicalExpression',
        model_class: Type
    ) -> Tuple[str, List[Any]]:
        """
        递归编译逻辑表达式

        Args:
            expr: LogicalExpression 对象
            model_class: 模型类

        Returns:
            (SQL 片段, 参数列表)
        """
        from .builder import BinaryExpression, LogicalExpression

        parts: List[str] = []
        params: List[Any] = []

        if expr.operator == 'NOT':
            # NOT 只有一个子表达式
            child = expr.expressions[0]
            if isinstance(child, LogicalExpression):
                child_sql, child_params = self._compile_logical_expression(child, model_class)
                return f'NOT ({child_sql})', child_params
            elif isinstance(child, BinaryExpression):
                child_sql, child_params = self._compile_binary_expression(child, model_class)
                return f'NOT ({child_sql})', child_params
            else:
                raise QueryError(
                    message=f"Unknown expression type in NOT: {type(child).__name__}",
                    details={"expression": repr(child)}
                )
        else:
            # AND 或 OR
            connector = ' AND ' if expr.operator == 'AND' else ' OR '
            for child in expr.expressions:
                if isinstance(child, LogicalExpression):
                    child_sql, child_params = self._compile_logical_expression(child, model_class)
                    parts.append(f'({child_sql})')
                    params.extend(child_params)
                elif isinstance(child, BinaryExpression):
                    child_sql, child_params = self._compile_binary_expression(child, model_class)
                    parts.append(child_sql)
                    params.extend(child_params)
                else:
                    raise QueryError(
                        message=f"Unknown expression type: {type(child).__name__}",
                        details={"expression": repr(child)}
                    )

            return connector.join(parts), params

    @staticmethod
    def _convert_op(op: str) -> str:
        """
        转换操作符为 SQL 操作符

        Args:
            op: Python 操作符

        Returns:
            SQL 操作符
        """
        op_map = {
            '==': '=',
            '=': '=',
            'eq': '=',
            'ne': '!=',
            '!=': '!=',
            'lt': '<',
            '<': '<',
            'le': '<=',
            '<=': '<=',
            'gt': '>',
            '>': '>',
            'ge': '>=',
            '>=': '>=',
            'IN': 'IN',
            'in': 'IN',
        }
        return op_map.get(op, op)

    def _quote(self, name: str) -> str:
        """引用标识符"""
        return self.dialect.quote_identifier(name)

    def _serialize_param(self, value: Any, model_class: Type, col_name: str) -> Any:
        """
        序列化参数值为 SQL 兼容格式

        Args:
            value: 原始值
            model_class: 模型类
            col_name: 列名

        Returns:
            序列化后的值
        """
        if value is None:
            return None

        # 获取列类型
        columns = getattr(model_class, '__columns__', {})
        col = columns.get(col_name)
        if col is None:
            # 没有列定义，尝试基于值类型序列化
            return self._serialize_value_by_type(value)

        col_type = col.col_type

        if col_type == bool:
            return 1 if value else 0
        elif col_type == bytes:
            return value
        elif col_type in (datetime, date, timedelta):
            return TypeRegistry.serialize_for_text(value, col_type)
        elif col_type in (list, dict):
            return json.dumps(value, ensure_ascii=False)
        else:
            return value

    @staticmethod
    def _serialize_value_by_type(value: Any) -> Any:
        """
        基于值的类型序列化

        Args:
            value: 原始值

        Returns:
            序列化后的值
        """
        if value is None:
            return None

        if isinstance(value, bool):
            return 1 if value else 0

        if isinstance(value, (datetime, date, timedelta)):
            return TypeRegistry.serialize_for_text(value, type(value))

        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)

        return value

    def _is_compilable_expression(self, expr: Any) -> bool:
        """
        检查表达式是否可以编译为 SQL

        支持：
        - BinaryExpression（简单比较操作符）
        - LogicalExpression（AND/OR/NOT 组合）

        Args:
            expr: 表达式对象

        Returns:
            是否可以编译
        """
        from .builder import BinaryExpression, LogicalExpression

        simple_ops = {
            '==', '=', '!=', '<', '<=', '>', '>=',
            'eq', 'ne', 'lt', 'le', 'gt', 'ge',
            'IN', 'in'
        }

        if isinstance(expr, BinaryExpression):
            return expr.operator in simple_ops
        elif isinstance(expr, LogicalExpression):
            # 递归检查所有子表达式
            return all(self._is_compilable_expression(child) for child in expr.expressions)
        else:
            return False

    # 保留旧方法名作为别名，保持向后兼容
    def _is_simple_expression(self, expr: Any) -> bool:
        """已废弃：使用 _is_compilable_expression 代替"""
        return self._is_compilable_expression(expr)
