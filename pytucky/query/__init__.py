"""
Pytuck 查询子系统

包含查询构建器、语句构建器、结果处理和 SQL 编译器
"""

from .builder import (
    Query, BinaryExpression, Condition,
    LogicalExpression, CompositeCondition, ConditionType,
    or_, and_, not_
)
from .statements import select, insert, update, delete, Statement, Select, Insert, Update, Delete
from .result import Result, CursorResult
from .compiler import QueryCompiler, CompiledQuery, SQLDialect

__all__ = [
    # Builder
    'Query',
    'BinaryExpression',
    'Condition',
    'LogicalExpression',
    'CompositeCondition',
    'ConditionType',
    'or_',
    'and_',
    'not_',
    # Statements
    'select',
    'insert',
    'update',
    'delete',
    'Statement',
    'Select',
    'Insert',
    'Update',
    'Delete',
    # Result
    'Result',
    'CursorResult',
    # Compiler
    'QueryCompiler',
    'CompiledQuery',
    'SQLDialect',
]
