"""
Pytuck 泛型类型定义

提供统一的 TypeVar 和泛型类型别名，供整个项目使用。
避免循环导入和重复定义。
"""
from datetime import datetime, date, timedelta
from typing import TypeVar, TYPE_CHECKING, Type, Union

if TYPE_CHECKING:
    # 在类型检查时导入，避免运行时循环导入
    from ..core.orm import PureBaseModel, CRUDBaseModel


T = TypeVar('T', bound='PureBaseModel')
'''
- 基础模型泛型参数，绑定到 PureBaseModel
- 用于大部分查询和结果类型
- 使用字符串绑定，避免运行时导入 PureBaseModel
'''


T_CRUD = TypeVar('T_CRUD', bound='CRUDBaseModel')
'''
- CRUD 模型泛型参数，绑定到 CRUDBaseModel
- 用于 Active Record 模式的 classmethod 返回类型
'''


RelationshipT = TypeVar('RelationshipT')
'''
- Relationship 泛型参数（无 bound，因为可能是 List[T] 或 Optional[T]）
- 用于 Relationship 描述符的类型提示
'''


ColumnTypes = Type[Union[int, float, str, bool, bytes, datetime, date, timedelta, list, dict]]
'''字段类型，python中的类型'''