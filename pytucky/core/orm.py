"""
Pytucky ORM层

提供对象关系映射功能，支持两种模式：
- PureBaseModel: 纯模型定义，通过 Session 操作数据
- CRUDBaseModel: Active Record 模式，模型自带 CRUD 方法
"""

from __future__ import annotations
import sys
import json
import base64
from typing import Any, Callable, TYPE_CHECKING, overload, Literal, Generic, cast
from datetime import datetime, date, timedelta, timezone

from ..common.exceptions import ValidationError, TypeConversionError, SchemaError
from ..common.options import SyncOptions
from ..common.typing import RelationshipT, ColumnTypes
from .types import TypeRegistry

if TYPE_CHECKING:
    from .storage import Storage
    from .session import Session
    from ..query import Query, BinaryExpression

# 无主键时使用的内部 rowid 保留键名
PSEUDO_PK_NAME: str = '_pytuck_rowid'

# ==================== JSON 序列化辅助 ====================

def _json_serial(obj: Any) -> Any:
    """JSON 序列化辅助：处理非原生 JSON 类型

    用于 json.dumps 的 default 参数，将 datetime/date/timedelta/bytes
    等类型转换为 JSON 可序列化的形式。

    Args:
        obj: 待序列化的对象

    Returns:
        JSON 可序列化的值

    Raises:
        TypeError: 不支持的类型
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# ==================== 类型转换函数（模块级别） ====================

def _convert_to_bool(value: Any) -> bool:
    """
    转换为布尔值

    True: True, 1, '1', 'true', 'True', 'yes', 'Yes'
    False: False, 0, '0', 'false', 'False', 'no', 'No', ''
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        lower_val = value.lower()
        if lower_val in ('1', 'true', 'yes'):
            return True
        elif lower_val in ('0', 'false', 'no', ''):
            return False
        else:
            raise TypeConversionError(
                f"Cannot convert '{value}' to bool",
                value=value,
                target_type='bool'
            )
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to bool",
        value=value,
        target_type='bool'
    )

def _convert_to_bytes(value: Any) -> bytes:
    """转换为字节类型"""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode('utf-8')
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to bytes",
        value=value,
        target_type='bytes'
    )

def _convert_to_datetime(value: Any) -> datetime:
    """
    转换为 datetime

    支持的格式：
    - datetime 对象：直接返回
    - str: ISO 8601 格式（如 '2024-01-15T10:30:00' 或 '2024-01-15T10:30:00+08:00'）
    - int/float: Unix 时间戳（秒）
    - date: 转换为当天 00:00:00
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        # 尝试解析 ISO 格式
        try:
            # datetime.fromisoformat 支持大部分 ISO 8601 格式
            return datetime.fromisoformat(value)
        except ValueError:
            # 尝试带 Z 后缀的 UTC 格式
            if value.endswith('Z'):
                return datetime.fromisoformat(value[:-1]).replace(tzinfo=timezone.utc)
            raise
    if isinstance(value, (int, float)):
        # Unix 时间戳
        return datetime.fromtimestamp(value)
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to datetime",
        value=value,
        target_type='datetime'
    )

def _convert_to_date(value: Any) -> date:
    """
    转换为 date

    支持的格式：
    - date 对象：直接返回
    - datetime 对象：取日期部分
    - str: ISO 格式（如 '2024-01-15'）
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to date",
        value=value,
        target_type='date'
    )

def _convert_to_timedelta(value: Any) -> timedelta:
    """
    转换为 timedelta

    支持的格式：
    - timedelta 对象：直接返回
    - int/float: 秒数
    - str: 'HH:MM:SS' 或 'D days, HH:MM:SS' 格式
    """
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    if isinstance(value, str):
        # 尝试解析常见格式
        parts = value.split(':')
        if len(parts) == 3:
            # HH:MM:SS 格式
            hours, minutes, seconds = parts
            return timedelta(
                hours=int(hours),
                minutes=int(minutes),
                seconds=float(seconds)
            )
        elif len(parts) == 2:
            # MM:SS 格式
            minutes, seconds = parts
            return timedelta(minutes=int(minutes), seconds=float(seconds))
        # 尝试纯秒数
        return timedelta(seconds=float(value))
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to timedelta",
        value=value,
        target_type='timedelta'
    )

def _convert_to_list(value: Any) -> list:
    """
    转换为 list

    支持的格式：
    - list: 直接返回
    - tuple: 转换为 list
    - str: 尝试 JSON 解析
    """
    import json
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        result = json.loads(value)
        if not isinstance(result, list):
            raise TypeConversionError(
                f"JSON string does not represent a list",
                value=value,
                target_type='list'
            )
        return result
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to list",
        value=value,
        target_type='list'
    )

def _convert_to_dict(value: Any) -> dict:
    """
    转换为 dict

    支持的格式：
    - dict: 直接返回
    - str: 尝试 JSON 解析
    """
    import json
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        result = json.loads(value)
        if not isinstance(result, dict):
            raise TypeConversionError(
                f"JSON string does not represent a dict",
                value=value,
                target_type='dict'
            )
        return result
    raise TypeConversionError(
        f"Cannot convert {type(value).__name__} to dict",
        value=value,
        target_type='dict'
    )

# 类型转换函数注册表
# 将 Python 类型映射到对应的转换函数
_TYPE_CONVERTERS: dict[type, Callable[[Any], Any]] = {
    bool: _convert_to_bool,
    bytes: _convert_to_bytes,
    int: int,
    float: float,
    str: str,
    datetime: _convert_to_datetime,
    date: _convert_to_date,
    timedelta: _convert_to_timedelta,
    list: _convert_to_list,
    dict: _convert_to_dict,
}

class Column:
    """列定义

    用法:
        # name 默认取变量名
        id = Column(int, primary_key=True)       # name='id'
        name = Column(str)                        # name='name'
        age = Column(int, nullable=True)          # name='age'

        # 显式指定列名（当列名与变量名不同时）
        email = Column(str, name='user_email')    # name='user_email'
    """
    __slots__ = ['name', 'col_type', 'nullable', 'primary_key',
                 'index', 'default', 'default_factory', 'foreign_key', 'comment',
                 '_type_code', '_attr_name', '_owner_class', 'strict', '_validators']

    def __init__(self,
                 col_type: ColumnTypes,
                 *,
                 name: str | None = None,
                 nullable: bool = True,
                 primary_key: bool = False,
                 index: bool | str = False,
                 default: Any = None,
                 default_factory: Callable[[], Any] | None = None,
                 foreign_key: tuple | None = None,
                 comment: str | None = None,
                 strict: bool = False,
                 validator: Callable[[Any], bool] | list[Callable[[Any], bool]] | None = None):
        """
        初始化列定义

        Args:
            col_type: Python类型（int, str, float, bool, bytes, datetime, date, timedelta, list, dict）
            name: 列名（可选，默认使用变量名）
            nullable: 是否可空
            primary_key: 是否为主键
            index: 索引设置。False=不建索引，True/'hash'=哈希索引，'sorted'=有序索引
            default: 静态默认值
            default_factory: 默认值工厂函数（无参可调用对象），每次创建实例时调用。
                            与 default 互斥，不可同时设置。
                            示例: Column(datetime, default_factory=datetime.now)
            foreign_key: 外键关系 (table_name, column_name)
            comment: 列备注/注释
            strict: 是否严格模式（不进行类型转换）
            validator: 自定义校验函数或校验函数列表。
                      每个函数接收类型转换后的值，返回 True 通过，
                      返回 False 或抛出异常则校验失败。
                      示例: Column(str, validator=lambda x: len(x) <= 100)
        """
        # 验证 default 和 default_factory 互斥
        if default is not None and default_factory is not None:
            raise ValidationError(
                f"Column '{name}': cannot specify both 'default' and 'default_factory'"
            )
        if default_factory is not None and not callable(default_factory):
            raise ValidationError(
                f"Column '{name}': 'default_factory' must be callable"
            )

        self.name = name  # 可能为 None，将在 __set_name__ 中设置
        self.col_type = col_type
        self.nullable = nullable
        self.primary_key = primary_key
        # 验证索引类型
        if isinstance(index, str) and index not in ('hash', 'sorted'):
            raise ValidationError(
                f"Unsupported index type: '{index}'. Use True, False, 'hash', or 'sorted'"
            )
        self.index: bool | str = index
        self.default = default
        self.default_factory = default_factory
        self.foreign_key = foreign_key
        self.comment = comment
        self.strict = strict

        # 校验器
        if validator is None:
            self._validators: list[Callable[[Any], bool]] = []
        elif callable(validator):
            self._validators = [validator]
        elif isinstance(validator, list):
            self._validators = validator
        else:
            raise ValidationError(
                f"Column '{name}': validator must be callable or list of callables"
            )

        # 获取类型编码
        try:
            self._type_code, _ = TypeRegistry.get_codec(col_type)
        except Exception as e:
            raise ValidationError(f"Unsupported column type {col_type}: {e}")

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'name': self.name,
            'type': self.col_type.__name__,
            'type_code': int(self._type_code),
            'nullable': self.nullable,
            'primary_key': self.primary_key,
            'index': self.index,
            'default': self.default,
            'foreign_key': self.foreign_key,
            'comment': self.comment,
        }

    def validate(self, value: Any) -> Any:
        """
        验证并转换值

        Args:
            value: 待验证的值

        Returns:
            验证/转换后的值

        Raises:
            ValidationError: 类型不匹配且无法转换，或自定义校验器校验失败
        """
        # 处理None值
        if value is None:
            if not self.nullable and not self.primary_key:
                raise ValidationError(f"Column '{self.name}' cannot be null")
            return None

        # 特殊处理：int 列拒绝 bool（bool 是 int 的子类）
        if self.col_type == int and isinstance(value, bool):
            if self.strict:
                raise ValidationError(
                    f"Column '{self.name}' expects type int, got bool (strict mode)"
                )
            else:
                raise ValidationError(
                    f"Column '{self.name}' expects type int, got bool"
                )

        # 如果已经是正确类型，跳过转换
        if not isinstance(value, self.col_type):
            # 严格模式：不进行类型转换
            if self.strict:
                raise ValidationError(
                    f"Column '{self.name}' expects type {self.col_type.__name__}, "
                    f"got {type(value).__name__} (strict mode)"
                )

            # 宽松模式：使用字典查找转换函数
            try:
                converter = _TYPE_CONVERTERS.get(self.col_type)
                if converter is not None:
                    value = converter(value)
                else:
                    # 回退：尝试直接调用类型构造函数
                    value = self.col_type(value)  # type: ignore[call-arg]
            except (ValueError, TypeError) as e:
                raise ValidationError(
                    f"Column '{self.name}' Cannot convert {type(value).__name__} "
                    f"to {self.col_type.__name__}: {e}"
                )

        # 自定义校验器（在类型转换之后调用）
        if self._validators:
            for v_func in self._validators:
                try:
                    result = v_func(value)
                except ValidationError:
                    raise
                except Exception as e:
                    raise ValidationError(
                        f"Column '{self.name}' validation failed: {e}"
                    )
                if result is False:
                    raise ValidationError(
                        f"Column '{self.name}' validation failed for value: {value!r}"
                    )

        return value

    def __repr__(self) -> str:
        return f"Column(name='{self.name}', type={self.col_type.__name__}, pk={self.primary_key})"

    def resolve_default(self) -> Any:
        """
        获取解析后的默认值

        优先使用 default_factory（每次调用生成新值），
        其次使用 default（静态值）。

        Returns:
            解析后的默认值，或 None（当 default 和 default_factory 均未设置时）
        """
        if self.default_factory is not None:
            return self.default_factory()
        return self.default

    def has_default(self) -> bool:
        """判断列是否设置了默认值（default 或 default_factory）"""
        return self.default is not None or self.default_factory is not None

    # ==================== 描述符协议 ====================

    def __set_name__(self, owner: type['PureBaseModel'], name: str) -> None:
        """
        在类定义时被调用，存储属性名和拥有者类

        这允许 Column 知道它属于哪个模型类。
        如果 name 未显式指定，则使用变量名作为列名。
        """
        self._attr_name = name
        self._owner_class = owner
        # 如果 name 未指定，使用变量名
        if self.name is None:
            self.name = name

    def __get__(self, instance: 'PureBaseModel' | None, owner: type['PureBaseModel']) -> 'Column' | Any:
        """
        描述符协议：
        - 类访问（instance=None）：返回 Column 对象（用于查询）
        - 实例访问：返回实例属性的值
        """
        if instance is None:
            # 类级别访问：Student.age -> Column 对象
            return self

        # 实例级别访问：student.age -> 实际值
        return instance.__dict__.get(self._attr_name, None)

    def __set__(self, instance: 'PureBaseModel', value: Any) -> None:
        """设置实例属性值"""
        validated_value = self.validate(value)
        instance.__dict__[self._attr_name] = validated_value

    # ==================== 查询表达式支持（魔术方法） ====================

    def __eq__(self, other: Any) -> 'BinaryExpression':  # type: ignore[override]
        """等于：Student.age == 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '=', other)

    def __ne__(self, other: Any) -> 'BinaryExpression':  # type: ignore[override]
        """不等于：Student.age != 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '!=', other)

    def __lt__(self, other: Any) -> 'BinaryExpression':
        """小于：Student.age < 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '<', other)

    def __le__(self, other: Any) -> 'BinaryExpression':
        """小于等于：Student.age <= 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '<=', other)

    def __gt__(self, other: Any) -> 'BinaryExpression':
        """大于：Student.age > 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '>', other)

    def __ge__(self, other: Any) -> 'BinaryExpression':
        """大于等于：Student.age >= 20"""
        from ..query import BinaryExpression
        return BinaryExpression(self, '>=', other)

    def in_(self, values: list) -> 'BinaryExpression':
        """IN 操作：Student.age.in_([18, 19, 20])"""
        from ..query import BinaryExpression
        return BinaryExpression(self, 'IN', values)

    def contains(self, value: str) -> 'BinaryExpression':
        """包含匹配（大小写不敏感）：User.name.contains('ali')"""
        from ..query import BinaryExpression
        return BinaryExpression(self, 'LIKE', value)

    def startswith(self, value: str) -> 'BinaryExpression':
        """前缀匹配（大小写不敏感）：User.name.startswith('Al')"""
        from ..query import BinaryExpression
        return BinaryExpression(self, 'STARTSWITH', value)

    def endswith(self, value: str) -> 'BinaryExpression':
        """后缀匹配（大小写不敏感）：User.name.endswith('ce')"""
        from ..query import BinaryExpression
        return BinaryExpression(self, 'ENDSWITH', value)

# ==================== 模型基类定义 ====================

class PureBaseModel:
    """
    纯模型基类 - 仅定义数据结构

    这是一个真实的基类，declarative_base() 返回的类会继承它。
    可用于 isinstance() 检查和类型注解。

    通过 Session 进行所有数据库操作：

        from pytucky import Storage, declarative_base, Session, Column
        from pytucky import PureBaseModel

        db = Storage(file_path='mydb.pytucky')
        Base: type[PureBaseModel] = declarative_base(db)

        class User(Base):
            __tablename__ = 'users'
            id = Column(int, primary_key=True)
            name = Column(str)

        user = User(name='Alice')
        isinstance(user, PureBaseModel)  # True

        session = Session(db)
        session.add(user)
        session.commit()
    """

    # 类属性
    __abstract__: bool = True
    __storage__: 'Storage' | None = None
    __tablename__: str | None = None
    __table_comment__: str | None = None
    __columns__: dict[str, Column] = {}
    __primary_key__: str | None = None  # None 表示无主键，使用隐式 rowid
    __relationships__: dict[str, 'Relationship'] = {}

    def __init__(self, **kwargs: Any):
        """初始化模型实例

        - 此方法应由 declarative_base 方法来重写。
        """

    def __setattr__(self, name: str, value: Any) -> None:
        """
        属性设置拦截，实现脏跟踪

        当设置 Column 属性时，自动将实例标记为 dirty，
        这样 session.flush()/commit() 就能检测到修改。
        """
        old_value = None
        session: 'Session' | None = None

        if (hasattr(self.__class__, name) and
            isinstance(getattr(self.__class__, name), Column) and
            hasattr(self, '_pytuck_session')
        ):
            old_value = self.__dict__.get(name)
            session = getattr(self, '_pytuck_session')

        object.__setattr__(self, name, value)

        if session is not None and old_value != value:
            session._mark_dirty(self)

    # ==================== 列名映射辅助方法 ====================

    @classmethod
    def _attr_to_column_name(cls, attr_name: str) -> str:
        """
        将属性名转换为 Column.name

        Args:
            attr_name: 模型属性名

        Returns:
            对应的 Column.name（如果存在），否则返回属性名本身
        """
        column = cls.__columns__.get(attr_name)
        return column.name if column and column.name else attr_name

    @classmethod
    def _column_to_attr_name(cls, col_name: str) -> str | None:
        """
        将 Column.name 转换为属性名

        Args:
            col_name: Column.name（数据库列名）

        Returns:
            对应的属性名，如果未找到返回 None
        """
        for attr_name, column in cls.__columns__.items():
            if column.name == col_name:
                return attr_name
        return None

    def to_dict(
        self,
        use_column_names: bool = False,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
        depth: int = 0,
    ) -> dict[str, Any]:
        """
        转换为字典

        Args:
            use_column_names: 如果为 True，使用 Column.name 作为字典键；
                             否则使用属性名（默认）
            include: 只包含的字段名集合（使用属性名）。传入时只输出集合内的字段
            exclude: 排除的字段名集合（使用属性名）。与 include 同时传入时 include 优先
            depth: 关联数据展开深度（默认 0 不展开）。
                   depth=1 展开一层 Relationship

        Returns:
            包含模型数据的字典

        Example:
            class User(Base):
                __tablename__ = 'users'
                lv = Column(str, name='level')

            user = User(lv='admin')
            user.to_dict()  # {'lv': 'admin'}
            user.to_dict(use_column_names=True)  # {'level': 'admin'}
            user.to_dict(include={'lv'})  # {'lv': 'admin'}
            user.to_dict(exclude={'lv'})  # {} (排除 lv 字段)
        """
        data = {}
        for attr_name, column in self.__columns__.items():
            # include/exclude 筛选（include 优先于 exclude）
            if include is not None:
                if attr_name not in include:
                    continue
            elif exclude is not None and attr_name in exclude:
                continue
            key = column.name if use_column_names and column.name else attr_name
            data[key] = getattr(self, attr_name, None)

        # depth > 0 时展开 Relationship
        if depth > 0:
            for rel_name in self.__relationships__:
                if include is not None:
                    if rel_name not in include:
                        continue
                elif exclude is not None and rel_name in exclude:
                    continue
                value = getattr(self, rel_name, None)
                if value is None:
                    data[rel_name] = None
                elif isinstance(value, list):
                    data[rel_name] = [
                        item.to_dict(
                            use_column_names=use_column_names,
                            depth=depth - 1,
                        )
                        if hasattr(item, 'to_dict') else item
                        for item in value
                    ]
                elif hasattr(value, 'to_dict'):
                    data[rel_name] = value.to_dict(
                        use_column_names=use_column_names,
                        depth=depth - 1,
                    )
                else:
                    data[rel_name] = value

        return data

    def to_json(
        self,
        use_column_names: bool = False,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
        depth: int = 0,
        ensure_ascii: bool = False,
        indent: int | None = None,
    ) -> str:
        """
        转换为 JSON 字符串

        自动处理 datetime/date/timedelta/bytes 等特殊类型的序列化。

        Args:
            use_column_names: 如果为 True，使用 Column.name 作为字典键
            include: 只包含的字段名集合（使用属性名）
            exclude: 排除的字段名集合（使用属性名）
            depth: 关联数据展开深度（默认 0 不展开）
            ensure_ascii: 是否强制 ASCII 编码（默认 False，支持中文）
            indent: 缩进空格数（默认 None，紧凑输出）

        Returns:
            JSON 字符串

        Example:
            user = User(name='Alice', age=25)
            user.to_json()  # '{"name": "Alice", "age": 25}'
            user.to_json(indent=2)  # 格式化输出
        """
        data = self.to_dict(
            use_column_names=use_column_names,
            include=include,
            exclude=exclude,
            depth=depth,
        )
        return json.dumps(
            data,
            ensure_ascii=ensure_ascii,
            indent=indent,
            default=_json_serial,
        )

    def __repr__(self) -> str:
        """字符串表示"""
        pk_name = self.__primary_key__
        if pk_name:
            pk_value = getattr(self, pk_name, None)
        else:
            # 无主键时，尝试获取隐式 rowid
            pk_value = getattr(self, '_pytuck_rowid', None)
        return f"<{self.__class__.__name__}(pk={pk_value})>"

class CRUDBaseModel(PureBaseModel):
    """
    Active Record 基类 - 模型自带 CRUD 方法

    这是一个真实的基类，declarative_base(crud=True) 返回的类会继承它。
    可用于 isinstance() 检查和类型注解。

    可以直接在模型实例/类上进行数据库操作：

        from pytucky import Storage, declarative_base, Column
        from pytucky import CRUDBaseModel

        db = Storage(file_path='mydb.pytucky')
        Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

        class User(Base):
            __tablename__ = 'users'
            id = Column(int, primary_key=True)
            name = Column(str)

        user = User.create(name='Alice')
        isinstance(user, CRUDBaseModel)  # True
        isinstance(user, PureBaseModel)  # True（继承关系）

        user.name = 'Bob'
        user.save()
        user.delete()
    """

    # 实例状态
    _loaded_from_db: bool = False

    # ==================== 实例方法 ====================

    def save(self) -> None:
        """
        保存记录（自动判断 insert 或 update）

        Example:
            user = User(name='Alice')
            user.save()  # INSERT
            user.name = 'Bob'
            user.save()  # UPDATE
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    def delete(self) -> None:
        """
        删除当前记录

        Example:
            user = User.get(1)
            user.delete()
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    def refresh(self) -> None:
        """
        从数据库刷新当前实例

        Example:
            user = User.get(1)
            # 数据库中被其他进程修改
            user.refresh()  # 获取最新数据
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    # ==================== 类方法 ====================

    @classmethod
    def create(cls, **kwargs: Any) -> 'CRUDBaseModel':
        """
        创建并保存新记录

        Example:
            user = User.create(name='Alice', age=20)
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    @classmethod
    def get(cls, pk: Any) -> 'CRUDBaseModel' | None:
        """
        根据主键获取记录

        Example:
            user = User.get(1)
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    @classmethod
    def filter(cls, *expressions: 'BinaryExpression') -> 'Query':
        """
        条件查询（表达式语法）

        Example:
            users = User.filter(User.age >= 18).all()
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    @classmethod
    def filter_by(cls, **kwargs: Any) -> 'Query':
        """
        简单等值查询

        Example:
            users = User.filter_by(name='Alice').all()
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

    @classmethod
    def all(cls) -> list['CRUDBaseModel']:
        """
        获取所有记录

        Example:
            users = User.all()
        """
        raise NotImplementedError("This method should be overridden by declarative_base")

class Relationship(Generic[RelationshipT]):
    """关联关系描述符（延迟加载，支持类型提示）

    为获得精确的 IDE 类型提示，直接声明返回类型（需要 type: ignore）。

    Usage:
        # 一对多（返回列表）- 直接声明 list[Order]
        orders: list[Order] = Relationship('orders', foreign_key='user_id')  # type: ignore

        # 多对一（返回单个对象或 None）- 直接声明 User | None
        user: User | None = Relationship('users', foreign_key='user_id')  # type: ignore

        # 自引用（需要显式指定 uselist）
        parent: Category | None = Relationship(  # type: ignore
            'categories', foreign_key='parent_id', uselist=False
        )
        children: list[Category] = Relationship(  # type: ignore
            'categories', foreign_key='parent_id', uselist=True
        )

    Note:
        由于 Python 类型系统限制，描述符的泛型参数无法自动推断返回类型。
        因此推荐直接声明期望的返回类型，并使用 type: ignore 抑制类型警告。
    """

    def __init__(self,
                 target_model: str | type[PureBaseModel],
                 foreign_key: str,
                 lazy: bool = True,
                 back_populates: str | None = None,
                 uselist: bool | None = None):
        """
        初始化关联关系

        Args:
            target_model: 目标模型类或表名（字符串）
            foreign_key: 外键字段名
            lazy: 是否延迟加载
            back_populates: 反向关联的属性名
            uselist: 是否返回列表（None=自动判断，True=强制列表，False=强制单个）
                - 用于自引用等无法自动判断的场景
        """
        self.target_model = target_model
        self.foreign_key = foreign_key
        self.lazy = lazy
        self.back_populates = back_populates
        self._uselist = uselist  # 用户指定的值
        self.is_one_to_many = False  # 自动判断的值
        self.name: str | None = None
        self.owner: type[PureBaseModel] | None = None

    def __set_name__(self, owner: type[PureBaseModel], name: str) -> None:
        """在类定义时调用"""
        self.name = name
        self.owner = owner

        # 判断是一对多还是多对一
        # 如果外键在目标模型中，则是一对多
        # 如果外键在当前模型中，则是多对一
        columns = getattr(owner, '__columns__', {})
        if self.foreign_key in columns:
            self.is_one_to_many = False
        else:
            self.is_one_to_many = True

    @overload
    def __get__(self, instance: None, owner: type[PureBaseModel]) -> 'Relationship[RelationshipT]': ...

    @overload
    def __get__(self, instance: PureBaseModel, owner: type[PureBaseModel]) -> RelationshipT: ...

    def __get__(
        self,
        instance: PureBaseModel | None,
        owner: type[PureBaseModel]
    ) -> 'Relationship[RelationshipT]' | RelationshipT:
        """获取关联对象"""
        if instance is None:
            return self

        # 检查缓存
        cache_key = f'_cached_{self.name}'
        if hasattr(instance, cache_key):
            return cast('Relationship[RelationshipT] | RelationshipT', getattr(instance, cache_key))

        # 延迟加载
        target_model = self._resolve_target_model(owner)

        primary_key = getattr(owner, '__primary_key__', 'id')

        # 确定是否返回列表：优先使用用户指定的 uselist，否则使用自动判断
        use_list = self._uselist if self._uselist is not None else self.is_one_to_many

        if use_list:
            # 一对多：查询外键指向当前实例的所有记录
            pk_value = getattr(instance, primary_key)
            # 使用 filter_by（如果目标模型支持）
            if hasattr(target_model, 'filter_by'):
                results: PureBaseModel | None | list[PureBaseModel] = target_model.filter_by(**{
                    self.foreign_key: pk_value
                }).all()
            else:
                results = []
        else:
            # 多对一：根据外键值查询目标对象
            fk_value = getattr(instance, self.foreign_key)
            if fk_value is None:
                results = None
            elif hasattr(target_model, 'get'):
                results = target_model.get(fk_value)
            else:
                results = None

        # 缓存结果
        setattr(instance, cache_key, results)
        return cast(RelationshipT, results)

    def _resolve_target_model(self, owner: type[PureBaseModel] | None = None) -> type[PureBaseModel]:
        """
        解析目标模型

        Args:
            owner: 所有者类（用于回退，当 self.owner 为 None 时使用）

        Returns:
            解析后的目标模型类
        """
        # 如果不是字符串，直接返回类对象
        if not isinstance(self.target_model, str):
            return self.target_model

        # 使用 owner 或 self.owner
        actual_owner = owner or self.owner
        if actual_owner is None:
            raise ValidationError(
                f"Cannot resolve model '{self.target_model}': owner not set"
            )

        # 优先从 Storage 注册表按表名查找
        storage = getattr(actual_owner, '__storage__', None)
        if storage:
            model = storage._get_model_by_table(self.target_model)
            if model:
                return cast(type[PureBaseModel], model)

        # 回退：从模块命名空间按类名查找（兼容旧用法）
        owner_module = sys.modules.get(actual_owner.__module__)
        if owner_module and hasattr(owner_module, self.target_model):
            return cast(type[PureBaseModel], getattr(owner_module, self.target_model))

        raise ValidationError(
            f"Cannot find model for '{self.target_model}'. "
            f"Use table name (e.g., 'users') or ensure the model class is defined."
        )

    def __repr__(self) -> str:
        return f"Relationship(target={self.target_model}, fk={self.foreign_key})"

# ==================== 工厂函数 ====================

@overload
def declarative_base(
    storage: 'Storage',
    *,
    crud: Literal[False] = ...,
    sync_schema: bool = ...,
    sync_options: SyncOptions | None = ...
) -> type[PureBaseModel]: ...

@overload
def declarative_base(
    storage: 'Storage',
    *,
    crud: Literal[True],
    sync_schema: bool = ...,
    sync_options: SyncOptions | None = ...
) -> type[CRUDBaseModel]: ...

def declarative_base(
    storage: 'Storage',
    *,
    crud: bool = False,
    sync_schema: bool = False,
    sync_options: SyncOptions | None = None
) -> type[PureBaseModel] | type[CRUDBaseModel]:
    """
    创建声明式基类工厂函数

    这是 SQLAlchemy 风格 API，用于创建绑定特定 Storage 的声明式基类。
    所有模型应继承自此函数返回的 Base 类。

    Args:
        storage: Storage 实例，用于绑定数据库连接
        crud: 是否包含 CRUD 方法（默认 False）
            - False: 返回 PureBaseModel 类型（纯模型定义，通过 Session 操作）
            - True: 返回 CRUDBaseModel 类型（Active Record 模式，模型自带 CRUD）
        sync_schema: 是否在表已存在时自动同步 schema（默认 False）
            - False: 表已存在时直接使用，不同步
            - True: 表已存在时自动同步备注、新增列等
        sync_options: 同步选项，控制同步行为（仅当 sync_schema=True 时生效）

    Returns:
        基类类型

    Examples:
        # 纯模型（默认，推荐）
        from pytucky import PureBaseModel

        Base: type[PureBaseModel] = declarative_base(db)

        class User(Base):
            __tablename__ = 'users'
            id = Column(int, primary_key=True)
            name = Column(str)

        # 通过 Session 操作
        session = Session(db)
        user = User(name='Alice')
        session.add(user)
        session.commit()

        # Active Record 模式
        from pytucky import CRUDBaseModel

        Base: type[CRUDBaseModel] = declarative_base(db, crud=True)

        class Post(Base):
            __tablename__ = 'posts'
            id = Column(int, primary_key=True)
            title = Column(str)

        # 直接在模型上操作
        post = Post.create(title='Hello')
        post.title = 'Updated'
        post.save()
        post.delete()

        # 自动同步 schema（第二次启动加载已有数据库时）
        from pytucky import SyncOptions

        Base = declarative_base(db, sync_schema=True)

        # 或自定义同步选项
        opts = SyncOptions(drop_missing_columns=False)
        Base = declarative_base(db, sync_schema=True, sync_options=opts)
    """

    if crud:
        return _create_crud_base(storage, sync_schema, sync_options)
    else:
        return _create_pure_base(storage, sync_schema, sync_options)

def _create_pure_base(
    storage: 'Storage',
    sync_schema: bool = False,
    sync_options: SyncOptions | None = None
) -> type[PureBaseModel]:
    """创建纯模型基类"""

    class DeclarativePureBase(PureBaseModel):
        """声明式纯模型基类"""

        # 类属性
        __abstract__ = True
        __storage__ = storage
        __tablename__: str | None = None
        __table_comment__: str | None = None
        __columns__: dict[str, Column] = {}
        __primary_key__: str | None = None  # None 表示无主键，使用隐式 rowid
        __relationships__: dict[str, Relationship] = {}

        def __init_subclass__(cls, **kwargs: Any):
            """子类初始化时自动收集字段并创建表"""
            super().__init_subclass__(**kwargs)

            # 收集列定义（从 MRO 父类继承 + 当前类自身）
            cls.__columns__ = {}
            cls.__relationships__ = {}

            # 从 MRO 父类继承列（从最基础到最近的父类，子类覆盖父类）
            for parent in reversed(cls.__mro__[1:]):
                parent_columns = parent.__dict__.get('__columns__')
                if parent_columns:
                    cls.__columns__.update(parent_columns)
                parent_rels = parent.__dict__.get('__relationships__')
                if parent_rels:
                    cls.__relationships__.update(parent_rels)

            # 收集当前类直接定义的列和关系（覆盖父类同名定义）
            for attr_name, attr_value in list(cls.__dict__.items()):
                if isinstance(attr_value, Column):
                    cls.__columns__[attr_name] = attr_value
                elif isinstance(attr_value, Relationship):
                    cls.__relationships__[attr_name] = attr_value
                    attr_value.__set_name__(cls, attr_name)

            # 跳过抽象类（列已收集供子类继承，但不创建表）
            if cls.__dict__.get('__abstract__', False):
                return

            # 具体模型：显式标记为非抽象
            cls.__abstract__ = False

            # 子类必须定义 __tablename__（否则应使用 __abstract__ = True 标记为抽象类）
            if not cls.__tablename__:
                raise ValidationError(
                    f"Model {cls.__name__} must define __tablename__. "
                    f"If this is a mixin or abstract class, set __abstract__ = True."
                )

            # 从最终的 __columns__ 中计算主键
            primary_keys = [name for name, col in cls.__columns__.items()
                            if col.primary_key]

            # 验证主键数量：只允许单主键或无主键
            if len(primary_keys) > 1:
                raise SchemaError(
                    f"Model {cls.__name__} has multiple primary keys: {primary_keys}. "
                    f"Pytuckyy only supports single-column primary key or no primary key.",
                    table_name=cls.__tablename__
                )

            # 设置主键（None 表示无主键，使用隐式 rowid）
            cls.__primary_key__ = primary_keys[0] if primary_keys else None

            # 自动创建或同步表
            if cls.__columns__:
                columns_list = list(cls.__columns__.values())
                table_comment = getattr(cls, '__table_comment__', None)
                table_name = cls.__tablename__

                # 检查表是否已存在
                if table_name in storage.tables:
                    # 表已存在，根据 sync_schema 决定是否同步
                    if sync_schema:
                        storage.sync_table_schema(
                            table_name,
                            columns_list,
                            table_comment,
                            sync_options
                        )
                else:
                    # 表不存在，创建新表
                    storage.create_table(table_name, columns_list, table_comment)

                # 注册模型类到 Storage（用于 Relationship 按表名解析）
                storage._register_model(table_name, cls)

        def __init__(self, **kwargs: Any):
            """初始化模型实例"""
            super().__init__(**kwargs)
            for col_name, column in self.__columns__.items():
                if col_name in kwargs:
                    value = column.validate(kwargs[col_name])
                    setattr(self, col_name, value)
                elif column.has_default():
                    setattr(self, col_name, column.resolve_default())
                elif column.nullable or column.primary_key:
                    setattr(self, col_name, None)
                else:
                    raise ValidationError(f"Missing required column '{col_name}'")

        # __setattr__ 继承自 PureBaseModel（实现脏跟踪）

    return DeclarativePureBase  # type: ignore

def _create_crud_base(
    storage: 'Storage',
    sync_schema: bool = False,
    sync_options: SyncOptions | None = None
) -> type[CRUDBaseModel]:
    """创建带 CRUD 方法的模型基类"""
    from .event import event

    class DeclarativeCRUDBase(CRUDBaseModel):
        """声明式 CRUD 模型基类"""

        # 类属性
        __abstract__ = True
        __storage__ = storage
        __tablename__: str | None = None
        __table_comment__: str | None = None
        __columns__: dict[str, Column] = {}
        __primary_key__: str | None = None  # None 表示无主键，使用隐式 rowid
        __relationships__: dict[str, Relationship] = {}

        def __init_subclass__(cls, **kwargs: Any):
            """子类初始化时自动收集字段并创建表"""
            super().__init_subclass__(**kwargs)

            # 收集列定义（从 MRO 父类继承 + 当前类自身）
            cls.__columns__ = {}
            cls.__relationships__ = {}

            # 从 MRO 父类继承列（从最基础到最近的父类，子类覆盖父类）
            for parent in reversed(cls.__mro__[1:]):
                parent_columns = parent.__dict__.get('__columns__')
                if parent_columns:
                    cls.__columns__.update(parent_columns)
                parent_rels = parent.__dict__.get('__relationships__')
                if parent_rels:
                    cls.__relationships__.update(parent_rels)

            # 收集当前类直接定义的列和关系（覆盖父类同名定义）
            for attr_name, attr_value in list(cls.__dict__.items()):
                if isinstance(attr_value, Column):
                    cls.__columns__[attr_name] = attr_value
                elif isinstance(attr_value, Relationship):
                    cls.__relationships__[attr_name] = attr_value
                    attr_value.__set_name__(cls, attr_name)

            # 跳过抽象类（列已收集供子类继承，但不创建表）
            if cls.__dict__.get('__abstract__', False):
                return

            # 具体模型：显式标记为非抽象
            cls.__abstract__ = False

            # 子类必须定义 __tablename__（否则应使用 __abstract__ = True 标记为抽象类）
            if not cls.__tablename__:
                raise ValidationError(
                    f"Model {cls.__name__} must define __tablename__. "
                    f"If this is a mixin or abstract class, set __abstract__ = True."
                )

            # 从最终的 __columns__ 中计算主键
            primary_keys = [name for name, col in cls.__columns__.items()
                            if col.primary_key]

            # 验证主键数量：只允许单主键或无主键
            if len(primary_keys) > 1:
                raise SchemaError(
                    f"Model {cls.__name__} has multiple primary keys: {primary_keys}. "
                    f"Pytuckyy only supports single-column primary key or no primary key.",
                    table_name=cls.__tablename__
                )

            # 设置主键（None 表示无主键，使用隐式 rowid）
            cls.__primary_key__ = primary_keys[0] if primary_keys else None

            # 自动创建或同步表
            if cls.__columns__:
                columns_list = list(cls.__columns__.values())
                table_comment = getattr(cls, '__table_comment__', None)
                table_name = cls.__tablename__

                # 检查表是否已存在
                if table_name in storage.tables:
                    # 表已存在，根据 sync_schema 决定是否同步
                    if sync_schema:
                        storage.sync_table_schema(
                            table_name,
                            columns_list,
                            table_comment,
                            sync_options
                        )
                else:
                    # 表不存在，创建新表
                    storage.create_table(table_name, columns_list, table_comment)

                # 注册模型类到 Storage（用于 Relationship 按表名解析）
                storage._register_model(table_name, cls)

        def __init__(self, **kwargs: Any):
            """初始化模型实例"""
            # CRUD 基类特有：跟踪是否从数据库加载
            super().__init__(**kwargs)
            self._loaded_from_db = False

            for col_name, column in self.__columns__.items():
                if col_name in kwargs:
                    value = column.validate(kwargs[col_name])
                    setattr(self, col_name, value)
                elif column.has_default():
                    setattr(self, col_name, column.resolve_default())
                elif column.nullable or column.primary_key:
                    setattr(self, col_name, None)
                else:
                    raise ValidationError(f"Missing required column '{col_name}'")

        # __setattr__ 继承自 PureBaseModel（通过 CRUDBaseModel）

        # ==================== 实例方法 ====================

        def save(self) -> None:
            """保存记录（insert or update）"""
            table_name = self.__tablename__
            assert table_name is not None, f"Model {self.__class__.__name__} must have __tablename__ defined"

            pk_name = self.__primary_key__

            # 判断是insert还是update
            if pk_name:
                pk_value = getattr(self, pk_name, None)
            else:
                pk_value = getattr(self, '_pytuck_rowid', None)

            if pk_value is None or not self._loaded_from_db:
                # Insert
                event.dispatch_model(self.__class__, 'before_insert', self)

                # 构建数据（在 before 事件之后，确保回调修改生效）
                data = {}
                for attr_name, column in self.__columns__.items():
                    value = getattr(self, attr_name, None)
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value

                pk_value = storage.insert(table_name, data)
                if pk_name:
                    setattr(self, pk_name, pk_value)
                else:
                    setattr(self, '_pytuck_rowid', pk_value)
                self._loaded_from_db = True
                event.dispatch_model(self.__class__, 'after_insert', self)
            else:
                # Update
                event.dispatch_model(self.__class__, 'before_update', self)

                # 构建数据（在 before 事件之后，确保回调修改生效）
                data = {}
                for attr_name, column in self.__columns__.items():
                    value = getattr(self, attr_name, None)
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value

                storage.update(table_name, pk_value, data)
                event.dispatch_model(self.__class__, 'after_update', self)

        def delete(self) -> None:
            """删除当前记录"""
            pk_name = self.__primary_key__
            if pk_name:
                pk_value = getattr(self, pk_name, None)
            else:
                pk_value = getattr(self, '_pytuck_rowid', None)

            if pk_value is None:
                raise ValidationError("Cannot delete record without primary key or rowid")

            table_name = self.__tablename__
            assert table_name is not None, f"Model {self.__class__.__name__} must have __tablename__ defined"

            # 触发 before_delete 事件
            event.dispatch_model(self.__class__, 'before_delete', self)

            storage.delete(table_name, pk_value)
            self._loaded_from_db = False

            # 触发 after_delete 事件
            event.dispatch_model(self.__class__, 'after_delete', self)

        def refresh(self) -> None:
            """从数据库刷新数据"""
            pk_name = self.__primary_key__
            if pk_name:
                pk_value = getattr(self, pk_name, None)
            else:
                pk_value = getattr(self, '_pytuck_rowid', None)

            if pk_value is None:
                raise ValidationError("Cannot refresh record without primary key or rowid")

            table_name = self.__tablename__
            assert table_name is not None, f"Model {self.__class__.__name__} must have __tablename__ defined"

            data = storage.select(table_name, pk_value)

            for db_col_name, value in data.items():
                if db_col_name != PSEUDO_PK_NAME:
                    # 将 Column.name 转换回属性名
                    attr_name = self._column_to_attr_name(db_col_name) or db_col_name
                    setattr(self, attr_name, value)

        # ==================== 类方法 ====================

        @classmethod
        def create(cls, **kwargs: Any) -> 'DeclarativeCRUDBase':
            """创建并保存新记录"""
            instance = cls(**kwargs)
            instance.save()
            return instance

        @classmethod
        def bulk_insert(cls, instances: list['DeclarativeCRUDBase']) -> list[Any]:
            """
            批量插入实例（立即写入内存）

            Args:
                instances: 模型实例列表

            Returns:
                插入的主键列表
            """
            if not instances:
                return []

            # 触发 before_bulk_insert 事件
            event.dispatch_model_bulk(cls, 'before_bulk_insert', instances)

            # 构建数据字典列表
            records: list[dict[str, Any]] = []
            for instance in instances:
                data: dict[str, Any] = {}
                for attr_name, column in cls.__columns__.items():
                    value = getattr(instance, attr_name, None)
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value
                records.append(data)

            table_name = cls.__tablename__
            assert table_name is not None, f"Model {cls.__name__} must have __tablename__ defined"

            # 批量插入到 Storage
            pks = storage.bulk_insert(table_name, records)

            # 设置主键到实例
            pk_name = cls.__primary_key__
            for i, instance in enumerate(instances):
                pk = pks[i]
                if pk_name:
                    setattr(instance, pk_name, pk)
                else:
                    setattr(instance, '_pytuck_rowid', pk)
                instance._loaded_from_db = True

            # 触发 after_bulk_insert 事件
            event.dispatch_model_bulk(cls, 'after_bulk_insert', instances)

            return pks

        @classmethod
        def bulk_update(cls, instances: list['DeclarativeCRUDBase']) -> int:
            """
            批量更新实例（立即写入内存，更新全部字段）

            Args:
                instances: 模型实例列表（必须已有主键）

            Returns:
                更新的记录数
            """
            if not instances:
                return 0

            # 触发 before_bulk_update 事件
            event.dispatch_model_bulk(cls, 'before_bulk_update', instances)

            table_name = cls.__tablename__
            assert table_name is not None, f"Model {cls.__name__} must have __tablename__ defined"
            pk_name = cls.__primary_key__

            # 构建 (pk, data) 元组列表
            updates: list[tuple[Any, dict[str, Any]]] = []
            for instance in instances:
                if pk_name:
                    pk = getattr(instance, pk_name, None)
                else:
                    pk = getattr(instance, '_pytuck_rowid', None)

                if pk is None:
                    raise ValidationError(
                        "Cannot bulk update instance without primary key or rowid"
                    )

                data: dict[str, Any] = {}
                for attr_name, column in cls.__columns__.items():
                    value = getattr(instance, attr_name, None)
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value
                updates.append((pk, data))

            # 批量更新到 Storage
            count = storage.bulk_update(table_name, updates)

            # 触发 after_bulk_update 事件
            event.dispatch_model_bulk(cls, 'after_bulk_update', instances)

            return count

        @classmethod
        def get(cls, pk: Any) -> 'DeclarativeCRUDBase' | None:
            """根据主键获取记录

            注意：无主键模型无法使用此方法，会返回 None。
            """
            # 无主键模型不支持 get()
            if cls.__primary_key__ is None:
                return None

            try:
                table_name = cls.__tablename__
                assert table_name is not None, f"Model {cls.__name__} must have __tablename__ defined"

                data = storage.select(table_name, pk)
                # 将 Column.name 转换为属性名
                attr_data = {}
                for db_col_name, value in data.items():
                    if db_col_name == PSEUDO_PK_NAME:
                        continue
                    attr_name = cls._column_to_attr_name(db_col_name) or db_col_name
                    attr_data[attr_name] = value
                instance = cls(**attr_data)
                instance._loaded_from_db = True
                return instance
            except Exception:
                return None

        @classmethod
        def filter(cls, *expressions: 'BinaryExpression') -> 'Query':
            """
            条件查询（表达式语法）

            Args:
                *expressions: BinaryExpression 对象

            Returns:
                Query 对象

            Example:
                users = User.filter(User.age >= 18).all()
            """
            from ..query import Query
            query = Query(cls)
            if expressions:
                query = query.filter(*expressions)
            return query

        @classmethod
        def filter_by(cls, **kwargs: Any) -> 'Query':
            """
            简单等值查询

            Args:
                **kwargs: 字段名=值 的等值条件

            Returns:
                Query 对象

            Example:
                users = User.filter_by(name='Alice').all()
            """
            from ..query import Query
            query = Query(cls)
            if kwargs:
                query = query.filter_by(**kwargs)
            return query

        @classmethod
        def all(cls) -> list['DeclarativeCRUDBase']:  # type: ignore[override]
            """获取所有记录"""
            from ..query import Query
            return Query(cls).all()

    return DeclarativeCRUDBase  # type: ignore
