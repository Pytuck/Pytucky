"""
Result - 查询结果包装器

提供简洁的查询结果处理接口，直接返回模型实例。
"""

from typing import Any, Dict, List, Optional, Type, Generic, TYPE_CHECKING

from ..common.typing import T
from ..common.exceptions import QueryError, UnsupportedOperationError
from ..core.orm import PSEUDO_PK_NAME

if TYPE_CHECKING:
    from ..core.session import Session


class _ScalarResult(Generic[T]):
    """
    内部类：标量结果处理器。

    负责将查询结果字典转换为模型实例，并处理 identity map。
    """

    def __init__(self, records: List[Dict[str, Any]], model_class: Type[T], session: Optional['Session'] = None) -> None:
        self._records = records
        self._model_class = model_class
        self._session = session

    def _create_instance(self, record: Dict[str, Any]) -> T:
        """创建模型实例并处理 identity map"""
        # 将 Column.name 映射为模型属性名
        mapped: Dict[str, Any] = {}
        rowid = None
        for db_col_name, value in record.items():
            if db_col_name == PSEUDO_PK_NAME:
                rowid = value
            else:
                # 使用模型的 _column_to_attr_name 方法转换
                attr_name = self._model_class._column_to_attr_name(db_col_name) or db_col_name
                mapped[attr_name] = value

        pk_name = getattr(self._model_class, '__primary_key__', None)

        if self._session:
            if pk_name:
                # 有主键：使用主键查找 identity map
                pk_value = mapped.get(pk_name)
                if pk_value is not None:
                    existing = self._session._get_from_identity_map(self._model_class, pk_value)
                    if existing is not None:
                        # 刷新实例属性以保持与存储同步
                        for key, value in mapped.items():
                            setattr(existing, key, value)
                        return existing
            elif rowid is not None:
                # 无主键：使用 rowid 查找 identity map
                identity_key = (self._model_class, (PSEUDO_PK_NAME, rowid))
                existing = self._session._identity_map.get(identity_key)  # type: ignore
                if existing is not None:
                    # 刷新实例属性以保持与存储同步
                    for key_name, value in mapped.items():
                        setattr(existing, key_name, value)
                    return existing

            # 创建新实例
            instance = self._model_class(**mapped)

            # 对于无主键模型，设置内部 rowid
            if rowid is not None and pk_name is None:
                setattr(instance, '_pytuck_rowid', rowid)

            # 注册到 identity map
            self._session._register_instance(instance)
            return instance
        else:
            # 没有 session，直接创建实例
            new_instance: T = self._model_class(**mapped)
            # 对于无主键模型，设置内部 rowid
            if rowid is not None and pk_name is None:
                setattr(new_instance, '_pytuck_rowid', rowid)
            return new_instance

    def all(self) -> List[T]:
        """返回所有模型实例"""
        instances: List[T] = []
        for record in self._records:
            instance = self._create_instance(record)
            instances.append(instance)
        return instances

    def first(self) -> Optional[T]:
        """返回第一个模型实例"""
        if not self._records:
            return None
        return self._create_instance(self._records[0])

    def one(self) -> T:
        """返回唯一的模型实例（必须恰好一条）"""
        if len(self._records) == 0:
            raise QueryError("Expected one result, got 0")
        if len(self._records) > 1:
            raise QueryError(f"Expected one result, got {len(self._records)}")
        return self._create_instance(self._records[0])

    def one_or_none(self) -> Optional[T]:
        """返回唯一的模型实例或 None（最多一条）"""
        if len(self._records) == 0:
            return None
        if len(self._records) > 1:
            raise QueryError(f"Expected at most one result, got {len(self._records)}")
        return self._create_instance(self._records[0])


class Result(Generic[T]):
    """
    SELECT 查询结果包装器。

    直接返回模型实例，提供简洁统一的 API：
    - all(): 返回所有结果为模型实例列表
    - first(): 返回第一个结果为模型实例
    - one(): 返回唯一结果为模型实例（必须恰好一条）
    - one_or_none(): 返回唯一结果或 None（最多一条）
    - rowcount(): 返回结果数量

    Example:
        result = session.execute(select(User).where(User.age >= 18))

        users = result.all()          # List[User]
        user = result.first()         # Optional[User]
        user = result.one()           # User（必须恰好一条）
        user = result.one_or_none()   # Optional[User]（最多一条）
        count = result.rowcount()     # int

    Attributes:
        _records: 查询结果字典列表
        _model_class: 模型类
        _operation: 操作类型 ('select', 'insert', 'update', 'delete')
        _session: Session 实例，用于 identity map 管理
    """

    def __init__(self, records: List[Dict[str, Any]], model_class: Type[T], operation: str = 'select', session: Optional['Session'] = None, options: Optional[List[Any]] = None) -> None:
        """
        Args:
            records: 查询结果（字典列表）
            model_class: 模型类
            operation: 操作类型 ('select', 'insert', 'update', 'delete')
            session: Session 实例，用于 identity map 管理
            options: 查询选项列表（如 PrefetchOption）
        """
        self._records = records
        self._model_class = model_class
        self._operation = operation
        self._session = session
        self._scalar_result = _ScalarResult(records, model_class, session)
        self._options: List[Any] = options or []

    def all(self) -> List[T]:
        """返回所有结果为模型实例列表"""
        if self._operation != 'select':
            raise UnsupportedOperationError("all() not supported for non-select operations")
        instances = self._scalar_result.all()
        # 执行预取选项
        self._apply_prefetch(instances)
        return instances

    def first(self) -> Optional[T]:
        """返回第一个结果为模型实例"""
        if self._operation != 'select':
            raise UnsupportedOperationError("first() not supported for non-select operations")
        return self._scalar_result.first()

    def one(self) -> T:
        """返回唯一的结果为模型实例（必须恰好一条）"""
        if self._operation != 'select':
            raise UnsupportedOperationError("one() not supported for non-select operations")
        return self._scalar_result.one()

    def one_or_none(self) -> Optional[T]:
        """返回唯一的结果为模型实例或 None（最多一条）"""
        if self._operation != 'select':
            raise UnsupportedOperationError("one_or_none() not supported for non-select operations")
        return self._scalar_result.one_or_none()

    def rowcount(self) -> int:
        """返回结果数量"""
        return len(self._records)

    def _apply_prefetch(self, instances: List[T]) -> None:
        """
        对查询结果执行预取选项

        Args:
            instances: 模型实例列表
        """
        if not instances or not self._options:
            return
        from ..core.prefetch import _do_prefetch, PrefetchOption
        for opt in self._options:
            if isinstance(opt, PrefetchOption):
                _do_prefetch(instances, *opt.rel_names)


class CursorResult(Result[T]):
    """
    CUD（Create/Update/Delete）操作结果包装器。

    用于 INSERT、UPDATE、DELETE 操作，提供：
    - rowcount(): 受影响的行数
    - inserted_primary_key: 插入记录的主键（仅 INSERT）

    注意：all()、first()、one()、one_or_none() 方法不可用于 CUD 操作。

    Example:
        # INSERT
        result = session.execute(insert(User).values(name='Alice'))
        new_id = result.inserted_primary_key
        count = result.rowcount()

        # UPDATE/DELETE
        result = session.execute(update(User).where(...).values(...))
        affected = result.rowcount()

    Attributes:
        _affected_rows: 受影响的行数
        _inserted_pk: 插入的主键（仅 INSERT）
    """

    def __init__(self, affected_rows: int, model_class: Type[T], operation: str, inserted_pk: Any = None) -> None:
        """
        Args:
            affected_rows: 受影响的行数
            model_class: 模型类
            operation: 操作类型
            inserted_pk: 插入的主键（仅 INSERT）
        """
        super().__init__([], model_class, operation)
        self._affected_rows = affected_rows
        self._inserted_pk = inserted_pk

    def rowcount(self) -> int:
        """返回受影响的行数"""
        return self._affected_rows

    @property
    def inserted_primary_key(self) -> Any:
        """返回插入的主键（仅 INSERT）"""
        return self._inserted_pk

    def all(self) -> List[T]:
        raise UnsupportedOperationError(f"all() not supported for {self._operation} operation")

    def first(self) -> Optional[T]:
        raise UnsupportedOperationError(f"first() not supported for {self._operation} operation")

    def one(self) -> T:
        raise UnsupportedOperationError(f"one() not supported for {self._operation} operation")

    def one_or_none(self) -> Optional[T]:
        raise UnsupportedOperationError(f"one_or_none() not supported for {self._operation} operation")
