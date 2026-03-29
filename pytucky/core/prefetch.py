"""
Pytuck 关系预取 API

提供批量预取关联数据功能，解决 Relationship 的 N+1 查询问题。

两种使用方式：

1. 独立函数（对已获取的实例列表批量预取）：
    from pytuck import prefetch

    users = session.execute(select(User)).all()
    prefetch(users, 'orders')          # 单次查询加载所有用户的 orders
    prefetch(users, 'orders', 'profile')  # 支持多个关系名

2. 查询选项（集成到 Select 链式调用）：
    stmt = select(User).options(prefetch('orders'))
    result = session.execute(stmt)
    users = result.all()  # all() 返回后，orders 已批量加载
"""

from typing import Any, Dict, List, Optional, Sequence, Type, Union, overload, TYPE_CHECKING

from ..query.builder import Condition
from .orm import PureBaseModel, Relationship, PSEUDO_PK_NAME

if TYPE_CHECKING:
    from .storage import Storage


class PrefetchOption:
    """
    预取选项（用于 Select.options）

    Example:
        stmt = select(User).options(PrefetchOption('orders'))
    """

    def __init__(self, *rel_names: str) -> None:
        """
        Args:
            rel_names: 要预取的关系属性名
        """
        self.rel_names: tuple = rel_names

    def __repr__(self) -> str:
        return f"PrefetchOption({', '.join(repr(n) for n in self.rel_names)})"


def prefetch(
    *args: Any
) -> Union[None, PrefetchOption]:
    """
    批量预取关联数据 / 创建预取选项

    两种调用方式：

    1. 独立函数（第一个参数是实例序列）：
        prefetch(users, 'orders')
        prefetch(users, 'orders', 'profile')

    2. 查询选项（第一个参数是字符串）：
        select(User).options(prefetch('orders'))
        select(User).options(prefetch('orders', 'profile'))

    Args:
        *args: 实例序列 + 关系名，或仅关系名字符串

    Returns:
        None（直接执行预取时）或 PrefetchOption（用于 Select.options 时）
    """
    if not args:
        raise ValueError("prefetch() requires at least one argument")

    # 判断调用方式
    if isinstance(args[0], str):
        # 查询选项模式：prefetch('orders', 'profile')
        return PrefetchOption(*args)
    else:
        # 直接执行模式：prefetch(users, 'orders', 'profile')
        instances = args[0]
        rel_names = args[1:]
        if not rel_names:
            raise ValueError(
                "prefetch() requires at least one relationship name. "
                "Usage: prefetch(instances, 'rel_name1', 'rel_name2', ...)"
            )
        for name in rel_names:
            if not isinstance(name, str):
                raise TypeError(f"Relationship name must be str, got {type(name).__name__}")
        _do_prefetch(instances, *rel_names)
        return None


def _do_prefetch(instances: Sequence[PureBaseModel], *rel_names: str) -> None:
    """
    执行批量预取

    Args:
        instances: 模型实例列表（必须为同一模型类）
        *rel_names: 要预取的关系属性名
    """
    if not instances:
        return

    owner_class = type(instances[0])

    for rel_name in rel_names:
        # 获取 Relationship 描述符
        rel = owner_class.__relationships__.get(rel_name)
        if rel is None:
            raise ValueError(
                f"'{owner_class.__name__}' has no relationship '{rel_name}'. "
                f"Available relationships: {list(owner_class.__relationships__.keys())}"
            )
        _prefetch_relationship(instances, rel, rel_name)


def _prefetch_relationship(
    instances: Sequence[PureBaseModel],
    rel: 'Relationship[Any]',
    rel_name: str
) -> None:
    """
    对单个 relationship 执行批量预取

    Args:
        instances: 模型实例列表
        rel: Relationship 描述符
        rel_name: 关系属性名
    """
    owner_class = type(instances[0])
    target_model = rel._resolve_target_model(owner_class)
    storage: Optional['Storage'] = getattr(owner_class, '__storage__', None)
    if storage is None:
        raise ValueError(f"Model '{owner_class.__name__}' is not bound to a Storage")

    target_table: Optional[str] = getattr(target_model, '__tablename__', None)
    if target_table is None:
        raise ValueError(f"Target model has no __tablename__")

    primary_key = getattr(owner_class, '__primary_key__', None) or '_pytuck_rowid'
    use_list = rel._uselist if rel._uselist is not None else rel.is_one_to_many
    cache_key = f'_cached_{rel_name}'

    if use_list:
        # 一对多
        _prefetch_one_to_many(
            instances, storage, target_model, target_table,
            primary_key, rel.foreign_key, cache_key
        )
    else:
        # 多对一
        _prefetch_many_to_one(
            instances, storage, target_model, target_table,
            rel.foreign_key, cache_key
        )


def _prefetch_one_to_many(
    instances: Sequence[PureBaseModel],
    storage: 'Storage',
    target_model: Type[PureBaseModel],
    target_table: str,
    pk_attr: str,
    foreign_key: str,
    cache_key: str
) -> None:
    """
    一对多批量预取

    收集所有 owner 的主键值，执行一次 IN 查询，
    按外键分组结果写入各实例缓存。

    Args:
        instances: owner 实例列表
        storage: Storage 实例
        target_model: 目标模型类
        target_table: 目标表名
        pk_attr: owner 主键属性名
        foreign_key: 目标表中的外键字段名
        cache_key: 缓存属性名
    """
    # 1. 收集 owner 主键值
    pk_values: List[Any] = []
    for inst in instances:
        pk_val = getattr(inst, pk_attr, None)
        if pk_val is not None:
            pk_values.append(pk_val)

    if not pk_values:
        for inst in instances:
            setattr(inst, cache_key, [])
        return

    # 2. 单次批量查询
    condition = Condition(foreign_key, 'IN', pk_values)
    records = storage.query(target_table, [condition])

    # 3. 将 records 转为 target_model 实例并按外键分组
    grouped: Dict[Any, List[PureBaseModel]] = {}
    for record in records:
        fk_val = record.get(foreign_key)
        target_instance = _record_to_instance(target_model, record)
        grouped.setdefault(fk_val, []).append(target_instance)

    # 4. 写入各 owner 的缓存
    for inst in instances:
        pk_val = getattr(inst, pk_attr, None)
        setattr(inst, cache_key, grouped.get(pk_val, []))


def _prefetch_many_to_one(
    instances: Sequence[PureBaseModel],
    storage: 'Storage',
    target_model: Type[PureBaseModel],
    target_table: str,
    foreign_key: str,
    cache_key: str
) -> None:
    """
    多对一批量预取

    收集所有实例的外键值（去重、去 None），执行一次 IN 查询，
    按目标主键映射结果写入各实例缓存。

    Args:
        instances: 实例列表
        storage: Storage 实例
        target_model: 目标模型类
        target_table: 目标表名
        foreign_key: 当前模型中的外键属性名
        cache_key: 缓存属性名
    """
    # 1. 收集外键值（去重去 None）
    fk_set = set()
    for inst in instances:
        fk_val = getattr(inst, foreign_key, None)
        if fk_val is not None:
            fk_set.add(fk_val)

    fk_values: List[Any] = list(fk_set)

    if not fk_values:
        for inst in instances:
            setattr(inst, cache_key, None)
        return

    # 2. 查询目标表
    target_pk: Optional[str] = getattr(target_model, '__primary_key__', None)
    if target_pk is None:
        raise ValueError("Many-to-one prefetch requires target model to have a primary key")

    # 获取数据库列名（Column.name 可能与属性名不同）
    target_pk_column = target_model.__columns__[target_pk]
    target_pk_col_name: str = target_pk_column.name if target_pk_column.name else target_pk

    condition = Condition(target_pk_col_name, 'IN', fk_values)
    records = storage.query(target_table, [condition])

    # 3. 按主键建立映射
    pk_map: Dict[Any, PureBaseModel] = {}
    for record in records:
        target_instance = _record_to_instance(target_model, record)
        pk_val = getattr(target_instance, target_pk)
        pk_map[pk_val] = target_instance

    # 4. 写入缓存
    for inst in instances:
        fk_val = getattr(inst, foreign_key, None)
        setattr(inst, cache_key, pk_map.get(fk_val))


def _record_to_instance(
    model_class: Type[PureBaseModel],
    record: Dict[str, Any]
) -> PureBaseModel:
    """
    将记录字典转换为模型实例

    参考 _ScalarResult._create_instance 的简化版本，
    处理 Column.name 到属性名的映射。

    Args:
        model_class: 模型类
        record: 记录字典（键为数据库列名）

    Returns:
        模型实例
    """
    mapped: Dict[str, Any] = {}
    for db_col_name, value in record.items():
        if db_col_name == PSEUDO_PK_NAME:
            continue
        attr_name = model_class._column_to_attr_name(db_col_name) or db_col_name
        mapped[attr_name] = value
    return model_class(**mapped)
