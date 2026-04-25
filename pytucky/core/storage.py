"""
Pytucky 存储引擎

提供数据存储和查询功能
"""

from __future__ import annotations

import copy
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Concatenate, Generator, Iterator, ParamSpec, TYPE_CHECKING, Sequence, TypeVar
from contextlib import contextmanager

from ..common.options import PytuckBackendOptions, SyncOptions, SyncResult
from .orm import Column, PSEUDO_PK_NAME
from .index import BaseIndex, HashIndex, SortedIndex
from .event import event
from ..query import Condition, CompositeCondition, ConditionType
from ..common.exceptions import (
    TableNotFoundError,
    RecordNotFoundError,
    DuplicateKeyError,
    ColumnNotFoundError,
    TransactionError,
    ValidationError,
    SchemaError
)

if TYPE_CHECKING:
    from ..backends.base import StorageBackend
    from .orm import PureBaseModel

P = ParamSpec("P")
R = TypeVar("R")


def _storage_locked(
    method: Callable[Concatenate["Storage", P], R],
) -> Callable[Concatenate["Storage", P], R]:
    @wraps(method)
    def wrapper(self: "Storage", *args: P.args, **kwargs: P.kwargs) -> R:
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper

class TransactionSnapshot:
    """
    事务快照类

    用于存储事务开始时的数据状态，支持回滚操作。
    采用深拷贝策略确保数据隔离。
    """

    def __init__(self, tables: dict[str, 'Table']):
        """
        创建快照

        Args:
            tables: 当前所有表的字典 {table_name: Table}
        """
        self.table_snapshots: dict[str, dict] = {}

        # 深拷贝所有表的关键状态
        for table_name, table in tables.items():
            # 额外保存 lazy 相关运行时元数据，避免回滚后丢失按需加载能力
            pk_offsets = copy.deepcopy(table._pk_offsets)
            data_file = copy.deepcopy(table._data_file)
            backend_ref = table._backend
            lazy_loaded = table._lazy_loaded

            self.table_snapshots[table_name] = {
                'data': copy.deepcopy(table.data),
                'indexes': copy.deepcopy(table.indexes),
                'next_id': table.next_id,
                'pk_offsets': pk_offsets,
                '_lazy_loaded': lazy_loaded,
                '_data_file': data_file,
                '_backend': backend_ref,
                '_data_dirty': table._data_dirty,
                '_schema_dirty': table._schema_dirty,
                'inserted': copy.deepcopy(getattr(table, 'inserted', set())),
                'updated': copy.deepcopy(getattr(table, 'updated', set())),
                'deleted': copy.deepcopy(getattr(table, 'deleted', set())),
            }

    def restore(self, tables: dict[str, 'Table']) -> None:
        """
        恢复快照到表对象

        Args:
            tables: 要恢复的表字典
        """
        for table_name, snapshot in self.table_snapshots.items():
            if table_name in tables:
                table = tables[table_name]
                # 直接替换引用（快照已经是深拷贝）
                table.data = snapshot['data']
                table.indexes = snapshot['indexes']
                table.next_id = snapshot['next_id']

                # 恢复 lazy 相关运行时元数据，确保回滚后仍可按需加载
                table._pk_offsets = copy.deepcopy(snapshot.get('pk_offsets'))
                table._lazy_loaded = bool(snapshot.get('_lazy_loaded', False))
                table._data_file = copy.deepcopy(snapshot.get('_data_file'))
                table._backend = snapshot.get('_backend')
                table._data_dirty = bool(snapshot.get('_data_dirty', False))
                table._schema_dirty = bool(snapshot.get('_schema_dirty', False))
                # 恢复显式的 dirty PK 集合
                table.inserted = set(snapshot.get('inserted', set()))
                table.updated = set(snapshot.get('updated', set()))
                table.deleted = set(snapshot.get('deleted', set()))

class Table:
    """表管理"""

    def __init__(
        self,
        name: str,
        columns: list[Column],
        primary_key: str | None = None,
        comment: str | None = None
    ):
        """
        初始化表

        Args:
            name: 表名
            columns: 列定义列表
            primary_key: 主键字段名（None 表示无主键，使用隐式 rowid）
            comment: 表备注/注释
        """
        self.name = name
        self.columns: dict[str, Column] = {}
        for col in columns:
            assert col.name is not None, "Column name must be set"
            self.columns[col.name] = col
        self.primary_key = primary_key  # None 表示无主键
        self.comment = comment
        self.data: dict[Any, dict[str, Any]] = {}  # {pk: record}
        self.indexes: dict[str, BaseIndex] = {}  # {column_name: BaseIndex}
        self.next_id = 1

        # 脏标记（用于增量保存优化）
        self._data_dirty: bool = False    # 数据是否被修改（insert/update/delete）
        self._schema_dirty: bool = False  # 结构是否被修改（add_column/drop_column 等）

        # 懒加载支持
        self._pk_offsets: dict[Any, int] | None = None  # {pk: file_offset}
        self._data_file: Path | None = None  # 数据文件路径
        self._backend: StorageBackend | None = None  # PTK7 后端引用（用于读取记录）
        self._lazy_loaded: bool = False  # 是否为懒加载模式

        # 在 runtime 维护显式的 dirty PK 集合（用于 PTK7 lazy fast-path）
        # semantics:
        # - inserted: 新插入且未刷盘的 PK
        # - updated: 在磁盘已有记录上做的更新（不包含 inserted PK）
        # - deleted: 从磁盘或内存删除的 PK（对 inserted 则直接移除 inserted）
        self.inserted: set = set()
        self.updated: set = set()
        self.deleted: set = set()

        # 自动为标记了index的列创建索引
        for col in columns:
            if col.index:
                assert col.name is not None, "Column name must be set"
                self.build_index(col.name)

    @property
    def is_dirty(self) -> bool:
        """表是否有任何变更（数据或结构）"""
        return self._data_dirty or self._schema_dirty

    @property
    def record_count(self) -> int:
        """返回表中的真实记录数（包含懒加载未入内存的记录）"""
        return len(self.all_pks())

    def reset_dirty(self) -> None:
        """重置脏标记（由 Storage.flush 在保存完成后调用）"""
        self._data_dirty = False
        self._schema_dirty = False
        # 清空显式维护的 dirty PK 集合
        self.inserted.clear()
        self.updated.clear()
        self.deleted.clear()

    def _normalize_pk(self, pk: Any) -> Any:
        """
        将主键值转换为正确的类型

        Args:
            pk: 原始主键值

        Returns:
            类型转换后的主键值
        """
        if pk is None:
            return None

        if self.primary_key and self.primary_key in self.columns:
            pk_column = self.columns[self.primary_key]
            return pk_column.validate(pk)

        return pk

    def insert(self, record: dict[str, Any]) -> Any:
        """
        插入记录

        Args:
            record: 记录字典

        Returns:
            主键值（用户主键或隐式 rowid）

        Raises:
            DuplicateKeyError: 主键重复
        """
        # 处理主键
        if self.primary_key and self.primary_key in self.columns:
            # 有用户主键
            pk = record.get(self.primary_key)
            # 转换主键类型
            pk = self._normalize_pk(pk)
            if pk is not None:
                # 将转换后的 pk 写回 record
                record[self.primary_key] = pk
            if pk is None:
                # 自动生成主键（仅支持int类型）
                pk_column = self.columns[self.primary_key]
                if pk_column.col_type == int:
                    pk = self.next_id
                    self.next_id += 1
                    record[self.primary_key] = pk
                else:
                    raise ValidationError(
                        f"Primary key '{self.primary_key}' must be provided",
                        table_name=self.name,
                        column_name=self.primary_key
                    )
            else:
                # 检查主键是否已存在
                if self.has_pk(pk):
                    raise DuplicateKeyError(self.name, pk)
        else:
            # 无用户主键：使用内部 rowid
            pk = self.next_id
            self.next_id += 1
            # 不将 pk 写入 record（隐式主键不作为列存在）

        # 验证和处理所有字段
        validated_record = {}
        for col_name, column in self.columns.items():
            value = record.get(col_name)
            validated_value = column.validate(value)
            validated_record[col_name] = validated_value

        # 存储记录
        self.data[pk] = validated_record

        # 更新索引
        for col_name, index in self.indexes.items():
            value = validated_record.get(col_name)
            if value is not None:
                index.insert(value, pk)

        # 更新next_id
        if isinstance(pk, int) and pk >= self.next_id:
            self.next_id = pk + 1

        # 维护 dirty PK 集合：新插入的 PK 归入 inserted
        self.inserted.add(pk)
        # 如果之前被标记为 deleted 或 updated，清理它们
        self.deleted.discard(pk)
        self.updated.discard(pk)

        self._data_dirty = True
        return pk

    def update(self, pk: Any, record: dict[str, Any]) -> None:
        """
        更新记录

        Args:
            pk: 主键值
            record: 新数据

        Raises:
            RecordNotFoundError: 记录不存在
        """
        # 转换主键类型
        pk = self._normalize_pk(pk)
        if pk not in self.data:
            if not self.has_pk(pk):
                raise RecordNotFoundError(self.name, pk)
            self.data[pk] = self.get(pk)

        old_record = self.data[pk]

        # 验证和处理字段
        validated_record = old_record.copy()
        for col_name, value in record.items():
            if col_name in self.columns:
                column = self.columns[col_name]
                validated_record[col_name] = column.validate(value)

        # 更新索引（先删除旧值，再插入新值）
        for col_name, index in self.indexes.items():
            old_value = old_record.get(col_name)
            new_value = validated_record.get(col_name)

            if old_value != new_value:
                if old_value is not None:
                    index.remove(old_value, pk)
                if new_value is not None:
                    index.insert(new_value, pk)

        # 存储记录
        self.data[pk] = validated_record
        # 维护 dirty PK 集合：如果是新插入保留 inserted，否则视为 updated
        if pk in self.inserted:
            # 已在 inserted 中，保持 inserted 不变
            pass
        else:
            self.updated.add(pk)
        # 删除可能的 deleted 标记
        self.deleted.discard(pk)
        self._data_dirty = True

    def delete(self, pk: Any) -> None:
        """
        删除记录

        Args:
            pk: 主键值

        Raises:
            RecordNotFoundError: 记录不存在
        """
        # 转换主键类型
        pk = self._normalize_pk(pk)
        if pk not in self.data:
            if not self.has_pk(pk):
                raise RecordNotFoundError(self.name, pk)
            self.data[pk] = self.get(pk)

        record = self.data[pk]

        # 更新索引
        for col_name, index in self.indexes.items():
            value = record.get(col_name)
            if value is not None:
                index.remove(value, pk)

        # 删除记录
        del self.data[pk]
        if self._pk_offsets is not None and pk in self._pk_offsets:
            del self._pk_offsets[pk]
        # 维护 dirty PK 集合：如果是刚插入的，移除 inserted；否则标记为 deleted 并清除 updated
        if pk in self.inserted:
            self.inserted.discard(pk)
        else:
            self.deleted.add(pk)
            self.updated.discard(pk)
        self._data_dirty = True

    def bulk_insert(self, records: list[dict[str, Any]]) -> list[Any]:
        """
        批量插入记录

        优化点：批量分配主键、批量验证字段、批量更新索引。

        Args:
            records: 记录字典列表

        Returns:
            插入的主键列表

        Raises:
            DuplicateKeyError: 主键重复
            ValidationError: 字段验证失败
        """
        if not records:
            return []

        pks: list[Any] = []

        # 第一阶段：批量分配主键
        has_user_pk = self.primary_key and self.primary_key in self.columns
        if has_user_pk:
            pk_column = self.columns[self.primary_key]  # type: ignore[index]
            auto_count = 0
            # 先统计需要自动分配主键的数量
            for record in records:
                pk = record.get(self.primary_key)  # type: ignore[arg-type]
                if pk is None:
                    if pk_column.col_type == int:
                        auto_count += 1
                    else:
                        raise ValidationError(
                            f"Primary key '{self.primary_key}' must be provided",
                            table_name=self.name,
                            column_name=self.primary_key
                        )
            # 一次性预留主键范围
            start_id = self.next_id
            self.next_id += auto_count
            auto_idx = 0

            for record in records:
                pk = record.get(self.primary_key)  # type: ignore[arg-type]
                pk = self._normalize_pk(pk)
                if pk is None:
                    pk = start_id + auto_idx
                    auto_idx += 1
                    record[self.primary_key] = pk  # type: ignore[index]
                else:
                    record[self.primary_key] = pk  # type: ignore[index]
                    if self.has_pk(pk):
                        raise DuplicateKeyError(self.name, pk)
                # 检查已分配的主键是否与前面的冲突
                if self.has_pk(pk):
                    raise DuplicateKeyError(self.name, pk)
                pks.append(pk)
        else:
            # 无用户主键：批量分配 rowid
            start_id = self.next_id
            self.next_id += len(records)
            for i in range(len(records)):
                pks.append(start_id + i)

        # 检查批次内主键无重复
        if len(set(pks)) != len(pks):
            # 找出重复的主键
            seen: dict[Any, int] = {}
            for pk in pks:
                if pk in seen:
                    raise DuplicateKeyError(self.name, pk)
                seen[pk] = 1

        # 第二阶段：批量验证字段并存储记录
        for i, record in enumerate(records):
            pk = pks[i]
            validated_record: dict[str, Any] = {}
            for col_name, column in self.columns.items():
                value = record.get(col_name)
                validated_value = column.validate(value)
                validated_record[col_name] = validated_value
            self.data[pk] = validated_record
            # 维护 dirty 集合：这些都是插入
            self.inserted.add(pk)
            self.deleted.discard(pk)
            self.updated.discard(pk)

        # 第三阶段：批量更新索引
        for col_name, index in self.indexes.items():
            for i, pk in enumerate(pks):
                value = self.data[pk].get(col_name)
                if value is not None:
                    index.insert(value, pk)

        # 更新 next_id（处理手动指定的大主键）
        for pk in pks:
            if isinstance(pk, int) and pk >= self.next_id:
                self.next_id = pk + 1

        self._data_dirty = True
        return pks

    def bulk_update(self, updates: list[tuple[Any, dict[str, Any]]]) -> int:
        """
        批量更新记录

        Args:
            updates: (pk, data) 元组列表

        Returns:
            更新的记录数

        Raises:
            RecordNotFoundError: 记录不存在
            ValidationError: 字段验证失败
        """
        if not updates:
            return 0

        count = 0

        for pk, record in updates:
            pk = self._normalize_pk(pk)
            if pk not in self.data:
                # 如果在懒加载模式且 pk 在磁盘上，先从文件加载
                if not self.has_pk(pk):
                    raise RecordNotFoundError(self.name, pk)
                self.data[pk] = self.get(pk)

            old_record = self.data[pk]

            # 验证和处理字段
            validated_record = old_record.copy()
            for col_name, value in record.items():
                if col_name in self.columns:
                    column = self.columns[col_name]
                    validated_record[col_name] = column.validate(value)

            # 更新索引（先删除旧值，再插入新值）
            for col_name, index in self.indexes.items():
                old_value = old_record.get(col_name)
                new_value = validated_record.get(col_name)

                if old_value != new_value:
                    if old_value is not None:
                        index.remove(old_value, pk)
                    if new_value is not None:
                        index.insert(new_value, pk)

            # 存储记录
            self.data[pk] = validated_record
            # 维护 dirty 集合：更新时若在 inserted 中则保持 inserted，否则加入 updated
            if pk in self.inserted:
                pass
            else:
                self.updated.add(pk)
            self.deleted.discard(pk)
            count += 1

        if count > 0:
            self._data_dirty = True
        return count

    def get(self, pk: Any) -> dict[str, Any]:
        """
        获取记录（支持懒加载）

        Args:
            pk: 主键值

        Returns:
            记录字典

        Raises:
            RecordNotFoundError: 记录不存在
        """
        # 转换主键类型
        pk = self._normalize_pk(pk)
        # 已加载的数据直接返回
        if pk in self.data:
            return self.data[pk].copy()

        # 懒加载模式：从文件读取
        if self._lazy_loaded and self._pk_offsets is not None:
            if pk not in self._pk_offsets:
                raise RecordNotFoundError(self.name, pk)

            # 从文件读取记录
            record = self._read_record_from_file(pk)
            return record

        raise RecordNotFoundError(self.name, pk)

    def _read_record_from_file(self, pk: Any) -> dict[str, Any]:
        """
        从文件读取单条记录（懒加载模式）

        委托给 backend.read_lazy_record()，支持加密和非加密文件

        Args:
            pk: 主键值

        Returns:
            记录字典

        Raises:
            RecordNotFoundError: 当记录不存在时
        """
        # 内部状态检查：这些是程序错误，不是用户错误
        assert self._backend is not None, "Backend must be set for lazy loading"
        assert self._pk_offsets is not None, "PK offsets must be set for lazy loading"
        assert self._data_file is not None, "Data file must be set for lazy loading"

        # 检查 pk 是否存在（这是真正的"记录未找到"情况）
        if pk not in self._pk_offsets:
            raise RecordNotFoundError(self.name, pk)

        offset: int = self._pk_offsets[pk]  # type: ignore

        record = self._backend.read_lazy_record(
            self._data_file, offset, self.columns, pk, table_name=self.name
        )
        return record

    def has_pk(self, pk: Any) -> bool:
        """判断主键是否存在（包含懒加载未入内存的记录）"""
        normalized_pk = self._normalize_pk(pk)
        return normalized_pk in self.data or (
            self._pk_offsets is not None and normalized_pk in self._pk_offsets
        )

    def _ensure_all_loaded(self) -> None:
        """
        将懒加载表中磁盘上的所有记录 materialize 到 self.data 中。

        实现要求：复用现有的 get/_read_record_from_file 逻辑，不直接操作后端文件格式。
        该方法只在 lazy 模式下有意义；如果已全部加载或不是 lazy 模式则为 no-op。
        """
        if not self._lazy_loaded or self._pk_offsets is None:
            return
        # 索引在 lazy 打开时已从文件恢复，这里只需要补齐 data 缓存
        for pk in list(self._pk_offsets.keys()):
            if pk in self.data:
                continue
            try:
                record = self.get(pk)
            except RecordNotFoundError:
                # 如果记录在索引中存在但实际被删除（可能的并发或损坏），跳过
                continue
            self.data[pk] = record

    def all_pks(self) -> list[Any]:
        """返回表中的所有主键（包含懒加载未入内存的记录）"""
        pks = set(self.data.keys())
        if self._pk_offsets is not None:
            pks.update(self._pk_offsets.keys())
        return list(pks)

    def scan(self) -> Iterator[tuple[Any, dict[str, Any]]]:
        """
        扫描所有记录

        Yields:
            (主键, 记录字典)
        """
        # 在懒加载模式下，确保先把磁盘记录 materialize 到内存
        self._ensure_all_loaded()
        for pk, record in self.data.items():
            yield pk, record.copy()

    def build_index(self, column_name: str) -> None:
        """
        为列创建索引

        根据 Column.index 的值决定创建哪种索引：
        - True 或 'hash'：创建 HashIndex（哈希索引，等值查询 O(1)）
        - 'sorted'：创建 SortedIndex（有序索引，支持范围查询和排序）

        Args:
            column_name: 列名

        Raises:
            ColumnNotFoundError: 列不存在
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)

        if column_name in self.indexes:
            # 索引已存在
            return

        # 根据 Column.index 决定索引类型
        column = self.columns[column_name]
        index_type = column.index
        if index_type is True:
            index_type = 'hash'

        # 创建索引
        if index_type == 'sorted':
            index: BaseIndex = SortedIndex(column_name)
        else:
            index = HashIndex(column_name)

        # 在懒加载模式下先确保加载磁盘记录，然后为现有数据建立索引
        self._ensure_all_loaded()
        for pk, record in self.data.items():
            value = record.get(column_name)
            if value is not None:
                index.insert(value, pk)

        self.indexes[column_name] = index

    # ========== Schema 操作方法 ==========

    def add_column(self, column: Column, default_value: Any = None) -> None:
        """
        添加列到表

        Args:
            column: 列定义
            default_value: 为现有记录填充的默认值（优先于 column.default）

        Raises:
            SchemaError: 列已存在或非空列无默认值
        """
        assert column.name is not None, "Column name must be set"
        col_name = column.name  # 创建局部变量，类型为 str

        if col_name in self.columns:
            raise SchemaError(f"Column '{col_name}' already exists in table '{self.name}'")

        # 在懒加载模式下确保加载磁盘数据再判断是否有数据
        self._ensure_all_loaded()
        # 检查非空约束：如果表中有数据，新增非空列必须有默认值
        has_data = len(self.data) > 0
        has_fill = default_value is not None or column.has_default()

        if has_data and not column.nullable and not has_fill:
            raise SchemaError(
                f"Cannot add non-nullable column '{col_name}' to table '{self.name}' "
                "without default value when table has existing data"
            )

        # 添加到 columns
        self.columns[col_name] = column

        # 为现有记录填充默认值
        if has_data:
            fill_value = default_value if default_value is not None else column.resolve_default()
            for record in self.data.values():
                if col_name not in record:
                    record[col_name] = fill_value

        # 如果需要索引，构建索引
        if column.index:
            self.build_index(col_name)

        self._schema_dirty = True
        self._data_dirty = True

    def drop_column(self, column_name: str) -> None:
        """
        从表中删除列

        Args:
            column_name: 字段名（Column.name），而非 Python 属性名。
                         例如定义 ``student_no = Column(str, name="Student No.")`` 时，
                         应传入 ``"Student No."`` 而非 ``"student_no"``

        Raises:
            ColumnNotFoundError: 列不存在
            SchemaError: 试图删除主键列
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)
        if column_name == self.primary_key:
            raise SchemaError(f"Cannot drop primary key column '{column_name}'")

        # 从 columns 中移除
        del self.columns[column_name]

        # 从所有记录中移除该列
        # 在懒加载模式下先确保加载磁盘记录
        self._ensure_all_loaded()
        for record in self.data.values():
            record.pop(column_name, None)

        # 移除索引
        if column_name in self.indexes:
            del self.indexes[column_name]

        self._schema_dirty = True
        self._data_dirty = True

    def alter_column(
        self,
        column_name: str,
        *,
        col_type: Any = ...,
        nullable: Any = ...,
        default: Any = ...
    ) -> None:
        """
        修改列属性（类型、可空性、默认值）

        按通用数据库行为处理约束：
        - 修改类型时，会尝试转换所有现有数据
        - nullable True→False 时，如果有 default 则将 None 值填为 default，否则报错
        - nullable False→True 无额外操作

        Args:
            column_name: 列名
            col_type: 新类型（... 表示不修改）
            nullable: 新的可空性（... 表示不修改）
            default: 新默认值（... 表示不修改）

        Raises:
            ColumnNotFoundError: 列不存在
            SchemaError: 修改后现有数据不满足新约束
            TypeConversionError: 类型转换失败
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)

        old_col = self.columns[column_name]

        # 确定变更内容
        new_type = col_type if col_type is not ... else old_col.col_type
        new_nullable = nullable if nullable is not ... else old_col.nullable
        new_default = default if default is not ... else old_col.default

        need_type_convert = col_type is not ... and col_type != old_col.col_type
        need_nullable_check = nullable is not ... and not new_nullable and old_col.nullable

        # 第一步：验证所有记录是否满足新约束（先验证再修改）
        converted_values: dict[Any, Any] = {}  # {pk: converted_value}

        # 在懒加载模式下先确保加载磁盘记录
        self._ensure_all_loaded()
        for pk, record in self.data.items():
            value = record.get(column_name)

            if value is None:
                # 处理 None 值
                if need_nullable_check:
                    # nullable True → False
                    if new_default is not None:
                        converted_values[pk] = new_default
                    else:
                        raise SchemaError(
                            f"Cannot set column '{column_name}' to non-nullable: "
                            f"existing record (pk={pk}) has null value and no default provided",
                            table_name=self.name
                        )
                continue

            if need_type_convert:
                # 尝试类型转换
                from .orm import _TYPE_CONVERTERS
                converter = _TYPE_CONVERTERS.get(new_type)
                if converter is None:
                    raise SchemaError(
                        f"Unsupported target type: {new_type.__name__}",
                        table_name=self.name
                    )
                try:
                    converted_values[pk] = converter(value)
                except (ValueError, TypeError) as e:
                    from ..common.exceptions import TypeConversionError
                    raise TypeConversionError(
                        f"Cannot convert value of column '{column_name}' "
                        f"in record (pk={pk}): {e}",
                        value=value,
                        target_type=new_type.__name__,
                        column_name=column_name
                    )

        # 第二步：验证通过，应用变更

        # 创建新的 Column 对象替换旧对象
        new_column = Column(
            new_type,
            name=old_col.name,
            nullable=new_nullable,
            primary_key=old_col.primary_key,
            index=old_col.index,
            default=new_default,
            foreign_key=old_col.foreign_key,
            comment=old_col.comment,
            strict=old_col.strict
        )
        self.columns[column_name] = new_column

        # 应用数据转换
        for pk, converted_value in converted_values.items():
            self.data[pk][column_name] = converted_value

        # 如果列有索引，重建索引
        if new_column.index and column_name in self.indexes:
            del self.indexes[column_name]
            self.build_index(column_name)

        self._schema_dirty = True

    def set_primary_key(self, column_name: str) -> None:
        """
        修改表的主键

        将指定列设置为新的主键。会验证该列的值唯一且非空，
        并重建 data 字典以新主键值为 key。

        Args:
            column_name: 新主键列名

        Raises:
            ColumnNotFoundError: 列不存在
            SchemaError: 列包含重复值或空值
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)

        # 相同主键，无需操作
        if column_name == self.primary_key:
            return

        target_col = self.columns[column_name]

        # 在懒加载模式下先确保加载磁盘记录
        self._ensure_all_loaded()
        # 验证新主键列的值唯一且非 null
        seen: set = set()
        for pk, record in self.data.items():
            value = record.get(column_name)
            if value is None:
                raise SchemaError(
                    f"Cannot set '{column_name}' as primary key: "
                    f"record (pk={pk}) has null value",
                    table_name=self.name
                )
            if value in seen:
                raise SchemaError(
                    f"Cannot set '{column_name}' as primary key: "
                    f"duplicate value '{value}'",
                    table_name=self.name
                )
            seen.add(value)

        # 重建 data 字典
        new_data: dict[Any, dict[str, Any]] = {}
        for record in self.data.values():
            new_pk = record[column_name]
            new_data[new_pk] = record
        self.data = new_data

        # 更新 Column.primary_key 属性
        if self.primary_key and self.primary_key in self.columns:
            self.columns[self.primary_key].primary_key = False
        target_col.primary_key = True
        self.primary_key = column_name

        # 更新 next_id
        if target_col.col_type == int and self.data:
            max_pk = max(pk for pk in self.data if isinstance(pk, int))
            self.next_id = max_pk + 1
        elif target_col.col_type == int:
            self.next_id = 1

        # 标记为 schema/data 脏，以便 Storage.flush 会保存
        self._schema_dirty = True
        self._data_dirty = True

        # 如果处于懒加载模式，现有的 pk -> offset 映射不再有效，清除它们
        # 旧的 _pk_offsets 是基于旧主键建立的，因此不能继续使用
        if self._lazy_loaded:
            self._pk_offsets = None

    def reorder_columns(self, new_order: list[str]) -> None:
        """
        重新排列列的顺序

        影响序列化时的列顺序（如 CSV 列顺序）。

        Args:
            new_order: 新的列名顺序列表，必须包含且仅包含所有列

        Raises:
            SchemaError: new_order 与现有列集合不一致
        """
        existing_cols = set(self.columns.keys())
        new_cols = set(new_order)

        if len(new_order) != len(new_cols):
            raise SchemaError(
                "new_order contains duplicate column names",
                table_name=self.name
            )

        if new_cols != existing_cols:
            missing = existing_cols - new_cols
            extra = new_cols - existing_cols
            parts = []
            if missing:
                parts.append(f"missing: {missing}")
            if extra:
                parts.append(f"unknown: {extra}")
            raise SchemaError(
                f"new_order does not match existing columns: {', '.join(parts)}",
                table_name=self.name
            )

        # 重建有序的 columns 字典
        self.columns = {name: self.columns[name] for name in new_order}

        # 在懒加载模式下先确保加载磁盘记录
        self._ensure_all_loaded()
        # 重建每条记录的字段顺序
        for pk in list(self.data.keys()):
            record = self.data[pk]
            self.data[pk] = {name: record.get(name) for name in new_order}

        # 标记为 schema/data 脏，以便 Storage.flush 会保存
        self._schema_dirty = True
        self._data_dirty = True

    def update_comment(self, comment: str | None) -> None:
        """
        更新表备注

        Args:
            comment: 新的备注（None 表示清空）
        """
        self.comment = comment

    def update_column_comment(self, column_name: str, comment: str | None) -> None:
        """
        更新列备注

        Args:
            column_name: 字段名（Column.name），而非 Python 属性名
            comment: 新的备注（None 表示清空）

        Raises:
            ColumnNotFoundError: 列不存在
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)
        self.columns[column_name].comment = comment

    def update_column_index(self, column_name: str, index: bool | str) -> None:
        """
        更新列的索引设置

        Args:
            column_name: 字段名（Column.name），而非 Python 属性名
            index: 索引设置。False=不建索引，True/'hash'=哈希索引，'sorted'=有序索引

        Raises:
            ColumnNotFoundError: 列不存在
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(self.name, column_name)

        column = self.columns[column_name]
        old_index = column.index
        column.index = index

        # 规范化新旧值用于比较
        old_normalized = 'hash' if old_index is True else old_index
        new_normalized = 'hash' if index is True else index

        if new_normalized and not old_normalized:
            # 需要创建索引
            self.build_index(column_name)
        elif not new_normalized and old_normalized:
            # 需要删除索引
            if column_name in self.indexes:
                del self.indexes[column_name]
        elif new_normalized and old_normalized and new_normalized != old_normalized:
            # 索引类型改变，删旧建新
            if column_name in self.indexes:
                del self.indexes[column_name]
            self.build_index(column_name)

    def __repr__(self) -> str:
        return f"Table(name='{self.name}', records={len(self.data)}, indexes={len(self.indexes)})"

class Storage:
    """存储引擎"""

    def __init__(
        self,
        file_path: str | Path | None = None,
        in_memory: bool = False,
        auto_flush: bool = False,
        backend_options: PytuckBackendOptions | None = None,
    ):
        """
        初始化存储引擎

        Args:
            file_path: 数据文件路径，支持字符串或 Path 对象（None 表示纯内存）
            in_memory: 是否纯内存模式
            auto_flush: 是否自动刷新到磁盘
            backend_options: PTK7 后端配置对象，通常使用 PytuckBackendOptions 传递 encryption/password 等参数
        """
        # 路径统一处理：边界转换为 Path 对象
        if file_path is not None and str(file_path) != '':
            self.file_path: Path | None = Path(file_path).expanduser()
        else:
            self.file_path = None
        self.in_memory: bool = in_memory or (file_path is None)
        self.auto_flush = auto_flush
        self.tables: dict[str, Table] = {}
        self._dirty = False
        self._lock = RLock()

        # 事务管理属性
        self._in_transaction: bool = False
        self._transaction_snapshot: TransactionSnapshot | None = None
        self._transaction_dirty_flag: bool = False

        # 模型注册表（表名 -> 模型类，用于 Relationship 解析）
        self._model_registry: dict[str, type[PureBaseModel]] = {}

        # 初始化后端
        self.backend: StorageBackend | None = None
        if not self.in_memory and self.file_path:
            if backend_options is None:
                backend_options = PytuckBackendOptions()

            from ..backends.backend_pytucky import PytuckyBackend
            self.backend = PytuckyBackend(self.file_path, backend_options)
            # ensure Storage.file_path reflects any normalization performed by backend
            self.file_path = self.backend.file_path

            # 如果文件存在，自动加载
            if self.backend.exists():
                self.tables = self.backend.load()
                self._dirty = False

    # ==================== 模型注册表方法 ====================

    @_storage_locked
    def _register_model(self, table_name: str, model_cls: type[PureBaseModel]) -> None:
        """
        注册模型类（按表名）

        Args:
            table_name: 表名
            model_cls: 模型类
        """
        self._model_registry[table_name] = model_cls

    @_storage_locked
    def _get_model_by_table(self, table_name: str) -> type[PureBaseModel] | None:
        """
        根据表名获取模型类

        Args:
            table_name: 表名

        Returns:
            模型类，如果不存在返回 None
        """
        return self._model_registry.get(table_name)

    @_storage_locked
    def create_table(
        self,
        name: str,
        columns: list[Column],
        comment: str | None = None
    ) -> None:
        """
        创建表

        Args:
            name: 表名
            columns: 列定义列表
            comment: 表备注/注释

        Raises:
            ValueError: 表已存在
        """
        if name in self.tables:
            # 表已存在，跳过
            return

        # 查找主键（可能为 None，表示无主键）
        primary_key = None
        for col in columns:
            if col.primary_key:
                primary_key = col.name
                break

        # 允许无主键（使用隐式 rowid）
        # 注意：无主键时，primary_key 为 None

        table = Table(name, columns, primary_key, comment)
        table._schema_dirty = True
        table._data_dirty = True
        self.tables[name] = table
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def get_table(self, name: str) -> Table:
        """
        获取表

        Args:
            name: 表名

        Returns:
            表对象

        Raises:
            TableNotFoundError: 表不存在
        """
        if name not in self.tables:
            raise TableNotFoundError(name)

        return self.tables[name]

    # ========== Schema 操作方法 ==========

    @_storage_locked
    def sync_table_schema(
        self,
        table_name: str,
        columns: list[Column],
        comment: str | None = None,
        options: SyncOptions | None = None
    ) -> SyncResult:
        """
        同步表结构（轻量迁移）

        根据给定的列定义同步已存在表的 schema，包括：
        - 同步表备注
        - 同步列备注
        - 添加新列
        - 删除缺失列（可选）

        Args:
            table_name: 表名
            columns: 新的列定义列表
            comment: 表备注
            options: 同步选项

        Returns:
            SyncResult: 同步结果（包含变更详情）

        Raises:
            TableNotFoundError: 表不存在
            SchemaError: 新增必填列无默认值时
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        opts = options or SyncOptions()
        table = self.tables[table_name]
        result = SyncResult(table_name=table_name)

        # 构建新列名到列的映射
        new_columns_map: dict[str, Column] = {}
        for col in columns:
            assert col.name is not None, "Column name must be set"
            new_columns_map[col.name] = col
        old_columns_set = set(table.columns.keys())
        new_columns_set = set(new_columns_map.keys())

        # 1. 同步表备注
        if opts.sync_table_comment and table.comment != comment:
            table.update_comment(comment)
            result.table_comment_updated = True

        # 2. 添加新列
        if opts.add_new_columns:
            columns_to_add = new_columns_set - old_columns_set
            for col_name in columns_to_add:
                col = new_columns_map[col_name]
                table.add_column(col)
                result.columns_added.append(col_name)

        # 3. 删除缺失列（危险操作，默认禁用）
        if opts.drop_missing_columns:
            columns_to_drop = old_columns_set - new_columns_set - {table.primary_key}
            for col_name in columns_to_drop:
                table.drop_column(col_name)
                result.columns_dropped.append(col_name)

        # 4. 同步列备注
        if opts.sync_column_comments:
            for col_name in old_columns_set & new_columns_set:
                old_col = table.columns[col_name]
                new_col = new_columns_map[col_name]
                if old_col.comment != new_col.comment:
                    table.update_column_comment(col_name, new_col.comment)
                    result.column_comments_updated.append(col_name)

        # 标记脏数据
        if result.has_changes:
            self._dirty = True
            if self.auto_flush:
                self.flush()

        return result

    @_storage_locked
    def drop_table(self, table_name: str) -> None:
        """
        删除表（包括所有数据）

        Args:
            table_name: 表名

        Raises:
            TableNotFoundError: 表不存在
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        del self.tables[table_name]
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def rename_table(self, old_name: str, new_name: str) -> None:
        """
        重命名表

        Args:
            old_name: 原表名
            new_name: 新表名

        Raises:
            TableNotFoundError: 原表不存在
            SchemaError: 新表名已存在
        """
        if old_name not in self.tables:
            raise TableNotFoundError(old_name)
        if new_name in self.tables:
            raise SchemaError(f"Table '{new_name}' already exists")

        table = self.tables.pop(old_name)
        table.name = new_name
        self.tables[new_name] = table
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def update_table_comment(self, table_name: str, comment: str | None) -> None:
        """
        更新表备注

        Args:
            table_name: 表名
            comment: 新备注

        Raises:
            TableNotFoundError: 表不存在
        """
        table = self.get_table(table_name)
        table.update_comment(comment)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def add_column(
        self,
        table_name: str,
        column: Column,
        default_value: Any = None
    ) -> None:
        """
        向表添加列

        Args:
            table_name: 表名
            column: 列定义
            default_value: 为现有记录填充的默认值

        Raises:
            TableNotFoundError: 表不存在
            SchemaError: 列已存在或非空列无默认值
        """
        table = self.get_table(table_name)

        table.add_column(column, default_value)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def drop_column(self, table_name: str, column_name: str) -> None:
        """
        从表中删除列

        Args:
            table_name: 表名
            column_name: 字段名（Column.name），而非 Python 属性名。
                         例如定义 ``student_no = Column(str, name="Student No.")`` 时，
                         应传入 ``"Student No."`` 而非 ``"student_no"``

        Raises:
            TableNotFoundError: 表不存在
            ColumnNotFoundError: 列不存在
            SchemaError: 试图删除主键列
        """
        table = self.get_table(table_name)

        table.drop_column(column_name)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def update_column(
        self,
        table_name: str,
        column_name: str,
        comment: Any = ...,
        index: Any = ...
    ) -> None:
        """
        更新列属性

        Args:
            table_name: 表名
            column_name: 字段名（Column.name），而非 Python 属性名
            comment: 新备注（... 表示不修改）
            index: 是否创建索引（... 表示不修改）

        Raises:
            TableNotFoundError: 表不存在
            ColumnNotFoundError: 列不存在
        """
        table = self.get_table(table_name)

        if comment is not ...:
            table.update_column_comment(column_name, comment)
            self._dirty = True

        if index is not ...:
            table.update_column_index(column_name, index)
            self._dirty = True

        if self._dirty and self.auto_flush:
            self.flush()

    @_storage_locked
    def alter_column(
        self,
        table_name: str,
        column_name: str,
        *,
        col_type: Any = ...,
        nullable: Any = ...,
        default: Any = ...
    ) -> None:
        """
        修改列属性（类型、可空性、默认值）

        Args:
            table_name: 表名
            column_name: 列名
            col_type: 新类型（... 表示不修改）
            nullable: 新的可空性（... 表示不修改）
            default: 新默认值（... 表示不修改）

        Raises:
            TableNotFoundError: 表不存在
            ColumnNotFoundError: 列不存在
            SchemaError: 修改后现有数据不满足新约束
            TypeConversionError: 类型转换失败
        """
        table = self.get_table(table_name)
        table.alter_column(column_name, col_type=col_type, nullable=nullable, default=default)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def set_primary_key(self, table_name: str, column_name: str) -> None:
        """
        修改表的主键

        Args:
            table_name: 表名
            column_name: 新主键列名

        Raises:
            TableNotFoundError: 表不存在
            ColumnNotFoundError: 列不存在
            SchemaError: 列包含重复值或空值
        """
        table = self.get_table(table_name)
        table.set_primary_key(column_name)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def reorder_columns(self, table_name: str, new_order: list[str]) -> None:
        """
        重新排列列的顺序

        Args:
            table_name: 表名
            new_order: 新的列名顺序列表

        Raises:
            TableNotFoundError: 表不存在
            SchemaError: new_order 与现有列集合不一致
        """
        table = self.get_table(table_name)
        table.reorder_columns(new_order)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def insert(self, table_name: str, data: dict[str, Any]) -> Any:
        """
        插入记录

        Args:
            table_name: 表名
            data: 数据字典

        Returns:
            主键值
        """
        table = self.get_table(table_name)

        pk = table.insert(data)
        self._dirty = True

        if self.auto_flush:
            self.flush()

        return pk

    @_storage_locked
    def update(self, table_name: str, pk: Any, data: dict[str, Any]) -> None:
        """
        更新记录

        Args:
            table_name: 表名
            pk: 主键值
            data: 新数据
        """
        table = self.get_table(table_name)

        table.update(pk, data)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def delete(self, table_name: str, pk: Any) -> None:
        """
        删除记录

        Args:
            table_name: 表名
            pk: 主键值
        """
        table = self.get_table(table_name)

        table.delete(pk)
        self._dirty = True

        if self.auto_flush:
            self.flush()

    @_storage_locked
    def bulk_insert(self, table_name: str, records: list[dict[str, Any]]) -> list[Any]:
        """
        批量插入记录

        Args:
            table_name: 表名
            records: 数据字典列表

        Returns:
            主键列表
        """
        if not records:
            return []

        table = self.get_table(table_name)

        pks = table.bulk_insert(records)
        self._dirty = True

        if self.auto_flush:
            self.flush()

        return pks

    @_storage_locked
    def bulk_update(self, table_name: str, updates: list[tuple[Any, dict[str, Any]]]) -> int:
        """
        批量更新记录

        Args:
            table_name: 表名
            updates: (pk, data) 元组列表

        Returns:
            更新的记录数
        """
        if not updates:
            return 0

        table = self.get_table(table_name)

        count = table.bulk_update(updates)
        self._dirty = True

        if self.auto_flush:
            self.flush()

        return count

    @_storage_locked
    def select(self, table_name: str, pk: Any) -> dict[str, Any]:
        """
        查询单条记录

        Args:
            table_name: 表名
            pk: 主键值（用户主键或内部 rowid）

        Returns:
            记录字典
        """
        table = self.get_table(table_name)

        record = table.get(pk)
        record_copy = record.copy()
        # 无主键表：注入内部 rowid
        if not table.primary_key:
            record_copy[PSEUDO_PK_NAME] = pk
        return record_copy

    @_storage_locked
    def count_rows(self, table_name: str) -> int:
        """
        获取表的记录数

        Args:
            table_name: 表名

        Returns:
            记录数

        Raises:
            TableNotFoundError: 表不存在
        """
        table = self.get_table(table_name)

        return table.record_count

    def _build_query_plan(
        self,
        table_name: str,
        conditions: Sequence[ConditionType],
    ) -> tuple[Table, set[Any], list[Condition], list[CompositeCondition]]:
        table = self.get_table(table_name)

        simple_conditions: list[Condition] = []
        composite_conditions: list[CompositeCondition] = []
        for condition in conditions:
            if isinstance(condition, CompositeCondition):
                composite_conditions.append(condition)
            else:
                simple_conditions.append(condition)

        candidate_pks: set[Any] | None = None
        remaining_simple_conditions: list[Condition] = []

        for condition in simple_conditions:
            if condition.operator == '=' and condition.field in table.indexes:
                index = table.indexes[condition.field]
                pks = index.lookup(condition.value)
                candidate_pks = pks if candidate_pks is None else candidate_pks.intersection(pks)
            elif (
                condition.operator in ('>', '>=', '<', '<=')
                and condition.field in table.indexes
                and table.indexes[condition.field].supports_range_query()
            ):
                sorted_idx = table.indexes[condition.field]
                min_val, max_val = None, None
                include_min, include_max = True, True

                if condition.operator == '>':
                    min_val, include_min = condition.value, False
                elif condition.operator == '>=':
                    min_val, include_min = condition.value, True
                elif condition.operator == '<':
                    max_val, include_max = condition.value, False
                elif condition.operator == '<=':
                    max_val, include_max = condition.value, True

                pks = sorted_idx.range_query(min_val, max_val, include_min, include_max)
                candidate_pks = pks if candidate_pks is None else candidate_pks.intersection(pks)
            else:
                remaining_simple_conditions.append(condition)

        if candidate_pks is None:
            candidate_pks = set(table.all_pks())
            remaining_simple_conditions = simple_conditions

        return table, candidate_pks, remaining_simple_conditions, composite_conditions

    @staticmethod
    def _record_matches(
        record: dict[str, Any],
        simple_conditions: Sequence[Condition],
        composite_conditions: Sequence[CompositeCondition],
    ) -> bool:
        if not all(cond.evaluate(record) for cond in simple_conditions):
            return False
        return all(cond.evaluate(record) for cond in composite_conditions)

    @staticmethod
    def _build_result_record(table: Table, pk: Any, record: dict[str, Any]) -> dict[str, Any]:
        record_copy = record.copy()
        if not table.primary_key:
            record_copy[PSEUDO_PK_NAME] = pk
        return record_copy

    @staticmethod
    def _apply_query_pagination(
        records: list[dict[str, Any]],
        limit: int | None,
        offset: int,
    ) -> list[dict[str, Any]]:
        if offset != 0:
            records = records[offset:]
        if limit is not None:
            records = records[:limit]
        return records

    @_storage_locked
    def _query_count(
        self,
        table_name: str,
        conditions: Sequence[ConditionType],
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> int:
        table, candidate_pks, remaining_simple_conditions, composite_conditions = self._build_query_plan(
            table_name,
            conditions,
        )

        matched_count = 0
        for pk in candidate_pks:
            try:
                record = table.get(pk)
            except RecordNotFoundError:
                continue
            if self._record_matches(record, remaining_simple_conditions, composite_conditions):
                matched_count += 1

        if offset > 0:
            matched_count = max(0, matched_count - offset)
        if limit is not None:
            matched_count = min(matched_count, max(limit, 0))
        return matched_count

    @_storage_locked
    def _query_window(
        self,
        table_name: str,
        conditions: Sequence[ConditionType],
        *,
        limit: int | None,
        offset: int,
        order_by: str | None,
        order_desc: bool,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        table, candidate_pks, remaining_simple_conditions, composite_conditions = self._build_query_plan(
            table_name,
            conditions,
        )
        can_stream_window = offset >= 0 and (limit is None or limit >= 0)
        use_index_order = (
            order_by
            and order_by in table.indexes
            and table.indexes[order_by].supports_range_query()
        )

        if use_index_order:
            assert order_by is not None
            sorted_idx = table.indexes[order_by]
            assert isinstance(sorted_idx, SortedIndex)
            ordered_pks = sorted_idx.get_sorted_pks(reverse=order_desc)
            indexed_pk_set = set(ordered_pks)
            none_value_pks = [pk for pk in candidate_pks if pk not in indexed_pk_set]

            none_results: list[dict[str, Any]] = []
            for pk in none_value_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                none_results.append(self._build_result_record(table, pk, record))

            def iter_sorted_results() -> Iterator[dict[str, Any]]:
                if order_desc:
                    for rec in none_results:
                        yield rec
                for pk in ordered_pks:
                    if pk not in candidate_pks:
                        continue
                    try:
                        record = table.get(pk)
                    except RecordNotFoundError:
                        continue
                    if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                        continue
                    yield self._build_result_record(table, pk, record)
                if not order_desc:
                    for rec in none_results:
                        yield rec

            if not can_stream_window:
                matched_records = list(iter_sorted_results())
                total_count = len(matched_records)
                records = self._apply_query_pagination(matched_records, limit, offset)
                has_more = limit is not None and limit >= 0 and offset >= 0 and (offset + len(records)) < total_count
                return records, total_count, has_more

            records = []
            total_count = 0
            for rec in iter_sorted_results():
                total_count += 1
                if total_count <= offset:
                    continue
                if limit is None or len(records) < limit:
                    records.append(rec)
            has_more = limit is not None and (offset + len(records)) < total_count
            return records, total_count, has_more

        if order_by and order_by in table.columns:
            matched_records = []
            for pk in candidate_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                matched_records.append(self._build_result_record(table, pk, record))

            def sort_key(_record: dict[str, Any]) -> tuple[Any, Any]:
                value = _record.get(order_by)
                if value is None:
                    return (1, 0)
                return (0, value)

            try:
                matched_records.sort(key=sort_key, reverse=order_desc)
            except TypeError:
                matched_records.sort(key=lambda r: str(r.get(order_by, '')), reverse=order_desc)

            total_count = len(matched_records)
            records = self._apply_query_pagination(matched_records, limit, offset)
            has_more = limit is not None and limit >= 0 and offset >= 0 and (offset + len(records)) < total_count
            return records, total_count, has_more

        if not can_stream_window:
            matched_records = []
            for pk in candidate_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                matched_records.append(self._build_result_record(table, pk, record))

            total_count = len(matched_records)
            records = self._apply_query_pagination(matched_records, limit, offset)
            has_more = limit is not None and limit >= 0 and offset >= 0 and (offset + len(records)) < total_count
            return records, total_count, has_more

        records = []
        total_count = 0
        for pk in candidate_pks:
            try:
                record = table.get(pk)
            except RecordNotFoundError:
                continue
            if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                continue

            total_count += 1
            if total_count <= offset:
                continue
            if limit is None or len(records) < limit:
                records.append(self._build_result_record(table, pk, record))

        has_more = limit is not None and (offset + len(records)) < total_count
        return records, total_count, has_more

    @_storage_locked
    def query(self,
              table_name: str,
              conditions: Sequence[ConditionType],
              limit: int | None = None,
              offset: int = 0,
              order_by: str | None = None,
              order_desc: bool = False) -> list[dict[str, Any]]:
        """
        查询多条记录

        Args:
            table_name: 表名
            conditions: 查询条件列表（支持 Condition 和 CompositeCondition）
            limit: 限制返回记录数（None 表示无限制）
            offset: 跳过的记录数
            order_by: 排序字段名
            order_desc: 是否降序排列

        Returns:
            记录字典列表
        """
        if limit == 0:
            return []

        table, candidate_pks, remaining_simple_conditions, composite_conditions = self._build_query_plan(
            table_name,
            conditions,
        )
        use_index_order = (
            order_by
            and order_by in table.indexes
            and table.indexes[order_by].supports_range_query()
        )
        can_stream_paging = offset >= 0 and (limit is None or limit > 0)

        if use_index_order:
            assert order_by is not None
            sorted_idx = table.indexes[order_by]
            assert isinstance(sorted_idx, SortedIndex)
            ordered_pks = sorted_idx.get_sorted_pks(reverse=order_desc)
            indexed_pk_set = set(ordered_pks)
            none_value_pks = [pk for pk in candidate_pks if pk not in indexed_pk_set]

            none_results: list[dict[str, Any]] = []
            for pk in none_value_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                none_results.append(self._build_result_record(table, pk, record))

            def iter_sorted_results() -> Iterator[dict[str, Any]]:
                if order_desc:
                    for rec in none_results:
                        yield rec
                for pk in ordered_pks:
                    if pk not in candidate_pks:
                        continue
                    try:
                        record = table.get(pk)
                    except RecordNotFoundError:
                        continue
                    if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                        continue
                    yield self._build_result_record(table, pk, record)
                if not order_desc:
                    for rec in none_results:
                        yield rec

            if not can_stream_paging:
                return self._apply_query_pagination(list(iter_sorted_results()), limit, offset)

            results = []
            skipped = 0
            for rec in iter_sorted_results():
                if skipped < offset:
                    skipped += 1
                    continue
                results.append(rec)
                if limit is not None and len(results) >= limit:
                    break
            return results

        if order_by and order_by in table.columns:
            results = []
            for pk in candidate_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                results.append(self._build_result_record(table, pk, record))

            def sort_key(_record: dict[str, Any]) -> tuple[Any, Any]:
                value = _record.get(order_by)
                if value is None:
                    return (1, 0)
                return (0, value)

            try:
                results.sort(key=sort_key, reverse=order_desc)
            except TypeError:
                results.sort(key=lambda r: str(r.get(order_by, '')), reverse=order_desc)

            return self._apply_query_pagination(results, limit, offset)

        if not can_stream_paging:
            results = []
            for pk in candidate_pks:
                try:
                    record = table.get(pk)
                except RecordNotFoundError:
                    continue
                if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                    continue
                results.append(self._build_result_record(table, pk, record))
            return self._apply_query_pagination(results, limit, offset)

        results = []
        skipped = 0
        for pk in candidate_pks:
            try:
                record = table.get(pk)
            except RecordNotFoundError:
                continue
            if not self._record_matches(record, remaining_simple_conditions, composite_conditions):
                continue
            if skipped < offset:
                skipped += 1
                continue
            results.append(self._build_result_record(table, pk, record))
            if limit is not None and len(results) >= limit:
                break
        return results

    @_storage_locked
    def query_table_data(self,
                        table_name: str,
                        limit: int | None = None,
                        offset: int = 0,
                        order_by: str | None = None,
                        order_desc: bool = False,
                        filters: dict[str, Any] | list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """
        查询表数据（专为 Web UI 设计）

        Args:
            table_name: 表名
            limit: 限制返回记录数
            offset: 跳过的记录数
            order_by: 排序字段名
            order_desc: 是否降序排列
            filters: 过滤条件，支持两种格式：
                - dict[str, Any]: 等值过滤 {field: value}（向后兼容）
                - list[dict[str, Any]]: 带操作符过滤
                  [{'field': str, 'operator': str, 'value': Any}, ...]
                  支持的操作符: '=', '!=', '>', '<', '>=', '<=', 'IN',
                              'LIKE', 'STARTSWITH', 'ENDSWITH'

        Returns:
            {
                'records': list[dict[str, Any]],  # 实际数据行
                'total_count': int,               # 总记录数（应用过滤后）
                'has_more': bool,                 # 是否还有更多数据
                'schema': list[Dict],             # 列结构信息
            }
        """
        if table_name not in self.tables:
            raise TableNotFoundError(f"Table '{table_name}' not found")

        table = self.get_table(table_name)

        backend_conditions: list[dict[str, Any]] = []
        if filters:
            if isinstance(filters, dict):
                for field, value in filters.items():
                    if field in table.columns:
                        backend_conditions.append({'field': field, 'operator': '=', 'value': value})
            elif isinstance(filters, list):
                for f in filters:
                    if f.get('field') in table.columns:
                        backend_conditions.append({
                            'field': f['field'],
                            'operator': f.get('operator', '='),
                            'value': f['value']
                        })

        if self.backend and not self._dirty and self.backend.supports_server_side_pagination():
            try:
                result = self.backend.query_with_pagination(
                    table_name=table_name,
                    conditions=backend_conditions,
                    limit=limit,
                    offset=offset,
                    order_by=order_by,
                    order_desc=order_desc
                )

                schema = [col.to_dict() for col in table.columns.values()]
                return {
                    'records': result.get('records', []),
                    'total_count': result.get('total_count', 0),
                    'has_more': result.get('has_more', False),
                    'schema': schema
                }
            except NotImplementedError:
                pass

        conditions: list[Condition] = []
        for bc in backend_conditions:
            conditions.append(Condition(bc['field'], bc['operator'], bc['value']))

        records, total_count, has_more = self._query_window(
            table_name=table_name,
            conditions=conditions,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_desc=order_desc,
        )

        schema = [col.to_dict() for col in table.columns.values()]
        return {
            'records': records,
            'total_count': total_count,
            'has_more': has_more,
            'schema': schema
        }

    @contextmanager
    def transaction(self) -> Generator['Storage', None, None]:
        """
        事务上下文管理器

        提供内存级事务支持：
        - 自动回滚：异常时自动恢复到事务开始前的状态
        - 单层事务：不支持嵌套
        - 内存事务：事务期间禁用 auto_flush

        Example:
            with storage.transaction():
                storage.insert('users', {'name': 'Alice'})
                storage.insert('users', {'name': 'Bob'})

        Raises:
            TransactionError: 尝试嵌套事务时
        """
        # 1. 检查嵌套事务
        with self._lock:
            if self._in_transaction:
                raise TransactionError("Nested transactions are not supported")

            # 2. 进入事务状态
            self._in_transaction = True
            self._transaction_snapshot = TransactionSnapshot(self.tables)
            self._transaction_dirty_flag = self._dirty

            # 3. 临时禁用 auto_flush
            old_auto_flush = self.auto_flush
            self.auto_flush = False

            try:
                # 4. 执行事务体
                yield self

                # 5. 提交成功：恢复 auto_flush 并刷新
                if old_auto_flush:
                    self.flush()

            except Exception:
                # 6. 回滚：恢复快照和状态
                if self._transaction_snapshot:
                    self._transaction_snapshot.restore(self.tables)
                self._dirty = self._transaction_dirty_flag
                raise

            finally:
                # 7. 清理：恢复状态
                self.auto_flush = old_auto_flush
                self._transaction_snapshot = None
                self._in_transaction = False

    @_storage_locked
    def flush(self) -> None:
        """强制写入磁盘"""
        if self.backend and self._dirty:
            event.dispatch_storage(self, 'before_flush')

            # 收集变更的表名（用于后端增量保存优化）
            changed_tables = {
                name for name, table in self.tables.items()
                if table.is_dirty
            }

            # 支持 lazy-loading 的后端（PTK7）只 materialize 变更表
            for table_name in changed_tables:
                table = self.tables[table_name]
                skip_materialize = (
                    getattr(table, '_lazy_loaded', False)
                    and not getattr(table, '_schema_dirty', False)
                )
                if not skip_materialize and table._lazy_loaded:
                    table._ensure_all_loaded()

            self.backend.save(self.tables, changed_tables=changed_tables)
            self._dirty = False

            # 重置所有表的脏标记
            for table in self.tables.values():
                table.reset_dirty()

            event.dispatch_storage(self, 'after_flush')

    @_storage_locked
    def close(self) -> None:
        """关闭数据库"""
        self.flush()

        if self.backend and hasattr(self.backend, 'close'):
            self.backend.close()

    def __repr__(self) -> str:
        return f"Storage(tables={len(self.tables)}, in_memory={self.in_memory})"
