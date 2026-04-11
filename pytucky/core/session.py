"""
Session - 会话管理器

提供类似 SQLAlchemy 的 Session 模式，统一管理数据库操作。
"""

from typing import Any, Dict, List, Optional, Type, Tuple, Union, Generator, overload
from contextlib import contextmanager

from ..common.typing import T
from ..common.exceptions import QueryError, TransactionError, ValidationError
from ..common.options import SyncOptions, SyncResult
from ..query.builder import Query, BinaryExpression
from ..query.result import Result, CursorResult
from ..query.statements import Statement, Insert, Select, Update, Delete
from .storage import Storage
from .orm import PureBaseModel, Column, PSEUDO_PK_NAME
from .event import event


class Session:
    """
    会话管理器

    统一管理所有数据库操作（CRUD），提供对象状态追踪和事务管理。

    使用方式:
        session = Session(storage)

        # 插入
        user = User(name='Alice', age=20)
        session.add(user)
        session.commit()

        # 查询
        user = session.get(User, 1)
        users = session.query(User).filter(age__gte=18).all()

        # 更新
        user.age = 21
        session.commit()

        # 删除
        session.delete(user)
        session.commit()

        # 事务
        with session.begin():
            session.add(User(name='Bob'))
            session.add(User(name='Charlie'))
    """

    def __init__(self, storage: Storage, autocommit: bool = False):
        """
        初始化 Session

        Args:
            storage: Storage 实例
            autocommit: 是否自动提交（默认 False）
        """
        self.storage = storage
        self.autocommit = autocommit

        # 对象状态追踪
        self._new_objects: List[PureBaseModel] = []      # 待插入对象
        self._dirty_objects: List[PureBaseModel] = []    # 待更新对象
        self._deleted_objects: List[PureBaseModel] = []  # 待删除对象

        # 标识映射：缓存已加载的对象 {(model_class, pk): instance}
        self._identity_map: Dict[Tuple[Type[PureBaseModel], Any], PureBaseModel] = {}

        # 事务状态
        self._in_transaction = False

    def add(self, instance: PureBaseModel) -> None:
        """
        添加对象到会话（标记为待插入）

        Args:
            instance: 模型实例
        """
        if instance not in self._new_objects:
            self._new_objects.append(instance)

        if self.autocommit:
            self.commit()

    def add_all(self, instances: List[PureBaseModel]) -> None:
        """
        批量添加对象到会话

        Args:
            instances: 模型实例列表
        """
        for instance in instances:
            self.add(instance)

    def delete(self, instance: PureBaseModel) -> None:
        """
        标记对象为待删除

        Args:
            instance: 模型实例
        """
        # 从新增列表中移除（如果还未持久化）
        if instance in self._new_objects:
            self._new_objects.remove(instance)
        else:
            # 已持久化的对象标记为待删除
            if instance not in self._deleted_objects:
                self._deleted_objects.append(instance)

        if self.autocommit:
            self.commit()

    def flush(self) -> None:
        """
        将待处理的修改刷新到数据库（不提交事务）
        """
        # 1. 处理待插入对象
        for instance in self._new_objects:
            table_name = instance.__tablename__
            assert table_name is not None, f"Model {instance.__class__.__name__} must have __tablename__ defined"

            # 触发 before_insert 事件（可在回调中修改实例字段）
            event.dispatch_model(instance.__class__, 'before_insert', instance)

            # 构建要插入的数据（使用 Column.name 作为存储键）
            data = {}
            for attr_name, column in instance.__columns__.items():
                value = getattr(instance, attr_name, None)
                if value is not None:
                    # 使用 Column.name 作为存储键
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value

            # 插入到数据库
            pk = self.storage.insert(table_name, data)

            # 设置主键（或隐式 rowid）
            pk_name = instance.__primary_key__
            if pk_name:
                setattr(instance, pk_name, pk)
            else:
                # 无主键时，使用隐式 rowid
                setattr(instance, '_pytuck_rowid', pk)

            # 从数据库重新读取并更新实例（刷新所有字段，类似 SQLAlchemy）
            db_record = self.storage.select(table_name, pk)
            for db_col_name, value in db_record.items():
                if db_col_name != PSEUDO_PK_NAME:
                    # 将 Column.name 转换回属性名
                    attr_name = instance._column_to_attr_name(db_col_name) or db_col_name
                    object.__setattr__(instance, attr_name, value)

            # 注册到标识映射（使用统一的方法，设置 session 引用）
            self._register_instance(instance)

            # 触发 after_insert 事件（实例已有 pk，已刷新）
            event.dispatch_model(instance.__class__, 'after_insert', instance)

        # 2. 处理待更新对象
        for instance in self._dirty_objects:
            table_name = instance.__tablename__
            assert table_name is not None, f"Model {instance.__class__.__name__} must have __tablename__ defined"

            pk_name = instance.__primary_key__
            if pk_name:
                pk = getattr(instance, pk_name)
            else:
                pk = getattr(instance, '_pytuck_rowid', None)

            # 触发 before_update 事件（可在回调中修改实例字段）
            event.dispatch_model(instance.__class__, 'before_update', instance)

            # 构建要更新的数据（使用 Column.name 作为存储键）
            data = {}
            for attr_name, column in instance.__columns__.items():
                value = getattr(instance, attr_name, None)
                if value is not None:
                    # 使用 Column.name 作为存储键
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value

            # 更新数据库
            self.storage.update(table_name, pk, data)

            # 从数据库重新读取并更新实例
            db_record = self.storage.select(table_name, pk)
            for db_col_name, value in db_record.items():
                if db_col_name != PSEUDO_PK_NAME:
                    # 将 Column.name 转换回属性名
                    attr_name = instance._column_to_attr_name(db_col_name) or db_col_name
                    object.__setattr__(instance, attr_name, value)

            # 触发 after_update 事件（实例已刷新）
            event.dispatch_model(instance.__class__, 'after_update', instance)

        # 3. 处理待删除对象
        for instance in self._deleted_objects:
            table_name = instance.__tablename__
            assert table_name is not None, f"Model {instance.__class__.__name__} must have __tablename__ defined"

            pk_name = instance.__primary_key__
            if pk_name:
                pk = getattr(instance, pk_name)
            else:
                pk = getattr(instance, '_pytuck_rowid', None)

            # 触发 before_delete 事件
            event.dispatch_model(instance.__class__, 'before_delete', instance)

            # 从数据库删除
            self.storage.delete(table_name, pk)

            # 从标识映射移除
            if pk_name:
                key = (instance.__class__, pk)
            else:
                key = (instance.__class__, (PSEUDO_PK_NAME, pk))
            if key in self._identity_map:
                del self._identity_map[key]

            # 触发 after_delete 事件（已从数据库和 identity map 移除）
            event.dispatch_model(instance.__class__, 'after_delete', instance)

        # 清空待处理列表
        self._new_objects.clear()
        self._dirty_objects.clear()
        self._deleted_objects.clear()

    def commit(self) -> None:
        """
        提交事务（刷新修改并持久化）
        """
        self.flush()

        # 原生 SQL 模式：提交连接器事务
        if self.storage._native_sql_mode:
            self.storage._native_sql_commit_transaction()

        # 如果启用了 auto_flush，触发持久化
        if self.storage.auto_flush:
            self.storage.flush()

    def rollback(self) -> None:
        """
        回滚事务（清空所有待处理修改）
        """
        # 原生 SQL 模式：回滚连接器事务
        if self.storage._native_sql_mode:
            self.storage._native_sql_rollback_transaction()

        self._new_objects.clear()
        self._dirty_objects.clear()
        self._deleted_objects.clear()
        self._identity_map.clear()

    def bulk_insert(self, instances: List[PureBaseModel]) -> List[Any]:
        """
        批量插入模型实例（立即写入内存）

        调用时直接写入 Storage 内存，commit() 仅负责 auto_flush 磁盘持久化。
        触发 before_bulk_insert / after_bulk_insert 事件，不触发逐条事件。

        与 add_all() 的区别：
        - add_all() 将实例标记为待插入，commit() 时逐条写入
        - bulk_insert() 立即批量写入 Storage，性能更优

        Args:
            instances: 模型实例列表（必须是同一模型类）

        Returns:
            插入的主键列表

        Raises:
            ValidationError: 包含不同模型类
            DuplicateKeyError: 主键重复
        """
        if not instances:
            return []

        # 验证：所有实例必须是同一模型类
        model_class = instances[0].__class__
        for inst in instances[1:]:
            if inst.__class__ is not model_class:
                raise ValidationError(
                    f"All instances must be the same model class. "
                    f"Expected {model_class.__name__}, got {inst.__class__.__name__}"
                )

        table_name = model_class.__tablename__
        assert table_name is not None, f"Model {model_class.__name__} must have __tablename__ defined"

        # 触发 before_bulk_insert 事件
        event.dispatch_model_bulk(model_class, 'before_bulk_insert', instances)

        # 构建数据字典列表
        records: List[Dict[str, Any]] = []
        for instance in instances:
            data: Dict[str, Any] = {}
            for attr_name, column in instance.__columns__.items():
                value = getattr(instance, attr_name, None)
                if value is not None:
                    db_col_name = column.name if column.name else attr_name
                    data[db_col_name] = value
            records.append(data)

        # 批量插入到 Storage
        pks = self.storage.bulk_insert(table_name, records)

        # 设置主键到实例 + 注册到 identity map
        pk_name = model_class.__primary_key__
        for i, instance in enumerate(instances):
            pk = pks[i]
            if pk_name:
                setattr(instance, pk_name, pk)
            else:
                setattr(instance, '_pytuck_rowid', pk)
            self._register_instance(instance)

        # 触发 after_bulk_insert 事件
        event.dispatch_model_bulk(model_class, 'after_bulk_insert', instances)

        return pks

    def bulk_update(self, instances: List[PureBaseModel]) -> int:
        """
        批量更新模型实例（立即写入内存，更新全部字段）

        调用时直接写入 Storage 内存，commit() 仅负责 auto_flush 磁盘持久化。
        触发 before_bulk_update / after_bulk_update 事件，不触发逐条事件。

        Args:
            instances: 模型实例列表（必须是同一模型类，且已有主键）

        Returns:
            更新的记录数

        Raises:
            ValidationError: 包含不同模型类或缺少主键
            RecordNotFoundError: 某条记录不存在
        """
        if not instances:
            return 0

        # 验证：所有实例必须是同一模型类
        model_class = instances[0].__class__
        for inst in instances[1:]:
            if inst.__class__ is not model_class:
                raise ValidationError(
                    f"All instances must be the same model class. "
                    f"Expected {model_class.__name__}, got {inst.__class__.__name__}"
                )

        table_name = model_class.__tablename__
        assert table_name is not None, f"Model {model_class.__name__} must have __tablename__ defined"
        pk_name = model_class.__primary_key__

        # 触发 before_bulk_update 事件
        event.dispatch_model_bulk(model_class, 'before_bulk_update', instances)

        # 构建 (pk, data) 元组列表
        updates: List[Tuple[Any, Dict[str, Any]]] = []
        for instance in instances:
            if pk_name:
                pk = getattr(instance, pk_name, None)
            else:
                pk = getattr(instance, '_pytuck_rowid', None)

            if pk is None:
                raise ValidationError(
                    "Cannot bulk update instance without primary key or rowid"
                )

            data: Dict[str, Any] = {}
            for attr_name, column in instance.__columns__.items():
                value = getattr(instance, attr_name, None)
                db_col_name = column.name if column.name else attr_name
                data[db_col_name] = value
            updates.append((pk, data))

        # 批量更新到 Storage
        count = self.storage.bulk_update(table_name, updates)

        # 触发 after_bulk_update 事件
        event.dispatch_model_bulk(model_class, 'after_bulk_update', instances)

        return count

    def get(self, model_class: Type[T], pk: Any) -> Optional[T]:
        """
        通过主键获取对象

        Args:
            model_class: 模型类
            pk: 主键值

        Returns:
            模型实例，如果不存在返回 None

        Raises:
            QueryError: 如果模型没有定义主键（reason='no_primary_key'）

        Note:
            无主键模型无法使用此方法，请使用 select() 语句进行查询。
        """
        # 无主键模型不支持 get() 方法，抛出明确错误
        if model_class.__primary_key__ is None:
            raise QueryError(
                f"Model '{model_class.__name__}' has no primary key. "
                f"Use select() statement to query records instead.",
                details={'model': model_class.__name__, 'reason': 'no_primary_key'}
            )

        # 先从标识映射查找
        instance = self._get_from_identity_map(model_class, pk)
        if instance is not None:
            return instance

        # 从数据库查询
        table_name = model_class.__tablename__
        assert table_name is not None, f"Model {model_class.__name__} must have __tablename__ defined"

        try:
            # 优化：直接使用 storage.select()，使得高层在需要时可以下推到后端（包括 PTK7 快速路径）
            record = self.storage.select(table_name, pk)

            # 将 Column.name 转换为属性名
            attr_data = {}
            for db_col_name, value in record.items():
                if db_col_name == PSEUDO_PK_NAME:
                    continue
                attr_name = model_class._column_to_attr_name(db_col_name) or db_col_name
                attr_data[attr_name] = value

            # 创建模型实例
            instance = model_class(**attr_data)

            # 注册到标识映射
            self._register_instance(instance)

            return instance
        except Exception:
            return None

    def refresh(self, instance: PureBaseModel) -> None:
        """
        从数据库刷新实例的所有属性

        类似 SQLAlchemy 的 session.refresh()，重新从数据库加载实例的所有字段值。

        Args:
            instance: 模型实例

        Raises:
            QueryError: 如果实例没有有效的主键或 rowid

        Example:
            user = session.get(User, 1)
            # ... 其他操作可能修改了数据库中的记录 ...
            session.refresh(user)  # 重新加载最新数据
        """
        model_class = instance.__class__
        pk_name = model_class.__primary_key__

        # 获取主键或 rowid
        if pk_name:
            pk_value = getattr(instance, pk_name, None)
        else:
            pk_value = getattr(instance, '_pytuck_rowid', None)

        if pk_value is None:
            raise QueryError(
                "Cannot refresh instance without primary key or rowid",
                details={'model': model_class.__name__}
            )

        table_name = instance.__tablename__
        assert table_name is not None, f"Model {model_class.__name__} must have __tablename__ defined"

        # 从数据库读取记录
        db_record = self.storage.select(table_name, pk_value)

        # 更新实例属性（使用 object.__setattr__ 避免触发脏跟踪）
        for db_col_name, value in db_record.items():
            if db_col_name != PSEUDO_PK_NAME:
                # 将 Column.name 转换回属性名
                attr_name = model_class._column_to_attr_name(db_col_name) or db_col_name
                object.__setattr__(instance, attr_name, value)

    # ==================== 语句执行（带类型重载） ====================

    @overload
    def execute(self, statement: Select[T]) -> Result[T]: ...

    @overload
    def execute(self, statement: Insert[T]) -> CursorResult[T]: ...

    @overload
    def execute(self, statement: Update[T]) -> CursorResult[T]: ...

    @overload
    def execute(self, statement: Delete[T]) -> CursorResult[T]: ...

    def execute(self, statement: Statement) -> Union[Result, CursorResult]:
        """
        执行 statement（SQLAlchemy 2.0 风格）

        Args:
            statement: Statement 对象 (Select, Insert, Update, Delete)

        Returns:
            Result 对象

        用法：
            # 查询
            stmt = select(User).where(User.age >= 18)
            result = session.execute(stmt)
            users = result.all()

            # 插入
            stmt = insert(User).values(name='Alice', age=20)
            result = session.execute(stmt)
            session.commit()
        """
        from ..query.statements import Select, Insert, Update, Delete
        from ..query.result import Result, CursorResult

        # 原生 SQL 模式：使用编译器执行
        if self.storage.is_native_sql_mode:
            return self._execute_native_sql(statement)

        # 内存模式：现有执行路径
        if isinstance(statement, Select):
            records = statement._execute(self.storage)
            # 传递 session 引用给 Result，用于自动注册实例
            # 传递 options（如 prefetch 选项）给 Result
            return Result(records, statement.model_class, 'select', session=self,
                          options=getattr(statement, '_options', []))

        elif isinstance(statement, Insert):
            result_pk = statement._execute(self.storage)
            # values_list 返回列表，单条返回标量
            if isinstance(result_pk, list):
                first_pk = result_pk[0] if result_pk else None
                return CursorResult(
                    len(result_pk), statement.model_class, 'insert', inserted_pk=first_pk
                )
            return CursorResult(1, statement.model_class, 'insert', inserted_pk=result_pk)

        elif isinstance(statement, Update):
            count = statement._execute(self.storage)
            return CursorResult(count, statement.model_class, 'update')

        elif isinstance(statement, Delete):
            count = statement._execute(self.storage)
            return CursorResult(count, statement.model_class, 'delete')

        else:
            raise QueryError(
                f"Unsupported statement type: {type(statement).__name__}",
                details={'statement_type': type(statement).__name__}
            )

    def _execute_native_sql(self, statement: Statement) -> Union[Result, CursorResult]:
        """
        原生 SQL 模式下执行语句

        使用 SQL 编译器将语句编译为 SQL 并直接在数据库上执行。

        Args:
            statement: Statement 对象

        Returns:
            Result 或 CursorResult
        """
        from ..query.statements import Select, Insert, Update, Delete
        from ..query.compiler import QueryCompiler
        from ..query.result import Result, CursorResult

        compiler = QueryCompiler()

        assert self.storage._connector is not None, "Connector must not be None in native SQL mode"
        connector = self.storage._connector

        if isinstance(statement, Select):
            # 编译并执行 SELECT
            if compiler.can_compile(statement):
                table = self.storage.get_table(statement.model_class.__tablename__)
                compiled = compiler.compile(statement)
                cursor = connector.execute(compiled.sql, compiled.params)
                rows_raw = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = [dict(zip(col_names, row)) for row in rows_raw]

                # 反序列化记录
                records = [self._deserialize_record(row, table.columns) for row in rows]
                return Result(records, statement.model_class, 'select', session=self,
                              options=getattr(statement, '_options', []))

            else:
                # 回退到内存执行
                records = statement._execute(self.storage)
                return Result(records, statement.model_class, 'select', session=self,
                              options=getattr(statement, '_options', []))

        elif isinstance(statement, Insert):
            # 编译并执行 INSERT
            table = self.storage.get_table(statement.model_class.__tablename__)

            # 批量插入路径（values_list）
            if statement._values_list is not None:
                validated_records: List[Dict[str, Any]] = []
                for record in statement._values_list:
                    validated_data_batch: Dict[str, Any] = {}
                    for attr_name, value in record.items():
                        if attr_name in statement.model_class.__columns__:
                            model_column = statement.model_class.__columns__[attr_name]
                            db_col_name = model_column.name if model_column.name else attr_name
                            if db_col_name in table.columns:
                                column = table.columns[db_col_name]
                                validated_data_batch[db_col_name] = column.validate(value)
                    validated_records.append(validated_data_batch)

                pks = self.storage.bulk_insert(
                    statement.model_class.__tablename__,
                    validated_records
                )
                first_pk = pks[0] if pks else None
                return CursorResult(
                    len(pks), statement.model_class, 'insert', inserted_pk=first_pk
                )

            # 单条插入路径
            # 验证和序列化值（使用 Column.name 作为存储键）
            validated_data = {}
            for attr_name, value in statement._values.items():
                # 将属性名转换为 Column.name
                if attr_name in statement.model_class.__columns__:
                    model_column = statement.model_class.__columns__[attr_name]
                    db_col_name = model_column.name if model_column.name else attr_name
                    if db_col_name in table.columns:
                        column = table.columns[db_col_name]
                        validated_data[db_col_name] = column.validate(value)

            pk = self.storage.insert(
                statement.model_class.__tablename__,
                validated_data
            )

            return CursorResult(1, statement.model_class, 'insert', inserted_pk=pk)

        elif isinstance(statement, Update):
            # 编译并执行 UPDATE
            table = self.storage.get_table(statement.model_class.__tablename__)
            pk_attr_name = statement.model_class.__primary_key__

            # 获取主键的 Column.name
            pk_col_name = pk_attr_name
            if pk_attr_name and pk_attr_name in statement.model_class.__columns__:
                pk_column = statement.model_class.__columns__[pk_attr_name]
                pk_col_name = pk_column.name if pk_column.name else pk_attr_name

            # 验证值（使用 Column.name 作为存储键）
            validated_data = {}
            for attr_name, value in statement._values.items():
                # 将属性名转换为 Column.name
                if attr_name in statement.model_class.__columns__:
                    model_column = statement.model_class.__columns__[attr_name]
                    db_col_name = model_column.name if model_column.name else attr_name
                    if db_col_name in table.columns:
                        column = table.columns[db_col_name]
                        validated_data[db_col_name] = column.validate(value)

            # 优化：主键直接更新（仅对简单 BinaryExpression 生效）
            pk_value = None
            if len(statement._where_clauses) == 1:
                expr = statement._where_clauses[0]
                if isinstance(expr, BinaryExpression):
                    # 比较属性名（_attr_name）而非 Column.name
                    if expr.column._attr_name == pk_attr_name and expr.operator in ('=', '=='):
                        pk_value = expr.value

            if pk_value is not None:
                # 主键直接更新
                count = connector.update_row(
                    statement.model_class.__tablename__,
                    pk_col_name,
                    pk_value,
                    validated_data
                )
                return CursorResult(count, statement.model_class, 'update')
            else:
                # 条件更新
                count = statement._execute(self.storage)
                return CursorResult(count, statement.model_class, 'update')

        elif isinstance(statement, Delete):
            # 编译并执行 DELETE
            pk_attr_name = statement.model_class.__primary_key__

            # 获取主键的 Column.name
            pk_col_name = pk_attr_name
            if pk_attr_name and pk_attr_name in statement.model_class.__columns__:
                pk_column = statement.model_class.__columns__[pk_attr_name]
                pk_col_name = pk_column.name if pk_column.name else pk_attr_name

            # 优化：主键直接删除（仅对简单 BinaryExpression 生效）
            pk_value = None
            if len(statement._where_clauses) == 1:
                expr = statement._where_clauses[0]
                if isinstance(expr, BinaryExpression):
                    # 比较属性名（_attr_name）而非 Column.name
                    if expr.column._attr_name == pk_attr_name and expr.operator in ('=', '=='):
                        pk_value = expr.value

            if pk_value is not None:
                # 主键直接删除
                count = connector.delete_row(
                    statement.model_class.__tablename__,
                    pk_col_name,
                    pk_value
                )
                return CursorResult(count, statement.model_class, 'delete')
            else:
                # 条件删除
                count = statement._execute(self.storage)
                return CursorResult(count, statement.model_class, 'delete')

        else:
            raise QueryError(
                f"Unsupported statement type: {type(statement).__name__}",
                details={'statement_type': type(statement).__name__}
            )

    @staticmethod
    def _deserialize_record(record: dict, columns: dict) -> dict:
        """
        反序列化数据库记录

        将数据库存储格式转换为 Python 对象。

        Args:
            record: 原始记录字典
            columns: 列定义字典

        Returns:
            反序列化后的记录字典
        """
        from datetime import datetime, date, timedelta
        from .types import TypeRegistry
        import json

        result: dict = {}
        for col_name, value in record.items():
            if col_name in columns and value is not None:
                column = columns[col_name]
                col_type = column.col_type

                if col_type == bool and isinstance(value, int):
                    value = bool(value)
                elif col_type in (datetime, date, timedelta) and isinstance(value, str):
                    value = TypeRegistry.deserialize_from_text(value, col_type)
                elif col_type in (list, dict) and isinstance(value, str):
                    value = json.loads(value)

            result[col_name] = value

        return result

    def _register_instance(self, instance: PureBaseModel) -> None:
        """
        注册实例到 identity map

        Args:
            instance: 模型实例
        """
        pk_name = instance.__primary_key__
        if pk_name:
            pk = getattr(instance, pk_name, None)
            if pk is not None:
                key = (instance.__class__, pk)
                self._identity_map[key] = instance
        else:
            # 无主键：使用隐式 rowid
            rowid = getattr(instance, '_pytuck_rowid', None)
            if rowid is not None:
                key = (instance.__class__, (PSEUDO_PK_NAME, rowid))
                self._identity_map[key] = instance

        # 设置实例的 session 引用，用于脏跟踪
        instance._pytuck_session = self
        instance._pytuck_state = 'persistent'

    def _get_from_identity_map(self, model_class: Type[T], pk: Any) -> Optional[T]:
        """
        从 identity map 获取实例

        Args:
            model_class: 模型类
            pk: 主键值

        Returns:
            模型实例，如果不存在返回 None
        """
        key = (model_class, pk)
        return self._identity_map.get(key)  # type: ignore

    def _mark_dirty(self, instance: PureBaseModel) -> None:
        """
        标记实例为 dirty（需要更新）

        Args:
            instance: 模型实例
        """
        if instance not in self._dirty_objects and instance not in self._new_objects:
            self._dirty_objects.append(instance)

    def merge(self, instance: PureBaseModel) -> PureBaseModel:
        """
        合并一个 detached 实例到会话中

        这个方法会检查实例是否已存在于 identity map 中：
        - 如果存在，更新现有实例的属性并返回现有实例
        - 如果不存在，从数据库加载或创建新实例，然后更新属性

        对于无主键模型，只能通过内部 rowid 来查找 identity map。
        如果没有 rowid，则作为新对象处理。

        Args:
            instance: 要合并的模型实例

        Returns:
            会话管理的实例（可能不是传入的同一个对象）

        Example:
            # 从外部来源获得的数据
            external_user = User(id=1, name="Updated Name", age=25)

            # 合并到会话
            managed_user = session.merge(external_user)
            session.commit()  # 提交更新
        """
        model_class = instance.__class__
        pk_name = model_class.__primary_key__

        # 获取主键值或 rowid
        if pk_name:
            pk_value = getattr(instance, pk_name, None)
        else:
            # 无主键模型：尝试获取 rowid
            pk_value = getattr(instance, '_pytuck_rowid', None)

        if pk_value is None:
            # 没有主键/rowid，作为新对象处理
            self.add(instance)
            return instance

        # 尝试从 identity map 获取现有实例
        if pk_name:
            existing = self._get_from_identity_map(model_class, pk_value)
        else:
            # 无主键模型使用特殊的 key 格式
            key = (model_class, (PSEUDO_PK_NAME, pk_value))
            existing = self._identity_map.get(key)  # type: ignore

        if existing is not None:
            # 已存在，更新其属性
            for col_name, column in model_class.__columns__.items():
                if hasattr(instance, col_name):
                    value = getattr(instance, col_name)
                    if getattr(existing, col_name) != value:
                        setattr(existing, col_name, value)
            return existing

        # 不存在于 identity map
        if pk_name:
            # 有主键：尝试从数据库加载
            existing = self.get(model_class, pk_value)

            if existing is not None:
                # 从数据库加载成功，更新属性
                for col_name, column in model_class.__columns__.items():
                    if hasattr(instance, col_name):
                        value = getattr(instance, col_name)
                        if getattr(existing, col_name) != value:
                            setattr(existing, col_name, value)
                return existing

        # 数据库中也不存在（或无主键模型），作为新对象处理
        self.add(instance)
        return instance

    def query(self, model_class: Type[T]) -> Query[T]:
        """
        创建查询构建器（SQLAlchemy 1.4 风格，不推荐，但也不警告）

        ⚠️ 不推荐使用：请改用 session.execute(select(...)) 风格

        推荐写法：
            from pytuck import select
            stmt = select(User).where(User.age >= 18)
            result = session.execute(stmt)
            users = result.all()

        旧写法（将依旧持续支持）：
            users = session.query(User).filter(User.age >= 18).all()

        Args:
            model_class: 模型类

        Returns:
            Query 对象
        """
        return Query(model_class, self.storage)

    @contextmanager
    def begin(self) -> Generator['Session', None, None]:
        """
        事务上下文管理器

        用法:
            with session.begin():
                session.add(User(name='Alice'))
                session.add(User(name='Bob'))
        """
        if self._in_transaction:
            raise TransactionError("Nested transactions are not supported in Session")

        self._in_transaction = True

        try:
            # 使用 Storage 的事务支持
            with self.storage.transaction():
                yield self
                # 提交 Session 级别的修改
                self.flush()
        except Exception:
            # 回滚 Session 状态
            self.rollback()
            raise
        finally:
            self._in_transaction = False

    def close(self) -> None:
        """
        关闭会话，清理所有状态
        """
        self.rollback()

    def __enter__(self) -> 'Session':
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Any) -> None:
        """上下文管理器出口"""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    # ==================== Schema 操作（面向模型） ====================

    @staticmethod
    def _resolve_table_name(model_or_table: Union[Type[PureBaseModel], str]) -> str:
        """
        解析表名

        Args:
            model_or_table: 模型类或表名字符串

        Returns:
            表名字符串
        """
        if isinstance(model_or_table, str):
            return model_or_table
        else:
            table_name = model_or_table.__tablename__
            assert table_name is not None, f"Model {model_or_table.__name__} must have __tablename__ defined"
            return table_name

    def sync_schema(
        self,
        model_class: Type[PureBaseModel],
        options: Optional[SyncOptions] = None
    ) -> SyncResult:
        """
        同步模型到数据库表结构

        从模型类中提取列定义，与数据库中的表结构对比并同步。

        Args:
            model_class: 模型类
            options: 同步选项

        Returns:
            SyncResult: 同步结果

        Example::

            from pytuck import SyncOptions

            result = session.sync_schema(User)
            if result.has_changes:
                print(f"Added columns: {result.columns_added}")

            # 自定义选项
            opts = SyncOptions(sync_column_comments=False)
            result = session.sync_schema(User, options=opts)
        """
        table_name = self._resolve_table_name(model_class)
        columns = list(model_class.__columns__.values())
        comment = getattr(model_class, '__table_comment__', None)
        return self.storage.sync_table_schema(table_name, columns, comment, options)

    def add_column(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        column: Column,
        default_value: Any = None
    ) -> None:
        """
        添加列

        Args:
            model_or_table: 模型类或表名字符串
            column: 列定义
            default_value: 为现有记录填充的默认值

        Example:
            from pytuck import Column

            # 通过模型类
            session.add_column(User, Column(int, nullable=True, name='age'))

            # 通过表名
            session.add_column('users', Column(int, nullable=True, name='age'))

            # 带默认值
            session.add_column(User, Column(str, name='status'), default_value='active')
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.add_column(table_name, column, default_value)

    def drop_column(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        column_name: str
    ) -> None:
        """
        删除列

        Args:
            model_or_table: 模型类或表名字符串
            column_name: 字段名（Column.name），而非 Python 属性名

        Example:
            # 通过模型类
            session.drop_column(User, 'old_field')

            # 通过表名
            session.drop_column('users', 'old_field')

            # 属性名与字段名不一致时，使用字段名
            # 定义：student_no = Column(str, name="Student No.")
            session.drop_column(Student, "Student No.")  # 正确
            # session.drop_column(Student, "student_no")  # 错误！
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.drop_column(table_name, column_name)

    def alter_column(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        column_name: str,
        *,
        col_type: Any = ...,
        nullable: Any = ...,
        default: Any = ...
    ) -> None:
        """
        修改列属性（类型、可空性、默认值）

        Args:
            model_or_table: 模型类或表名字符串
            column_name: 列名
            col_type: 新类型（... 表示不修改）
            nullable: 新的可空性（... 表示不修改）
            default: 新默认值（... 表示不修改）

        Example:
            # 修改列类型
            session.alter_column(User, 'age', col_type=str)

            # 设置为非空并提供默认值
            session.alter_column(User, 'status', nullable=False, default='active')

            # 通过表名修改
            session.alter_column('users', 'score', col_type=float)
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.alter_column(
            table_name, column_name,
            col_type=col_type, nullable=nullable, default=default
        )

    def set_primary_key(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        column_name: str
    ) -> None:
        """
        修改表的主键

        Args:
            model_or_table: 模型类或表名字符串
            column_name: 新主键列名

        Example:
            session.set_primary_key(User, 'email')
            session.set_primary_key('users', 'email')
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.set_primary_key(table_name, column_name)

    def reorder_columns(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        new_order: List[str]
    ) -> None:
        """
        重新排列列的顺序

        Args:
            model_or_table: 模型类或表名字符串
            new_order: 新的列名顺序列表

        Example:
            session.reorder_columns(User, ['id', 'email', 'name', 'age'])
            session.reorder_columns('users', ['id', 'email', 'name', 'age'])
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.reorder_columns(table_name, new_order)

    def update_table_comment(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        comment: Optional[str]
    ) -> None:
        """
        更新表备注

        Args:
            model_or_table: 模型类或表名字符串
            comment: 新的表备注

        Example:
            session.update_table_comment(User, '用户信息表')
            session.update_table_comment('users', '用户信息表')
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.update_table_comment(table_name, comment)

    def update_column(
        self,
        model_or_table: Union[Type[PureBaseModel], str],
        column_name: str,
        comment: Optional[str] = None,
        index: Optional[bool] = None
    ) -> None:
        """
        更新列属性

        Args:
            model_or_table: 模型类或表名字符串
            column_name: 字段名（Column.name），而非 Python 属性名
            comment: 新的列备注（None 表示不修改）
            index: 是否索引（None 表示不修改）

        Example:
            # 更新备注
            session.update_column(User, 'name', comment='用户名')

            # 添加索引
            session.update_column(User, 'email', index=True)

            # 同时更新
            session.update_column('users', 'phone', comment='电话号码', index=True)

            # 属性名与字段名不一致时，使用字段名
            # 定义：student_no = Column(str, name="Student No.")
            session.update_column(Student, "Student No.", comment="学号")
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.update_column(table_name, column_name, comment, index)

    def drop_table(self, model_or_table: Union[Type[PureBaseModel], str]) -> None:
        """
        删除表

        Args:
            model_or_table: 模型类或表名字符串

        Example:
            session.drop_table(TempData)
            session.drop_table('temp_data')
        """
        table_name = self._resolve_table_name(model_or_table)
        self.storage.drop_table(table_name)

    def rename_table(
        self,
        old_model_or_table: Union[Type[PureBaseModel], str],
        new_name: str
    ) -> None:
        """
        重命名表

        Args:
            old_model_or_table: 旧模型类或表名
            new_name: 新表名

        Example:
            session.rename_table(User, 'user_accounts')
            session.rename_table('users', 'user_accounts')
        """
        old_name = self._resolve_table_name(old_model_or_table)
        self.storage.rename_table(old_name, new_name)
