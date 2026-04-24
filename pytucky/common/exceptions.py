"""
Pytucky 异常定义

提供统一的异常层次结构，便于用户捕获和处理错误。
所有 Pytucky 异常都继承自 PytuckyException 基类。
"""

from __future__ import annotations

from typing import Any

class PytuckyException(Exception):
    """
    Pytucky 基础异常类

    所有 Pytucky 异常都继承自此类，提供统一的字段和方法。

    Attributes:
        message: 错误消息
        table_name: 相关的表名（可选）
        column_name: 相关的列名（可选）
        pk: 相关的主键值（可选）
        details: 额外的详细信息字典（可选）
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        pk: Any = None,
        details: dict[str, Any] | None = None
    ):
        self.message = message
        self.table_name = table_name
        self.column_name = column_name
        self.pk = pk
        self.details = details or {}
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """
        将异常转换为字典，便于日志记录和序列化

        Returns:
            包含异常信息的字典
        """
        result: dict[str, Any] = {
            'error': self.__class__.__name__,
            'message': self.message
        }
        if self.table_name:
            result['table_name'] = self.table_name
        if self.column_name:
            result['column_name'] = self.column_name
        if self.pk is not None:
            result['pk'] = self.pk
        if self.details:
            result['details'] = self.details
        return result

# =============================================================================
# 表/记录相关异常
# =============================================================================

class TableNotFoundError(PytuckyException):
    """表不存在异常"""

    def __init__(self, table_name: str):
        super().__init__(
            f"Table '{table_name}' not found",
            table_name=table_name
        )

class RecordNotFoundError(PytuckyException):
    """记录不存在异常"""

    def __init__(self, table_name: str, pk: Any):
        super().__init__(
            f"Record with primary key '{pk}' not found in table '{table_name}'",
            table_name=table_name,
            pk=pk
        )

class DuplicateKeyError(PytuckyException):
    """主键重复异常"""

    def __init__(self, table_name: str, pk: Any):
        super().__init__(
            f"Duplicate primary key '{pk}' in table '{table_name}'",
            table_name=table_name,
            pk=pk
        )

class ColumnNotFoundError(PytuckyException):
    """列不存在异常"""

    def __init__(self, table_name: str, column_name: str):
        super().__init__(
            f"Column '{column_name}' not found in table '{table_name}'",
            table_name=table_name,
            column_name=column_name
        )

# =============================================================================
# 验证相关异常
# =============================================================================

class ValidationError(PytuckyException):
    """
    数据验证异常

    当数据不符合预期格式或约束时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        pk: Any = None,
        details: dict[str, Any] | None = None
    ):
        super().__init__(
            message,
            table_name=table_name,
            column_name=column_name,
            pk=pk,
            details=details
        )

class TypeConversionError(ValidationError):
    """
    类型转换异常

    当值无法转换为目标类型时抛出。
    继承自 ValidationError，便于统一捕获验证错误。

    Attributes:
        value: 无法转换的原始值
        target_type: 目标类型名称
    """

    def __init__(
        self,
        message: str,
        *,
        value: Any = None,
        target_type: str | None = None,
        column_name: str | None = None,
        details: dict[str, Any] | None = None
    ):
        extra_details = details or {}
        if value is not None:
            extra_details['value'] = repr(value)
            extra_details['value_type'] = type(value).__name__
        if target_type:
            extra_details['target_type'] = target_type

        super().__init__(
            message,
            column_name=column_name,
            details=extra_details
        )
        self.value = value
        self.target_type = target_type

# =============================================================================
# 配置相关异常
# =============================================================================

class ConfigurationError(PytuckyException):
    """
    配置异常

    当引擎配置、后端选项或其他配置不正确时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

class SchemaError(ConfigurationError):
    """
    Schema 定义异常

    当表结构定义不正确时抛出（如缺少主键）。
    继承自 ConfigurationError。
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        details: dict[str, Any] | None = None
    ):
        # 需要重新调用 PytuckyException.__init__ 以设置 table_name
        PytuckyException.__init__(
            self,
            message,
            table_name=table_name,
            details=details
        )

# =============================================================================
# 查询相关异常
# =============================================================================

class QueryError(PytuckyException):
    """
    查询异常

    当查询构建或执行失败时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        details: dict[str, Any] | None = None
    ):
        super().__init__(
            message,
            table_name=table_name,
            column_name=column_name,
            details=details
        )

# =============================================================================
# 事务相关异常
# =============================================================================

class TransactionError(PytuckyException):
    """
    事务异常

    当事务操作失败时抛出（如嵌套事务不支持）。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

# =============================================================================
# 连接相关异常
# =============================================================================

class DatabaseConnectionError(PytuckyException):
    """
    数据库连接异常

    当数据库连接未建立或已断开时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

# =============================================================================
# 序列化相关异常
# =============================================================================

class SerializationError(PytuckyException):
    """
    序列化/反序列化异常

    当数据序列化或反序列化失败时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, table_name=table_name, details=details)

# =============================================================================
# 加密相关异常
# =============================================================================

class EncryptionError(PytuckyException):
    """
    加密/解密异常

    当加密或解密操作失败时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

# =============================================================================
# 迁移相关异常
# =============================================================================

class MigrationError(PytuckyException):
    """
    数据迁移异常

    当数据迁移操作失败时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

# =============================================================================
# 索引相关异常
# =============================================================================

class PytuckyIndexError(PytuckyException):
    """
    索引异常

    当索引操作失败时抛出。
    """

    def __init__(
        self,
        message: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        details: dict[str, Any] | None = None
    ):
        super().__init__(
            message,
            table_name=table_name,
            column_name=column_name,
            details=details
        )

# =============================================================================
# 不支持的操作异常
# =============================================================================

class UnsupportedOperationError(PytuckyException):
    """
    不支持的操作异常

    当请求的操作在当前上下文中不支持时抛出。
    例如：在非 SELECT 结果上调用 all()、不支持的后端功能等。
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None
    ):
        super().__init__(message, details=details)

# 兼容旧名称，避免外部导入立即断裂
PytuckException = PytuckyException
PytuckIndexError = PytuckyIndexError
