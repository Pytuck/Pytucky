"""
Pytuck 类型系统

定义数据类型编码和编解码器
"""

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Any, Callable, Tuple, Dict, Type
import struct
import json
import base64
from datetime import datetime, date, timedelta, timezone

from ..common.exceptions import SerializationError
from ..common.typing import ColumnTypes


class TypeCode(IntEnum):
    """类型编码"""
    INT = 1
    STR = 2
    FLOAT = 3
    BOOL = 4
    BYTES = 5
    DATETIME = 6
    DATE = 7
    TIMEDELTA = 8
    LIST = 9
    DICT = 10


class TypeCodec(ABC):
    """类型编解码器抽象基类"""

    @abstractmethod
    def encode(self, value: Any) -> bytes:
        """编码值为字节"""
        pass

    @abstractmethod
    def decode(self, data: bytes) -> Tuple[Any, int]:
        """解码字节为值，返回(值, 消耗的字节数)"""
        pass


class IntCodec(TypeCodec):
    """整型编解码器"""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, int):
            raise SerializationError(f"Expected int, got {type(value)}")
        return struct.pack('<q', value)  # 8 bytes, signed long long, little-endian

    def decode(self, data: bytes) -> Tuple[int, int]:
        if len(data) < 8:
            raise SerializationError(f"Not enough data to decode int (need 8 bytes, got {len(data)})")
        value = struct.unpack('<q', data[:8])[0]
        return value, 8


class StrCodec(TypeCodec):
    """字符串编解码器"""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, str):
            raise SerializationError(f"Expected str, got {type(value)}")
        encoded = value.encode('utf-8')
        length = len(encoded)
        return struct.pack('<H', length) + encoded  # 2 bytes length + data

    def decode(self, data: bytes) -> Tuple[str, int]:
        if len(data) < 2:
            raise SerializationError(f"Not enough data to decode str length")
        length = struct.unpack('<H', data[:2])[0]
        if len(data) < 2 + length:
            raise SerializationError(f"Not enough data to decode str (need {2 + length}, got {len(data)})")
        value = data[2:2+length].decode('utf-8')
        return value, 2 + length


class FloatCodec(TypeCodec):
    """浮点型编解码器"""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, (float, int)):
            raise SerializationError(f"Expected float, got {type(value)}")
        return struct.pack('<d', float(value))  # 8 bytes, double, little-endian

    def decode(self, data: bytes) -> Tuple[float, int]:
        if len(data) < 8:
            raise SerializationError(f"Not enough data to decode float (need 8 bytes, got {len(data)})")
        value = struct.unpack('<d', data[:8])[0]
        return value, 8


class BoolCodec(TypeCodec):
    """布尔型编解码器"""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, bool):
            raise SerializationError(f"Expected bool, got {type(value)}")
        return struct.pack('<?', value)  # 1 byte

    def decode(self, data: bytes) -> Tuple[bool, int]:
        if len(data) < 1:
            raise SerializationError(f"Not enough data to decode bool")
        value = struct.unpack('<?', data[:1])[0]
        return value, 1


class BytesCodec(TypeCodec):
    """字节型编解码器"""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, bytes):
            raise SerializationError(f"Expected bytes, got {type(value)}")
        length = len(value)
        return struct.pack('<I', length) + value  # 4 bytes length + data

    def decode(self, data: bytes) -> Tuple[bytes, int]:
        if len(data) < 4:
            raise SerializationError(f"Not enough data to decode bytes length")
        length = struct.unpack('<I', data[:4])[0]
        if len(data) < 4 + length:
            raise SerializationError(f"Not enough data to decode bytes (need {4 + length}, got {len(data)})")
        value = data[4:4+length]
        return value, 4 + length


class DatetimeCodec(TypeCodec):
    """日期时间编解码器（支持时区）

    编码格式：
    - 8 bytes: UTC 时间戳（微秒，int64）
    - 2 bytes: 时区偏移（分钟，int16，0x7FFF 表示 naive datetime）
    """

    NAIVE_TZ_MARKER = 0x7FFF  # 32767，表示无时区信息

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, datetime):
            raise SerializationError(f"Expected datetime, got {type(value)}")

        # 计算时间戳（微秒）
        if value.tzinfo is not None:
            # 有时区：转换为 UTC 时间戳
            utc_dt = value.astimezone(timezone.utc)
            timestamp_us = int(utc_dt.timestamp() * 1_000_000)
            # 获取时区偏移（分钟）
            offset = value.utcoffset()
            if offset is not None:
                tz_offset_minutes = int(offset.total_seconds() // 60)
            else:
                tz_offset_minutes = self.NAIVE_TZ_MARKER
        else:
            # 无时区（naive）：直接使用本地时间戳
            timestamp_us = int(value.timestamp() * 1_000_000)
            tz_offset_minutes = self.NAIVE_TZ_MARKER

        return struct.pack('<qh', timestamp_us, tz_offset_minutes)

    def decode(self, data: bytes) -> Tuple[datetime, int]:
        if len(data) < 10:
            raise SerializationError(f"Not enough data to decode datetime (need 10 bytes, got {len(data)})")

        timestamp_us, tz_offset_minutes = struct.unpack('<qh', data[:10])

        if tz_offset_minutes == self.NAIVE_TZ_MARKER:
            # naive datetime：从本地时间戳恢复
            value = datetime.fromtimestamp(timestamp_us / 1_000_000)
        else:
            # aware datetime：从 UTC 时间戳恢复，然后转换到原始时区
            utc_dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
            tz = timezone(timedelta(minutes=tz_offset_minutes))
            value = utc_dt.astimezone(tz)

        return value, 10


class DateCodec(TypeCodec):
    """日期编解码器

    编码格式：
    - 4 bytes: 从 1970-01-01 开始的天数（int32）
    """

    EPOCH = date(1970, 1, 1)

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, date):
            raise SerializationError(f"Expected date, got {type(value)}")
        # 如果是 datetime，只取日期部分
        if isinstance(value, datetime):
            value = value.date()
        days = (value - self.EPOCH).days
        return struct.pack('<i', days)

    def decode(self, data: bytes) -> Tuple[date, int]:
        if len(data) < 4:
            raise SerializationError(f"Not enough data to decode date (need 4 bytes, got {len(data)})")
        days = struct.unpack('<i', data[:4])[0]
        value = self.EPOCH + timedelta(days=days)
        return value, 4


class TimedeltaCodec(TypeCodec):
    """时间间隔编解码器

    编码格式：
    - 8 bytes: 总秒数（float64，支持微秒精度）
    """

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, timedelta):
            raise SerializationError(f"Expected timedelta, got {type(value)}")
        total_seconds = value.total_seconds()
        return struct.pack('<d', total_seconds)

    def decode(self, data: bytes) -> Tuple[timedelta, int]:
        if len(data) < 8:
            raise SerializationError(f"Not enough data to decode timedelta (need 8 bytes, got {len(data)})")
        total_seconds = struct.unpack('<d', data[:8])[0]
        value = timedelta(seconds=total_seconds)
        return value, 8


class ListCodec(TypeCodec):
    """列表编解码器（JSON 序列化）

    编码格式：
    - 4 bytes: JSON 字符串长度（uint32）
    - N bytes: JSON 字符串（UTF-8）
    """

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, list):
            raise SerializationError(f"Expected list, got {type(value)}")
        json_str = json.dumps(value, ensure_ascii=False)
        encoded = json_str.encode('utf-8')
        return struct.pack('<I', len(encoded)) + encoded

    def decode(self, data: bytes) -> Tuple[list, int]:
        if len(data) < 4:
            raise SerializationError(f"Not enough data to decode list length")
        length = struct.unpack('<I', data[:4])[0]
        if len(data) < 4 + length:
            raise SerializationError(f"Not enough data to decode list (need {4 + length}, got {len(data)})")
        json_str = data[4:4+length].decode('utf-8')
        value = json.loads(json_str)
        return value, 4 + length


class DictCodec(TypeCodec):
    """字典编解码器（JSON 序列化）

    编码格式：
    - 4 bytes: JSON 字符串长度（uint32）
    - N bytes: JSON 字符串（UTF-8）
    """

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b''
        if not isinstance(value, dict):
            raise SerializationError(f"Expected dict, got {type(value)}")
        json_str = json.dumps(value, ensure_ascii=False)
        encoded = json_str.encode('utf-8')
        return struct.pack('<I', len(encoded)) + encoded

    def decode(self, data: bytes) -> Tuple[dict, int]:
        if len(data) < 4:
            raise SerializationError(f"Not enough data to decode dict length")
        length = struct.unpack('<I', data[:4])[0]
        if len(data) < 4 + length:
            raise SerializationError(f"Not enough data to decode dict (need {4 + length}, got {len(data)})")
        json_str = data[4:4+length].decode('utf-8')
        value = json.loads(json_str)
        return value, 4 + length


# ========== 文本序列化函数 ==========

def _serialize_bytes(value: Any) -> str:
    """序列化 bytes 为 base64 字符串"""
    return base64.b64encode(value).decode('ascii')


def _serialize_datetime(value: Any) -> str:
    """序列化 datetime 为 ISO 格式字符串"""
    return value.isoformat()


def _serialize_date(value: Any) -> str:
    """序列化 date 为 ISO 格式字符串"""
    return value.isoformat()


def _serialize_timedelta(value: Any) -> float:
    """序列化 timedelta 为总秒数"""
    return value.total_seconds()


def _serialize_json(value: Any) -> str:
    """序列化 list/dict 为 JSON 字符串"""
    return json.dumps(value, ensure_ascii=False)


# 文本序列化函数注册表
_TEXT_SERIALIZERS: Dict[type, Callable[[Any], Any]] = {
    bytes: _serialize_bytes,
    datetime: _serialize_datetime,
    date: _serialize_date,
    timedelta: _serialize_timedelta,
    list: _serialize_json,
    dict: _serialize_json,
}


# ========== 文本反序列化函数 ==========

def _deserialize_bytes(value: Any) -> bytes:
    """反序列化 bytes"""
    if isinstance(value, bytes):
        return value
    return base64.b64decode(value)


def _deserialize_datetime(value: Any) -> datetime:
    """反序列化 datetime"""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _deserialize_date(value: Any) -> date:
    """反序列化 date"""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(value)


def _deserialize_timedelta(value: Any) -> timedelta:
    """反序列化 timedelta"""
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=float(value))


def _deserialize_list(value: Any) -> list:
    """反序列化 list"""
    if isinstance(value, list):
        return value
    return json.loads(value)


def _deserialize_dict(value: Any) -> dict:
    """反序列化 dict"""
    if isinstance(value, dict):
        return value
    return json.loads(value)


def _deserialize_bool(value: Any) -> bool:
    """反序列化 bool"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    return bool(value)


def _deserialize_int(value: Any) -> int:
    """反序列化 int"""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return int(value)


def _deserialize_float(value: Any) -> float:
    """反序列化 float"""
    if isinstance(value, float):
        return value
    return float(value)


# 文本反序列化函数注册表
_TEXT_DESERIALIZERS: Dict[Type, Callable[[Any], Any]] = {
    bytes: _deserialize_bytes,
    datetime: _deserialize_datetime,
    date: _deserialize_date,
    timedelta: _deserialize_timedelta,
    list: _deserialize_list,
    dict: _deserialize_dict,
    bool: _deserialize_bool,
    int: _deserialize_int,
    float: _deserialize_float,
}


class TypeRegistry:
    """类型注册表"""

    _codecs: Dict[ColumnTypes, Tuple[TypeCode, TypeCodec]] = {
        int: (TypeCode.INT, IntCodec()),
        str: (TypeCode.STR, StrCodec()),
        float: (TypeCode.FLOAT, FloatCodec()),
        bool: (TypeCode.BOOL, BoolCodec()),
        bytes: (TypeCode.BYTES, BytesCodec()),
        datetime: (TypeCode.DATETIME, DatetimeCodec()),
        date: (TypeCode.DATE, DateCodec()),
        timedelta: (TypeCode.TIMEDELTA, TimedeltaCodec()),
        list: (TypeCode.LIST, ListCodec()),
        dict: (TypeCode.DICT, DictCodec()),
    }

    _type_code_to_type: Dict[TypeCode, ColumnTypes] = {
        TypeCode.INT: int,
        TypeCode.STR: str,
        TypeCode.FLOAT: float,
        TypeCode.BOOL: bool,
        TypeCode.BYTES: bytes,
        TypeCode.DATETIME: datetime,
        TypeCode.DATE: date,
        TypeCode.TIMEDELTA: timedelta,
        TypeCode.LIST: list,
        TypeCode.DICT: dict,
    }

    @classmethod
    def get_codec(cls, col_type: ColumnTypes) -> Tuple[TypeCode, TypeCodec]:
        """获取类型的编解码器"""
        if col_type not in cls._codecs:
            raise SerializationError(f"Unsupported type: {col_type}")
        return cls._codecs[col_type]

    @classmethod
    def get_type_from_code(cls, type_code: TypeCode) -> ColumnTypes:
        """根据类型编码获取Python类型"""
        if type_code not in cls._type_code_to_type:
            raise SerializationError(f"Unknown type code: {type_code}")
        return cls._type_code_to_type[type_code]

    @classmethod
    def get_codec_by_code(cls, type_code: TypeCode) -> Tuple[TypeCode, TypeCodec]:
        """根据类型编码获取编解码器"""
        py_type = cls.get_type_from_code(type_code)
        return cls.get_codec(py_type)

    @classmethod
    def register(cls, py_type: ColumnTypes, type_code: TypeCode, codec: TypeCodec) -> None:
        """注册自定义类型"""
        cls._codecs[py_type] = (type_code, codec)
        cls._type_code_to_type[type_code] = py_type

    # ========== 文本格式序列化支持 ==========

    # 类型名称映射（用于文本格式存储）
    _type_names: Dict[ColumnTypes, str] = {
        int: 'int',
        str: 'str',
        float: 'float',
        bool: 'bool',
        bytes: 'bytes',
        datetime: 'datetime',
        date: 'date',
        timedelta: 'timedelta',
        list: 'list',
        dict: 'dict',
    }

    # 名称到类型的反向映射
    _name_to_type = {v: k for k, v in _type_names.items()}

    @classmethod
    def get_type_name(cls, col_type: ColumnTypes) -> str:
        """获取类型的字符串名称

        Args:
            col_type: Python 类型

        Returns:
            类型名称字符串，未知类型返回 'str'
        """
        return cls._type_names.get(col_type, 'str')

    @classmethod
    def get_type_by_name(cls, name: str) -> ColumnTypes:
        """根据名称获取类型

        Args:
            name: 类型名称字符串

        Returns:
            Python 类型，未知名称返回 str
        """
        return cls._name_to_type.get(name, str)

    @classmethod
    def serialize_for_text(cls, value: Any, col_type: ColumnTypes) -> Any:
        """序列化值为文本格式存储

        将 Python 值转换为适合文本格式（JSON、CSV、Excel、XML）存储的形式。

        Args:
            value: 要序列化的值
            col_type: 列的类型

        Returns:
            序列化后的值（可能是字符串、数字或 None）
        """
        if value is None:
            return None

        serializer = _TEXT_SERIALIZERS.get(col_type)
        if serializer is not None:
            return serializer(value)
        # int, str, float, bool 保持原样
        return value

    @classmethod
    def deserialize_from_text(cls, value: Any, col_type: ColumnTypes) -> Any:
        """从文本格式反序列化值

        将文本格式存储的值转换回 Python 类型。

        Args:
            value: 存储的值
            col_type: 目标类型

        Returns:
            反序列化后的 Python 值
        """
        if value is None or value == '':
            return None

        deserializer = _TEXT_DESERIALIZERS.get(col_type)
        if deserializer is not None:
            return deserializer(value)
        # str 或其他类型保持原样
        return value
