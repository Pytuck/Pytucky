from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Any, Dict, List, Optional, Tuple

from ..common.exceptions import SerializationError
from ..core.orm import Column
from ..core.types import TypeRegistry


MAGIC_V7 = b"PTK7"
HEADER_STRUCT = struct.Struct("<4sHHIQQQQQQI")
PK_DIR_INT_STRUCT = struct.Struct("<qQI")
TABLE_REF_PREFIX_STRUCT = struct.Struct("<H")
TABLE_REF_BODY_STRUCT = struct.Struct("<QQQQQQQQQQ")
NULL_BITMAP_STRUCT = struct.Struct("<I")


@dataclass(frozen=True)
class FileHeader:
    magic: bytes = MAGIC_V7
    version: int = 7
    flags: int = 0
    table_count: int = 0
    schema_offset: int = 64
    schema_size: int = 0
    table_ref_offset: int = 0
    table_ref_size: int = 0
    file_size: int = 0
    checksum: int = 0
    reserved: int = 0

    def pack(self) -> bytes:
        return HEADER_STRUCT.pack(
            self.magic,
            self.version,
            self.flags,
            self.table_count,
            self.schema_offset,
            self.schema_size,
            self.table_ref_offset,
            self.table_ref_size,
            self.file_size,
            self.checksum,
            self.reserved,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "FileHeader":
        if len(data) < HEADER_STRUCT.size:
            raise SerializationError(
                f"Not enough data to decode FileHeader (need {HEADER_STRUCT.size}, got {len(data)})"
            )
        unpacked = HEADER_STRUCT.unpack(data[: HEADER_STRUCT.size])
        header = cls(*unpacked)
        if header.magic != MAGIC_V7:
            raise SerializationError(f"Invalid PTK7 magic: expected {MAGIC_V7!r}, got {header.magic!r}")
        if header.version != 7:
            raise SerializationError(f"Unsupported PTK7 version: {header.version}")
        return header


@dataclass(frozen=True)
class TableBlockRef:
    name: str
    record_count: int
    next_id: int
    data_offset: int
    data_size: int
    pk_dir_offset: int
    pk_dir_size: int
    index_meta_offset: int
    index_meta_size: int
    index_data_offset: int
    index_data_size: int

    def pack(self) -> bytes:
        name_bytes = self.name.encode("utf-8")
        if len(name_bytes) > 0xFFFF:
            raise SerializationError(
                f"Table name too long for PTK7 TableBlockRef: {len(name_bytes)} bytes"
            )
        return b"".join(
            [
                TABLE_REF_PREFIX_STRUCT.pack(len(name_bytes)),
                name_bytes,
                TABLE_REF_BODY_STRUCT.pack(
                    self.record_count,
                    self.next_id,
                    self.data_offset,
                    self.data_size,
                    self.pk_dir_offset,
                    self.pk_dir_size,
                    self.index_meta_offset,
                    self.index_meta_size,
                    self.index_data_offset,
                    self.index_data_size,
                ),
            ]
        )

    @classmethod
    def unpack(cls, data: bytes) -> Tuple["TableBlockRef", int]:
        if len(data) < TABLE_REF_PREFIX_STRUCT.size:
            raise SerializationError("Not enough data to decode table name length")
        name_length = TABLE_REF_PREFIX_STRUCT.unpack(data[: TABLE_REF_PREFIX_STRUCT.size])[0]
        start = TABLE_REF_PREFIX_STRUCT.size
        end = start + name_length
        body_end = end + TABLE_REF_BODY_STRUCT.size
        if len(data) < body_end:
            raise SerializationError("Not enough data to decode TableBlockRef")
        try:
            name = data[start:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SerializationError("Invalid UTF-8 table name in TableBlockRef") from exc
        body = TABLE_REF_BODY_STRUCT.unpack(data[end:body_end])
        return cls(name, *body), body_end


@dataclass(frozen=True)
class PkDirEntry:
    pk: Any
    offset: int
    length: int

    def pack_int(self) -> bytes:
        if not isinstance(self.pk, int):
            raise SerializationError(f"Expected int pk, got {type(self.pk)}")
        return PK_DIR_INT_STRUCT.pack(self.pk, self.offset, self.length)

    @classmethod
    def unpack_int(cls, data: bytes) -> "PkDirEntry":
        if len(data) < PK_DIR_INT_STRUCT.size:
            raise SerializationError(
                f"Not enough data to decode int pk entry (need {PK_DIR_INT_STRUCT.size}, got {len(data)})"
            )
        pk, offset, length = PK_DIR_INT_STRUCT.unpack(data[: PK_DIR_INT_STRUCT.size])
        return cls(pk=pk, offset=offset, length=length)


@dataclass(frozen=True)
class ColumnIndexMeta:
    column_name: str
    offset: int
    size: int
    entry_count: int
    type_code: int

    # 格式: <H name_len><name_bytes><Q offset><Q size><I entry_count><B type_code>
    STRUCT = struct.Struct('<H')
    BODY_STRUCT = struct.Struct('<Q Q I B')

    def pack(self) -> bytes:
        name_bytes = self.column_name.encode('utf-8')
        if len(name_bytes) > 0xFFFF:
            raise SerializationError('column name too long')
        return b''.join([
            self.STRUCT.pack(len(name_bytes)),
            name_bytes,
            self.BODY_STRUCT.pack(self.offset, self.size, self.entry_count, int(self.type_code)),
        ])

    @classmethod
    def unpack(cls, data: bytes) -> Tuple['ColumnIndexMeta', int]:
        if len(data) < cls.STRUCT.size:
            raise SerializationError('not enough data for ColumnIndexMeta name length')
        name_len = cls.STRUCT.unpack(data[: cls.STRUCT.size])[0]
        pos = cls.STRUCT.size
        end_name = pos + name_len
        if len(data) < end_name + cls.BODY_STRUCT.size:
            raise SerializationError('not enough data for ColumnIndexMeta body')
        try:
            name = data[pos:end_name].decode('utf-8')
        except UnicodeDecodeError as exc:
            raise SerializationError('invalid utf-8 in column name') from exc
        body = cls.BODY_STRUCT.unpack(data[end_name: end_name + cls.BODY_STRUCT.size])
        offset, size, entry_count, type_code = body
        total = end_name + cls.BODY_STRUCT.size
        return cls(column_name=name, offset=offset, size=size, entry_count=entry_count, type_code=type_code), total


def _payload_columns(columns: List[Column], pk_name: Optional[str]) -> List[Column]:
    return [column for column in columns if column.name != pk_name]


def encode_row(columns: List[Column], record: Dict[str, Any], pk_name: Optional[str] = None) -> bytes:
    payload_columns = _payload_columns(columns, pk_name)
    if len(payload_columns) > 32:
        raise SerializationError("encode_row currently supports at most 32 non-pk columns")

    null_bits = 0
    payload = bytearray()
    for index, column in enumerate(payload_columns):
        value = record.get(column.name)
        if value is None:
            null_bits |= 1 << index
            continue
        _, codec = TypeRegistry.get_codec(column.col_type)
        payload.extend(codec.encode(value))
    return NULL_BITMAP_STRUCT.pack(null_bits) + bytes(payload)


def decode_row(columns: List[Column], payload: bytes, pk_name: Optional[str] = None) -> Dict[str, Any]:
    payload_columns = _payload_columns(columns, pk_name)
    if len(payload) < NULL_BITMAP_STRUCT.size:
        raise SerializationError("Not enough data to decode row null bitmap")

    null_bits = NULL_BITMAP_STRUCT.unpack(payload[: NULL_BITMAP_STRUCT.size])[0]
    offset = NULL_BITMAP_STRUCT.size
    decoded: Dict[str, Any] = {}
    for index, column in enumerate(payload_columns):
        if null_bits & (1 << index):
            decoded[column.name] = None
            continue
        _, codec = TypeRegistry.get_codec(column.col_type)
        try:
            value, consumed = codec.decode(payload[offset:])
        except SerializationError:
            raise
        except Exception as exc:
            raise SerializationError(
                f"Failed to decode column {column.name!r} at payload offset {offset}"
            ) from exc
        if consumed <= 0 or offset + consumed > len(payload):
            raise SerializationError(
                f"Invalid consumed size while decoding column {column.name!r}: {consumed}"
            )
        decoded[column.name] = value
        offset += consumed
    return decoded
