"""
Pytuck 二进制存储引擎

默认的持久化引擎，使用自定义二进制格式，无外部依赖
"""

import io
import json
import os
import struct
import tempfile
import zlib
from dataclasses import dataclass, field, replace
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, Union, TYPE_CHECKING, BinaryIO, Tuple, Optional, Iterator, Type, cast

if TYPE_CHECKING:
    from ..core.storage import Table

from .base import StorageBackend
from ..common.exceptions import SerializationError, EncryptionError
from ..core.types import TypeRegistry, TypeCode
from ..core.orm import Column, PSEUDO_PK_NAME
from ..core.index import BaseIndex, HashIndex, SortedIndex
from .versions import get_format_version

from ..common.options import BinaryBackendOptions
from ..common.crypto import (
    CryptoProvider, get_cipher, get_encryption_level_code, get_encryption_level_name,
    ENCRYPTION_LEVELS, CipherType
)


# ============== 索引值反序列化函数 ==============

def _deserialize_short_str(data: bytes) -> str:
    """反序列化短字符串（<= 255 字节）"""
    length = data[1]
    return data[2:2+length].decode('utf-8')


def _deserialize_long_str(data: bytes) -> str:
    """反序列化长字符串（<= 65535 字节）"""
    length = struct.unpack('<H', data[1:3])[0]
    return data[3:3+length].decode('utf-8')


def _deserialize_json(data: bytes) -> Any:
    """反序列化 JSON 回退"""
    length = struct.unpack('<H', data[1:3])[0]
    return json.loads(data[3:3+length].decode('utf-8'))


# 索引值反序列化函数注册表
# 类型码 -> 反序列化函数
_INDEX_VALUE_DESERIALIZERS: Dict[int, Callable[[bytes], Any]] = {
    0x00: lambda data: None,
    0x01: lambda data: data[1] == 1,
    0x02: lambda data: struct.unpack('<b', data[1:2])[0],
    0x03: lambda data: struct.unpack('<h', data[1:3])[0],
    0x04: lambda data: struct.unpack('<i', data[1:5])[0],
    0x05: lambda data: struct.unpack('<q', data[1:9])[0],
    0x06: lambda data: struct.unpack('<d', data[1:9])[0],
    0x07: _deserialize_short_str,
    0x08: _deserialize_long_str,
    0xFF: _deserialize_json,
}


# ============== PTK5 数据结构定义 ==============

class WALOpType(IntEnum):
    """WAL 操作类型"""
    INSERT = 1
    UPDATE = 2
    DELETE = 3


@dataclass
class HeaderV5:
    """PTK5 文件头结构 (128 bytes)"""
    magic: bytes = b'PTK5'
    version: int = 5
    generation: int = 0
    schema_offset: int = 0
    schema_size: int = 0
    data_offset: int = 0
    data_size: int = 0
    index_offset: int = 0
    index_size: int = 0
    wal_offset: int = 0
    wal_size: int = 0
    checkpoint_lsn: int = 0
    flags: int = 0
    crc32: int = 0
    # 加密元数据（存储在 reserved 区域）
    salt: bytes = field(default_factory=lambda: b'\x00' * 16)
    key_check: bytes = field(default_factory=lambda: b'\x00' * 4)

    HEADER_SIZE = 128
    MAGIC_V5 = b'PTK5'

    # flags 位定义
    FLAG_INDEX_COMPRESSED = 0x01    # bit 0: 索引区已压缩
    FLAG_ENCRYPTION_ENABLED = 0x02  # bit 1: 加密已启用
    FLAG_ENCRYPTION_LEVEL_MASK = 0x0C  # bit 2-3: 加密等级 (00=none, 01=low, 10=medium, 11=high)
    FLAG_ENCRYPTION_LEVEL_SHIFT = 2

    def pack(self) -> bytes:
        """序列化为 128 字节"""
        buf = bytearray(self.HEADER_SIZE)

        buf[0:4] = self.magic
        struct.pack_into('<H', buf, 4, self.version)
        struct.pack_into('<Q', buf, 6, self.generation)
        struct.pack_into('<Q', buf, 14, self.schema_offset)
        struct.pack_into('<Q', buf, 22, self.schema_size)
        struct.pack_into('<Q', buf, 30, self.data_offset)
        struct.pack_into('<Q', buf, 38, self.data_size)
        struct.pack_into('<Q', buf, 46, self.index_offset)
        struct.pack_into('<Q', buf, 54, self.index_size)
        struct.pack_into('<Q', buf, 62, self.wal_offset)
        struct.pack_into('<Q', buf, 70, self.wal_size)
        struct.pack_into('<Q', buf, 78, self.checkpoint_lsn)
        struct.pack_into('<I', buf, 86, self.flags)

        crc = zlib.crc32(buf[:90]) & 0xFFFFFFFF
        struct.pack_into('<I', buf, 90, crc)

        buf[94:110] = self.salt[:16].ljust(16, b'\x00')
        buf[110:114] = self.key_check[:4].ljust(4, b'\x00')

        return bytes(buf)

    @classmethod
    def unpack(cls: Type['HeaderV5'], data: bytes) -> 'HeaderV5':
        """从 128 字节反序列化"""
        if len(data) < cls.HEADER_SIZE:
            raise SerializationError(f"Header too short: {len(data)} bytes")

        header = cls()
        header.magic = data[0:4]
        header.version = struct.unpack('<H', data[4:6])[0]
        header.generation = struct.unpack('<Q', data[6:14])[0]
        header.schema_offset = struct.unpack('<Q', data[14:22])[0]
        header.schema_size = struct.unpack('<Q', data[22:30])[0]
        header.data_offset = struct.unpack('<Q', data[30:38])[0]
        header.data_size = struct.unpack('<Q', data[38:46])[0]
        header.index_offset = struct.unpack('<Q', data[46:54])[0]
        header.index_size = struct.unpack('<Q', data[54:62])[0]
        header.wal_offset = struct.unpack('<Q', data[62:70])[0]
        header.wal_size = struct.unpack('<Q', data[70:78])[0]
        header.checkpoint_lsn = struct.unpack('<Q', data[78:86])[0]
        header.flags = struct.unpack('<I', data[86:90])[0]
        header.crc32 = struct.unpack('<I', data[90:94])[0]
        header.salt = data[94:110]
        header.key_check = data[110:114]

        return header

    def verify_crc(self, data: bytes) -> bool:
        """验证 CRC32"""
        expected = zlib.crc32(data[:90]) & 0xFFFFFFFF
        return self.crc32 == expected

    def is_encrypted(self) -> bool:
        """检查是否启用加密"""
        return (self.flags & self.FLAG_ENCRYPTION_ENABLED) != 0

    def get_encryption_level(self) -> Optional[str]:
        """获取加密等级名称"""
        if not self.is_encrypted():
            return None
        level_code = (self.flags & self.FLAG_ENCRYPTION_LEVEL_MASK) >> self.FLAG_ENCRYPTION_LEVEL_SHIFT
        return get_encryption_level_name(level_code)

    def set_encryption(self, level: str, salt: bytes, key_check: bytes) -> None:
        """设置加密标志和元数据"""
        level_code = get_encryption_level_code(level)
        self.flags |= self.FLAG_ENCRYPTION_ENABLED
        self.flags = (self.flags & ~self.FLAG_ENCRYPTION_LEVEL_MASK) | (level_code << self.FLAG_ENCRYPTION_LEVEL_SHIFT)
        self.salt = salt
        self.key_check = key_check


@dataclass
class WALEntry:
    """WAL 日志条目"""
    lsn: int
    op_type: WALOpType
    table_name: str
    pk_bytes: bytes
    record_bytes: bytes = b''

    def pack(self) -> bytes:
        """序列化 WAL 条目"""
        buf = bytearray()

        # LSN (8B)
        buf += struct.pack('<Q', self.lsn)

        # Op type (1B)
        buf += struct.pack('B', self.op_type)

        # Table name (2B len + data)
        name_bytes = self.table_name.encode('utf-8')
        buf += struct.pack('<H', len(name_bytes))
        buf += name_bytes

        # PK bytes (2B len + data)
        buf += struct.pack('<H', len(self.pk_bytes))
        buf += self.pk_bytes

        # Record bytes (4B len + data)
        buf += struct.pack('<I', len(self.record_bytes))
        buf += self.record_bytes

        # CRC32 (4B)
        crc = zlib.crc32(buf) & 0xFFFFFFFF
        buf += struct.pack('<I', crc)

        # Entry length at beginning (4B)
        entry_data = struct.pack('<I', len(buf)) + buf

        return bytes(entry_data)

    @classmethod
    def unpack(cls, data: bytes) -> Tuple['WALEntry', int]:
        """
        从字节反序列化

        Returns:
            Tuple[WALEntry, bytes_consumed]
        """
        if len(data) < 4:
            raise SerializationError("WAL entry too short")

        entry_len = struct.unpack('<I', data[0:4])[0]
        if len(data) < 4 + entry_len:
            raise SerializationError("Incomplete WAL entry")

        entry_data = data[4:4 + entry_len]

        # 验证 CRC
        crc_stored = struct.unpack('<I', entry_data[-4:])[0]
        crc_calc = zlib.crc32(entry_data[:-4]) & 0xFFFFFFFF
        if crc_stored != crc_calc:
            raise SerializationError("WAL entry CRC mismatch")

        offset = 0

        # LSN
        lsn = struct.unpack('<Q', entry_data[offset:offset + 8])[0]
        offset += 8

        # Op type
        op_type = WALOpType(entry_data[offset])
        offset += 1

        # Table name
        name_len = struct.unpack('<H', entry_data[offset:offset + 2])[0]
        offset += 2
        table_name = entry_data[offset:offset + name_len].decode('utf-8')
        offset += name_len

        # PK bytes
        pk_len = struct.unpack('<H', entry_data[offset:offset + 2])[0]
        offset += 2
        pk_bytes = entry_data[offset:offset + pk_len]
        offset += pk_len

        # Record bytes
        rec_len = struct.unpack('<I', entry_data[offset:offset + 4])[0]
        offset += 4
        record_bytes = entry_data[offset:offset + rec_len]

        entry = cls(
            lsn=lsn,
            op_type=op_type,
            table_name=table_name,
            pk_bytes=pk_bytes,
            record_bytes=record_bytes
        )

        return entry, 4 + entry_len


class BinaryBackend(StorageBackend):
    """Pytuck format storage engine (default, no dependencies)"""

    ENGINE_NAME = 'pytuck'
    REQUIRED_DEPENDENCIES = []

    # 文件格式常量
    FORMAT_VERSION = get_format_version('pytuck')

    # PTK5 格式常量
    MAGIC_V5 = HeaderV5.MAGIC_V5
    HEADER_SIZE = HeaderV5.HEADER_SIZE
    DUAL_HEADER_SIZE = HeaderV5.HEADER_SIZE * 2
    WAL_SIDECAR_SUFFIX = '.wal'

    def __init__(self, file_path: Union[str, Path], options: BinaryBackendOptions):
        """
        初始化 Pytuck 后端

        Args:
            file_path: Pytuck 数据文件路径
            options: BinaryBackendOptions 配置选项
        """
        assert isinstance(options, BinaryBackendOptions), "options must be an instance of BinaryBackendOptions"
        super().__init__(file_path, options)
        # 类型安全：将 options 转为具体的 BinaryBackendOptions 类型
        self.options: BinaryBackendOptions = options

        # v5 运行时状态
        self._active_header: Optional[HeaderV5] = None
        self._active_slot: int = 0  # 0 = Header A, 1 = Header B
        self._current_lsn: int = 0
        self._file_handle: Optional[BinaryIO] = None

        # WAL 缓冲（减少 I/O 次数）
        self._wal_buffer: List[WALEntry] = []
        self._wal_buffer_size: int = 0  # 缓冲区字节大小
        self._wal_flush_threshold: int = 32 * 1024  # 32KB 阈值

        # 懒加载加密支持
        self._lazy_cipher: Optional[CipherType] = None
        self._lazy_data_offset: int = 0

    def _get_wal_sidecar_path(self) -> Path:
        """获取隐藏的 sidecar WAL 文件路径。"""
        return self.file_path.with_name('.' + self.file_path.name + self.WAL_SIDECAR_SUFFIX)

    def _has_sidecar_wal(self) -> bool:
        """检查是否存在非空 sidecar WAL 文件。"""
        wal_path = self._get_wal_sidecar_path()
        return wal_path.exists() and wal_path.stat().st_size > 0

    def _clear_sidecar_wal(self) -> None:
        """删除 sidecar WAL 文件（如果存在）。"""
        wal_path = self._get_wal_sidecar_path()
        try:
            wal_path.unlink()
        except FileNotFoundError:
            pass

    @staticmethod
    def _iter_packed_wal_entries(wal_data: bytes) -> Iterator[WALEntry]:
        """按顺序解析 WAL 条目，遇到损坏条目时停止。"""
        offset = 0
        while offset < len(wal_data):
            try:
                entry, consumed = WALEntry.unpack(wal_data[offset:])
            except SerializationError:
                break
            yield entry
            offset += consumed

    def _read_active_dual_header(self, f: BinaryIO) -> HeaderV5:
        """读取并选择当前生效的 v5 双 Header。"""
        f.seek(0)
        header_a_data = f.read(self.HEADER_SIZE)
        header_b_data = f.read(self.HEADER_SIZE)

        if len(header_a_data) < self.HEADER_SIZE or len(header_b_data) < self.HEADER_SIZE:
            raise SerializationError("Pytuck dual header is incomplete")

        header_a = HeaderV5.unpack(header_a_data)
        header_b = HeaderV5.unpack(header_b_data)

        valid_magics = {self.MAGIC_V5}
        header_a_valid = header_a.magic in valid_magics and header_a.verify_crc(header_a_data)
        header_b_valid = header_b.magic in valid_magics and header_b.verify_crc(header_b_data)

        if header_a_valid and header_b_valid:
            if header_a.generation >= header_b.generation:
                header = header_a
                self._active_slot = 0
            else:
                header = header_b
                self._active_slot = 1
        elif header_a_valid:
            header = header_a
            self._active_slot = 0
        elif header_b_valid:
            header = header_b
            self._active_slot = 1
        else:
            raise SerializationError("Both headers are corrupted")

        self._active_header = header
        return header

    def save(self, tables: Dict[str, 'Table'], *, changed_tables: Optional[Set[str]] = None) -> None:
        """保存所有表数据到二进制文件（默认写出 v5 双 Header 格式）"""
        # 清空 WAL 缓冲区（checkpoint 会包含所有数据）
        self._wal_buffer.clear()
        self._wal_buffer_size = 0

        # 对于新文件或全量保存，使用 checkpoint
        self._checkpoint_v5(tables)
        self._clear_sidecar_wal()

    def _checkpoint_v5(self, tables: Dict[str, 'Table']) -> None:
        """
        执行 v5 checkpoint（全量写入）

        v5 文件布局:
        - Header A (128B)
        - Header B (128B)
        - Schema Region（不加密）
        - Data Region（可加密，记录编码升级为紧凑格式）
        - Index Region（可加密）
        """
        encryption_level = self.options.encryption
        cipher: Optional[CipherType] = None
        salt = b'\x00' * 16
        key_check = b'\x00' * 4

        if encryption_level:
            if not self.options.password:
                raise EncryptionError("加密需要提供密码")
            if encryption_level not in ENCRYPTION_LEVELS:
                raise EncryptionError(f"无效的加密等级: {encryption_level}，必须是 {ENCRYPTION_LEVELS} 之一")

            salt = os.urandom(16)
            key = CryptoProvider.derive_key(self.options.password, salt, encryption_level)
            key_check = CryptoProvider.compute_key_check(key)
            cipher = get_cipher(encryption_level, key)

        fd, temp_path_str = tempfile.mkstemp(
            dir=str(self.file_path.parent),
            prefix=f'.{self.file_path.stem}.',
            suffix='.tmp'
        )
        temp_path = Path(temp_path_str)

        all_table_index_data: Dict[str, Dict[str, Any]] = {}

        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(b'\x00' * self.DUAL_HEADER_SIZE)

                schema_offset = f.tell()
                for table in tables.values():
                    self._write_table_schema(f, table)
                schema_size = f.tell() - schema_offset

                data_offset = f.tell()
                data_buffer = io.BytesIO()
                for table_name, table in tables.items():
                    pk_offsets = self._write_table_data_v5(data_buffer, table)
                    all_table_index_data[table_name] = {
                        'pk_offsets': pk_offsets,
                        'indexes': table.indexes
                    }
                data_bytes = data_buffer.getvalue()

                if cipher:
                    data_bytes = cipher.encrypt(data_bytes)

                f.write(data_bytes)
                data_size = len(data_bytes)

                index_offset = f.tell()
                compressed_index = self._write_index_region_compressed(all_table_index_data)

                if cipher:
                    compressed_index = cipher.encrypt(compressed_index)

                f.write(compressed_index)
                index_size = len(compressed_index)

                new_generation = 1
                if self._active_header:
                    new_generation = self._active_header.generation + 1

                flags = HeaderV5.FLAG_INDEX_COMPRESSED
                header = HeaderV5(
                    magic=self.MAGIC_V5,
                    version=5,
                    generation=new_generation,
                    schema_offset=schema_offset,
                    schema_size=schema_size,
                    data_offset=data_offset,
                    data_size=data_size,
                    index_offset=index_offset,
                    index_size=index_size,
                    wal_offset=0,
                    wal_size=0,
                    checkpoint_lsn=self._current_lsn,
                    flags=flags
                )

                if encryption_level:
                    header.set_encryption(encryption_level, salt, key_check)

                f.seek(0)
                f.write(header.pack())
                f.seek(self.HEADER_SIZE)
                f.write(header.pack())

            temp_path.replace(self.file_path)
            self._active_header = header
            self._active_slot = 0

        except Exception as e:
            try:
                temp_path.unlink()
            except (FileNotFoundError, OSError):
                pass
            raise SerializationError(f"Failed to save Pytuck file: {e}")

    def load(self) -> Dict[str, 'Table']:
        """从 Pytuck 文件加载所有表数据（仅支持 PTK5 与懒加载）"""
        if not self.exists():
            raise FileNotFoundError(f"Pytuck file not found: {self.file_path}")

        try:
            with open(self.file_path, 'rb') as f:
                magic = f.read(4)
                f.seek(0)

                if magic == self.MAGIC_V5:
                    return self._load_checkpoint_format(f)

                raise SerializationError(
                    f"不支持的文件格式: {magic!r}，当前仅支持 PTK5（.pytuck）格式"
                )

        except EncryptionError:
            raise
        except Exception as e:
            raise SerializationError(f"Failed to load Pytuck file: {e}")

    def _load_checkpoint_format(
        self,
        f: BinaryIO,
        *,
        force_lazy: Optional[bool] = None
    ) -> Dict[str, 'Table']:
        """加载 v5 双 Header checkpoint 格式文件。"""
        header = self._read_active_dual_header(f)
        self._current_lsn = header.checkpoint_lsn
        self._lazy_cipher = None
        self._lazy_data_offset = 0

        cipher: Optional[CipherType] = None
        if header.is_encrypted():
            if not self.options.password:
                raise EncryptionError("文件已加密，需要提供密码")

            encryption_level = header.get_encryption_level()
            if not encryption_level:
                raise EncryptionError("无法识别加密等级")

            key = CryptoProvider.derive_key(
                self.options.password, header.salt, encryption_level
            )
            if not CryptoProvider.verify_key(key, header.key_check):
                raise EncryptionError("密码错误")

            cipher = get_cipher(encryption_level, key)

        f.seek(header.schema_offset)
        tables_schema = []
        while f.tell() < header.data_offset:
            schema = self._read_table_schema(f)
            tables_schema.append(schema)

        index_data: Dict[str, Dict[str, Any]] = {}
        if header.index_offset > 0 and header.index_size > 0:
            f.seek(header.index_offset)
            is_compressed = (header.flags & HeaderV5.FLAG_INDEX_COMPRESSED) != 0

            if cipher:
                encrypted_index = f.read(header.index_size)
                decrypted_index = cipher.decrypt(encrypted_index)
                index_data = self._parse_index_region(decrypted_index, compressed=is_compressed)
            else:
                index_data = self._read_index_region(f, compressed=is_compressed)

        tables: Dict[str, 'Table'] = {}
        lazy_load_enabled = self.options.lazy_load if force_lazy is None else force_lazy

        if lazy_load_enabled and index_data:
            self._lazy_cipher = cipher
            self._lazy_data_offset = header.data_offset
            for schema in tables_schema:
                table = self._create_lazy_table(schema, index_data, header.data_offset)
                tables[table.name] = table
            return tables

        data_stream: BinaryIO
        if cipher:
            f.seek(header.data_offset)
            encrypted_data = f.read(header.data_size)
            decrypted_data = cipher.decrypt(encrypted_data)
            data_stream = io.BytesIO(decrypted_data)
            data_region_offset = 0
        else:
            f.seek(header.data_offset)
            data_stream = f
            data_region_offset = header.data_offset

        if header.version != 5:
            raise SerializationError(f"Unsupported Pytuck checkpoint version: {header.version}")

        for schema in tables_schema:
            table = self._read_table_data_v5(data_stream, schema, index_data, data_region_offset)
            tables[table.name] = table

        return tables

    def _create_lazy_table(
        self,
        schema: Dict[str, Any],
        index_data: Dict[str, Dict[str, Any]],
        data_offset: int
    ) -> 'Table':
        """
        创建懒加载表（只加载 schema 和索引，不加载数据）

        Args:
            schema: 表结构信息
            index_data: 从索引区读取的索引数据
            data_offset: 数据区在文件中的起始偏移量

        Returns:
            懒加载的 Table 对象
        """
        from ..core.storage import Table

        table_name = schema['table_name']

        # 创建 Table 对象（不加载数据）
        table = Table(
            table_name,
            schema['columns'],
            schema['primary_key'],
            comment=schema.get('table_comment')
        )
        table.next_id = schema['next_id']

        # 设置懒加载属性
        table._lazy_loaded = True
        table._data_file = self.file_path
        table._backend = self

        # 从索引区获取数据，并修正偏移量为绝对偏移
        table_idx_data = index_data.get(table_name, {})
        relative_pk_offsets = table_idx_data.get('pk_offsets', {})
        # 将相对偏移量转换为绝对偏移量
        table._pk_offsets = {
            pk: relative_offset + data_offset
            for pk, relative_offset in relative_pk_offsets.items()
        }

        # 恢复索引
        idx_maps = table_idx_data.get('indexes', {})
        for col_name, idx_map in idx_maps.items():
            if col_name in table.indexes:
                del table.indexes[col_name]

            column = table.columns[col_name]
            index_type = column.index
            if index_type is True:
                index_type = 'hash'

            index: BaseIndex
            if index_type == 'sorted':
                index = SortedIndex(col_name)
                cast(SortedIndex, index).value_to_pks = idx_map
                cast(SortedIndex, index).sorted_values = sorted(idx_map.keys())
            else:
                index = HashIndex(col_name)
                cast(HashIndex, index).map = idx_map
            table.indexes[col_name] = index

        return table

    def read_lazy_record(
        self,
        file_path: Path,
        offset: int,
        columns: Dict[str, 'Column'],
        pk: Any = None
    ) -> Dict[str, Any]:
        """
        读取单条懒加载记录，支持加密文件的按需解密。

        通过 cipher.decrypt_at() 实现随机位置解密，无需解密整个数据区。
        """
        with open(str(file_path), 'rb') as f:
            f.seek(offset)

            if self._lazy_cipher:
                relative_offset = offset - self._lazy_data_offset
                enc_len = f.read(4)
                dec_len = self._lazy_cipher.decrypt_at(relative_offset, enc_len)
                record_len = struct.unpack('<I', dec_len)[0]

                enc_data = f.read(record_len)
                dec_data = self._lazy_cipher.decrypt_at(relative_offset + 4, enc_data)
                stream = io.BytesIO(dec_len + dec_data)
                _, record = self._read_record_v5(stream, columns, pk)
            else:
                _, record = self._read_record_v5(f, columns, pk)

        return record

    def exists(self) -> bool:
        """检查文件是否存在"""
        return self.file_path.exists()

    def delete(self) -> None:
        """删除文件"""
        if self.file_path.exists():
            self.file_path.unlink()

    def supports_lazy_loading(self) -> bool:
        """
        检查是否启用了懒加载模式

        Returns:
            True 如果 options.lazy_load=True
        """
        return self.options.lazy_load

    def populate_tables_with_data(self, tables: Dict[str, 'Table']) -> None:
        """
        填充懒加载表的数据（用于迁移场景）

        在懒加载模式下，load() 只加载 schema 和索引，此方法用于
        在需要时（如迁移）填充实际数据。

        Args:
            tables: 需要填充数据的表字典
        """
        if not self.options.lazy_load:
            return  # 非懒加载模式，数据已在 load() 时加载

        for table in tables.values():
            if table.data:  # 已有数据，跳过
                continue

            if not getattr(table, '_lazy_loaded', False):
                continue

            pk_offsets = getattr(table, '_pk_offsets', None)
            if pk_offsets is None:
                continue

            # 通过 get() 逐条加载数据
            for pk in pk_offsets:
                record = table.get(pk)
                table.data[pk] = record

    def supports_server_side_pagination(self) -> bool:
        """Pytuck 后端支持基于索引和按需读取的分页查询。"""
        return True

    def query_with_pagination(
        self,
        table_name: str,
        conditions: List[Dict[str, Any]],
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> Dict[str, Any]:
        """使用索引和懒加载能力实现按需分页查询。"""
        if not self.exists():
            return {'records': [], 'total_count': 0, 'has_more': False}

        from ..query import Condition

        has_pending_wal = self.has_pending_wal()
        query_options = replace(self.options, lazy_load=not has_pending_wal)
        query_backend = BinaryBackend(str(self.file_path), query_options)
        if has_pending_wal and self._wal_buffer:
            query_backend._wal_buffer = list(self._wal_buffer)
            query_backend._wal_buffer_size = self._wal_buffer_size
        tables = query_backend.load()

        if has_pending_wal:
            query_backend.replay_wal(tables)

        table = tables.get(table_name)
        if table is None:
            return {'records': [], 'total_count': 0, 'has_more': False}

        parsed_conditions = [
            Condition(cond['field'], cond.get('operator', '='), cond['value'])
            for cond in conditions
        ]

        effective_limit = limit if limit is not None and limit > 0 else None
        all_pks = list(table._pk_offsets.keys()) if table._pk_offsets is not None else list(table.data.keys())
        candidate_pks: Optional[Set[Any]] = None
        remaining_conditions: List[Condition] = []

        def _intersect(pks: Set[Any]) -> None:
            nonlocal candidate_pks
            if candidate_pks is None:
                candidate_pks = set(pks)
            else:
                candidate_pks = candidate_pks.intersection(pks)

        for condition in parsed_conditions:
            if table.primary_key and condition.field == table.primary_key and condition.operator == '=':
                normalized_pk = table._normalize_pk(condition.value)
                exists_in_file = table._pk_offsets is not None and normalized_pk in table._pk_offsets
                exists_in_memory = normalized_pk in table.data
                _intersect({normalized_pk} if (exists_in_file or exists_in_memory) else set())
            elif (
                table.primary_key
                and condition.field == table.primary_key
                and condition.operator == 'IN'
                and isinstance(condition.value, (list, tuple, set))
            ):
                normalized_pks = {table._normalize_pk(value) for value in condition.value}
                existing_pks = {
                    pk for pk in normalized_pks
                    if (table._pk_offsets is not None and pk in table._pk_offsets) or pk in table.data
                }
                _intersect(existing_pks)
            elif condition.operator == '=' and condition.field in table.indexes:
                _intersect(table.indexes[condition.field].lookup(condition.value))
            elif (
                condition.operator == 'IN'
                and condition.field in table.indexes
                and isinstance(condition.value, (list, tuple, set))
            ):
                matched_pks: Set[Any] = set()
                for value in condition.value:
                    matched_pks.update(table.indexes[condition.field].lookup(value))
                _intersect(matched_pks)
            else:
                remaining_conditions.append(condition)

        if candidate_pks is None:
            ordered_candidate_pks = all_pks
        else:
            ordered_candidate_pks = [pk for pk in all_pks if pk in candidate_pks]
            for pk in table.data.keys():
                if pk in candidate_pks and pk not in ordered_candidate_pks:
                    ordered_candidate_pks.append(pk)

        def _get_record(pk: Any) -> Dict[str, Any]:
            if pk in table.data:
                return table.data[pk]
            return table.get(pk)

        def _build_result_record(pk: Any, record: Dict[str, Any]) -> Dict[str, Any]:
            result = record.copy()
            if not table.primary_key:
                result[PSEUDO_PK_NAME] = pk
            return result

        def _matches(record: Dict[str, Any]) -> bool:
            return all(condition.evaluate(record) for condition in remaining_conditions)

        def _sort_records(records: List[Dict[str, Any]]) -> None:
            if not order_by or order_by not in table.columns:
                return

            def sort_key(_record: Dict[str, Any]) -> tuple:
                value = _record.get(order_by)
                if value is None:
                    return (1, 0) if not order_desc else (0, 0)
                return (0, value) if not order_desc else (1, value)

            try:
                records.sort(key=sort_key, reverse=order_desc)
            except TypeError:
                records.sort(key=lambda r: str(r.get(order_by, '')), reverse=order_desc)

        if order_by and order_by in table.columns:
            matched_records: List[Dict[str, Any]] = []
            for pk in ordered_candidate_pks:
                record = _get_record(pk)
                if _matches(record):
                    matched_records.append(_build_result_record(pk, record))

            total_count = len(matched_records)
            _sort_records(matched_records)
            if offset > 0:
                matched_records = matched_records[offset:]
            if effective_limit is not None:
                matched_records = matched_records[:effective_limit]

            return {
                'records': matched_records,
                'total_count': total_count,
                'has_more': (offset + len(matched_records)) < total_count if effective_limit is not None else False
            }

        total_count = 0
        records: List[Dict[str, Any]] = []
        skipped = 0

        for pk in ordered_candidate_pks:
            record = _get_record(pk)
            if not _matches(record):
                continue

            total_count += 1
            if skipped < offset:
                skipped += 1
                continue

            if effective_limit is None or len(records) < effective_limit:
                records.append(_build_result_record(pk, record))

        return {
            'records': records,
            'total_count': total_count,
            'has_more': (offset + len(records)) < total_count if effective_limit is not None else False
        }

    # ============== WAL 操作方法 ==============

    def append_wal_entry(
        self,
        op_type: WALOpType,
        table_name: str,
        pk: Any,
        record: Optional[Dict[str, Any]] = None,
        columns: Optional[Dict[str, 'Column']] = None
    ) -> int:
        """
        追加 WAL 条目到缓冲区

        Args:
            op_type: 操作类型 (INSERT/UPDATE/DELETE)
            table_name: 表名
            pk: 主键值
            record: 记录数据（INSERT/UPDATE 时需要）
            columns: 列定义（用于序列化）

        Returns:
            新的 LSN
        """
        # 序列化 PK
        pk_bytes = self._serialize_index_value(pk)

        # 序列化记录（如果有）
        record_bytes = b''
        if record is not None and columns is not None:
            record_bytes = self._serialize_record_bytes(pk, record, columns)

        # 创建 WAL 条目
        self._current_lsn += 1
        entry = WALEntry(
            lsn=self._current_lsn,
            op_type=op_type,
            table_name=table_name,
            pk_bytes=pk_bytes,
            record_bytes=record_bytes
        )

        # 添加到缓冲区
        entry_bytes = entry.pack()
        self._wal_buffer.append(entry)
        self._wal_buffer_size += len(entry_bytes)

        # 如果缓冲区达到阈值，刷新到磁盘
        if self._wal_buffer_size >= self._wal_flush_threshold:
            self.flush_wal_buffer()

        return self._current_lsn

    def flush_wal_buffer(self) -> None:
        """将 WAL 缓冲区刷新到磁盘"""
        if not self._wal_buffer:
            return

        # 序列化所有条目
        all_bytes = bytearray()
        for entry in self._wal_buffer:
            all_bytes.extend(entry.pack())

        if self.options.sidecar_wal:
            wal_path = self._get_wal_sidecar_path()
            with open(wal_path, 'ab') as f:
                f.write(all_bytes)
                f.flush()
        else:
            with open(self.file_path, 'r+b') as f:
                # 读取当前活跃 header
                active_header = self._active_header
                if active_header is None:
                    active_header = self._read_active_dual_header(f)

                # 计算 WAL 写入位置
                if active_header.wal_offset == 0:
                    # 首次写 WAL，在文件末尾
                    f.seek(0, 2)  # 移到文件末尾
                    wal_offset = f.tell()
                else:
                    # 追加到现有 WAL
                    wal_offset = active_header.wal_offset
                    f.seek(wal_offset + active_header.wal_size)

                # 写入所有 WAL 条目
                f.write(all_bytes)
                f.flush()

                # 更新 header 中的 WAL 信息
                new_wal_size = active_header.wal_size + len(all_bytes)
                if active_header.wal_offset == 0:
                    active_header.wal_offset = wal_offset

                active_header.wal_size = new_wal_size
                active_header.checkpoint_lsn = self._current_lsn
                self._active_header = active_header

                # 更新 header（写入当前活跃槽）
                header_bytes = active_header.pack()
                f.seek(self._active_slot * self.HEADER_SIZE)
                f.write(header_bytes)
                f.flush()

        # 清空缓冲区
        self._wal_buffer.clear()
        self._wal_buffer_size = 0

    @staticmethod
    def _serialize_record_bytes(
        pk: Any,
        record: Dict[str, Any],
        columns: Dict[str, 'Column']
    ) -> bytes:
        """
        序列化记录为字节

        Args:
            pk: 主键值
            record: 记录数据
            columns: 列定义

        Returns:
            序列化后的字节
        """
        buf = bytearray()

        # 预构建列索引映射
        col_idx_map = {col.name: idx for idx, col in enumerate(columns.values())}

        # Primary Key
        pk_col = None
        for col in columns.values():
            if col.primary_key:
                pk_col = col
                break

        if pk_col:
            type_code, codec = TypeRegistry.get_codec(pk_col.col_type)
            pk_bytes = codec.encode(pk)
            buf += struct.pack('<H', len(pk_bytes))
            buf += pk_bytes

        # Field Count
        field_count = len(record)
        buf += struct.pack('<H', field_count)

        # Fields
        for col_name, value in record.items():
            if col_name not in columns:
                continue
            column = columns[col_name]

            # Column Index
            col_idx = col_idx_map.get(col_name, 0)
            buf += struct.pack('<H', col_idx)

            # Type Code
            type_code, codec = TypeRegistry.get_codec(column.col_type)
            buf += struct.pack('B', type_code)

            # Value
            if value is None:
                buf += struct.pack('<I', 0)
            else:
                value_bytes = codec.encode(value)
                buf += struct.pack('<I', len(value_bytes))
                buf += value_bytes

        return bytes(buf)

    def read_wal_entries(self) -> Iterator[WALEntry]:
        """
        读取所有 WAL 条目（包括磁盘和缓冲区）

        Yields:
            WALEntry 对象
        """
        # 优先读取 sidecar WAL；若不存在，再兼容读取主文件内嵌 WAL
        if self._has_sidecar_wal():
            wal_path = self._get_wal_sidecar_path()
            with open(wal_path, 'rb') as f:
                wal_data = f.read()
            for entry in self._iter_packed_wal_entries(wal_data):
                yield entry
        elif self.exists():
            with open(self.file_path, 'rb') as f:
                try:
                    header = self._read_active_dual_header(f)
                except SerializationError:
                    header = None

                if header and header.wal_offset > 0 and header.wal_size > 0:
                    f.seek(header.wal_offset)
                    wal_data = f.read(header.wal_size)

                    for entry in self._iter_packed_wal_entries(wal_data):
                        yield entry

        # 然后返回缓冲区中的条目
        for entry in self._wal_buffer:
            yield entry

    def replay_wal(self, tables: Dict[str, 'Table']) -> int:
        """
        回放 WAL 到内存中的表

        Args:
            tables: 表字典

        Returns:
            回放的条目数量
        """
        count = 0

        for entry in self.read_wal_entries():
            table = tables.get(entry.table_name)
            if table is None:
                continue

            # 反序列化 PK
            pk = self._deserialize_index_value(entry.pk_bytes)

            if entry.op_type == WALOpType.DELETE:
                # 删除操作
                if pk in table.data:
                    old_record = table.data[pk]
                    del table.data[pk]
                    # 更新索引
                    for col_name, idx in table.indexes.items():
                        if col_name in old_record:
                            idx.remove(old_record[col_name], pk)

            elif entry.op_type in (WALOpType.INSERT, WALOpType.UPDATE):
                # 插入或更新操作
                if entry.record_bytes:
                    record = self._deserialize_record_bytes(
                        entry.record_bytes,
                        table.columns
                    )
                    existing_record: Optional[Dict[str, Any]] = table.data.get(pk)
                    table.data[pk] = record

                    # 更新索引
                    for col_name, idx in table.indexes.items():
                        old_value = existing_record.get(col_name) if existing_record is not None else None
                        new_value = record.get(col_name)
                        if old_value != new_value:
                            if old_value is not None:
                                idx.remove(old_value, pk)
                            if new_value is not None:
                                idx.insert(new_value, pk)

            count += 1
            self._current_lsn = max(self._current_lsn, entry.lsn)

        return count

    @staticmethod
    def _deserialize_record_bytes(data: bytes, columns: Dict[str, 'Column']) -> Dict[str, Any]:
        """
        反序列化记录字节

        Args:
            data: 序列化的字节
            columns: 列定义

        Returns:
            记录字典
        """
        record = {}
        offset = 0

        # 列列表（按顺序）
        col_list = list(columns.values())

        # Primary Key Length + Data
        pk_len = struct.unpack('<H', data[offset:offset + 2])[0]
        offset += 2
        # pk_bytes = data[offset:offset + pk_len]  # PK 在记录中不存储
        offset += pk_len

        # Field Count
        field_count = struct.unpack('<H', data[offset:offset + 2])[0]
        offset += 2

        # Fields
        for _ in range(field_count):
            # Column Index
            col_idx = struct.unpack('<H', data[offset:offset + 2])[0]
            offset += 2

            # Type Code
            type_code = TypeCode(data[offset])
            offset += 1

            # Value Length
            value_len = struct.unpack('<I', data[offset:offset + 4])[0]
            offset += 4

            if value_len == 0:
                value = None
            else:
                value_bytes = data[offset:offset + value_len]
                offset += value_len
                _, codec = TypeRegistry.get_codec_by_code(type_code)
                value, _ = codec.decode(value_bytes)

            # 获取列名
            if col_idx < len(col_list):
                col_name = col_list[col_idx].name
                record[col_name] = value

        return record

    def has_pending_wal(self) -> bool:
        """检查是否有未 checkpoint 的 WAL（包括缓冲区）"""
        # 先检查内存缓冲区
        if self._wal_buffer:
            return True

        if self._has_sidecar_wal():
            return True

        if not self.exists():
            return False

        with open(self.file_path, 'rb') as f:
            try:
                header = self._read_active_dual_header(f)
            except SerializationError:
                return False
            return header.wal_size > 0

    def _write_table_schema(self, f: BinaryIO, table: 'Table') -> None:
        """
        写入单个表的 Schema（元数据）

        格式：
        - Table Name Length (2 bytes)
        - Table Name (UTF-8)
        - Primary Key Length (2 bytes)
        - Primary Key (UTF-8) - 空字符串表示无主键
        - Table Comment Length (2 bytes)
        - Table Comment (UTF-8)
        - Column Count (2 bytes)
        - Next ID (8 bytes)
        - Columns Data
        """
        # Table Name
        table_name_bytes = table.name.encode('utf-8')
        f.write(struct.pack('<H', len(table_name_bytes)))
        f.write(table_name_bytes)

        # Primary Key（None 用空字符串表示）
        pk_str = table.primary_key if table.primary_key else ''
        pk_bytes = pk_str.encode('utf-8')
        f.write(struct.pack('<H', len(pk_bytes)))
        if pk_bytes:
            f.write(pk_bytes)

        # Table Comment
        comment_bytes = (table.comment or '').encode('utf-8')
        f.write(struct.pack('<H', len(comment_bytes)))
        if comment_bytes:
            f.write(comment_bytes)

        # Column Count
        f.write(struct.pack('<H', len(table.columns)))

        # Next ID
        f.write(struct.pack('<Q', table.next_id))

        # Columns
        for col_name, column in table.columns.items():
            self._write_column(f, column)

    def _read_table_schema(self, f: BinaryIO) -> Dict[str, Any]:
        """读取单个表的 Schema，返回 schema 字典"""
        # Table Name
        name_len = struct.unpack('<H', f.read(2))[0]
        table_name = f.read(name_len).decode('utf-8')

        # Primary Key（空字符串表示无主键）
        pk_len = struct.unpack('<H', f.read(2))[0]
        primary_key: Optional[str] = f.read(pk_len).decode('utf-8') if pk_len > 0 else None
        if primary_key == '':
            primary_key = None

        # Table Comment
        comment_len = struct.unpack('<H', f.read(2))[0]
        table_comment = f.read(comment_len).decode('utf-8') if comment_len > 0 else None

        # Column Count
        col_count = struct.unpack('<H', f.read(2))[0]

        # Next ID
        next_id = struct.unpack('<Q', f.read(8))[0]

        # Columns
        columns = []
        for _ in range(col_count):
            column = self._read_column(f)
            columns.append(column)

        return {
            'table_name': table_name,
            'primary_key': primary_key,
            'table_comment': table_comment,
            'next_id': next_id,
            'columns': columns
        }

    @staticmethod
    def _write_table_data_v5(f: BinaryIO, table: 'Table') -> Dict[Any, int]:
        """
        写入单个表的数据（v5 紧凑记录格式）

        格式：
        - Record Count (4 bytes)
        - Records Data
          - Record Length (4 bytes)
          - Null Bitmap (按列顺序，主键列不重复存储)
          - Value Payloads（按 schema 顺序拼接，NULL 列跳过）

        Returns:
            pk_offsets: 主键到数据区相对偏移的映射
        """
        pk_offsets: Dict[Any, int] = {}
        stored_columns = [col for col in table.columns.values() if not col.primary_key]
        bitmap_size = (len(stored_columns) + 7) // 8
        codec_cache = []
        for col in stored_columns:
            assert col.name is not None, "Column name must be set"
            _, codec = TypeRegistry.get_codec(col.col_type)
            codec_cache.append((col.name, codec))

        f.write(struct.pack('<I', len(table.data)))

        buf = bytearray()
        base_offset = f.tell()
        buf_offset = 0

        for pk, record in table.data.items():
            pk_offsets[pk] = base_offset + buf_offset

            null_bitmap = bytearray(bitmap_size)
            payload = bytearray()
            for idx, (col_name, codec) in enumerate(codec_cache):
                value = record.get(col_name)
                if value is None:
                    null_bitmap[idx // 8] |= 1 << (idx % 8)
                    continue
                payload.extend(codec.encode(value))

            record_data = bytes(null_bitmap) + bytes(payload)
            record_len = len(record_data)
            buf.extend(struct.pack('<I', record_len))
            buf.extend(record_data)
            buf_offset += 4 + record_len

            if len(buf) > 1024 * 1024:
                f.write(buf)
                base_offset = f.tell()
                buf_offset = 0
                buf.clear()

        if buf:
            f.write(buf)

        return pk_offsets

    @staticmethod
    def _read_table_data_v5(
        f: BinaryIO,
        schema: Dict[str, Any],
        index_data: Dict[str, Dict[str, Any]],
        data_region_offset: int = 0
    ) -> 'Table':
        """根据 schema 和索引区读取 v5 紧凑记录格式表数据。"""
        from ..core.storage import Table

        table_name = schema['table_name']
        table = Table(
            table_name,
            schema['columns'],
            schema['primary_key'],
            comment=schema.get('table_comment')
        )
        table.next_id = schema['next_id']

        columns_dict = {col.name: col for col in schema['columns']}
        table_idx_data = index_data.get(table_name, {})
        relative_pk_offsets = table_idx_data.get('pk_offsets', {})

        record_count_bytes = f.read(4)
        if len(record_count_bytes) < 4:
            raise SerializationError(f"读取表 {table_name} 的记录数失败：文件意外结束")
        record_count = struct.unpack('<I', record_count_bytes)[0]

        if record_count > 0 and not relative_pk_offsets:
            raise SerializationError(f"读取表 {table_name} 的 v5 数据失败：缺少 pk_offsets")
        offset_to_pk = {relative_offset: pk for pk, relative_offset in relative_pk_offsets.items()}

        for _ in range(record_count):
            record_offset = f.tell() - data_region_offset
            pk = offset_to_pk.get(record_offset)
            if pk is None:
                raise SerializationError(
                    f"读取表 {table_name} 的 v5 数据失败：找不到偏移 {record_offset} 对应的主键"
                )

            _, record = BinaryBackend._read_record_v5(f, columns_dict, pk)
            table.data[pk] = record

        idx_maps = table_idx_data.get('indexes', {})
        if idx_maps:
            for col_name, idx_map in idx_maps.items():
                if col_name in table.indexes:
                    del table.indexes[col_name]
                index = HashIndex(col_name)
                index.map = idx_map
                table.indexes[col_name] = index
        else:
            for col_name, column in table.columns.items():
                if column.index:
                    if col_name in table.indexes:
                        del table.indexes[col_name]
                    table.build_index(col_name)

        return table

    @staticmethod
    def _write_column(f: BinaryIO, column: 'Column') -> None:
        """
        写入列定义

        格式：
        - Column Name Length (2 bytes)
        - Column Name (UTF-8)
        - Type Code (1 byte)
        - Flags (1 byte): nullable, primary_key, index
        - Column Comment Length (2 bytes)
        - Column Comment (UTF-8)
        """
        # Column Name
        assert column.name is not None, "Column name must be set"
        col_name_bytes = column.name.encode('utf-8')
        f.write(struct.pack('<H', len(col_name_bytes)))
        f.write(col_name_bytes)

        # Type Code
        type_code, _ = TypeRegistry.get_codec(column.col_type)
        f.write(struct.pack('B', type_code))

        # Flags (bit field)
        flags = 0
        if column.nullable:
            flags |= 0x01
        if column.primary_key:
            flags |= 0x02
        if column.index:
            flags |= 0x04
        if column.index == 'sorted':
            flags |= 0x08
        f.write(struct.pack('B', flags))

        # Column Comment
        comment_bytes = (column.comment or '').encode('utf-8')
        f.write(struct.pack('<H', len(comment_bytes)))
        if comment_bytes:
            f.write(comment_bytes)

    @staticmethod
    def _read_column(f: BinaryIO) -> Column:
        """读取列定义"""
        from ..core.orm import Column

        # Column Name
        name_len = struct.unpack('<H', f.read(2))[0]
        col_name = f.read(name_len).decode('utf-8')

        # Type Code
        type_code = TypeCode(struct.unpack('B', f.read(1))[0])
        col_type = TypeRegistry.get_type_from_code(type_code)

        # Flags
        flags = struct.unpack('B', f.read(1))[0]
        nullable = bool(flags & 0x01)
        primary_key = bool(flags & 0x02)
        index: Union[bool, str]
        if flags & 0x08:
            index = 'sorted'
        else:
            index = bool(flags & 0x04)

        # Column Comment
        comment_len = struct.unpack('<H', f.read(2))[0]
        comment = f.read(comment_len).decode('utf-8') if comment_len > 0 else None

        return Column(
            col_type,
            name=col_name,
            nullable=nullable,
            primary_key=primary_key,
            index=index,
            comment=comment
        )

    @staticmethod
    def _read_record_v5(f: BinaryIO, columns: Dict[str, Column], pk: Any = None) -> tuple:
        """读取单条 v5 紧凑记录，返回 (pk, record_dict)。"""
        record_len_bytes = f.read(4)
        if len(record_len_bytes) < 4:
            raise SerializationError("读取 v5 记录长度失败：文件意外结束")
        record_len = struct.unpack('<I', record_len_bytes)[0]
        record_data = f.read(record_len)
        if len(record_data) < record_len:
            raise SerializationError("读取 v5 记录数据失败：文件意外结束")

        pk_col_name: Optional[str] = None
        stored_columns: List[Column] = []
        for column in columns.values():
            assert column.name is not None, "Column name must be set"
            if column.primary_key:
                pk_col_name = column.name
                continue
            stored_columns.append(column)

        bitmap_size = (len(stored_columns) + 7) // 8
        if len(record_data) < bitmap_size:
            raise SerializationError("读取 v5 记录失败：null bitmap 长度不足")

        null_bitmap = record_data[:bitmap_size]
        payload = record_data[bitmap_size:]
        payload_offset = 0
        record: Dict[str, Any] = {}

        for idx, column in enumerate(stored_columns):
            col_name = column.name
            assert col_name is not None, "Column name must be set"
            if null_bitmap[idx // 8] & (1 << (idx % 8)):
                record[col_name] = None
                continue

            _, codec = TypeRegistry.get_codec(column.col_type)
            value, consumed = codec.decode(payload[payload_offset:])
            payload_offset += consumed
            record[col_name] = value

        if payload_offset != len(payload):
            raise SerializationError("读取 v5 记录失败：payload 长度不匹配")

        if pk_col_name is not None:
            record[pk_col_name] = pk

        return pk, record

    def get_metadata(self) -> Dict[str, Any]:
        """获取元数据"""
        if not self.exists():
            return {}

        file_stat = self.file_path.stat()
        file_size = file_stat.st_size
        modified_time = file_stat.st_mtime

        return {
            'engine': 'pytuck',
            'file_size': file_size,
            'modified': modified_time,
        }

    @classmethod
    def probe(cls, file_path: Union[str, Path]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        轻量探测文件是否为 Pytuck 引擎格式

        通过检查文件头的魔数来识别 PTK5 / .pytuck 格式文件。

        Returns:
            Tuple[bool, Optional[Dict]]: (是否匹配, 元数据信息或None)
        """
        try:
            file_path = Path(file_path).expanduser()
            if not file_path.exists():
                return False, {'error': 'file_not_found'}

            # 检查文件大小是否足够包含魔数
            file_stat = file_path.stat()
            file_size = file_stat.st_size
            if file_size < 4:
                return False, {'error': 'file_too_small'}

            # 读取并检查文件头
            with open(file_path, 'rb') as f:
                magic = f.read(4)

                if magic == cls.MAGIC_V5:
                    if file_size < cls.DUAL_HEADER_SIZE:
                        return False, {'error': 'file_too_small_for_checkpoint'}

                    return True, {
                        'engine': 'pytuck',
                        'format_version': 5,
                        'file_size': file_size,
                        'modified': file_stat.st_mtime,
                        'confidence': 'high'
                    }

                return False, None  # 不是错误，只是不匹配

        except Exception as e:
            return False, {'error': f'probe_exception: {str(e)}'}

    # ========== 索引区读写方法 ==========

    def _write_index_region_compressed(
        self,
        all_table_data: Dict[str, Dict[str, Any]]
    ) -> bytes:
        """
        构建压缩的索引区数据

        Returns:
            压缩后的索引区字节数据
        """
        buf = bytearray()

        # Index Format Version (固定 2 字节)
        buf += struct.pack('<H', 1)

        # Table Count (4 bytes)
        buf += struct.pack('<I', len(all_table_data))

        for table_name, table_data in all_table_data.items():
            # Table Name
            name_bytes = table_name.encode('utf-8')
            buf += struct.pack('<H', len(name_bytes))
            buf += name_bytes

            # PK Offsets
            pk_offsets = table_data.get('pk_offsets', {})
            buf += struct.pack('<I', len(pk_offsets))
            for pk, offset in pk_offsets.items():
                pk_bytes = self._serialize_index_value(pk)
                buf += struct.pack('<H', len(pk_bytes))
                buf += pk_bytes
                buf += struct.pack('<Q', offset)

            # Indexes
            indexes = table_data.get('indexes', {})
            buf += struct.pack('<H', len(indexes))

            for col_name, index in indexes.items():
                # Column Name
                col_bytes = col_name.encode('utf-8')
                buf += struct.pack('<H', len(col_bytes))
                buf += col_bytes

                # 获取索引映射
                if isinstance(index, SortedIndex):
                    idx_map = index.value_to_pks
                elif hasattr(index, 'map'):
                    idx_map = index.map
                else:
                    idx_map = {}

                # Entry Count
                buf += struct.pack('<I', len(idx_map))

                for value, pk_set in idx_map.items():
                    # Value
                    value_bytes = self._serialize_index_value(value)
                    buf += struct.pack('<H', len(value_bytes))
                    buf += value_bytes

                    # PK List
                    pk_list = list(pk_set)
                    buf += struct.pack('<I', len(pk_list))
                    for pk in pk_list:
                        pk_bytes = self._serialize_index_value(pk)
                        buf += struct.pack('<H', len(pk_bytes))
                        buf += pk_bytes

        # 使用 zlib 压缩
        compressed = zlib.compress(bytes(buf), level=6)
        return compressed

    def _write_index_region(
        self,
        f: BinaryIO,
        all_table_data: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        写入索引区（批量写入优化，固定宽度整数）

        格式（v1，使用固定宽度整数）：
        - Index Format Version (2 bytes): 值为 1
        - Table Count (4 bytes)
        - For each table:
            - Table Name Length (2 bytes) + Name
            - PK Offsets Count (4 bytes)
            - PK Offsets: [(pk_bytes_len 2 bytes, pk_bytes, offset 8 bytes), ...]
            - Index Count (2 bytes)
            - For each index:
                - Column Name Length (2 bytes) + Name
                - Entry Count (4 bytes)
                - Entries: [(value_bytes_len 2 bytes, value_bytes, pk_count 4 bytes, [pk_bytes...]), ...]
        """
        buf = bytearray()

        # Index Format Version (固定 2 字节)
        buf += struct.pack('<H', 1)

        # Table Count (4 bytes)
        buf += struct.pack('<I', len(all_table_data))

        for table_name, table_data in all_table_data.items():
            # Table Name
            name_bytes = table_name.encode('utf-8')
            buf += struct.pack('<H', len(name_bytes))
            buf += name_bytes

            # PK Offsets
            pk_offsets = table_data.get('pk_offsets', {})
            buf += struct.pack('<I', len(pk_offsets))
            for pk, offset in pk_offsets.items():
                pk_bytes = self._serialize_index_value(pk)
                buf += struct.pack('<H', len(pk_bytes))
                buf += pk_bytes
                buf += struct.pack('<Q', offset)

            # Indexes
            indexes = table_data.get('indexes', {})
            buf += struct.pack('<H', len(indexes))

            for col_name, index in indexes.items():
                # Column Name
                col_bytes = col_name.encode('utf-8')
                buf += struct.pack('<H', len(col_bytes))
                buf += col_bytes

                # 获取索引映射
                if isinstance(index, SortedIndex):
                    idx_map = index.value_to_pks
                elif hasattr(index, 'map'):
                    idx_map = index.map
                else:
                    idx_map = {}

                # Entry Count
                buf += struct.pack('<I', len(idx_map))

                for value, pk_set in idx_map.items():
                    # Value
                    value_bytes = self._serialize_index_value(value)
                    buf += struct.pack('<H', len(value_bytes))
                    buf += value_bytes

                    # PK Set
                    pk_list = list(pk_set)
                    buf += struct.pack('<I', len(pk_list))
                    for pk in pk_list:
                        pk_bytes = self._serialize_index_value(pk)
                        buf += struct.pack('<H', len(pk_bytes))
                        buf += pk_bytes

        # 一次性写入
        f.write(buf)

    def _read_index_region(
        self,
        f: BinaryIO,
        compressed: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        读取索引区（批量读取 + 固定宽度整数）

        Args:
            f: 文件句柄
            compressed: 索引区是否已压缩

        Returns:
            {table_name: {'pk_offsets': {...}, 'indexes': {...}}}
        """
        # 一次性读取整个索引区数据
        raw_data = f.read()
        if not raw_data or len(raw_data) < 2:
            return {}

        # 如果是压缩数据，先解压
        if compressed:
            try:
                data = zlib.decompress(raw_data)
            except zlib.error:
                # 解压失败，尝试作为未压缩数据处理
                data = raw_data
        else:
            data = raw_data

        result: Dict[str, Dict[str, Any]] = {}

        # Index Format Version (固定 2 字节)
        idx_version = struct.unpack('<H', data[0:2])[0]
        if idx_version != 1:
            # 只支持 v1 格式
            return {}

        offset = 2

        # Table Count (4 bytes)
        table_count = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4

        for _ in range(table_count):
            # Table Name
            name_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            table_name = data[offset:offset+name_len].decode('utf-8')
            offset += name_len

            # PK Offsets
            pk_count = struct.unpack('<I', data[offset:offset+4])[0]
            offset += 4
            pk_offsets: Dict[Any, int] = {}
            for _ in range(pk_count):
                pk_len = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                pk = self._deserialize_index_value(data[offset:offset+pk_len])
                offset += pk_len
                file_offset = struct.unpack('<Q', data[offset:offset+8])[0]
                offset += 8
                pk_offsets[pk] = file_offset

            # Indexes
            idx_count = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            indexes: Dict[str, Dict[Any, Set[Any]]] = {}

            for _ in range(idx_count):
                # Column Name
                col_len = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                col_name = data[offset:offset+col_len].decode('utf-8')
                offset += col_len

                # Entry Count
                entry_count = struct.unpack('<I', data[offset:offset+4])[0]
                offset += 4
                idx_map: Dict[Any, Set[Any]] = {}

                for _ in range(entry_count):
                    # Value
                    val_len = struct.unpack('<H', data[offset:offset+2])[0]
                    offset += 2
                    value = self._deserialize_index_value(data[offset:offset+val_len])
                    offset += val_len

                    # PK Set
                    pk_list_len = struct.unpack('<I', data[offset:offset+4])[0]
                    offset += 4
                    pk_set: Set[Any] = set()
                    for _ in range(pk_list_len):
                        pk_len = struct.unpack('<H', data[offset:offset+2])[0]
                        offset += 2
                        pk = self._deserialize_index_value(data[offset:offset+pk_len])
                        offset += pk_len
                        pk_set.add(pk)

                    idx_map[value] = pk_set

                indexes[col_name] = idx_map

            result[table_name] = {
                'pk_offsets': pk_offsets,
                'indexes': indexes
            }

        return result

    def _parse_index_region(
        self,
        raw_data: bytes,
        compressed: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        解析索引区数据（从 bytes 解析，用于解密后的数据）

        Args:
            raw_data: 索引区原始数据（可能是压缩或加密后解密的）
            compressed: 数据是否已压缩

        Returns:
            {table_name: {'pk_offsets': {...}, 'indexes': {...}}}
        """
        if not raw_data or len(raw_data) < 2:
            return {}

        # 如果是压缩数据，先解压
        if compressed:
            try:
                data = zlib.decompress(raw_data)
            except zlib.error:
                # 解压失败，尝试作为未压缩数据处理
                data = raw_data
        else:
            data = raw_data

        result: Dict[str, Dict[str, Any]] = {}

        # Index Format Version (固定 2 字节)
        idx_version = struct.unpack('<H', data[0:2])[0]
        if idx_version != 1:
            # 只支持 v1 格式
            return {}

        offset = 2

        # Table Count (4 bytes)
        table_count = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4

        for _ in range(table_count):
            # Table Name
            name_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            table_name = data[offset:offset+name_len].decode('utf-8')
            offset += name_len

            # PK Offsets
            pk_count = struct.unpack('<I', data[offset:offset+4])[0]
            offset += 4
            pk_offsets: Dict[Any, int] = {}
            for _ in range(pk_count):
                pk_len = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                pk = self._deserialize_index_value(data[offset:offset+pk_len])
                offset += pk_len
                file_offset = struct.unpack('<Q', data[offset:offset+8])[0]
                offset += 8
                pk_offsets[pk] = file_offset

            # Indexes
            idx_count = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            indexes: Dict[str, Dict[Any, Set[Any]]] = {}

            for _ in range(idx_count):
                # Column Name
                col_len = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                col_name = data[offset:offset+col_len].decode('utf-8')
                offset += col_len

                # Entry Count
                entry_count = struct.unpack('<I', data[offset:offset+4])[0]
                offset += 4
                idx_map: Dict[Any, Set[Any]] = {}

                for _ in range(entry_count):
                    # Value
                    val_len = struct.unpack('<H', data[offset:offset+2])[0]
                    offset += 2
                    value = self._deserialize_index_value(data[offset:offset+val_len])
                    offset += val_len

                    # PK Set
                    pk_list_len = struct.unpack('<I', data[offset:offset+4])[0]
                    offset += 4
                    pk_set: Set[Any] = set()
                    for _ in range(pk_list_len):
                        pk_len = struct.unpack('<H', data[offset:offset+2])[0]
                        offset += 2
                        pk = self._deserialize_index_value(data[offset:offset+pk_len])
                        offset += pk_len
                        pk_set.add(pk)

                    idx_map[value] = pk_set

                indexes[col_name] = idx_map

            result[table_name] = {
                'pk_offsets': pk_offsets,
                'indexes': indexes
            }

        return result

    # ========== 高效值序列化（避免 JSON 开销） ==========

    @staticmethod
    def _serialize_index_value(value: Any) -> bytes:
        """
        高效序列化值（msgpack 风格）

        类型码：
        - 0x00: None
        - 0x01: bool
        - 0x02: int (1 byte, -128 ~ 127)
        - 0x03: int (2 bytes)
        - 0x04: int (4 bytes)
        - 0x05: int (8 bytes)
        - 0x06: float (8 bytes)
        - 0x07: str (short, <= 255 bytes)
        - 0x08: str (long, <= 65535 bytes)
        - 0xFF: JSON fallback
        """
        if value is None:
            return b'\x00'
        elif isinstance(value, bool):
            return b'\x01\x01' if value else b'\x01\x00'
        elif isinstance(value, int):
            if -128 <= value <= 127:
                return b'\x02' + struct.pack('<b', value)
            elif -32768 <= value <= 32767:
                return b'\x03' + struct.pack('<h', value)
            elif -2147483648 <= value <= 2147483647:
                return b'\x04' + struct.pack('<i', value)
            else:
                return b'\x05' + struct.pack('<q', value)
        elif isinstance(value, float):
            return b'\x06' + struct.pack('<d', value)
        elif isinstance(value, str):
            utf8 = value.encode('utf-8')
            if len(utf8) <= 255:
                return b'\x07' + struct.pack('<B', len(utf8)) + utf8
            else:
                return b'\x08' + struct.pack('<H', len(utf8)) + utf8
        else:
            # 回退到 JSON（罕见情况）
            json_bytes = json.dumps(value).encode('utf-8')
            return b'\xFF' + struct.pack('<H', len(json_bytes)) + json_bytes

    @staticmethod
    def _deserialize_index_value(data: bytes) -> Any:
        """
        反序列化值

        Args:
            data: 完整的序列化数据

        Returns:
            反序列化后的值
        """
        if not data:
            return None

        type_code = data[0]
        deserializer = _INDEX_VALUE_DESERIALIZERS.get(type_code)

        if deserializer is not None:
            return deserializer(data)
        else:
            raise SerializationError(f"Unknown type code: {type_code}")
