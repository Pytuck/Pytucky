"""
PTK6 / pytucky 页式存储后端。

当前阶段目标：
- 使用独立魔数 PTKY 与 .pytucky 文件后缀
- 使用 4KB 固定页、schema 页和数据叶页
- 支持当前 Storage API 下的最小 load/save/lazy-read 闭环
- 利用 changed_tables 对变更表做追加式增量保存

说明：
- 当前仍未实现真正的 B+Tree 分裂与页内更新。
- 当前的增量保存策略是：仅为变更表追加新的叶页链，覆盖 schema/header，旧页暂时保留。
- 通过 rollback journal 保护 header 与 schema 页，确保崩溃后可回滚到上一次一致状态。
- V1 仅支持显式主键表；无主键表后续再补。
"""

import os
import struct
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING

from ..common.exceptions import SerializationError, UnsupportedOperationError
from .backend_binary import BinaryBackend
from .base import StorageBackend
from .pager import (
    MAGIC,
    PAGE_SIZE,
    FileHeader,
    PageHeader,
    PageType,
    Pager,
    build_slotted_page,
    estimate_slotted_page_size,
    iter_slotted_page_cells,
)
from .schema_v6 import TableSchemaEntry, build_schema_page, parse_schema_page
from .versions import get_format_version

if TYPE_CHECKING:
    from ..core.orm import Column
    from ..core.storage import Table


_JOURNAL_HEADER_STRUCT = struct.Struct('<4sII')
_JOURNAL_ENTRY_PAGE_STRUCT = struct.Struct('<I')


class PytuckyBackend(StorageBackend):
    """PTK6 页式后端。"""

    ENGINE_NAME = 'pytucky'
    FORMAT_VERSION = get_format_version(ENGINE_NAME)
    JOURNAL_MAGIC = b'PTKJ'

    def __init__(self, file_path: Path, options: Any):
        super().__init__(file_path, options)
        if self.file_path.suffix.lower() in {'', '.pytuck'}:
            self.file_path = self.file_path.with_suffix('.pytucky')
        self.pager = Pager(self.file_path)

    def exists(self) -> bool:
        return self.pager.exists()

    def delete(self) -> None:
        if self.file_path.exists():
            self.file_path.unlink()
        journal_path = self._journal_path()
        if journal_path.exists():
            journal_path.unlink()

    def supports_lazy_loading(self) -> bool:
        return bool(getattr(self.options, 'lazy_load', False))

    @classmethod
    def probe(cls, file_path: Path) -> Tuple[bool, Optional[Dict[str, Any]]]:
        try:
            file_path = Path(file_path).expanduser()
            if not file_path.exists() or file_path.stat().st_size < PAGE_SIZE:
                return False, None

            with open(file_path, 'rb') as handle:
                magic = handle.read(4)
            if magic != MAGIC:
                return False, None

            pager = Pager(file_path)
            header = pager.read_file_header()
            file_size = file_path.stat().st_size
            if file_size % PAGE_SIZE != 0:
                return False, None
            if header.page_count * PAGE_SIZE != file_size:
                return False, None

            info: Dict[str, Any] = {
                'engine': cls.ENGINE_NAME,
                'format_version': str(header.version),
                'page_size': header.page_size,
                'page_count': header.page_count,
                'confidence': 'high',
            }

            try:
                schema_entries = parse_schema_page(pager.read_page(header.schema_root_page))
                info['table_count'] = len(schema_entries)
            except Exception:
                pass

            return True, info
        except Exception:
            return False, None

    def save(self, tables: Dict[str, 'Table'], *, changed_tables: Optional[Set[str]] = None) -> None:
        """将当前表数据保存为 PTK6 页式文件。"""
        self._recover_from_journal()

        if not self.exists():
            self._save_full(tables)
            return

        if changed_tables is None:
            self._save_full(tables)
            return

        if not changed_tables:
            return

        self._save_incremental(tables, changed_tables)

    def save_full(self, tables: Dict[str, 'Table']) -> None:
        """强制全量保存。"""
        self._recover_from_journal()
        self._save_full(tables)

    def _save_full(self, tables: Dict[str, 'Table']) -> None:
        generation = 1
        if self.exists():
            try:
                generation = self.pager.read_file_header().generation + 1
            except Exception:
                generation = 1

        pages: List[bytes] = [b'\x00' * PAGE_SIZE, b'\x00' * PAGE_SIZE]
        schema_entries: List[TableSchemaEntry] = []

        for table_name in sorted(tables.keys()):
            table = tables[table_name]
            if table.primary_key is None:
                raise UnsupportedOperationError('PTK6 V1 currently requires explicit primary keys')

            root_page, table_pages = self._serialize_table_pages(table, start_page_no=len(pages))
            pages.extend(table_pages)
            schema_entries.append(self._table_to_schema_entry(table, root_page))

        pages[1] = build_schema_page(schema_entries)
        header = FileHeader(
            page_count=len(pages),
            free_list_head=0,
            schema_root_page=1,
            generation=generation,
            flags=0,
            version=self.FORMAT_VERSION,
        )
        pages[0] = header.pack()

        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temp_path_str = tempfile.mkstemp(
            dir=str(self.file_path.parent),
            prefix=f'.{self.file_path.stem}.',
            suffix='.tmp',
        )
        os.close(file_descriptor)
        temp_path = Path(temp_path_str)
        try:
            Pager(temp_path).write_pages(pages)
            temp_path.replace(self.file_path)
        except Exception as exc:
            if temp_path.exists():
                temp_path.unlink()
            raise SerializationError(f'Failed to save PTK6 database: {exc}') from exc

    def _save_incremental(self, tables: Dict[str, 'Table'], changed_tables: Set[str]) -> None:
        """仅为变更表追加新页并覆写 schema/header。"""
        current_header = self.pager.read_file_header()
        schema_entries_by_name = {
            entry.name: entry for entry in self._read_schema_entries(current_header)
        }

        next_page_no = current_header.page_count
        appended_pages: List[bytes] = []
        updated_entries: List[TableSchemaEntry] = []

        for table_name in sorted(tables.keys()):
            table = tables[table_name]
            if table.primary_key is None:
                raise UnsupportedOperationError('PTK6 V1 currently requires explicit primary keys')

            if table_name in changed_tables or table_name not in schema_entries_by_name:
                root_page, table_pages = self._serialize_table_pages(table, start_page_no=next_page_no)
                appended_pages.extend(table_pages)
                next_page_no += len(table_pages)
            else:
                root_page = schema_entries_by_name[table_name].root_page

            updated_entries.append(self._table_to_schema_entry(table, root_page))

        schema_page = build_schema_page(updated_entries)
        new_header = FileHeader(
            page_count=current_header.page_count + len(appended_pages),
            free_list_head=current_header.free_list_head,
            schema_root_page=current_header.schema_root_page,
            generation=current_header.generation + 1,
            flags=current_header.flags,
            version=self.FORMAT_VERSION,
        )

        self._write_journal(
            page_numbers=[0, current_header.schema_root_page],
            original_page_count=current_header.page_count,
        )

        try:
            with open(self.file_path, 'r+b') as handle:
                handle.seek(Pager.page_offset(current_header.page_count))
                for page in appended_pages:
                    handle.write(page)

                handle.seek(Pager.page_offset(current_header.schema_root_page))
                handle.write(schema_page)

                handle.seek(0)
                handle.write(new_header.pack())
                handle.flush()
                os.fsync(handle.fileno())
        except Exception as exc:
            raise SerializationError(f'Failed to incrementally save PTK6 database: {exc}') from exc

        self._remove_journal()

    def load(self) -> Dict[str, 'Table']:
        """加载 PTK6 文件。"""
        self._recover_from_journal()
        if not self.exists():
            raise FileNotFoundError(self.file_path)

        header = self.pager.read_file_header()
        if header.magic != MAGIC:
            raise SerializationError(f'Invalid PTK6 magic: {header.magic!r}')
        if header.version != self.FORMAT_VERSION:
            raise SerializationError(f'Unsupported PTK6 version: {header.version}')

        schema_entries = self._read_schema_entries(header)

        from ..core.storage import Table

        tables: Dict[str, Table] = {}
        for entry in schema_entries:
            table = Table(entry.name, entry.columns, entry.primary_key, entry.comment)
            table.next_id = entry.next_id
            table._backend = self
            table._data_file = self.file_path

            if self.supports_lazy_loading():
                table._pk_offsets = self._collect_pk_offsets(entry.root_page)
                table._lazy_loaded = True
            else:
                for pk, record in self._load_table_records(entry.root_page, table.columns):
                    record_with_pk = record.copy()
                    if table.primary_key is not None:
                        record_with_pk[table.primary_key] = pk
                    table.insert(record_with_pk)
                table.next_id = entry.next_id

            table.reset_dirty()
            tables[entry.name] = table

        return tables

    def populate_tables_with_data(self, tables: Dict[str, 'Table']) -> None:
        for table in tables.values():
            if table._lazy_loaded:
                table._ensure_all_loaded()
                table.reset_dirty()

    def read_lazy_record(
        self,
        file_path: Path,
        offset: int,
        columns: Dict[str, 'Column'],
        pk: Any,
    ) -> Dict[str, Any]:
        """从给定 cell 偏移读取单条记录。"""
        del pk  # 仅用于接口兼容；当前偏移已唯一指向目标记录

        file_path = Path(file_path).expanduser()
        file_size = file_path.stat().st_size
        if offset < 0 or offset + 2 > file_size:
            raise SerializationError('PTK6 lazy record offset out of bounds')

        with open(file_path, 'rb') as handle:
            handle.seek(offset)
            cell_length_bytes = handle.read(2)
            if len(cell_length_bytes) != 2:
                raise SerializationError('PTK6 lazy record cell length is truncated')
            cell_length = struct.unpack('<H', cell_length_bytes)[0]
            if offset + 2 + cell_length > file_size:
                raise SerializationError('PTK6 lazy record cell payload is truncated')
            cell_payload = handle.read(cell_length)
            if len(cell_payload) != cell_length:
                raise SerializationError('PTK6 lazy record cell payload is truncated')

        stored_pk, record = self._decode_record_cell(cell_payload, columns)
        pk_column_name = self._find_primary_key_column(columns)
        if pk_column_name is not None:
            record[pk_column_name] = stored_pk
        return record

    def _serialize_table_pages(self, table: 'Table', start_page_no: int) -> Tuple[int, List[bytes]]:
        """将单表数据序列化为一个或多个叶页。"""
        records: List[Tuple[Any, Dict[str, Any]]]
        try:
            records = sorted(table.scan(), key=lambda item: item[0])
        except TypeError:
            records = list(table.scan())

        pages_cells: List[List[bytes]] = []
        current_page_cells: List[bytes] = []

        for pk, record in records:
            cell = self._encode_record_cell(pk, record, table.columns)
            if estimate_slotted_page_size(current_page_cells + [cell]) > PAGE_SIZE:
                if not current_page_cells:
                    raise SerializationError(
                        f"Record in table '{table.name}' is too large for a single PTK6 page"
                    )
                pages_cells.append(current_page_cells)
                current_page_cells = [cell]
            else:
                current_page_cells.append(cell)

        if current_page_cells or not pages_cells:
            pages_cells.append(current_page_cells)

        root_page = start_page_no
        page_numbers = [start_page_no + index for index in range(len(pages_cells))]
        pages: List[bytes] = []
        for index, cells in enumerate(pages_cells):
            right_pointer = page_numbers[index + 1] if index + 1 < len(page_numbers) else 0
            pages.append(build_slotted_page(PageType.LEAF, cells, right_pointer=right_pointer))

        return root_page, pages

    def _collect_pk_offsets(self, root_page: int) -> Dict[Any, int]:
        """扫描叶页链，建立主键到 cell 偏移的映射。"""
        pk_offsets: Dict[Any, int] = {}
        for page_no, cell_offset, cell_payload in self._iter_leaf_cells(root_page):
            key_length = struct.unpack('<H', cell_payload[:2])[0]
            key_start = 2
            key_end = key_start + key_length
            pk = BinaryBackend._deserialize_index_value(cell_payload[key_start:key_end])
            absolute_offset = Pager.page_offset(page_no) + cell_offset
            pk_offsets[pk] = absolute_offset
        return pk_offsets

    def _load_table_records(
        self,
        root_page: int,
        columns: Dict[str, 'Column'],
    ) -> Iterator[Tuple[Any, Dict[str, Any]]]:
        """读取完整表记录。"""
        for _, _, cell_payload in self._iter_leaf_cells(root_page):
            yield self._decode_record_cell(cell_payload, columns)

    def _iter_leaf_cells(self, root_page: int) -> Iterator[Tuple[int, int, bytes]]:
        """遍历叶页链中的所有 cell。"""
        page_no = root_page
        while page_no != 0:
            page_data = self.pager.read_page(page_no)
            header = PageHeader.unpack(page_data)
            if header.page_type != PageType.LEAF:
                raise SerializationError(f'PTK6 page {page_no} is not a leaf page')

            for cell_offset, cell_payload in iter_slotted_page_cells(page_data):
                yield page_no, cell_offset, cell_payload

            page_no = header.right_pointer

    def _read_schema_entries(self, header: Optional[FileHeader] = None) -> List[TableSchemaEntry]:
        if header is None:
            header = self.pager.read_file_header()
        return parse_schema_page(self.pager.read_page(header.schema_root_page))

    def _journal_path(self) -> Path:
        return self.file_path.with_name(f'.{self.file_path.name}.journal')

    def _write_journal(self, page_numbers: List[int], original_page_count: int) -> None:
        journal_path = self._journal_path()
        with open(journal_path, 'wb') as handle:
            handle.write(
                _JOURNAL_HEADER_STRUCT.pack(
                    self.JOURNAL_MAGIC,
                    original_page_count,
                    len(page_numbers),
                )
            )
            for page_number in page_numbers:
                handle.write(_JOURNAL_ENTRY_PAGE_STRUCT.pack(page_number))
                handle.write(self.pager.read_page(page_number))
            handle.flush()
            os.fsync(handle.fileno())

    def _recover_from_journal(self) -> None:
        journal_path = self._journal_path()
        if not journal_path.exists():
            return
        if not self.file_path.exists():
            raise SerializationError('PTK6 journal exists but database file is missing')

        with open(journal_path, 'rb') as handle:
            header_bytes = handle.read(_JOURNAL_HEADER_STRUCT.size)
            if len(header_bytes) != _JOURNAL_HEADER_STRUCT.size:
                raise SerializationError('PTK6 journal header is truncated')
            magic, original_page_count, backup_page_count = _JOURNAL_HEADER_STRUCT.unpack(header_bytes)
            if magic != self.JOURNAL_MAGIC:
                raise SerializationError(f'Invalid PTK6 journal magic: {magic!r}')

            backups: List[Tuple[int, bytes]] = []
            for _ in range(backup_page_count):
                page_no_bytes = handle.read(_JOURNAL_ENTRY_PAGE_STRUCT.size)
                if len(page_no_bytes) != _JOURNAL_ENTRY_PAGE_STRUCT.size:
                    raise SerializationError('PTK6 journal page entry is truncated')
                page_number = _JOURNAL_ENTRY_PAGE_STRUCT.unpack(page_no_bytes)[0]
                page_data = handle.read(PAGE_SIZE)
                if len(page_data) != PAGE_SIZE:
                    raise SerializationError('PTK6 journal page backup is truncated')
                backups.append((page_number, page_data))

        with open(self.file_path, 'r+b') as handle:
            for page_number, page_data in backups:
                handle.seek(Pager.page_offset(page_number))
                handle.write(page_data)
            handle.truncate(Pager.page_offset(original_page_count))
            handle.flush()
            os.fsync(handle.fileno())

        journal_path.unlink()

    def _remove_journal(self) -> None:
        journal_path = self._journal_path()
        if journal_path.exists():
            journal_path.unlink()

    @staticmethod
    def _table_to_schema_entry(table: 'Table', root_page: int) -> TableSchemaEntry:
        return TableSchemaEntry(
            name=table.name,
            primary_key=table.primary_key,
            comment=table.comment,
            next_id=table.next_id,
            root_page=root_page,
            columns=list(table.columns.values()),
        )

    @staticmethod
    def _encode_record_cell(pk: Any, record: Dict[str, Any], columns: Dict[str, 'Column']) -> bytes:
        key_bytes = BinaryBackend._serialize_index_value(pk)
        record_bytes = BinaryBackend._serialize_record_bytes(pk, record, columns)
        return (
            struct.pack('<H', len(key_bytes))
            + key_bytes
            + struct.pack('<I', len(record_bytes))
            + record_bytes
        )

    @staticmethod
    def _decode_record_cell(
        cell_payload: bytes,
        columns: Dict[str, 'Column'],
    ) -> Tuple[Any, Dict[str, Any]]:
        offset = 0
        key_length = struct.unpack('<H', cell_payload[offset:offset + 2])[0]
        offset += 2
        key_bytes = cell_payload[offset:offset + key_length]
        offset += key_length

        record_length = struct.unpack('<I', cell_payload[offset:offset + 4])[0]
        offset += 4
        record_bytes = cell_payload[offset:offset + record_length]
        offset += record_length

        if offset != len(cell_payload):
            raise SerializationError('PTK6 record cell contains trailing bytes')

        pk = BinaryBackend._deserialize_index_value(key_bytes)
        record = BinaryBackend._deserialize_record_bytes(record_bytes, columns)
        return pk, record

    @staticmethod
    def _find_primary_key_column(columns: Dict[str, 'Column']) -> Optional[str]:
        for column_name, column in columns.items():
            if column.primary_key:
                return column_name
        return None
