from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import struct
from typing import Any, BinaryIO, Dict, List, Optional, Tuple

from ..common.exceptions import (
    DuplicateKeyError,
    RecordNotFoundError,
    SerializationError,
    TableNotFoundError,
    ValidationError,
)
from ..core.orm import Column
from ..core.types import TypeRegistry
from .format import (
    NULL_BITMAP_STRUCT,
    ColumnIndexMeta,
    FileHeader,
    PkDirEntry,
    TableBlockRef,
    decode_row,
)


ROW_LENGTH_STRUCT = struct.Struct("<I")
U16_STRUCT = struct.Struct("<H")
U32_STRUCT = struct.Struct("<I")
COLUMN_FLAGS_STRUCT = struct.Struct("<I")
NONE_NAME_MARKER = 0xFFFF
FLAG_NULLABLE = 1 << 0
FLAG_PRIMARY_KEY = 1 << 1
FLAG_INDEXED = 1 << 2


@dataclass
class TableOverlay:
    inserted: Dict[Any, Dict[str, Any]] = field(default_factory=dict)
    updated: Dict[Any, Dict[str, Any]] = field(default_factory=dict)
    deleted: set[Any] = field(default_factory=set)
    row_cache: Dict[Any, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class TableState:
    name: str
    columns: List[Column]
    primary_key: Optional[str]
    next_id: int
    record_count: int
    data_offset: int
    data_size: int
    pk_index: Dict[Any, Tuple[int, int]] = field(default_factory=dict)
    index_meta: Dict[str, Any] = field(default_factory=dict)
    overlay: TableOverlay = field(default_factory=TableOverlay)


@dataclass(frozen=True)
class _EncodedTable:
    schema_blob: bytes
    records: List[Tuple[Any, Dict[str, Any]]]
    pk_entries: List[PkDirEntry]
    data_blob: bytes
    pk_dir_blob: bytes
    record_count: int
    next_id: int


class Store:
    def __init__(self, file_path: Path | str, *, open_existing: bool = True) -> None:
        self.file_path = Path(file_path)
        self._tables: Dict[str, TableState] = {}
        self._reader: Optional[BinaryIO] = None
        # Optionally open existing file. When open_existing is False we start with an empty in-memory state
        if open_existing and self.file_path.exists() and self.file_path.is_file() and self.file_path.stat().st_size > 0:
            self.open()

    def table_state(self, table_name: str) -> TableState:
        try:
            return self._tables[table_name]
        except KeyError as exc:
            raise TableNotFoundError(table_name) from exc

    def create_table(self, name: str, columns: List[Column]) -> None:
        primary_key = None
        for column in columns:
            if column.primary_key:
                primary_key = column.name
                break
        self._tables[name] = TableState(
            name=name,
            columns=columns,
            primary_key=primary_key,
            next_id=1,
            record_count=0,
            data_offset=0,
            data_size=0,
        )

    def close(self) -> None:
        if self._reader is not None and not self._reader.closed:
            self._reader.close()
        self._reader = None

    def _get_reader(self) -> BinaryIO:
        if self._reader is None or self._reader.closed:
            self._reader = self.file_path.open("rb")
        return self._reader

    def _read_bytes_at(self, offset: int, size: int) -> bytes:
        reader = self._get_reader()
        reader.seek(offset)
        return reader.read(size)

    def open(self) -> None:
        self.close()
        with self.file_path.open("rb") as file_obj:
            header = FileHeader.unpack(file_obj.read(64))
            file_obj.seek(header.schema_offset)
            schema_blob = file_obj.read(header.schema_size)
            schemas = self._decode_schema_catalog(schema_blob, header.table_count)
            file_obj.seek(header.table_ref_offset)
            table_ref_blob = file_obj.read(header.table_ref_size)
            refs = self._decode_table_refs(table_ref_blob, header.table_count)

            tables: Dict[str, TableState] = {}
            for ref in refs:
                columns, primary_key = schemas[ref.name]
                pk_index = self._read_pk_dir(file_obj, ref)
                state = TableState(
                    name=ref.name,
                    columns=columns,
                    primary_key=primary_key,
                    next_id=ref.next_id,
                    record_count=ref.record_count,
                    data_offset=ref.data_offset,
                    data_size=ref.data_size,
                    pk_index=pk_index,
                )
                # read index meta entries into state.index_meta
                state.index_meta = {}
                if ref.index_meta_size and ref.index_data_size:
                    file_obj.seek(ref.index_meta_offset)
                    meta_blob = file_obj.read(ref.index_meta_size)
                    off = 0
                    from .format import ColumnIndexMeta
                    while off < len(meta_blob):
                        cim, consumed = ColumnIndexMeta.unpack(meta_blob[off:])
                        # convert offsets to absolute file offsets for easier reading later
                        cim_abs = ColumnIndexMeta(
                            column_name=cim.column_name,
                            offset=ref.index_data_offset + cim.offset,
                            size=cim.size,
                            entry_count=cim.entry_count,
                            type_code=cim.type_code,
                        )
                        state.index_meta[cim.column_name] = cim_abs
                        off += consumed
                tables[ref.name] = state
            self._tables = tables

    def select(self, table_name: str, pk: Any) -> Dict[str, Any]:
        state = self.table_state(table_name)
        pk = self._normalize_pk(state, pk)
        overlay = state.overlay
        if pk in overlay.deleted:
            raise RecordNotFoundError(table_name, pk)
        if pk in overlay.updated:
            return dict(overlay.updated[pk])
        if pk in overlay.inserted:
            return dict(overlay.inserted[pk])
        if pk in overlay.row_cache:
            return dict(overlay.row_cache[pk])
        try:
            offset, length = state.pk_index[pk]
        except KeyError as exc:
            raise RecordNotFoundError(table_name, pk) from exc
        record = self._read_row_at(state, pk, offset, length)
        overlay.row_cache[pk] = record
        return dict(record)

    def insert(self, table_name: str, data: Dict[str, Any]) -> Any:
        state = self.table_state(table_name)
        pk = self._resolve_insert_pk(state, data)
        if self._pk_exists(state, pk):
            raise DuplicateKeyError(table_name, pk)
        record = self._validate_record(state, data, pk)
        state.overlay.inserted[pk] = record
        state.overlay.updated.pop(pk, None)
        state.overlay.deleted.discard(pk)
        state.overlay.row_cache[pk] = record
        return pk

    def update(self, table_name: str, pk: Any, data: Dict[str, Any]) -> None:
        state = self.table_state(table_name)
        pk = self._normalize_pk(state, pk)
        if pk in state.overlay.inserted:
            base = dict(state.overlay.inserted[pk])
        else:
            base = self.select(table_name, pk)
        base.update(data)
        record = self._validate_record(state, base, pk)
        if pk in state.overlay.inserted:
            state.overlay.inserted[pk] = record
        else:
            state.overlay.updated[pk] = record
        state.overlay.deleted.discard(pk)
        state.overlay.row_cache[pk] = record

    def delete(self, table_name: str, pk: Any) -> None:
        state = self.table_state(table_name)
        pk = self._normalize_pk(state, pk)
        if pk in state.overlay.inserted:
            del state.overlay.inserted[pk]
            state.overlay.row_cache.pop(pk, None)
            return
        if pk not in state.pk_index and pk not in state.overlay.updated:
            raise RecordNotFoundError(table_name, pk)
        state.overlay.updated.pop(pk, None)
        state.overlay.deleted.add(pk)
        state.overlay.row_cache.pop(pk, None)

    def flush(self) -> None:
        schema_blobs: Dict[str, bytes] = {}
        encoded_tables: Dict[str, _EncodedTable] = {}
        index_meta_entries: Dict[str, List[ColumnIndexMeta]] = {}
        index_meta_blobs: Dict[str, bytes] = {}
        index_data_blobs: Dict[str, bytes] = {}
        ordered_tables = list(self._tables.keys())

        from . import index

        for table_name in ordered_tables:
            state = self._tables[table_name]
            overlay = state.overlay

            # 未改表直通路径：跳过 O(N) decode + encode，直接读取原始字节
            if (
                not overlay.inserted
                and not overlay.updated
                and not overlay.deleted
                and state.data_offset > 0
            ):
                encoded = self._passthrough_unchanged_table(state)
                schema_blobs[table_name] = encoded.schema_blob
                encoded_tables[table_name] = encoded

                pt_meta_entries: List[ColumnIndexMeta] = []
                pt_meta_blob = bytearray()
                pt_data_blob = bytearray()
                for column in state.columns:
                    if not column.index:
                        continue
                    cim = state.index_meta.get(column.name)
                    if cim is None:
                        continue
                    col_idx_data = self._read_bytes_at(cim.offset, cim.size)
                    new_cim = ColumnIndexMeta(
                        column_name=cim.column_name,
                        offset=len(pt_data_blob),
                        size=cim.size,
                        entry_count=cim.entry_count,
                        type_code=cim.type_code,
                    )
                    pt_meta_entries.append(new_cim)
                    pt_meta_blob.extend(new_cim.pack())
                    pt_data_blob.extend(col_idx_data)
                index_meta_entries[table_name] = pt_meta_entries
                index_meta_blobs[table_name] = bytes(pt_meta_blob)
                index_data_blobs[table_name] = bytes(pt_data_blob)
                continue

            live_records = self._materialize_records(state)
            encoded = self._encode_table(state, live_records)
            schema_blobs[table_name] = encoded.schema_blob
            encoded_tables[table_name] = encoded

            meta_entries: List[ColumnIndexMeta] = []
            meta_blob = bytearray()
            data_blob = bytearray()
            for column in state.columns:
                if not column.index:
                    continue
                pairs = index.build_sorted_pairs(live_records, column)
                column_data_blob = index.encode_sorted_pairs(pairs, column)
                column_meta = ColumnIndexMeta(
                    column_name=column.name,
                    offset=len(data_blob),
                    size=len(column_data_blob),
                    entry_count=len(pairs),
                    type_code=TypeRegistry.get_codec(column.col_type)[0],
                )
                meta_entries.append(column_meta)
                meta_blob.extend(column_meta.pack())
                data_blob.extend(column_data_blob)
            index_meta_entries[table_name] = meta_entries
            index_meta_blobs[table_name] = bytes(meta_blob)
            index_data_blobs[table_name] = bytes(data_blob)

        schema_catalog = self._encode_schema_catalog(schema_blobs)
        table_ref_offset = 64 + len(schema_catalog)

        temp_refs: List[TableBlockRef] = []
        for table_name in ordered_tables:
            encoded = encoded_tables[table_name]
            temp_refs.append(
                TableBlockRef(
                    name=table_name,
                    record_count=encoded.record_count,
                    next_id=encoded.next_id,
                    data_offset=0,
                    data_size=len(encoded.data_blob),
                    pk_dir_offset=0,
                    pk_dir_size=len(encoded.pk_dir_blob),
                    index_meta_offset=0,
                    index_meta_size=len(index_meta_blobs[table_name]),
                    index_data_offset=0,
                    index_data_size=len(index_data_blobs[table_name]),
                )
            )
        all_refs_size = sum(len(ref.pack()) for ref in temp_refs)
        current_offset = table_ref_offset + all_refs_size

        refs: List[TableBlockRef] = []
        table_refs_blob = bytearray()
        for table_name in ordered_tables:
            encoded = encoded_tables[table_name]
            meta_blob = index_meta_blobs[table_name]
            index_blob = index_data_blobs[table_name]

            data_offset = current_offset
            current_offset += len(encoded.data_blob)
            pk_dir_offset = current_offset
            current_offset += len(encoded.pk_dir_blob)
            index_meta_offset = current_offset
            current_offset += len(meta_blob)
            index_data_offset = current_offset
            current_offset += len(index_blob)

            ref = TableBlockRef(
                name=table_name,
                record_count=encoded.record_count,
                next_id=encoded.next_id,
                data_offset=data_offset,
                data_size=len(encoded.data_blob),
                pk_dir_offset=pk_dir_offset,
                pk_dir_size=len(encoded.pk_dir_blob),
                index_meta_offset=index_meta_offset,
                index_meta_size=len(meta_blob),
                index_data_offset=index_data_offset,
                index_data_size=len(index_blob),
            )
            refs.append(ref)
            table_refs_blob.extend(ref.pack())

        table_refs_blob_bytes = bytes(table_refs_blob)
        header = FileHeader(
            table_count=len(refs),
            schema_offset=64,
            schema_size=len(schema_catalog),
            table_ref_offset=table_ref_offset,
            table_ref_size=len(table_refs_blob_bytes),
            file_size=current_offset,
        )

        self.close()
        temp_path = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        with temp_path.open("wb") as file_obj:
            file_obj.write(header.pack())
            file_obj.write(schema_catalog)
            file_obj.write(table_refs_blob_bytes)
            for table_name in ordered_tables:
                encoded = encoded_tables[table_name]
                file_obj.write(encoded.data_blob)
                file_obj.write(encoded.pk_dir_blob)
                file_obj.write(index_meta_blobs[table_name])
                file_obj.write(index_data_blobs[table_name])
        temp_path.replace(self.file_path)
        self._refresh_tables_after_flush(refs, encoded_tables, index_meta_entries)

    def _refresh_tables_after_flush(
        self,
        refs: List[TableBlockRef],
        encoded_tables: Dict[str, _EncodedTable],
        index_meta_entries: Dict[str, List[ColumnIndexMeta]],
    ) -> None:
        previous_tables = self._tables
        refreshed: Dict[str, TableState] = {}
        for ref in refs:
            previous_state = previous_tables[ref.name]
            encoded = encoded_tables[ref.name]
            pk_index = {
                entry.pk: (ref.data_offset + entry.offset, entry.length)
                for entry in encoded.pk_entries
            }
            meta_by_column = {
                meta.column_name: ColumnIndexMeta(
                    column_name=meta.column_name,
                    offset=ref.index_data_offset + meta.offset,
                    size=meta.size,
                    entry_count=meta.entry_count,
                    type_code=meta.type_code,
                )
                for meta in index_meta_entries.get(ref.name, [])
            }
            refreshed[ref.name] = TableState(
                name=previous_state.name,
                columns=previous_state.columns,
                primary_key=previous_state.primary_key,
                next_id=ref.next_id,
                record_count=ref.record_count,
                data_offset=ref.data_offset,
                data_size=ref.data_size,
                pk_index=pk_index,
                index_meta=meta_by_column,
            )
        self._tables = refreshed

    def _encode_table(
        self,
        state: TableState,
        live_records: Optional[List[Tuple[Any, Dict[str, Any]]]] = None,
    ) -> _EncodedTable:
        if live_records is None:
            live_records = self._materialize_records(state)
        payload_columns = [column for column in state.columns if column.name != state.primary_key]
        if len(payload_columns) > 32:
            raise SerializationError("encode_row currently supports at most 32 non-pk columns")
        payload_layout = [
            (column, TypeRegistry.get_codec(column.col_type)[1])
            for column in payload_columns
        ]

        data_blob = bytearray()
        entries: List[PkDirEntry] = []
        for pk, record in live_records:
            null_bits = 0
            payload = bytearray()
            for index, (column, codec) in enumerate(payload_layout):
                value = record.get(column.name)
                if value is None:
                    null_bits |= 1 << index
                    continue
                payload.extend(codec.encode(value))
            row_payload = NULL_BITMAP_STRUCT.pack(null_bits) + bytes(payload)
            row_blob = ROW_LENGTH_STRUCT.pack(len(row_payload)) + row_payload
            offset = len(data_blob)
            data_blob.extend(row_blob)
            entries.append(PkDirEntry(pk=pk, offset=offset, length=len(row_blob)))

        pk_dir_blob = bytearray()
        for entry in entries:
            pk_dir_blob.extend(entry.pack_int())

        return _EncodedTable(
            schema_blob=self._encode_table_schema(state),
            records=live_records,
            pk_entries=entries,
            data_blob=bytes(data_blob),
            pk_dir_blob=bytes(pk_dir_blob),
            record_count=len(live_records),
            next_id=state.next_id,
        )

    def _materialize_records(self, state: TableState) -> List[Tuple[Any, Dict[str, Any]]]:
        live: Dict[Any, Dict[str, Any]] = {}
        for pk in state.pk_index:
            if pk in state.overlay.deleted:
                continue
            if pk in state.overlay.updated:
                live[pk] = dict(state.overlay.updated[pk])
                continue
            live[pk] = self.select(state.name, pk)
        for pk, record in state.overlay.inserted.items():
            if pk in state.overlay.deleted:
                continue
            live[pk] = dict(record)
        return sorted(live.items(), key=lambda item: item[0])

    def _passthrough_unchanged_table(self, state: TableState) -> _EncodedTable:
        """未改表直通：从磁盘读取原始字节，跳过 decode+encode。"""
        data_blob = self._read_bytes_at(state.data_offset, state.data_size)
        pk_entries = sorted(
            [
                PkDirEntry(
                    pk=pk,
                    offset=abs_offset - state.data_offset,
                    length=length,
                )
                for pk, (abs_offset, length) in state.pk_index.items()
            ],
            key=lambda e: e.pk,
        )
        pk_dir_blob = b''.join(entry.pack_int() for entry in pk_entries)
        return _EncodedTable(
            schema_blob=self._encode_table_schema(state),
            records=[],
            pk_entries=pk_entries,
            data_blob=data_blob,
            pk_dir_blob=pk_dir_blob,
            record_count=state.record_count,
            next_id=state.next_id,
        )

    def _read_row_at(self, state: TableState, pk: Any, offset: int, length: int) -> Dict[str, Any]:
        row_blob = self._read_bytes_at(offset, length)
        if len(row_blob) < ROW_LENGTH_STRUCT.size:
            raise SerializationError("Not enough data to decode row length")
        payload_length = ROW_LENGTH_STRUCT.unpack(row_blob[: ROW_LENGTH_STRUCT.size])[0]
        payload = row_blob[ROW_LENGTH_STRUCT.size :]
        if len(payload) != payload_length:
            raise SerializationError(
                f"Row payload length mismatch: expected {payload_length}, got {len(payload)}"
            )
        record = decode_row(state.columns, payload, pk_name=state.primary_key)
        if state.primary_key is not None:
            record[state.primary_key] = pk
        return record

    def _resolve_insert_pk(self, state: TableState, data: Dict[str, Any]) -> Any:
        if state.primary_key is None:
            pk = state.next_id
            state.next_id += 1
            return pk
        pk = data.get(state.primary_key)
        if pk is None:
            pk_column = self._primary_key_column(state)
            if pk_column is None or pk_column.col_type is not int:
                raise ValidationError(f"Primary key '{state.primary_key}' must be provided")
            pk = state.next_id
            state.next_id += 1
        else:
            pk = self._normalize_pk(state, pk)
            if isinstance(pk, int) and pk >= state.next_id:
                state.next_id = pk + 1
        return pk

    def _validate_record(self, state: TableState, data: Dict[str, Any], pk: Any) -> Dict[str, Any]:
        validated: Dict[str, Any] = {}
        for column in state.columns:
            assert column.name is not None
            if column.name == state.primary_key:
                validated[column.name] = pk
                continue
            validated[column.name] = column.validate(data.get(column.name))
        if state.primary_key is not None:
            validated[state.primary_key] = pk
        return validated

    def _pk_exists(self, state: TableState, pk: Any) -> bool:
        if pk in state.overlay.deleted:
            return False
        if pk in state.overlay.inserted:
            return True
        if pk in state.overlay.updated:
            return True
        return pk in state.pk_index

    def _normalize_pk(self, state: TableState, pk: Any) -> Any:
        column = self._primary_key_column(state)
        if column is None:
            return pk
        return column.validate(pk)

    def _primary_key_column(self, state: TableState) -> Optional[Column]:
        if state.primary_key is None:
            return None
        for column in state.columns:
            if column.name == state.primary_key:
                return column
        return None

    def _encode_schema_catalog(self, table_schemas: Dict[str, bytes]) -> bytes:
        blob = bytearray()
        for table_name in self._tables:
            schema_blob = table_schemas[table_name]
            blob.extend(schema_blob)
        return bytes(blob)

    def _decode_schema_catalog(
        self, blob: bytes, table_count: int
    ) -> Dict[str, Tuple[List[Column], Optional[str]]]:
        offset = 0
        schemas: Dict[str, Tuple[List[Column], Optional[str]]] = {}
        for _ in range(table_count):
            table_name, offset = self._unpack_string(blob, offset)
            primary_key_name, offset = self._unpack_optional_string(blob, offset)
            column_count = U16_STRUCT.unpack(blob[offset : offset + U16_STRUCT.size])[0]
            offset += U16_STRUCT.size
            columns: List[Column] = []
            for _ in range(column_count):
                column_name, offset = self._unpack_string(blob, offset)
                type_name, offset = self._unpack_string(blob, offset)
                flags = COLUMN_FLAGS_STRUCT.unpack(blob[offset : offset + COLUMN_FLAGS_STRUCT.size])[0]
                offset += COLUMN_FLAGS_STRUCT.size
                columns.append(
                    Column(
                        TypeRegistry.get_type_by_name(type_name),
                        name=column_name,
                        nullable=bool(flags & FLAG_NULLABLE),
                        primary_key=bool(flags & FLAG_PRIMARY_KEY),
                        index=bool(flags & FLAG_INDEXED),
                    )
                )
            schemas[table_name] = (columns, primary_key_name)
        return schemas

    def _encode_table_schema(self, state: TableState) -> bytes:
        blob = bytearray()
        blob.extend(self._pack_string(state.name))
        blob.extend(self._pack_optional_string(state.primary_key))
        blob.extend(U16_STRUCT.pack(len(state.columns)))
        for column in state.columns:
            assert column.name is not None
            flags = 0
            if column.nullable:
                flags |= FLAG_NULLABLE
            if column.primary_key:
                flags |= FLAG_PRIMARY_KEY
            if column.index:
                flags |= FLAG_INDEXED
            blob.extend(self._pack_string(column.name))
            blob.extend(self._pack_string(TypeRegistry.get_type_name(column.col_type)))
            blob.extend(COLUMN_FLAGS_STRUCT.pack(flags))
        return bytes(blob)

    def _decode_table_refs(self, blob: bytes, table_count: int) -> List[TableBlockRef]:
        offset = 0
        refs: List[TableBlockRef] = []
        for _ in range(table_count):
            ref, consumed = TableBlockRef.unpack(blob[offset:])
            refs.append(ref)
            offset += consumed
        return refs

    def _read_pk_dir(self, file_obj: Any, ref: TableBlockRef) -> Dict[Any, Tuple[int, int]]:
        pk_index: Dict[Any, Tuple[int, int]] = {}
        if ref.pk_dir_size == 0:
            return pk_index
        file_obj.seek(ref.pk_dir_offset)
        entry_size = PkDirEntry(pk=0, offset=0, length=0).pack_int().__len__()
        blob = file_obj.read(ref.pk_dir_size)
        offset = 0
        for _ in range(ref.record_count):
            entry = PkDirEntry.unpack_int(blob[offset : offset + entry_size])
            pk_index[entry.pk] = (ref.data_offset + entry.offset, entry.length)
            offset += entry_size
        return pk_index

    def _pack_string(self, value: str) -> bytes:
        encoded = value.encode("utf-8")
        if len(encoded) > 0xFFFF:
            raise SerializationError(f"String too long for PTK7 schema field: {len(encoded)} bytes")
        return U16_STRUCT.pack(len(encoded)) + encoded

    def _pack_optional_string(self, value: Optional[str]) -> bytes:
        if value is None:
            return U16_STRUCT.pack(NONE_NAME_MARKER)
        return self._pack_string(value)

    def _unpack_string(self, blob: bytes, offset: int) -> Tuple[str, int]:
        if offset + U16_STRUCT.size > len(blob):
            raise SerializationError("Not enough data to decode string length")
        length = U16_STRUCT.unpack(blob[offset : offset + U16_STRUCT.size])[0]
        offset += U16_STRUCT.size
        end = offset + length
        if end > len(blob):
            raise SerializationError("Not enough data to decode string bytes")
        try:
            return blob[offset:end].decode("utf-8"), end
        except UnicodeDecodeError as exc:
            raise SerializationError("Invalid UTF-8 string in PTK7 schema") from exc

    def _unpack_optional_string(self, blob: bytes, offset: int) -> Tuple[Optional[str], int]:
        if offset + U16_STRUCT.size > len(blob):
            raise SerializationError("Not enough data to decode optional string length")
        length = U16_STRUCT.unpack(blob[offset : offset + U16_STRUCT.size])[0]
        if length == NONE_NAME_MARKER:
            return None, offset + U16_STRUCT.size
        return self._unpack_string(blob, offset)

    def search_index(self, table_name: str, column_name: str, value: Any) -> List[Any]:
        """在指定表的索引列上查找等值，返回匹配的 pk 列表。合并 overlay 语义：
        - overlay.inserted/updated 的匹配应可见
        - overlay.deleted 中的 pk 应被剔除
        - 即便磁盘上没有 index_meta，只要列标记为 indexed，也应基于 overlay 返回匹配结果
        """
        state = self.table_state(table_name)
        # find column object and ensure it's indexed
        col = None
        for c in state.columns:
            if c.name == column_name:
                col = c
                break
        if col is None or not col.index:
            return []

        results: set[Any] = set()
        # first, load on-disk index results if available
        cim = state.index_meta.get(column_name)
        if cim is not None:
            blob = self._read_bytes_at(cim.offset, cim.size)
            from . import index
            results.update(index.search_sorted_pairs(blob, value, col))

        # merge overlay: inserted and updated that match value
        # check inserted
        for pk, rec in state.overlay.inserted.items():
            if pk in state.overlay.deleted:
                continue
            if rec.get(column_name) == value:
                results.add(pk)
        # check updated
        for pk, rec in state.overlay.updated.items():
            if pk in state.overlay.deleted:
                continue
            if rec.get(column_name) == value:
                results.add(pk)
            else:
                # if updated changed away from value, ensure removed from results
                results.discard(pk)

        # finally remove deleted
        for pk in state.overlay.deleted:
            results.discard(pk)

        return sorted(results)
