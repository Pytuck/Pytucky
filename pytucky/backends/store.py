from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import struct
from typing import Any, BinaryIO

from ..common.exceptions import (
    ConfigurationError,
    DuplicateKeyError,
    EncryptionError,
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
    _payload_columns,
    CRYPTO_META_STRUCT,
    CryptoMetadataV7,
)
from ..common.crypto import (
    CipherType,
    CryptoProvider,
    get_cipher,
    ENCRYPTION_LEVELS,
)
from ..common.options import PytuckBackendOptions

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
    inserted: dict[Any, dict[str, Any]] = field(default_factory=dict)
    updated: dict[Any, dict[str, Any]] = field(default_factory=dict)
    deleted: set[Any] = field(default_factory=set)
    row_cache: dict[Any, dict[str, Any]] = field(default_factory=dict)

@dataclass
class TableState:
    name: str
    columns: list[Column]
    primary_key: str | None
    next_id: int
    record_count: int
    data_offset: int
    data_size: int
    pk_index: dict[Any, tuple[int, int]] = field(default_factory=dict)
    index_meta: dict[str, Any] = field(default_factory=dict)
    overlay: TableOverlay = field(default_factory=TableOverlay)

@dataclass(frozen=True)
class _EncodedTable:
    schema_blob: bytes
    records: list[tuple[Any, dict[str, Any]]]
    pk_entries: list[PkDirEntry]
    data_blob: bytes
    pk_dir_blob: bytes
    record_count: int
    next_id: int

class Store:
    def __init__(self, file_path: Path | str, options: PytuckBackendOptions | None = None, *, open_existing: bool = True) -> None:
        self.file_path = Path(file_path)
        self.options = options or PytuckBackendOptions()
        self._tables: dict[str, TableState] = {}
        self._reader: BinaryIO | None = None
        self._cipher: CipherType | None = None
        self._payload_offset = 0
        self._loaded_encryption_level: str | None = None
        # Optionally open existing file. When open_existing is False we start with an empty in-memory state
        if open_existing and self.file_path.exists() and self.file_path.is_file() and self.file_path.stat().st_size > 0:
            self.open()

    def table_state(self, table_name: str) -> TableState:
        try:
            return self._tables[table_name]
        except KeyError as exc:
            raise TableNotFoundError(table_name) from exc

    def create_table(self, name: str, columns: list[Column]) -> None:
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

    def _decrypt_region(self, offset: int, data: bytes) -> bytes:
        if self._cipher is None:
            return data
        if offset >= self._payload_offset:
            rel_off = offset - self._payload_offset
            return self._cipher.decrypt_at(rel_off, data)
        return data

    def _read_region(self, offset: int, size: int) -> bytes:
        """Read a region from file, decrypting payload bytes when needed."""
        raw = self._read_bytes_at(offset, size)
        if len(raw) < size:
            raise SerializationError(
                f"PTK7 region is incomplete at offset {offset} (need {size}, got {len(raw)})"
            )
        return self._decrypt_region(offset, raw)

    def open(self) -> None:
        self.close()
        self._cipher = None
        self._payload_offset = 0
        self._loaded_encryption_level = None
        with self.file_path.open("rb") as file_obj:
            header = FileHeader.unpack(file_obj.read(64))
            if header.is_encrypted():
                file_obj.seek(64)
                crypto_blob = file_obj.read(CRYPTO_META_STRUCT.size)
                crypto_meta = CryptoMetadataV7.unpack(crypto_blob)
                level = header.get_encryption_level()
                if level is None:
                    raise EncryptionError("无法识别加密等级")
                if not self.options.password:
                    raise EncryptionError("文件已加密，需要提供密码")
                key = CryptoProvider.derive_key(self.options.password, crypto_meta.salt, level)
                if not CryptoProvider.verify_key(key, crypto_meta.key_check):
                    raise EncryptionError("密码错误")
                self._cipher = get_cipher(level, key)
                self._loaded_encryption_level = level
            # read schema and table refs (schema_offset may include crypto metadata)
            file_obj.seek(header.schema_offset)
            schema_blob = file_obj.read(header.schema_size)
            schemas = self._decode_schema_catalog(schema_blob, header.table_count)
            file_obj.seek(header.table_ref_offset)
            table_ref_blob = file_obj.read(header.table_ref_size)
            refs = self._decode_table_refs(table_ref_blob, header.table_count)

            # payload offset is the area after table refs — set before reading pk dirs so pk_dir can be decrypted
            self._payload_offset = header.table_ref_offset + header.table_ref_size

            tables: dict[str, TableState] = {}
            for ref in refs:
                columns, primary_key = schemas[ref.name]
                # read pk dir using helpers that will decrypt when necessary
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
                    if len(meta_blob) < ref.index_meta_size:
                        raise SerializationError(
                            f"PTK7 region is incomplete at offset {ref.index_meta_offset} (need {ref.index_meta_size}, got {len(meta_blob)})"
                        )
                    meta_blob = self._decrypt_region(ref.index_meta_offset, meta_blob)
                    state.index_meta = self._decode_index_meta(meta_blob, ref)
                tables[ref.name] = state
            self._tables = tables

    def select(self, table_name: str, pk: Any) -> dict[str, Any]:
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

    def insert(self, table_name: str, data: dict[str, Any]) -> Any:
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

    def update(self, table_name: str, pk: Any, data: dict[str, Any]) -> None:
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
        schema_blobs: dict[str, bytes] = {}
        encoded_tables: dict[str, _EncodedTable] = {}
        index_meta_entries: dict[str, list[ColumnIndexMeta]] = {}
        index_meta_blobs: dict[str, bytes] = {}
        index_data_blobs: dict[str, bytes] = {}
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

                import json

                pt_meta_entries: list[ColumnIndexMeta] = []
                pt_meta_json: list[dict[str, Any]] = []
                pt_data_blob = bytearray()
                for column in state.columns:
                    if not column.index:
                        continue
                    assert column.name is not None
                    cim = state.index_meta.get(column.name)
                    if cim is None:
                        continue
                    col_idx_data = self._read_region(cim.offset, cim.size)
                    new_cim = ColumnIndexMeta(
                        column_name=cim.column_name,
                        offset=len(pt_data_blob),
                        size=cim.size,
                        entry_count=cim.entry_count,
                        type_code=cim.type_code,
                    )
                    pt_meta_entries.append(new_cim)
                    pt_meta_json.append(
                        {
                            "column": new_cim.column_name,
                            "type": "sorted" if column.index == "sorted" else "hash",
                            "offset": new_cim.offset,
                            "size": new_cim.size,
                        }
                    )
                    pt_data_blob.extend(col_idx_data)
                index_meta_entries[table_name] = pt_meta_entries
                index_meta_blobs[table_name] = json.dumps(pt_meta_json, ensure_ascii=False).encode("utf-8")
                index_data_blobs[table_name] = bytes(pt_data_blob)
                continue

            live_records = self._materialize_records(state)
            encoded = self._encode_table(state, live_records)
            schema_blobs[table_name] = encoded.schema_blob
            encoded_tables[table_name] = encoded

            import json

            meta_entries: list[ColumnIndexMeta] = []
            meta_json: list[dict[str, Any]] = []
            data_blob = bytearray()
            for column in state.columns:
                if not column.index:
                    continue
                assert column.name is not None
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
                meta_json.append(
                    {
                        "column": column.name,
                        "type": "sorted" if column.index == "sorted" else "hash",
                        "offset": column_meta.offset,
                        "size": column_meta.size,
                    }
                )
                data_blob.extend(column_data_blob)
            index_meta_entries[table_name] = meta_entries
            index_meta_blobs[table_name] = json.dumps(meta_json, ensure_ascii=False).encode("utf-8")
            index_data_blobs[table_name] = bytes(data_blob)

        schema_catalog = self._encode_schema_catalog(schema_blobs)
        effective_encryption = (
            self.options.encryption
            if self.options.encryption is not None
            else self._loaded_encryption_level
        )

        crypto_meta: CryptoMetadataV7 | None = None
        cipher = None
        if effective_encryption is not None:
            if effective_encryption not in ENCRYPTION_LEVELS:
                raise ConfigurationError(f"无效的加密等级: {effective_encryption}")
            if not self.options.password:
                raise ConfigurationError("加密需要提供密码")
            import os
            salt = os.urandom(16)
            key = CryptoProvider.derive_key(self.options.password, salt, effective_encryption)
            key_check = CryptoProvider.compute_key_check(key)
            crypto_meta = CryptoMetadataV7(salt=salt, key_check=key_check)
            cipher = get_cipher(effective_encryption, key)

        schema_offset = 64 + (CRYPTO_META_STRUCT.size if crypto_meta is not None else 0)
        table_ref_offset = schema_offset + len(schema_catalog)

        temp_refs: list[TableBlockRef] = []
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

        refs: list[TableBlockRef] = []
        table_refs_blob = bytearray()
        payload_buffer = bytearray()
        for table_name in ordered_tables:
            encoded = encoded_tables[table_name]
            meta_blob = index_meta_blobs[table_name]
            index_blob = index_data_blobs[table_name]

            data_offset = current_offset
            current_offset += len(encoded.data_blob)
            # build pk dir bytes using absolute offsets (write absolute offsets for cross-library compatibility)
            # encoded.pk_entries contains entries with offsets relative to the start of the data blob.
            pk_dir_bytes = bytearray()
            for entry in encoded.pk_entries:
                abs_offset = data_offset + entry.offset
                pk_dir_bytes.extend(PkDirEntry(pk=entry.pk, offset=abs_offset, length=entry.length).pack_int())
            pk_dir_offset = current_offset
            current_offset += len(pk_dir_bytes)
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
                pk_dir_size=len(pk_dir_bytes),
                index_meta_offset=index_meta_offset,
                index_meta_size=len(meta_blob),
                index_data_offset=index_data_offset,
                index_data_size=len(index_blob),
            )
            refs.append(ref)
            table_refs_blob.extend(ref.pack())

            payload_buffer.extend(encoded.data_blob)
            payload_buffer.extend(pk_dir_bytes)
            payload_buffer.extend(meta_blob)
            payload_buffer.extend(index_blob)

        table_refs_blob_bytes = bytes(table_refs_blob)
        payload_blob = bytes(payload_buffer)
        if cipher is not None:
            payload_blob = cipher.encrypt(payload_blob)

        header = FileHeader(
            table_count=len(refs),
            schema_offset=schema_offset,
            schema_size=len(schema_catalog),
            table_ref_offset=table_ref_offset,
            table_ref_size=len(table_refs_blob_bytes),
            file_size=current_offset,
        )
        if crypto_meta is not None:
            header = header.set_encryption(effective_encryption)
        else:
            header = header.set_encryption(None)

        self.close()
        temp_path = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        with temp_path.open("wb") as file_obj:
            file_obj.write(header.pack())
            if crypto_meta is not None:
                file_obj.write(crypto_meta.pack())
            file_obj.write(schema_catalog)
            file_obj.write(table_refs_blob_bytes)
            file_obj.write(payload_blob)
        temp_path.replace(self.file_path)
        self._cipher = cipher
        self._payload_offset = header.table_ref_offset + header.table_ref_size if cipher is not None else 0
        self._loaded_encryption_level = effective_encryption if crypto_meta is not None else None
        self._refresh_tables_after_flush(refs, encoded_tables, index_meta_entries)

    def _refresh_tables_after_flush(
        self,
        refs: list[TableBlockRef],
        encoded_tables: dict[str, _EncodedTable],
        index_meta_entries: dict[str, list[ColumnIndexMeta]],
    ) -> None:
        previous_tables = self._tables
        refreshed: dict[str, TableState] = {}
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
        live_records: list[tuple[Any, dict[str, Any]]] | None = None,
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
        entries: list[PkDirEntry] = []
        for pk, record in live_records:
            null_bits = 0
            payload = bytearray()
            for index, (column, codec) in enumerate(payload_layout):
                assert column.name is not None
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

    def _materialize_records(self, state: TableState) -> list[tuple[Any, dict[str, Any]]]:
        live: dict[Any, dict[str, Any]] = {}
        # overlay 覆盖部分：updated 和 inserted 优先
        for pk in state.overlay.updated:
            if pk not in state.overlay.deleted:
                live[pk] = dict(state.overlay.updated[pk])
        for pk, record in state.overlay.inserted.items():
            if pk not in state.overlay.deleted:
                live[pk] = dict(record)
        # 需从磁盘读的 pk（排除 overlay 已覆盖和已删除的）
        disk_pks = [
            pk for pk in state.pk_index
            if pk not in state.overlay.deleted
            and pk not in state.overlay.updated
        ]
        if disk_pks:
            payload_cols = _payload_columns(state.columns, state.primary_key)
            codecs = [TypeRegistry.get_codec(c.col_type)[1] for c in payload_cols]
            # If cipher is None, bulk read the entire data blob for performance.
            if self._cipher is None:
                data_blob = self._read_region(state.data_offset, state.data_size)
                for pk in disk_pks:
                    abs_off, length = state.pk_index[pk]
                    rel_off = abs_off - state.data_offset
                    row_blob = data_blob[rel_off:rel_off + length]
                    payload_length = ROW_LENGTH_STRUCT.unpack(row_blob[:ROW_LENGTH_STRUCT.size])[0]
                    payload = row_blob[ROW_LENGTH_STRUCT.size:]
                    record = decode_row(state.columns, payload, pk_name=state.primary_key, codecs=codecs)
                    if state.primary_key is not None:
                        record[state.primary_key] = pk
                    live[pk] = record
            else:
                # read each row individually to avoid decrypting large contiguous blob
                for pk in disk_pks:
                    abs_off, length = state.pk_index[pk]
                    row_blob = self._read_region(abs_off, length)
                    if len(row_blob) < ROW_LENGTH_STRUCT.size:
                        raise SerializationError("Not enough data to decode row length")
                    payload_length = ROW_LENGTH_STRUCT.unpack(row_blob[:ROW_LENGTH_STRUCT.size])[0]
                    payload = row_blob[ROW_LENGTH_STRUCT.size:]
                    record = decode_row(state.columns, payload, pk_name=state.primary_key, codecs=codecs)
                    if state.primary_key is not None:
                        record[state.primary_key] = pk
                    live[pk] = record
        return sorted(live.items(), key=lambda item: item[0])

    def _passthrough_unchanged_table(self, state: TableState) -> _EncodedTable:
        """未改表直通：从磁盘读取原始字节，跳过 decode+encode。"""
        data_blob = self._read_region(state.data_offset, state.data_size)
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

    def _read_row_at(self, state: TableState, pk: Any, offset: int, length: int) -> dict[str, Any]:
        row_blob = self._read_region(offset, length)
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

    def _resolve_insert_pk(self, state: TableState, data: dict[str, Any]) -> Any:
        if state.primary_key is None:
            pk: Any = state.next_id
            state.next_id += 1
            return pk
        primary_key = state.primary_key
        assert primary_key is not None
        pk = data.get(primary_key)
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

    def _validate_record(self, state: TableState, data: dict[str, Any], pk: Any) -> dict[str, Any]:
        validated: dict[str, Any] = {}
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

    def _primary_key_column(self, state: TableState) -> Column | None:
        if state.primary_key is None:
            return None
        for column in state.columns:
            if column.name == state.primary_key:
                return column
        return None

    def _encode_schema_catalog(self, table_schemas: dict[str, bytes]) -> bytes:
        """Produce a pytuck-compatible JSON schema document."""
        import json

        del table_schemas
        tables = []
        for name, state in self._tables.items():
            cols = []
            for col in state.columns:
                assert col.name is not None
                cols.append(
                    {
                        "name": col.name,
                        "type_name": TypeRegistry.get_type_name(col.col_type),
                        "nullable": bool(col.nullable),
                        "primary_key": bool(col.primary_key),
                        "index": col.index,
                        "comment": col.comment,
                    }
                )
            tables.append(
                {
                    "name": name,
                    "primary_key": state.primary_key,
                    "next_id": state.next_id,
                    "comment": None,
                    "columns": cols,
                }
            )
        catalog = {"tables": tables}
        return json.dumps(catalog, ensure_ascii=False).encode("utf-8")

    def _decode_schema_catalog(
        self, blob: bytes, table_count: int
    ) -> dict[str, tuple[list[Column], str | None]]:
        """Decode either the legacy binary schema catalog or a pytuck JSON schema document.

        Returns mapping table_name -> (columns_list, primary_key_name)
        """
        import json

        # Heuristic: if blob starts with '{' or '[' treat as JSON
        stripped = blob.lstrip()
        if stripped.startswith(b"{") or stripped.startswith(b"["):
            try:
                parsed = json.loads(blob.decode("utf-8"))
            except Exception:
                # fall back to legacy parser
                return self._decode_schema_catalog_legacy(blob, table_count)

            tables = {}
            entries = []
            if isinstance(parsed, dict) and "tables" in parsed:
                entries = parsed["tables"]
            elif isinstance(parsed, list):
                entries = parsed
            else:
                # unknown JSON shape, try legacy
                return self._decode_schema_catalog_legacy(blob, table_count)

            for ent in entries:
                try:
                    name = ent.get("name")
                    pk = ent.get("primary_key")
                    cols = []
                    for c in ent.get("columns", []):
                        col_name = c.get("name")
                        type_name = c.get("type") or c.get("type_name")
                        nullable = bool(c.get("nullable", True))
                        primary = bool(c.get("primary_key", False))
                        index = c.get("index", False)
                        cols.append(
                            Column(
                                TypeRegistry.get_type_by_name(type_name),
                                name=col_name,
                                nullable=nullable,
                                primary_key=primary,
                                index=index,
                                comment=c.get("comment"),
                            )
                        )
                    tables[name] = (cols, pk)
                except Exception:
                    # skip malformed entries
                    continue
            return tables

        # else legacy binary format
        return self._decode_schema_catalog_legacy(blob, table_count)

    def _decode_schema_catalog_legacy(self, blob: bytes, table_count: int) -> dict[str, tuple[list[Column], str | None]]:
        offset = 0
        schemas: dict[str, tuple[list[Column], str | None]] = {}
        for _ in range(table_count):
            table_name, offset = self._unpack_string(blob, offset)
            primary_key_name, offset = self._unpack_optional_string(blob, offset)
            column_count = U16_STRUCT.unpack(blob[offset : offset + U16_STRUCT.size])[0]
            offset += U16_STRUCT.size
            columns: list[Column] = []
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

    def _decode_table_refs(self, blob: bytes, table_count: int) -> list[TableBlockRef]:
        offset = 0
        refs: list[TableBlockRef] = []
        for _ in range(table_count):
            ref, consumed = TableBlockRef.unpack(blob[offset:])
            refs.append(ref)
            offset += consumed
        return refs

    def _read_pk_dir(self, file_obj: Any, ref: TableBlockRef) -> dict[Any, tuple[int, int]]:
        pk_index: dict[Any, tuple[int, int]] = {}
        if ref.pk_dir_size == 0:
            return pk_index
        if file_obj is not None:
            file_obj.seek(ref.pk_dir_offset)
            blob = file_obj.read(ref.pk_dir_size)
            if len(blob) < ref.pk_dir_size:
                raise SerializationError(
                    f"PTK7 region is incomplete at offset {ref.pk_dir_offset} (need {ref.pk_dir_size}, got {len(blob)})"
                )
            blob = self._decrypt_region(ref.pk_dir_offset, blob)
        else:
            blob = self._read_region(ref.pk_dir_offset, ref.pk_dir_size)
        entry_size = PkDirEntry(pk=0, offset=0, length=0).pack_int().__len__()
        if ref.record_count == 0:
            return pk_index
        if len(blob) < entry_size:
            raise SerializationError("PTK7 pk dir is truncated")
        if len(blob) % entry_size != 0:
            raise SerializationError("PTK7 pk dir size is invalid")

        entries = []
        offset = 0
        while offset < len(blob):
            entries.append(PkDirEntry.unpack_int(blob[offset : offset + entry_size]))
            offset += entry_size

        data_end = ref.data_offset + max(1, ref.data_size)
        is_absolute = all(ref.data_offset <= entry.offset < data_end for entry in entries)
        for entry in entries:
            if is_absolute:
                abs_off = entry.offset
            else:
                abs_off = ref.data_offset + entry.offset
            pk_index[entry.pk] = (abs_off, entry.length)
        return pk_index

    def _decode_index_meta(self, blob: bytes, ref: TableBlockRef) -> dict[str, ColumnIndexMeta]:
        import json

        stripped = blob.lstrip()
        if stripped.startswith(b"{") or stripped.startswith(b"["):
            try:
                parsed = json.loads(blob.decode("utf-8"))
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                decoded: dict[str, ColumnIndexMeta] = {}
                for entry in parsed:
                    column_name = entry.get("column")
                    if not column_name:
                        continue
                    rel_offset = int(entry.get("offset", 0))
                    size = int(entry.get("size", 0))
                    decoded[column_name] = ColumnIndexMeta(
                        column_name=column_name,
                        offset=ref.index_data_offset + rel_offset,
                        size=size,
                        entry_count=0,
                        type_code=0,
                    )
                return decoded

        decoded = {}
        offset = 0
        while offset < len(blob):
            cim, consumed = ColumnIndexMeta.unpack(blob[offset:])
            decoded[cim.column_name] = ColumnIndexMeta(
                column_name=cim.column_name,
                offset=ref.index_data_offset + cim.offset,
                size=cim.size,
                entry_count=cim.entry_count,
                type_code=cim.type_code,
            )
            offset += consumed
        return decoded

    def _pack_string(self, value: str) -> bytes:
        encoded = value.encode("utf-8")
        if len(encoded) > 0xFFFF:
            raise SerializationError(f"String too long for PTK7 schema field: {len(encoded)} bytes")
        return U16_STRUCT.pack(len(encoded)) + encoded

    def _pack_optional_string(self, value: str | None) -> bytes:
        if value is None:
            return U16_STRUCT.pack(NONE_NAME_MARKER)
        return self._pack_string(value)

    def _unpack_string(self, blob: bytes, offset: int) -> tuple[str, int]:
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

    def _unpack_optional_string(self, blob: bytes, offset: int) -> tuple[str | None, int]:
        if offset + U16_STRUCT.size > len(blob):
            raise SerializationError("Not enough data to decode optional string length")
        length = U16_STRUCT.unpack(blob[offset : offset + U16_STRUCT.size])[0]
        if length == NONE_NAME_MARKER:
            return None, offset + U16_STRUCT.size
        return self._unpack_string(blob, offset)

    def search_index(self, table_name: str, column_name: str, value: Any) -> list[Any]:
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
            blob = self._read_region(cim.offset, cim.size)
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
