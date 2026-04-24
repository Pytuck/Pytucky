from __future__ import annotations

from functools import wraps
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Concatenate, ParamSpec, TypeVar

from ..backends.store import Store, TableState, TableOverlay
from ..core.storage import Table
from ..core.orm import Column
from ..core.index import BaseIndex
from ..common.options import PytuckBackendOptions
from ..common.exceptions import SerializationError
from .base import StorageBackend
from .format import FileHeader, HEADER_STRUCT

P = ParamSpec("P")
R = TypeVar("R")


def _backend_locked(
    method: Callable[Concatenate["PytuckyBackend", P], R],
) -> Callable[Concatenate["PytuckyBackend", P], R]:
    @wraps(method)
    def wrapper(self: "PytuckyBackend", *args: P.args, **kwargs: P.kwargs) -> R:
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper

class PytuckyBackend(StorageBackend):
    """Adapter backend that exposes Store to the high-level Storage API.

    Minimal required methods implemented:
    - exists, load, save, delete, supports_lazy_loading, populate_tables_with_data, read_lazy_record
    """

    ENGINE_NAME = 'pytucky'

    def __init__(self, file_path: Path, options: PytuckBackendOptions | None = None):
        super().__init__(file_path, options or PytuckBackendOptions())
        self._lock = RLock()
        # ensure suffix: only when no suffix provided, default to .pytuck
        if self.file_path.suffix == '':
            self.file_path = self.file_path.with_suffix('.pytuck')
        # initialize store; offset map built lazily on demand
        # pass backend options through to Store so encryption/password etc. are honored
        self.store = Store(self.file_path, self.options)
        self._offset_map: dict[int, tuple[str, Any]] | None = None

    @_backend_locked
    def _rebuild_offset_map(self) -> None:
        """Rebuild internal offset lookup from the current Store state (lazy, on demand only).

        Maps each data offset to (table_name, pk) for O(1) table/record resolution by file offset.
        """
        omap: dict[int, tuple[str, Any]] = {}
        for tname, state in self.store._tables.items():
            for pk, (off, length) in state.pk_index.items():
                omap[off] = (tname, pk)
        self._offset_map = omap

    @classmethod
    def is_available(cls) -> bool:
        return True

    def exists(self) -> bool:
        return self.file_path.exists()

    @_backend_locked
    def delete(self) -> None:
        self.store.close()
        if self.file_path.exists():
            self.file_path.unlink()

    def supports_lazy_loading(self) -> bool:
        # V7 supports lazy semantics via pk_index and file offsets
        return True

    @staticmethod
    def probe(file_path: str | Path) -> tuple[bool, dict[str, Any] | None]:
        """Probe whether given file_path is a PTK7 store.

        Returns (True, info) on match, (False, None) otherwise.
        This probe only reads the file header and does not attempt to fully open
        the Store, so it works for encrypted files without providing passwords.
        """
        try:
            path = Path(file_path).expanduser()
            if not path.exists() or not path.is_file() or path.stat().st_size < HEADER_STRUCT.size:
                return False, None
            with path.open('rb') as handle:
                hdr_bytes = handle.read(HEADER_STRUCT.size)
                if len(hdr_bytes) < HEADER_STRUCT.size:
                    return False, None
                try:
                    FileHeader.unpack(hdr_bytes)
                    return True, {"engine": PytuckyBackend.ENGINE_NAME, "version": 7}
                except Exception:
                    return False, None
        except Exception:
            return False, None

    @_backend_locked
    def load(self) -> dict[str, Table]:
        # Map Store table states to core.Table objects without materializing rows
        tables: dict[str, Table] = {}
        for name, state in self.store._tables.items():
            # build Column list expected by Table
            cols = [col for col in state.columns]
            # state.columns is list[Column]
            table = Table(state.name, state.columns, state.primary_key, None)
            table.next_id = state.next_id
            table._backend = self
            table._data_file = self.file_path
            table._lazy_loaded = True
            # pk_index in Store maps pk -> (offset,length)
            table._pk_offsets = {pk: off for pk, (off, length) in state.pk_index.items()}

            # restore indexes lazily: create proxy index objects that defer decoding to lookup/range operations
            # proxies keep local overlay for in-memory insert/remove and call Store helpers on demand
            from ..core.index import HashIndex, SortedIndex

            class HashIndexProxy(HashIndex):
                """延迟物化哈希索引代理。

                首次 lookup 时从磁盘解码索引 blob 并填充父类 HashIndex 的
                内存结构，后续 lookup 直接走 O(1) 的 dict 查找。
                物化前通过 _added/_removed overlay 暂存内存变更，
                物化时合并进父类数据结构。
                """

                def __init__(self, column_name: str, store: 'Store', table_name: str, column_obj: Column):
                    super().__init__(column_name)
                    self._store = store
                    self._table = table_name
                    self._column = column_obj
                    self._materialized = False
                    self._added: dict[Any, set] = {}
                    self._removed: dict[Any, set] = {}

                def _materialize(self) -> None:
                    if self._materialized:
                        return
                    # 从磁盘读取索引 blob 并解码
                    state = self._store.table_state(self._table)
                    cim = state.index_meta.get(self.column_name)
                    if cim is not None:
                        blob = self._store._read_region(cim.offset, cim.size)
                        from . import index as idx_mod
                        pairs = idx_mod.decode_sorted_pairs(blob, self._column)
                        for val, pk in pairs:
                            HashIndex.insert(self, val, pk)
                    # 合并 Store overlay（inserted/updated/deleted）
                    overlay = state.overlay
                    col_name = self.column_name
                    for pk, rec in overlay.inserted.items():
                        if pk not in overlay.deleted:
                            v = rec.get(col_name)
                            if v is not None:
                                HashIndex.insert(self, v, pk)
                    for pk, rec in overlay.updated.items():
                        if pk not in overlay.deleted:
                            v = rec.get(col_name)
                            if v is not None:
                                HashIndex.insert(self, v, pk)
                    for pk in overlay.deleted:
                        # 需要知道被删除记录的旧值才能从索引中移除
                        # 但 overlay 不保存旧值，已在磁盘索引中被包含
                        # 用暴力方式：遍历 map 移除该 pk
                        for pk_set in self.map.values():
                            pk_set.discard(pk)
                    # 合并 proxy 自身的 overlay
                    for val, pks in self._added.items():
                        for pk in pks:
                            HashIndex.insert(self, val, pk)
                    for val, pks in self._removed.items():
                        for pk in pks:
                            HashIndex.remove(self, val, pk)
                    self._added.clear()
                    self._removed.clear()
                    self._materialized = True

                def insert(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    if self._materialized:
                        HashIndex.insert(self, value, pk)
                    else:
                        self._added.setdefault(value, set()).add(pk)
                        if value in self._removed:
                            self._removed[value].discard(pk)

                def remove(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    if self._materialized:
                        HashIndex.remove(self, value, pk)
                    else:
                        if value in self._added:
                            self._added[value].discard(pk)
                            if not self._added[value]:
                                del self._added[value]
                        self._removed.setdefault(value, set()).add(pk)

                def lookup(self, value: Any):
                    self._materialize()
                    return HashIndex.lookup(self, value)

            class SortedIndexProxy(SortedIndex):
                """延迟物化有序索引代理。

                首次 lookup/range_query 时从磁盘解码索引并填充父类
                SortedIndex 的内存结构，后续操作走 O(log n) 的二分查找。
                """

                def __init__(self, column_name: str, store: 'Store', table_name: str, column_obj: Column):
                    super().__init__(column_name)
                    self._store = store
                    self._table = table_name
                    self._column = column_obj
                    self._materialized = False
                    self._added: dict[Any, set] = {}
                    self._removed: dict[Any, set] = {}

                def _materialize(self) -> None:
                    if self._materialized:
                        return
                    state = self._store.table_state(self._table)
                    cim = state.index_meta.get(self.column_name)
                    if cim is not None:
                        blob = self._store._read_region(cim.offset, cim.size)
                        from . import index as idx_mod
                        pairs = idx_mod.decode_sorted_pairs(blob, self._column)
                        for val, pk in pairs:
                            SortedIndex.insert(self, val, pk)
                    # 合并 Store overlay
                    overlay = state.overlay
                    col_name = self.column_name
                    for pk, rec in overlay.inserted.items():
                        if pk not in overlay.deleted:
                            v = rec.get(col_name)
                            if v is not None:
                                SortedIndex.insert(self, v, pk)
                    for pk, rec in overlay.updated.items():
                        if pk not in overlay.deleted:
                            v = rec.get(col_name)
                            if v is not None:
                                SortedIndex.insert(self, v, pk)
                    for pk in overlay.deleted:
                        for pk_set in self.value_to_pks.values():
                            pk_set.discard(pk)
                    # 合并 proxy overlay
                    for val, pks in self._added.items():
                        for pk in pks:
                            SortedIndex.insert(self, val, pk)
                    for val, pks in self._removed.items():
                        for pk in pks:
                            SortedIndex.remove(self, val, pk)
                    self._added.clear()
                    self._removed.clear()
                    self._materialized = True

                def insert(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    if self._materialized:
                        SortedIndex.insert(self, value, pk)
                    else:
                        self._added.setdefault(value, set()).add(pk)
                        if value in self._removed:
                            self._removed[value].discard(pk)

                def remove(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    if self._materialized:
                        SortedIndex.remove(self, value, pk)
                    else:
                        if value in self._added:
                            self._added[value].discard(pk)
                            if not self._added[value]:
                                del self._added[value]
                        self._removed.setdefault(value, set()).add(pk)

                def lookup(self, value: Any):
                    self._materialize()
                    return SortedIndex.lookup(self, value)

                def supports_range_query(self) -> bool:
                    return True

                def range_query(self, min_val=None, max_val=None, include_min=True, include_max=True):
                    if self._materialized:
                        return SortedIndex.range_query(self, min_val, max_val, include_min, include_max)
                    # 未物化时保持原有 blob-based 快速路径
                    state = self._store.table_state(self._table)
                    cim = state.index_meta.get(self.column_name)
                    result = set()
                    if cim is not None:
                        blob = self._store._read_region(cim.offset, cim.size)
                        from ..backends import index
                        pks = index.range_search_sorted_pairs(blob, self._column, min_val, max_val, include_min, include_max)
                        result.update(pks)
                    else:
                        for pk in list(state.pk_index.keys()):
                            try:
                                rec = self._store.select(self._table, pk)
                            except Exception:
                                continue
                            val = rec.get(self.column_name)
                            if val is None:
                                continue
                            if min_val is not None:
                                if (val < min_val) or (not include_min and val == min_val):
                                    continue
                            if max_val is not None:
                                if (val > max_val) or (not include_max and val == max_val):
                                    continue
                            result.add(pk)
                    # 合并 proxy overlays
                    for val, pks in self._added.items():
                        if min_val is not None and ((val < min_val) or (not include_min and val == min_val)):
                            continue
                        if max_val is not None and ((val > max_val) or (not include_max and val == max_val)):
                            continue
                        result.update(pks)
                    for val, pks in self._removed.items():
                        if val in result:
                            result.difference_update(pks)
                    return result

            for col_name, cim in state.index_meta.items():
                # find column object
                col_obj = None
                for c in state.columns:
                    if c.name == col_name:
                        col_obj = c
                        break
                if col_obj is None:
                    continue
                index_obj: BaseIndex
                if col_obj.index == 'sorted':
                    index_obj = SortedIndexProxy(col_name, self.store, state.name, col_obj)
                else:
                    index_obj = HashIndexProxy(col_name, self.store, state.name, col_obj)
                table.indexes[col_name] = index_obj

            # leave table.data empty to preserve lazy behavior
            table.reset_dirty()
            tables[name] = table
        return tables

    def populate_tables_with_data(self, tables: dict[str, Table]) -> None:
        # Ensure all lazy tables materialize via Table._ensure_all_loaded
        for table in tables.values():
            if table._lazy_loaded:
                table._ensure_all_loaded()

    @_backend_locked
    def read_lazy_record(self, file_path: Path, offset: int, columns: dict[str, Column], pk: Any, *, table_name: str | None = None) -> dict[str, Any]:
        try:
            # 快速路径：调用方已知 table_name，直接 select，跳过 offset_map
            if table_name is not None:
                return self.store.select(table_name, pk)
            # 慢路径：延迟构建 offset_map 后查找
            if self._offset_map is None:
                self._rebuild_offset_map()
            assert self._offset_map is not None
            entry = self._offset_map.get(offset)
            if entry is None:
                for tname, state in self.store._tables.items():
                    for candidate_pk, (candidate_offset, _length) in state.pk_index.items():
                        if candidate_offset == offset:
                            entry = (tname, candidate_pk)
                            self._offset_map[offset] = entry
                            break
                    if entry is not None:
                        break
            if entry is None:
                raise KeyError(f'Offset {offset} not known')
            resolved_table, resolved_pk = entry
            return self.store.select(resolved_table, resolved_pk)
        except Exception as exc:
            raise SerializationError(f'V7 read lazy record failed: {exc}') from exc

    @_backend_locked
    def save(self, tables: dict[str, Table], *, changed_tables: set | None = None) -> None:
        # Convert high-level tables into Store internal states and flush via store.flush.
        # Rebuild directly from table scan results for changed tables to avoid per-row Store.insert overhead.
        # For unchanged tables, reuse existing Store.TableState to avoid materializing lazy tables.
        changed_tables = set(changed_tables or set())
        # capture previous on-disk states to allow reusing unchanged tables without materializing
        prev_states: dict[str, TableState] = {k: v for k, v in getattr(self.store, '_tables', {}).items()}
        # close current store to prepare writing new file
        # preserve loaded encryption level so that reopening with only a password
        # can keep writing back the same encryption level
        prev_loaded_encryption = getattr(self.store, '_loaded_encryption_level', None)
        self.store.close()
        # create a fresh store object representing the new on-disk layout
        # ensure backend options are passed through when recreating Store
        self.store = Store(self.file_path, self.options, open_existing=False)
        # restore loaded encryption level into the fresh Store instance so flush() can
        # pick it up when backend_options.encryption is not explicitly set
        # Only restore previous loaded encryption level when caller did not explicitly
        # set an encryption level in backend options. If backend options specify
        # encryption (including None explicitly), prefer that value and do not
        # overwrite the newly created Store's state.
        if prev_loaded_encryption is not None and getattr(self.options, 'encryption', None) is None:
            self.store._loaded_encryption_level = prev_loaded_encryption
        rebuilt_tables: dict[str, TableState] = {}

        for name, table in tables.items():
            if name not in changed_tables and name in prev_states:
                # build a NEW TableState copying scalar data from previous state but with a fresh overlay
                prev = prev_states[name]
                rebuilt = TableState(
                    name=prev.name,
                    columns=list(prev.columns),
                    primary_key=prev.primary_key,
                    next_id=table.next_id,
                    record_count=prev.record_count,
                    data_offset=prev.data_offset,
                    data_size=prev.data_size,
                    pk_index=dict(prev.pk_index),
                    index_meta=dict(prev.index_meta),
                    overlay=TableOverlay(),
                )
                rebuilt_tables[name] = rebuilt
                continue

            # build fresh state from high-level table (changed or absent prev state)
            cols = list(table.columns.values())
            # Fast-path for changed lazy tables: if table is lazy-loaded and schema unchanged and
            # we have a previous on-disk state, attempt to construct overlay from explicit dirty PK sets.
            # However, for robustness we require that all inserted/updated dirty PKs have corresponding
            # in-memory records present in table.data. If any are missing, fall back to the safe full scan
            # path (do not attempt to recover via table.get or similar, since updated items may need old on-disk
            # values).
            if table._lazy_loaded and name in prev_states and not table._schema_dirty and getattr(table, 'inserted', None) is not None:
                # quick pre-check: ensure inserted/updated PKs are present in table.data
                inserted_set = set(getattr(table, 'inserted', set()))
                updated_set = set(getattr(table, 'updated', set()))
                missing_pk = False
                for pk in inserted_set | updated_set:
                    if pk not in table.data:
                        missing_pk = True
                        break
                if missing_pk:
                    # fallback to full scan path
                    state = TableState(
                        name=name,
                        columns=cols,
                        primary_key=table.primary_key,
                        next_id=table.next_id,
                        record_count=0,
                        data_offset=0,
                        data_size=0,
                    )
                    for pk, rec in table.scan():
                        state.overlay.inserted[pk] = dict(rec)
                    rebuilt_tables[name] = state
                else:
                    previous_state = prev_states.get(name)
                    assert previous_state is not None
                    state = TableState(
                        name=previous_state.name,
                        columns=list(previous_state.columns),
                        primary_key=previous_state.primary_key,
                        next_id=table.next_id,
                        record_count=previous_state.record_count,
                        data_offset=previous_state.data_offset,
                        data_size=previous_state.data_size,
                        pk_index=dict(previous_state.pk_index),
                        index_meta=dict(previous_state.index_meta),
                        overlay=TableOverlay(),
                    )
                    # Apply explicit dirty PK sets to overlay without materializing entire table
                    # Inserted: must exist in table.data
                    for pk in inserted_set:
                        inserted_record = table.data.get(pk)
                        if inserted_record is not None:
                            state.overlay.inserted[pk] = dict(inserted_record)
                    # Updated: records that exist on disk and were updated in memory
                    for pk in updated_set:
                        updated_record = table.data.get(pk)
                        if updated_record is not None:
                            state.overlay.updated[pk] = dict(updated_record)
                    # Deleted: mark for deletion in overlay
                    for pk in getattr(table, 'deleted', set()):
                        state.overlay.deleted.add(pk)
                        # ensure removed from pk_index to avoid writing old record
                        if pk in state.pk_index:
                            del state.pk_index[pk]
                    rebuilt_tables[name] = state
            else:
                state = TableState(
                    name=name,
                    columns=cols,
                    primary_key=table.primary_key,
                    next_id=table.next_id,
                    record_count=0,
                    data_offset=0,
                    data_size=0,
                )
                for pk, rec in table.scan():
                    state.overlay.inserted[pk] = dict(rec)
                rebuilt_tables[name] = state

        # assign rebuilt tables and flush
        self.store._tables = rebuilt_tables
        self.store.flush()
        # invalidate offset map; will be rebuilt lazily if needed
        self._offset_map = None

    @_backend_locked
    def close(self) -> None:
        self.store.close()
