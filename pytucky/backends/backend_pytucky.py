from __future__ import annotations

from functools import wraps
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Concatenate, ParamSpec, TypeVar

from ..backends.store import Store, TableState, TableOverlay
from ..core.storage import Table
from ..core.orm import Column
from ..core.index import HashIndex, SortedIndex
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


class HashIndexProxy(HashIndex):
    """延迟物化哈希索引代理。"""

    def __init__(self, column_name: str, store: Store, table_name: str, column_obj: Column):
        super().__init__(column_name)
        self._store = store
        self._table = table_name
        self._column = column_obj
        self._materialized = False
        self._added: dict[Any, set[Any]] = {}
        self._removed: dict[Any, set[Any]] = {}

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
                HashIndex.insert(self, val, pk)

        overlay = state.overlay
        col_name = self.column_name
        for pk, rec in overlay.inserted.items():
            if pk not in overlay.deleted:
                value = rec.get(col_name)
                if value is not None:
                    HashIndex.insert(self, value, pk)
        for pk, rec in overlay.updated.items():
            if pk not in overlay.deleted:
                value = rec.get(col_name)
                if value is not None:
                    HashIndex.insert(self, value, pk)
        for pk in overlay.deleted:
            for pk_set in self.map.values():
                pk_set.discard(pk)

        for value, pks in self._added.items():
            for pk in pks:
                HashIndex.insert(self, value, pk)
        for value, pks in self._removed.items():
            for pk in pks:
                HashIndex.remove(self, value, pk)
        self._added.clear()
        self._removed.clear()
        self._materialized = True

    def insert(self, value: Any, pk: Any) -> None:
        if value is None:
            return
        if self._materialized:
            HashIndex.insert(self, value, pk)
            return
        self._added.setdefault(value, set()).add(pk)
        if value in self._removed:
            self._removed[value].discard(pk)

    def remove(self, value: Any, pk: Any) -> None:
        if value is None:
            return
        if self._materialized:
            HashIndex.remove(self, value, pk)
            return
        if value in self._added:
            self._added[value].discard(pk)
            if not self._added[value]:
                del self._added[value]
        self._removed.setdefault(value, set()).add(pk)

    def lookup(self, value: Any):
        self._materialize()
        return HashIndex.lookup(self, value)


class SortedIndexProxy(SortedIndex):
    """延迟物化有序索引代理。"""

    def __init__(self, column_name: str, store: Store, table_name: str, column_obj: Column):
        super().__init__(column_name)
        self._store = store
        self._table = table_name
        self._column = column_obj
        self._materialized = False
        self._added: dict[Any, set[Any]] = {}
        self._removed: dict[Any, set[Any]] = {}

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

        overlay = state.overlay
        col_name = self.column_name
        for pk, rec in overlay.inserted.items():
            if pk not in overlay.deleted:
                value = rec.get(col_name)
                if value is not None:
                    SortedIndex.insert(self, value, pk)
        for pk, rec in overlay.updated.items():
            if pk not in overlay.deleted:
                value = rec.get(col_name)
                if value is not None:
                    SortedIndex.insert(self, value, pk)
        for pk in overlay.deleted:
            for pk_set in self.value_to_pks.values():
                pk_set.discard(pk)

        for value, pks in self._added.items():
            for pk in pks:
                SortedIndex.insert(self, value, pk)
        for value, pks in self._removed.items():
            for pk in pks:
                SortedIndex.remove(self, value, pk)
        self._added.clear()
        self._removed.clear()
        self._materialized = True

    def insert(self, value: Any, pk: Any) -> None:
        if value is None:
            return
        if self._materialized:
            SortedIndex.insert(self, value, pk)
            return
        self._added.setdefault(value, set()).add(pk)
        if value in self._removed:
            self._removed[value].discard(pk)

    def remove(self, value: Any, pk: Any) -> None:
        if value is None:
            return
        if self._materialized:
            SortedIndex.remove(self, value, pk)
            return
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
        state = self._store.table_state(self._table)
        cim = state.index_meta.get(self.column_name)
        result: set[Any] = set()
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
                value = rec.get(self.column_name)
                if value is None:
                    continue
                if min_val is not None and ((value < min_val) or (not include_min and value == min_val)):
                    continue
                if max_val is not None and ((value > max_val) or (not include_max and value == max_val)):
                    continue
                result.add(pk)

        for value, pks in self._added.items():
            if min_val is not None and ((value < min_val) or (not include_min and value == min_val)):
                continue
            if max_val is not None and ((value > max_val) or (not include_max and value == max_val)):
                continue
            result.update(pks)
        for value, pks in self._removed.items():
            if value in result:
                result.difference_update(pks)
        return result


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
        return self.store.load_tables(
            self,
            self.file_path,
            hash_index_factory=HashIndexProxy,
            sorted_index_factory=SortedIndexProxy,
        )

    def populate_tables_with_data(self, tables: dict[str, Table]) -> None:
        # Ensure all lazy tables materialize via Table._ensure_all_loaded
        for table in tables.values():
            if table._lazy_loaded:
                table._ensure_all_loaded()

    @_backend_locked
    def read_lazy_record(
        self,
        file_path: Path,
        offset: int,
        columns: dict[str, Column],
        pk: Any,
        *,
        table_name: str | None = None,
        copy_result: bool = True,
    ) -> dict[str, Any]:
        try:
            select_record = self.store.select if copy_result else self.store.select_raw
            # 快速路径：调用方已知 table_name，直接 select，跳过 offset_map
            if table_name is not None:
                return select_record(table_name, pk)
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
            return select_record(resolved_table, resolved_pk)
        except Exception as exc:
            raise SerializationError(f'V7 read lazy record failed: {exc}') from exc

    def _rebind_lazy_tables(self, tables: dict[str, Table]) -> None:
        for name, table in tables.items():
            if not getattr(table, '_lazy_loaded', False):
                continue
            state = self.store._tables.get(name)
            if state is None:
                continue
            self.store.rebind_lazy_table(
                table,
                state,
                self,
                self.file_path,
                hash_index_factory=HashIndexProxy,
                sorted_index_factory=SortedIndexProxy,
            )

    @_backend_locked
    def save(self, tables: dict[str, Table], *, changed_tables: set | None = None) -> None:
        # Convert high-level tables into Store internal states and flush via store.flush.
        # Rebuild directly from table scan results for changed tables to avoid per-row Store.insert overhead.
        # For unchanged tables, reuse existing Store.TableState to avoid materializing lazy tables.
        changed_tables = set(changed_tables or set())
        prev_states: dict[str, TableState] = dict(getattr(self.store, '_tables', {}))
        rebuilt_tables: dict[str, TableState] = {}

        for name, table in tables.items():
            if name not in changed_tables and name in prev_states:
                rebuilt_tables[name] = prev_states[name]
                continue

            cols = list(table.columns.values())
            if table._lazy_loaded and name in prev_states and not table._schema_dirty and getattr(table, 'inserted', None) is not None:
                inserted_set = set(getattr(table, 'inserted', set()))
                updated_set = set(getattr(table, 'updated', set()))
                missing_pk = any(pk not in table.data for pk in inserted_set | updated_set)
                if missing_pk:
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
                    previous_state = prev_states[name]
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
                    for pk in inserted_set:
                        inserted_record = table.data.get(pk)
                        if inserted_record is not None:
                            state.overlay.inserted[pk] = dict(inserted_record)
                    for pk in updated_set:
                        updated_record = table.data.get(pk)
                        if updated_record is not None:
                            state.overlay.updated[pk] = dict(updated_record)
                    for pk in getattr(table, 'deleted', set()):
                        state.overlay.deleted.add(pk)
                        if pk in state.pk_index:
                            del state.pk_index[pk]
                    rebuilt_tables[name] = state
                continue

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

        self.store._tables = rebuilt_tables
        self.store.flush()
        self._rebind_lazy_tables(tables)
        self._offset_map = None

    @_backend_locked
    def close(self) -> None:
        self.store.close()
