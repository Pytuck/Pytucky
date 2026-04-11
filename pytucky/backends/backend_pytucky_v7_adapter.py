from pathlib import Path
from typing import Any, Dict, Optional

from ..backends.store_v7 import StoreV7, TableState, TableOverlay
from ..core.storage import Table
from ..core.orm import Column
from ..common.options import BackendOptions, BinaryBackendOptions
from ..common.exceptions import SerializationError
from .base import StorageBackend


class PytuckyV7Backend(StorageBackend):
    """Adapter backend that exposes StoreV7 to the high-level Storage API.

    Minimal required methods implemented:
    - exists, load, save, delete, supports_lazy_loading, populate_tables_with_data, read_lazy_record
    """

    ENGINE_NAME = 'pytucky'

    def __init__(self, file_path: Path, options: Optional[BackendOptions] = None):
        super().__init__(file_path, options or BinaryBackendOptions())
        # ensure suffix
        if self.file_path.suffix.lower() in {'', '.pytuck'}:
            self.file_path = self.file_path.with_suffix('.pytucky')
        # initialize store and build offset lookup for fast lazy reads
        self.store = StoreV7(self.file_path)
        # offset -> (table_name, pk)
        self._offset_map: Dict[int, tuple[str, Any]] = {}
        self._rebuild_offset_map()

    def _rebuild_offset_map(self) -> None:
        """Rebuild internal offset lookup from the current StoreV7 state.

        Maps each data offset to (table_name, pk) for O(1) table/record resolution by file offset.
        """
        self._offset_map.clear()
        for tname, state in self.store._tables.items():
            for pk, (off, length) in state.pk_index.items():
                # prefer first-seen mapping; pk uniqueness assumed per table
                self._offset_map[off] = (tname, pk)

    @classmethod
    def is_available(cls) -> bool:
        return True

    def exists(self) -> bool:
        return self.file_path.exists()

    def delete(self) -> None:
        self.store.close()
        if self.file_path.exists():
            self.file_path.unlink()

    def supports_lazy_loading(self) -> bool:
        # V7 supports lazy semantics via pk_index and file offsets
        return True

    @staticmethod
    def probe(file_path: Path):
        """Probe whether given file_path is a PTK7 store.

        Returns (True, info) on match, (False, None) otherwise.
        """
        try:
            path = Path(file_path).expanduser()
            if not path.exists() or not path.is_file() or path.stat().st_size < 64:
                return False, None
            # Do not raise on init; StoreV7 will attempt to open and validate header
            StoreV7(path)
            return True, {"engine": PytuckyV7Backend.ENGINE_NAME, "version": 7}
        except Exception:
            return False, None

    def load(self) -> Dict[str, Table]:
        # Map StoreV7 table states to core.Table objects without materializing rows
        tables: Dict[str, Table] = {}
        for name, state in self.store._tables.items():
            # build Column list expected by Table
            cols = [col for col in state.columns]
            # state.columns is List[Column]
            table = Table(state.name, state.columns, state.primary_key, None)
            table.next_id = state.next_id
            table._backend = self
            table._data_file = self.file_path
            table._lazy_loaded = True
            # pk_index in StoreV7 maps pk -> (offset,length)
            table._pk_offsets = {pk: off for pk, (off, length) in state.pk_index.items()}

            # restore indexes lazily: create proxy index objects that defer decoding to lookup/range operations
            # proxies keep local overlay for in-memory insert/remove and call StoreV7 helpers on demand
            from ..core.index import HashIndex, SortedIndex

            class HashIndexProxy(HashIndex):
                def __init__(self, column_name: str, store: 'StoreV7', table_name: str, column_obj: Column):
                    super().__init__(column_name)
                    self._store = store
                    self._table = table_name
                    self._column = column_obj
                    # overlays for unflushed in-memory changes applied via index.insert/remove
                    self._added: Dict[Any, set] = {}
                    self._removed: Dict[Any, set] = {}

                def insert(self, value: Any, pk: Any) -> None:
                    # maintain local added overlay; do not write to disk
                    if value is None:
                        return
                    self._added.setdefault(value, set()).add(pk)
                    # if previously marked removed for this value, unmark
                    if value in self._removed:
                        self._removed[value].discard(pk)

                def remove(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    # prefer removing from added overlay first
                    if value in self._added:
                        self._added[value].discard(pk)
                        if not self._added[value]:
                            del self._added[value]
                    # mark as removed
                    self._removed.setdefault(value, set()).add(pk)

                def lookup(self, value: Any):
                    # base results from store (on-disk + store overlay)
                    try:
                        base = set(self._store.search_index(self._table, self.column_name, value))
                    except Exception:
                        base = set()
                    # apply additions and removals from proxy overlay
                    base.update(self._added.get(value, set()))
                    for pk in self._removed.get(value, set()):
                        base.discard(pk)
                    return set(base)

            class SortedIndexProxy(SortedIndex):
                def __init__(self, column_name: str, store: 'StoreV7', table_name: str, column_obj: Column):
                    super().__init__(column_name)
                    self._store = store
                    self._table = table_name
                    self._column = column_obj
                    self._added: Dict[Any, set] = {}
                    self._removed: Dict[Any, set] = {}

                def insert(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    self._added.setdefault(value, set()).add(pk)
                    if value in self._removed:
                        self._removed[value].discard(pk)

                def remove(self, value: Any, pk: Any) -> None:
                    if value is None:
                        return
                    if value in self._added:
                        self._added[value].discard(pk)
                        if not self._added[value]:
                            del self._added[value]
                    self._removed.setdefault(value, set()).add(pk)

                def lookup(self, value: Any):
                    try:
                        base = set(self._store.search_index(self._table, self.column_name, value))
                    except Exception:
                        base = set()
                    base.update(self._added.get(value, set()))
                    for pk in self._removed.get(value, set()):
                        base.discard(pk)
                    return set(base)

                def supports_range_query(self) -> bool:
                    return True

                def range_query(self, min_val=None, max_val=None, include_min=True, include_max=True):
                    # If store has index metadata, use index_v7.range_search_sorted_pairs reading the blob on demand
                    state = self._store.table_state(self._table)
                    cim = state.index_meta.get(self.column_name)
                    result = set()
                    if cim is not None:
                        blob = self._store._read_bytes_at(cim.offset, cim.size)
                        from ..backends import index_v7
                        pks = index_v7.range_search_sorted_pairs(blob, self._column, min_val, max_val, include_min, include_max)
                        result.update(pks)
                    else:
                        # fallback: scan pk list via store.select (may be slower)
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
                    # merge proxy overlays
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
                if col_obj.index == 'sorted':
                    index_obj = SortedIndexProxy(col_name, self.store, state.name, col_obj)
                else:
                    index_obj = HashIndexProxy(col_name, self.store, state.name, col_obj)
                table.indexes[col_name] = index_obj

            # leave table.data empty to preserve lazy behavior
            table.reset_dirty()
            tables[name] = table
        return tables

    def populate_tables_with_data(self, tables: Dict[str, Table]) -> None:
        # Ensure all lazy tables materialize via Table._ensure_all_loaded
        for table in tables.values():
            if table._lazy_loaded:
                table._ensure_all_loaded()

    def read_lazy_record(self, file_path: Path, offset: int, columns: Dict[str, Column], pk: Any) -> Dict[str, Any]:
        # Use pre-built offset map to resolve table name and pk quickly. Do NOT scan all tables.
        try:
            entry = self._offset_map.get(offset)
            if entry is None:
                # fallback: attempt a full scan as a best-effort (should be rare)
                for tname, state in self.store._tables.items():
                    if pk in state.pk_index:
                        entry = (tname, pk)
                        self._offset_map[offset] = entry
                        break
            if entry is None:
                raise KeyError(f'Offset {offset} not known')
            table_name, resolved_pk = entry
            self._offset_map[offset] = (table_name, resolved_pk)
            # prefer the provided pk if consistent
            try:
                rec = self.store.select(table_name, pk)
            except Exception:
                # try with resolved_pk
                rec = self.store.select(table_name, resolved_pk)
            return rec
        except Exception as exc:
            raise SerializationError(f'V7 read lazy record failed: {exc}') from exc

    def save(self, tables: Dict[str, Table], *, changed_tables: Optional[set] = None) -> None:
        # Convert high-level tables into StoreV7 internal states and flush via store.flush.
        # Rebuild directly from table scan results for changed tables to avoid per-row StoreV7.insert overhead.
        # For unchanged tables, reuse existing StoreV7.TableState to avoid materializing lazy tables.
        changed_tables = set(changed_tables or set())
        # capture previous on-disk states to allow reusing unchanged tables without materializing
        prev_states: Dict[str, TableState] = {k: v for k, v in getattr(self.store, '_tables', {}).items()}
        # close current store to prepare writing new file
        self.store.close()
        # create a fresh store object representing the new on-disk layout
        self.store = StoreV7(self.file_path, open_existing=False)
        rebuilt_tables: Dict[str, TableState] = {}

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
                    prev = prev_states.get(name)
                    state = TableState(
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
                    # Apply explicit dirty PK sets to overlay without materializing entire table
                    # Inserted: must exist in table.data
                    for pk in inserted_set:
                        rec = table.data.get(pk)
                        if rec is not None:
                            state.overlay.inserted[pk] = dict(rec)
                    # Updated: records that exist on disk and were updated in memory
                    for pk in updated_set:
                        rec = table.data.get(pk)
                        if rec is not None:
                            state.overlay.updated[pk] = dict(rec)
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
        # rebuild offset lookup so future lazy reads use up-to-date offsets
        self._rebuild_offset_map()

    def close(self) -> None:
        self.store.close()
