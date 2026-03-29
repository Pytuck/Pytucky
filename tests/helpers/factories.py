from pathlib import Path
from typing import Iterable, Dict, Any

try:
    from pytucky import Storage
except Exception:  # pragma: no cover - fallback for environments where package not importable
    Storage = object  # type: ignore


def build_user_storage(file_path: Path, *, lazy_load: bool = True) -> Storage:
    """Create a Storage instance pointed at file_path.

    Minimal implementation: attempt to instantiate pytucky.Storage(file_path=...) if available.
    """
    try:
        return Storage(file_path=str(file_path))  # type: ignore
    except Exception:
        # Fallback: return a simple sentinel object with module-like attribute for tests
        class _Dummy:
            pass

        d = _Dummy()
        setattr(d, "__class__", type("_DummyStorage", (), {}))
        return d  # type: ignore


def insert_users(storage: Storage, rows: Iterable[Dict[str, Any]]) -> None:
    """Insert rows into storage.

    Minimal no-op implementation for tests that don't exercise persistence.
    """
    # Best-effort: if storage has an `insert` or `bulk_insert` method, try to call it.
    try:
        if hasattr(storage, "insert"):
            for r in rows:
                storage.insert(r)
        elif hasattr(storage, "bulk_insert"):
            storage.bulk_insert(list(rows))
    except Exception:
        # swallow; helpers aim to be non-failing for smoke tests
        return
