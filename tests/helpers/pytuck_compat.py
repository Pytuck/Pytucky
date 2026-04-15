from pathlib import Path
import sys
import importlib.util
import json
from typing import Dict, Any


def _locate_repo_root(start: Path) -> Path:
    cur = start.resolve()
    while cur != cur.parent:
        if (cur / 'pyproject.toml').exists():
            return cur
        cur = cur.parent
    raise AssertionError("Cannot locate pytucky repo root (pyproject.toml not found in ancestors)")


def load_pytuck_symbols(repo_root: Path | None = None) -> Dict[str, Any]:
    """Load symbols from a sibling pytuck repository.

    Returns a dict with keys: Storage, Column, BinaryBackendOptions, and local pytucky Storage/Column/Options as well.
    Raises AssertionError with a clear message when sibling repo is missing or import fails.
    """
    start = Path(__file__) if repo_root is None else Path(repo_root)
    repo = _locate_repo_root(start)

    # prefer sibling at repo.parent / 'pytuck', but also accept other locations in ancestors
    sibling = None
    cur = Path(__file__).resolve()
    for ancestor in [cur] + list(cur.parents):
        cand = ancestor.parent / 'pytuck'
        if cand.exists():
            sibling = cand
            break
    if sibling is None:
        raise AssertionError("Cannot locate sibling pytuck repository by searching parent directories.")

    # try to import by manipulating sys.path temporarily
    sys.path.insert(0, str(sibling))
    try:
        spec = importlib.util.find_spec('pytuck')
        if spec is None:
            raise AssertionError(f"Found {sibling} but 'pytuck' package not importable")
        import pytuck as real_pytuck  # type: ignore
    except Exception as e:
        raise AssertionError(f"Failed to import real pytuck from {sibling!s}: {e}")
    finally:
        try:
            # remove the inserted path to avoid polluting other imports
            sys.path.pop(0)
        except Exception:
            pass

    # validate expected exports
    try:
        PytuckStorage = real_pytuck.Storage
        PytuckColumn = real_pytuck.Column
        from pytuck.common.options import BinaryBackendOptions as PytuckBinaryBackendOptions  # type: ignore
    except Exception as e:
        raise AssertionError(f"Imported pytuck but expected attributes missing: {e}")

    # also load local pytucky symbols lazily
    try:
        import pytucky as local_pytucky  # type: ignore
        PytuckyStorage = local_pytucky.Storage
        PytuckyColumn = local_pytucky.Column
        from pytucky.common.options import BinaryBackendOptions as PytuckyBinaryBackendOptions  # type: ignore
    except Exception as e:
        raise AssertionError(f"Failed to import local pytucky symbols: {e}")

    return {
        'Storage': PytuckStorage,
        'Column': PytuckColumn,
        'BinaryBackendOptions': PytuckBinaryBackendOptions,
        'PytuckyStorage': PytuckyStorage,
        'PytuckyColumn': PytuckyColumn,
        'PytuckyBinaryBackendOptions': PytuckyBinaryBackendOptions,
        'pytuck_path': sibling,
    }
