from __future__ import annotations

from dataclasses import is_dataclass
from pathlib import Path
import importlib
import importlib.util
import inspect
import sys
from typing import Any

def _locate_repo_root(start: Path) -> Path:
    cur = start.resolve()
    while cur != cur.parent:
        if (cur / 'pyproject.toml').exists():
            return cur
        cur = cur.parent
    raise AssertionError("Cannot locate pytucky repo root (pyproject.toml not found in ancestors)")

def _resolve_backend_options_type(options_module: Any) -> type[Any]:
    candidates = []
    for attr in dir(options_module):
        value = getattr(options_module, attr)
        if not inspect.isclass(value):
            continue
        if not attr.endswith('Options') or 'Backend' not in attr:
            continue
        if not is_dataclass(value):
            continue
        field_names = set(getattr(value, '__dataclass_fields__', {}).keys())
        if {'encryption', 'password'}.issubset(field_names):
            candidates.append(value)

    if len(candidates) == 1:
        return candidates[0]

    preferred = [candidate for candidate in candidates if candidate.__name__.startswith('Pytuck')]
    if len(preferred) == 1:
        return preferred[0]

    if not candidates:
        raise AttributeError('Cannot resolve backend options type from pytuck.common.options')

    raise AttributeError(
        'Ambiguous backend options types in pytuck.common.options: '
        + ', '.join(candidate.__name__ for candidate in candidates)
    )

def load_pytuck_symbols(repo_root: Path | None = None) -> dict[str, Any]:
    """Load symbols from a sibling pytuck repository.

    Returns a dict with keys: Storage, Column, PytuckBackendOptions, and local pytucky Storage/Column/Options as well.
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
        options_module = importlib.import_module('pytuck.common.options')
        PytuckBackendOptions = _resolve_backend_options_type(options_module)
    except Exception as e:
        raise AssertionError(f"Imported pytuck but expected attributes missing: {e}")

    # also load local pytucky symbols lazily
    try:
        import pytucky as local_pytucky  # type: ignore
        PytuckyStorage = local_pytucky.Storage
        PytuckyColumn = local_pytucky.Column
        from pytucky.common.options import PytuckBackendOptions as LocalPytuckBackendOptions  # type: ignore
    except Exception as e:
        raise AssertionError(f"Failed to import local pytucky symbols: {e}")

    return {
        'Storage': PytuckStorage,
        'Column': PytuckColumn,
        'PytuckBackendOptions': PytuckBackendOptions,
        'PytuckyStorage': PytuckyStorage,
        'PytuckyColumn': PytuckyColumn,
        'PytuckyBackendOptions': LocalPytuckBackendOptions,
        'pytuck_path': sibling,
    }
