from __future__ import annotations

from dataclasses import is_dataclass
from pathlib import Path
import importlib
import importlib.util
import inspect
import os
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

def _candidate_pytuck_paths(repo: Path) -> list[Path]:
    candidates: list[Path] = []
    env_path = os.environ.get('PYTUCK_REPO_PATH')
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(repo.parent / 'pytuck')

    cur = Path(__file__).resolve()
    for ancestor in [cur] + list(cur.parents):
        candidates.append(ancestor.parent / 'pytuck')

    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(candidate)
    return unique_candidates

def load_pytuck_symbols(repo_root: Path | None = None) -> dict[str, Any]:
    """Load symbols from installed pytuck or a local pytuck checkout.

    Returns a dict with keys: Storage, Column, PytuckBackendOptions, and local pytucky Storage/Column/Options as well.
    Raises AssertionError with a clear message when pytuck is unavailable or import fails.
    """
    start = Path(__file__) if repo_root is None else Path(repo_root)
    repo = _locate_repo_root(start)

    sibling = None
    inserted_path = False
    spec = importlib.util.find_spec('pytuck')
    if spec is None:
        for candidate in _candidate_pytuck_paths(repo):
            if not candidate.exists():
                continue
            sibling = candidate
            sys.path.insert(0, str(candidate))
            inserted_path = True
            spec = importlib.util.find_spec('pytuck')
            if spec is not None:
                break
            sys.path.pop(0)
            inserted_path = False

    if spec is None:
        raise AssertionError(
            "Cannot import 'pytuck'. Install development dependencies with "
            "`uv sync --extra dev`, or set `PYTUCK_REPO_PATH`, or check out "
            "https://github.com/Pytuck/Pytuck beside this repository."
        )

    try:
        real_pytuck = importlib.import_module('pytuck')
    except Exception as e:
        source = str(sibling) if sibling is not None else "installed environment"
        raise AssertionError(f"Failed to import real pytuck from {source}: {e}")
    finally:
        if inserted_path:
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
