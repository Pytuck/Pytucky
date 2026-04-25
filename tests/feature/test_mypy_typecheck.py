from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.feature
def test_package_passes_mypy(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    cache_dir = tmp_path / ".mypy_cache"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file",
            str(repo_root / "pyproject.toml"),
            "--cache-dir",
            str(cache_dir),
            "pytucky",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        "mypy 类型检查失败\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
