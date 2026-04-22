from __future__ import annotations

from typing import Any, Iterable

def assert_scan_matches(rows: Iterable[tuple[Any, dict[str, Any]]], expected_pks: Iterable[Any]) -> None:
    actual_pks = [pk for pk, _ in rows]
    assert actual_pks == list(expected_pks)
