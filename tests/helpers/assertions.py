from typing import Any, Dict, Iterable, Tuple


def assert_scan_matches(rows: Iterable[Tuple[Any, Dict[str, Any]]], expected_pks: Iterable[Any]) -> None:
    actual_pks = [pk for pk, _ in rows]
    assert actual_pks == list(expected_pks)
