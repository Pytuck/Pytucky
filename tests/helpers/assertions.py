from typing import Iterable, Any, Sequence


def assert_scan_matches(rows: Iterable[dict], expected_pks: Sequence[Any]) -> None:
    # Expect rows to be iterable of dict-like objects with 'id' primary key
    pks = [r.get("id") for r in rows]
    assert list(pks) == list(expected_pks), f"Primary keys do not match: {pks} != {expected_pks}"
