from tests.helpers.factories import build_user_storage
from tests.helpers.matrix import lazy_modes


import pytest


@pytest.mark.feature
@pytest.mark.parametrize("lazy_load", lazy_modes())
def test_build_user_storage_uses_pytucky_by_default(tmp_path: Path, lazy_load: bool):
    db_path = tmp_path / f"smoke_{int(lazy_load)}.pytucky"

    storage = build_user_storage(db_path, lazy_load=lazy_load)

    assert storage is not None
    assert getattr(storage, "engine_name", "pytucky") == "pytucky"

    storage.close()
