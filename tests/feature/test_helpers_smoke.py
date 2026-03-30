from pathlib import Path

import pytest

from tests.helpers.factories import build_user_storage
from tests.helpers.matrix import lazy_modes


@pytest.mark.feature
@pytest.mark.parametrize("lazy_load", lazy_modes())
def test_build_user_storage_defaults_to_pytucky(tmp_path: Path, lazy_load: bool) -> None:
    db_path = tmp_path / f"helpers-{lazy_load}.pytucky"
    storage = build_user_storage(db_path, lazy_load=lazy_load)
    assert storage.engine_name == "pytucky"
    storage.close()
