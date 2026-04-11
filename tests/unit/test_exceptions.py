from pytucky import (
    PytuckyException,
    PytuckException,
    PytuckyIndexError,
    PytuckIndexError,
)
from pytucky.common import PytuckyException as CommonPytuckyException
from pytucky.common import PytuckyIndexError as CommonPytuckyIndexError


def test_exception_aliases_are_exported_consistently() -> None:
    assert PytuckException is PytuckyException
    assert PytuckIndexError is PytuckyIndexError
    assert CommonPytuckyException is PytuckyException
    assert CommonPytuckyIndexError is PytuckyIndexError

    err = PytuckyException("boom", table_name="users")
    assert err.to_dict() == {
        "error": "PytuckyException",
        "message": "boom",
        "table_name": "users",
    }
