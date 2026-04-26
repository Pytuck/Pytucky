"""
Pytucky 存储后端抽象基类

定义持久化引擎必须实现的接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, TYPE_CHECKING

from ..common.options import PytuckBackendOptions

if TYPE_CHECKING:
    from ..core.storage import Table

class StorageBackend(ABC):
    """
    存储后端抽象基类

    持久化引擎实现此接口，提供统一的 save/load API。
    """

    ENGINE_NAME: str  # type: ignore

    def __init__(self, file_path: str | Path, options: PytuckBackendOptions):
        self.file_path: Path = Path(file_path).expanduser()
        self.options = options

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file_path='{self.file_path}')"

    @abstractmethod
    def save(self, tables: dict[str, 'Table'], *, changed_tables: set[str] | None = None) -> None:
        pass

    @abstractmethod
    def load(self) -> dict[str, 'Table']:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass

    @classmethod
    def is_available(cls) -> bool:
        return True

    def supports_lazy_loading(self) -> bool:
        return False

    def populate_tables_with_data(self, tables: dict[str, 'Table']) -> None:
        pass

    def read_lazy_record(
        self,
        file_path: Path,
        offset: int,
        columns: dict[str, Any],
        pk: Any,
        *,
        table_name: str | None = None,
        copy_result: bool = True,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def supports_server_side_pagination(self) -> bool:
        return False

    def query_with_pagination(
        self,
        *,
        table_name: str,
        conditions: list[dict[str, Any]],
        limit: int | None,
        offset: int,
        order_by: str | None,
        order_desc: bool,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def probe(cls, file_path: str | Path) -> tuple[bool, dict[str, Any] | None]:
        return False, None
