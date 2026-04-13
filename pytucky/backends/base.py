"""
Pytucky 存储后端抽象基类

定义持久化引擎必须实现的接口。
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, Set, Union, TYPE_CHECKING, Tuple

from ..common.options import BackendOptions

if TYPE_CHECKING:
    from ..core.storage import Table


class StorageBackend(ABC):
    """
    存储后端抽象基类

    持久化引擎实现此接口，提供统一的 save/load API。
    """

    ENGINE_NAME: str  # type: ignore

    def __init__(self, file_path: Union[str, Path], options: BackendOptions):
        self.file_path: Path = Path(file_path).expanduser()
        self.options = options

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(file_path='{self.file_path}')"

    @abstractmethod
    def save(self, tables: Dict[str, 'Table'], *, changed_tables: Optional[Set[str]] = None) -> None:
        pass

    @abstractmethod
    def load(self) -> Dict[str, 'Table']:
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

    def populate_tables_with_data(self, tables: Dict[str, 'Table']) -> None:
        pass

    def read_lazy_record(self, file_path: Path, offset: int, columns: Dict[str, Any], pk: Any) -> Dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def probe(cls, file_path: Union[str, Path]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        return False, None
