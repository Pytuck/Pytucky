from typing import Any, Iterable, List
from itertools import product


def lazy_modes() -> List[bool]:
    return [False, True]


def dict_matrix(**dimensions: Iterable[Any]) -> List[dict]:
    """Create a list of dictionaries representing the cartesian product of dimensions.

    Example: dict_matrix(a=[1,2], b=[True,False]) ->
      [{'a':1,'b':True}, {'a':1,'b':False}, {'a':2,'b':True}, {'a':2,'b':False}]
    """
    keys = list(dimensions.keys())
    if not keys:
        return [{}]

    values_lists = [list(dimensions[k]) for k in keys]
    result: List[dict] = []
    for combo in product(*values_lists):
        d = {k: v for k, v in zip(keys, combo)}
        result.append(d)
    return result
