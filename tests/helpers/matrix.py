from typing import List, Dict, Any
import itertools


def lazy_modes() -> List[bool]:
    return [False, True]


def dict_matrix(**dimensions: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """Create a list of dictionaries representing the cartesian product of dimensions.
    Example: dict_matrix(a=[1,2], b=[True,False]) ->
      [{'a':1,'b':True}, {'a':1,'b':False}, {'a':2,'b':True}, {'a':2,'b':False}]
    """
    keys = list(dimensions.keys())
    if not keys:
        return [{}]

    values_lists = [dimensions[k] for k in keys]
    result: List[Dict[str, Any]] = []
    for combo in itertools.product(*values_lists):
        d: Dict[str, Any] = {k: v for k, v in zip(keys, combo)}
        result.append(d)
    return result
