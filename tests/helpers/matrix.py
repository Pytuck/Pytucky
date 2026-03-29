from typing import List, Dict, Any


def lazy_modes() -> List[bool]:
    return [True, False]


def dict_matrix(**dimensions: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """Create a list of dictionaries representing the cartesian product of dimensions.
    Example: dict_matrix(a=[1,2], b=[True,False]) ->
      [{'a':1,'b':True}, {'a':1,'b':False}, {'a':2,'b':True}, {'a':2,'b':False}]
    """
    keys = list(dimensions.keys())
    if not keys:
        return [{}]

    result: List[Dict[str, Any]] = [{}]
    for k in keys:
        vals = dimensions[k]
        new: List[Dict[str, Any]] = []
        for r in result:
            for v in vals:
                nr = dict(r)
                nr[k] = v
                new.append(nr)
        result = new
    return result
