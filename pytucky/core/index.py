"""
Pytuck 索引实现

提供哈希索引和有序索引支持
"""

from abc import ABC, abstractmethod
from bisect import bisect_left, bisect_right
from typing import Any, Dict, List, Optional, Set


class BaseIndex(ABC):
    """索引基类（抽象接口）"""

    def __init__(self, column_name: str):
        """
        初始化索引

        Args:
            column_name: 索引的列名
        """
        self.column_name = column_name

    @abstractmethod
    def insert(self, value: Any, pk: Any) -> None:
        """
        插入索引条目

        Args:
            value: 字段值
            pk: 主键值
        """
        ...

    @abstractmethod
    def remove(self, value: Any, pk: Any) -> None:
        """
        删除索引条目

        Args:
            value: 字段值
            pk: 主键值
        """
        ...

    @abstractmethod
    def lookup(self, value: Any) -> Set[Any]:
        """
        精确查找

        Args:
            value: 字段值

        Returns:
            匹配的主键集合
        """
        ...

    @abstractmethod
    def clear(self) -> None:
        """清空索引"""
        ...

    @abstractmethod
    def __len__(self) -> int:
        """返回索引条目总数"""
        ...

    def supports_range_query(self) -> bool:
        """是否支持范围查询"""
        return False

    def range_query(
        self,
        min_val: Optional[Any] = None,
        max_val: Optional[Any] = None,
        include_min: bool = True,
        include_max: bool = True
    ) -> Set[Any]:
        """
        范围查询（默认不支持）

        Args:
            min_val: 最小值（None 表示无下界）
            max_val: 最大值（None 表示无上界）
            include_min: 是否包含最小值
            include_max: 是否包含最大值

        Returns:
            匹配的主键集合

        Raises:
            NotImplementedError: 当索引不支持范围查询时
        """
        raise NotImplementedError("This index does not support range queries")


class HashIndex(BaseIndex):
    """
    哈希索引（基于 dict 实现）

    用于等值查询，O(1) 的插入、删除和查找性能。
    """

    def __init__(self, column_name: str):
        """
        初始化哈希索引

        Args:
            column_name: 索引的列名
        """
        super().__init__(column_name)
        self.map: Dict[Any, Set[Any]] = {}

    def insert(self, value: Any, pk: Any) -> None:
        """
        插入索引条目

        Args:
            value: 字段值（None 值不会被索引）
            pk: 主键值
        """
        if value is None:
            return
        pk_set = self.map.get(value)
        if pk_set is None:
            pk_set = set()
            self.map[value] = pk_set
        pk_set.add(pk)

    def remove(self, value: Any, pk: Any) -> None:
        """
        删除索引条目

        Args:
            value: 字段值
            pk: 主键值
        """
        pk_set = self.map.get(value)
        if not pk_set:
            return
        pk_set.discard(pk)
        if not pk_set:
            del self.map[value]

    def lookup(self, value: Any) -> Set[Any]:
        """
        查找索引

        Args:
            value: 字段值

        Returns:
            主键集合的副本
        """
        pk_set = self.map.get(value)
        return set(pk_set) if pk_set else set()

    def clear(self) -> None:
        """清空索引"""
        self.map.clear()

    def __len__(self) -> int:
        """返回索引条目总数"""
        return sum(len(pk_set) for pk_set in self.map.values())

    def __repr__(self) -> str:
        return f"HashIndex(column='{self.column_name}', entries={len(self)}, values={len(self.map)})"


class SortedIndex(BaseIndex):
    """
    有序索引（基于 bisect 实现）

    支持范围查询和排序，适合需要 ORDER BY 或范围过滤的场景。
    插入和删除为 O(n)，查找为 O(log n)，范围查询为 O(log n + k)。
    """

    def __init__(self, column_name: str):
        """
        初始化有序索引

        Args:
            column_name: 索引的列名
        """
        super().__init__(column_name)
        self.sorted_values: List[Any] = []
        self.value_to_pks: Dict[Any, Set[Any]] = {}

    def insert(self, value: Any, pk: Any) -> None:
        """
        插入索引条目

        Args:
            value: 字段值（None 值不会被索引）
            pk: 主键值
        """
        if value is None:
            return
        if value not in self.value_to_pks:
            # 新值，插入排序列表
            idx = bisect_left(self.sorted_values, value)
            self.sorted_values.insert(idx, value)
            self.value_to_pks[value] = {pk}
        else:
            # 值已存在，添加到集合
            self.value_to_pks[value].add(pk)

    def remove(self, value: Any, pk: Any) -> None:
        """
        删除索引条目

        Args:
            value: 字段值
            pk: 主键值
        """
        if value not in self.value_to_pks:
            return
        self.value_to_pks[value].discard(pk)
        if not self.value_to_pks[value]:
            # 该值的所有 PK 都删除了，从排序列表移除
            del self.value_to_pks[value]
            idx = bisect_left(self.sorted_values, value)
            if idx < len(self.sorted_values) and self.sorted_values[idx] == value:
                self.sorted_values.pop(idx)

    def lookup(self, value: Any) -> Set[Any]:
        """
        精确查找

        Args:
            value: 字段值

        Returns:
            主键集合的副本
        """
        return set(self.value_to_pks.get(value, set()))

    def clear(self) -> None:
        """清空索引"""
        self.sorted_values.clear()
        self.value_to_pks.clear()

    def __len__(self) -> int:
        """返回索引条目总数"""
        return sum(len(pk_set) for pk_set in self.value_to_pks.values())

    def supports_range_query(self) -> bool:
        """是否支持范围查询"""
        return True

    def range_query(
        self,
        min_val: Optional[Any] = None,
        max_val: Optional[Any] = None,
        include_min: bool = True,
        include_max: bool = True
    ) -> Set[Any]:
        """
        范围查询

        Args:
            min_val: 最小值（None 表示无下界）
            max_val: 最大值（None 表示无上界）
            include_min: 是否包含最小值
            include_max: 是否包含最大值

        Returns:
            匹配的主键集合
        """
        if min_val is None:
            left = 0
        else:
            if include_min:
                left = bisect_left(self.sorted_values, min_val)
            else:
                left = bisect_right(self.sorted_values, min_val)

        if max_val is None:
            right = len(self.sorted_values)
        else:
            if include_max:
                right = bisect_right(self.sorted_values, max_val)
            else:
                right = bisect_left(self.sorted_values, max_val)

        result: Set[Any] = set()
        for value in self.sorted_values[left:right]:
            result.update(self.value_to_pks[value])
        return result

    def get_sorted_pks(self, reverse: bool = False) -> List[Any]:
        """
        获取按值排序的所有主键（用于 ORDER BY）

        Args:
            reverse: 是否降序

        Returns:
            排序后的主键列表
        """
        result: List[Any] = []
        values = reversed(self.sorted_values) if reverse else self.sorted_values
        for value in values:
            result.extend(self.value_to_pks[value])
        return result

    def __repr__(self) -> str:
        return f"SortedIndex(column='{self.column_name}', entries={len(self)}, values={len(self.value_to_pks)})"
