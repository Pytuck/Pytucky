"""
Pytuck ORM 事件钩子系统

提供轻量级事件回调机制，支持 Model 级和 Storage 级事件。

Model 级事件：
- before_insert / after_insert
- before_update / after_update
- before_delete / after_delete
- before_bulk_insert / after_bulk_insert
- before_bulk_update / after_bulk_update

Storage 级事件：
- before_flush / after_flush

使用方式：
    from pytuck import event

    # 装饰器注册
    @event.listens_for(User, 'before_insert')
    def set_timestamp(instance):
        instance.created_at = datetime.now()

    # 函数式注册
    event.listen(User, 'after_update', audit_changes)

    # Storage 级事件
    event.listen(db, 'before_flush', lambda storage: print("flushing..."))

    # 移除监听器
    event.remove(User, 'before_insert', set_timestamp)
"""

from typing import Any, Callable, Dict, List, Set, Tuple


# 有效的事件名称
MODEL_EVENTS: Set[str] = {
    'before_insert', 'after_insert',
    'before_update', 'after_update',
    'before_delete', 'after_delete',
    'before_bulk_insert', 'after_bulk_insert',
    'before_bulk_update', 'after_bulk_update',
}
STORAGE_EVENTS: Set[str] = {
    'before_flush', 'after_flush',
}
ALL_EVENTS: Set[str] = MODEL_EVENTS | STORAGE_EVENTS


class EventManager:
    """
    事件管理器

    全局单例，管理所有 Model 级和 Storage 级事件监听器。
    """

    def __init__(self) -> None:
        # Model 级: {(model_class, event_name): [callbacks]}
        self._model_listeners: Dict[Tuple[type, str], List[Callable[..., Any]]] = {}
        # Storage 级: {(id(storage), event_name): [callbacks]}
        self._storage_listeners: Dict[Tuple[int, str], List[Callable[..., Any]]] = {}
        # 保存 storage 引用，防止 id 复用
        self._storage_refs: Dict[int, Any] = {}

    def listen(self, target: Any, event_name: str, fn: Callable[..., Any]) -> None:
        """
        注册事件监听器

        Args:
            target: 模型类（Model 级事件）或 Storage 实例（Storage 级事件）
            event_name: 事件名称
            fn: 回调函数
        """
        if event_name not in ALL_EVENTS:
            raise ValueError(
                f"Unknown event: '{event_name}'. "
                f"Valid events: {', '.join(sorted(ALL_EVENTS))}"
            )

        if event_name in MODEL_EVENTS:
            key = (target, event_name)
            self._model_listeners.setdefault(key, []).append(fn)
        else:
            key = (id(target), event_name)
            self._storage_listeners.setdefault(key, []).append(fn)
            self._storage_refs[id(target)] = target

    def listens_for(self, target: Any, event_name: str) -> Callable[..., Any]:
        """
        装饰器方式注册事件监听器

        Args:
            target: 模型类或 Storage 实例
            event_name: 事件名称

        Returns:
            装饰器函数
        """
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.listen(target, event_name, fn)
            return fn
        return decorator

    def remove(self, target: Any, event_name: str, fn: Callable[..., Any]) -> None:
        """
        移除事件监听器

        Args:
            target: 模型类或 Storage 实例
            event_name: 事件名称
            fn: 要移除的回调函数
        """
        if event_name in MODEL_EVENTS:
            key = (target, event_name)
            listeners = self._model_listeners.get(key, [])
            if fn in listeners:
                listeners.remove(fn)
        else:
            key = (id(target), event_name)
            listeners = self._storage_listeners.get(key, [])
            if fn in listeners:
                listeners.remove(fn)

    def dispatch_model(self, model_class: type, event_name: str, instance: Any) -> None:
        """
        分发 Model 级事件

        Args:
            model_class: 模型类
            event_name: 事件名称
            instance: 模型实例
        """
        key = (model_class, event_name)
        for fn in self._model_listeners.get(key, []):
            fn(instance)

    def dispatch_model_bulk(self, model_class: type, event_name: str, instances: List[Any]) -> None:
        """
        分发 Model 级批量事件

        Args:
            model_class: 模型类
            event_name: 事件名称（如 'before_bulk_insert'）
            instances: 模型实例列表
        """
        key = (model_class, event_name)
        for fn in self._model_listeners.get(key, []):
            fn(instances)

    def dispatch_storage(self, storage: Any, event_name: str) -> None:
        """
        分发 Storage 级事件

        Args:
            storage: Storage 实例
            event_name: 事件名称
        """
        key = (id(storage), event_name)
        for fn in self._storage_listeners.get(key, []):
            fn(storage)

    def clear(self, target: Any = None) -> None:
        """
        清除监听器

        Args:
            target: 要清除的目标。None 清除所有，类型清除该模型的，实例清除该 Storage 的。
        """
        if target is None:
            self._model_listeners.clear()
            self._storage_listeners.clear()
            self._storage_refs.clear()
        elif isinstance(target, type):
            model_keys = [mk for mk in self._model_listeners if mk[0] is target]
            for mk in model_keys:
                del self._model_listeners[mk]
        else:
            sid = id(target)
            storage_keys = [sk for sk in self._storage_listeners if sk[0] == sid]
            for sk in storage_keys:
                del self._storage_listeners[sk]
            self._storage_refs.pop(sid, None)


# 全局单例
event = EventManager()
