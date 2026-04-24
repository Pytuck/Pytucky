import pytest

from pytucky.core.event import EventManager


class DummyModel:
    def __init__(self, name):
        self.name = name


class DummyStorage:
    def __init__(self, ident):
        self.ident = ident


def test_listen_and_dispatch_model():
    em = EventManager()
    called = []

    def cb(instance):
        called.append(instance)

    em.listen(DummyModel, 'before_insert', cb)
    m = DummyModel('a')
    em.dispatch_model(DummyModel, 'before_insert', m)

    assert called == [m]


def test_listens_for_decorator_returns_function_and_registers():
    em = EventManager()
    record = []

    @em.listens_for(DummyModel, 'after_update')
    def fn(instance):
        record.append(instance.name)

    # decorator must return original function
    assert fn.__name__ == 'fn'

    m = DummyModel('bob')
    em.dispatch_model(DummyModel, 'after_update', m)
    assert record == ['bob']


def test_dispatch_model_bulk_calls_with_list():
    em = EventManager()
    received = []

    def cb(instances):
        # should receive the same list object
        received.append(list(instances))

    em.listen(DummyModel, 'before_bulk_insert', cb)
    items = [DummyModel('x'), DummyModel('y')]
    em.dispatch_model_bulk(DummyModel, 'before_bulk_insert', items)

    assert len(received) == 1
    assert [i.name for i in received[0]] == ['x', 'y']


def test_dispatch_storage_calls_with_storage():
    em = EventManager()
    called = []

    def cb(storage):
        called.append(storage)

    s = DummyStorage(1)
    em.listen(s, 'before_flush', cb)
    em.dispatch_storage(s, 'before_flush')

    assert called == [s]


def test_remove_listener_and_no_error_on_remove_nonexistent():
    em = EventManager()
    called = []

    def cb(instance):
        called.append(instance)

    em.listen(DummyModel, 'after_delete', cb)
    # remove then dispatch: should not be called
    em.remove(DummyModel, 'after_delete', cb)

    em.dispatch_model(DummyModel, 'after_delete', DummyModel('z'))
    assert called == []

    # removing a non-existent listener should not raise
    em.remove(DummyModel, 'after_delete', cb)


def test_clear_none_clears_all():
    em = EventManager()
    called = []

    def cb_model(i):
        called.append(('m', i))

    def cb_storage(s):
        called.append(('s', s))

    s = DummyStorage(2)
    em.listen(DummyModel, 'before_insert', cb_model)
    em.listen(s, 'after_flush', cb_storage)

    em.clear(None)

    em.dispatch_model(DummyModel, 'before_insert', DummyModel('a'))
    em.dispatch_storage(s, 'after_flush')

    assert called == []


def test_clear_by_model_class_only_removes_model_listeners():
    em = EventManager()
    called = []

    def cb_model(i):
        called.append(('m', i))

    def cb_storage(s):
        called.append(('s', s))

    s = DummyStorage(3)
    em.listen(DummyModel, 'before_update', cb_model)
    em.listen(s, 'before_flush', cb_storage)

    em.clear(DummyModel)

    em.dispatch_model(DummyModel, 'before_update', DummyModel('b'))
    em.dispatch_storage(s, 'before_flush')

    assert called == [('s', s)]


def test_clear_by_storage_instance_only_removes_storage_listeners():
    em = EventManager()
    called = []

    def cb_model(i):
        called.append(('m', i))

    def cb_storage(s):
        called.append(('s', s))

    s1 = DummyStorage(4)
    s2 = DummyStorage(5)
    em.listen(DummyModel, 'after_insert', cb_model)
    em.listen(s1, 'before_flush', cb_storage)
    em.listen(s2, 'before_flush', cb_storage)

    em.clear(s1)

    em.dispatch_storage(s1, 'before_flush')
    em.dispatch_storage(s2, 'before_flush')
    em.dispatch_model(DummyModel, 'after_insert', DummyModel('c'))

    # only s2 and model should have been called
    assert len(called) == 2
    tags = [c[0] for c in called]
    assert 's' in tags and 'm' in tags
    storage_call = [c for c in called if c[0] == 's'][0]
    assert storage_call[1] is s2


def test_listen_invalid_event_name_raises():
    em = EventManager()

    with pytest.raises(ValueError):
        em.listen(DummyModel, 'not_an_event', lambda x: x)


def test_multiple_listeners_fire_in_order():
    em = EventManager()
    order = []

    def a(i):
        order.append('a')

    def b(i):
        order.append('b')

    def c(i):
        order.append('c')

    em.listen(DummyModel, 'before_delete', a)
    em.listen(DummyModel, 'before_delete', b)
    em.listen(DummyModel, 'before_delete', c)

    em.dispatch_model(DummyModel, 'before_delete', DummyModel('o'))
    assert order == ['a', 'b', 'c']
