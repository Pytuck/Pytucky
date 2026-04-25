import pytest

from pytucky.query import builder
from pytucky.core.orm import Column
from pytucky.common.exceptions import QueryError


def make_col(name="age", col_type=int):
    # create a lightweight Column-like object; Column class expects parameters in core.orm
    return Column(col_type, name=name)


class DummyModel:
    __columns__ = {"age": Column(int, name="age"), "name": Column(str, name="name")}
    __tablename__ = "dummy"

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @staticmethod
    def _column_to_attr_name(col_name: str):
        # direct mapping for tests
        return col_name


def test_condition_evaluate_all_ops():
    rec = {"age": 30, "name": "HelloWorld", "tags": [1, 2, 3]}

    # equality
    assert builder.Condition("age", "=", 30).evaluate(rec)
    assert not builder.Condition("age", "=", 31).evaluate(rec)

    # greater than
    assert builder.Condition("age", ">", 20).evaluate(rec)
    assert not builder.Condition("age", ">", 30).evaluate(rec)

    # less than
    assert builder.Condition("age", "<", 40).evaluate(rec)
    assert not builder.Condition("age", "<", 30).evaluate(rec)

    # >= and <=
    assert builder.Condition("age", ">=", 30).evaluate(rec)
    assert builder.Condition("age", "<=", 30).evaluate(rec)

    # !=
    assert builder.Condition("age", "!=", 25).evaluate(rec)
    assert not builder.Condition("age", "!=", 30).evaluate(rec)

    # IN membership (field value is a member of the provided collection)
    in_condition = builder.Condition("age", "IN", [25, 30, 35])
    assert isinstance(in_condition.value, frozenset)
    assert in_condition.evaluate(rec)
    assert not builder.Condition("age", "IN", [25, 35]).evaluate(rec)

    # LIKE contains (case insensitive)
    assert builder.Condition("name", "LIKE", "helloworld").evaluate(rec)
    assert builder.Condition("name", "LIKE", "hell").evaluate(rec)
    assert not builder.Condition("name", "LIKE", "xyz").evaluate(rec)

    # STARTSWITH / ENDSWITH (case insensitive)
    assert builder.Condition("name", "STARTSWITH", "hello").evaluate(rec)
    assert builder.Condition("name", "ENDSWITH", "world").evaluate(rec)
    assert not builder.Condition("name", "STARTSWITH", "world").evaluate(rec)


def test_condition_in_falls_back_for_unhashable_values():
    rec = {"tags": [1, 2]}
    condition = builder.Condition("tags", "IN", [[1, 2], [3, 4]])

    assert isinstance(condition.value, list)
    assert condition.evaluate(rec)


def test_condition_unsupported_operator_raises():
    with pytest.raises(QueryError):
        builder.Condition("age", "INVALID_OP", 1)


def test_binaryexpression_to_condition():
    col = make_col("age")
    beq = builder.BinaryExpression(col, "=", 5)
    cond = beq.to_condition()
    assert isinstance(cond, builder.Condition)
    assert cond.field == "age"
    assert cond.operator == "="
    assert cond.value == 5

    # different operator
    bgt = builder.BinaryExpression(col, ">", 3)
    c2 = bgt.to_condition()
    assert c2.operator == ">"


def test_logical_and_or_not_evaluation():
    rec = {"age": 20, "name": "alice"}
    c1 = builder.Condition("age", ">=", 18)
    c2 = builder.Condition("name", "LIKE", "ali")

    and_comp = builder.CompositeCondition("AND", [c1, c2])
    assert and_comp.evaluate(rec)

    or_comp = builder.CompositeCondition("OR", [c1, builder.Condition("age", "<", 10)])
    assert or_comp.evaluate(rec)

    not_comp = builder.CompositeCondition("NOT", [builder.Condition("age", "<", 10)])
    assert not_comp.evaluate(rec)
    assert not builder.CompositeCondition("NOT", [c1]).evaluate(rec)


def test_or_and_not_helpers_and_validation():
    col = make_col("age")
    expr1 = builder.BinaryExpression(col, ">=", 18)
    expr2 = builder.BinaryExpression(col, "<", 30)

    or_expr = builder.or_(expr1, expr2)
    assert isinstance(or_expr, builder.LogicalExpression)
    assert or_expr.operator == "OR"

    and_expr = builder.and_(expr1, expr2)
    assert and_expr.operator == "AND"

    not_expr = builder.not_(expr1)
    assert not_expr.operator == "NOT"

    # validation: or_ and and_ require at least 2 expressions
    with pytest.raises(QueryError):
        builder.or_(expr1)

    with pytest.raises(QueryError):
        builder.and_(expr1)


def test_nested_composite_evaluates_correctly():
    rec = {"age": 25, "name": "Bob"}
    col = make_col("age")
    e1 = builder.BinaryExpression(col, ">=", 18)
    e2 = builder.BinaryExpression(col, "<", 30)
    e3 = builder.BinaryExpression(make_col("age"), "=", 99)

    nested = builder.or_(builder.and_(e1, e2), e3)
    cond = nested.to_condition()
    assert cond.evaluate(rec)

    # change record so only e3 would satisfy
    rec2 = {"age": 99}
    assert nested.to_condition().evaluate(rec2)


def test_query_filter_accepts_binary_and_logical():
    # minimal storage mock that returns provided conditions as records for testing _execute flow
    class MockStorage:
        def __init__(self, records):
            self._records = records

        def query(self, table_name, conditions, limit=None, offset=0, order_by=None, order_desc=False):
            # evaluate conditions against records and return matching ones
            out = []
            for r in self._records:
                if all(cond.evaluate(r) for cond in conditions):
                    out.append(r)
            if offset != 0:
                out = out[offset:]
            if limit is not None:
                out = out[:limit]
            return out

        def _query_count(self, table_name, conditions, limit=None, offset=0):
            matches = [r for r in self._records if all(cond.evaluate(r) for cond in conditions)]
            if offset > 0:
                matches = matches[offset:]
            if limit is not None:
                matches = matches[:limit]
            return len(matches)

    storage = MockStorage([{"age": 21, "name": "Alice"}, {"age": 17, "name": "Bob"}])

    class M(DummyModel):
        __storage__ = storage

    q = builder.Query(M, storage=storage)
    col = M.__columns__["age"]
    q.filter(builder.BinaryExpression(col, ">=", 18))
    res = q.all()
    assert len(res) == 1
    assert res[0].age == 21


def test_query_pushes_limit_offset_and_order_to_storage():
    class MockStorage:
        def __init__(self):
            self.calls = []

        def query(self, table_name, conditions, limit=None, offset=0, order_by=None, order_desc=False):
            self.calls.append(
                {
                    "table_name": table_name,
                    "conditions": conditions,
                    "limit": limit,
                    "offset": offset,
                    "order_by": order_by,
                    "order_desc": order_desc,
                }
            )
            return [{"age": 30, "name": "Alice"}]

        def _query_count(self, table_name, conditions, limit=None, offset=0):
            raise AssertionError("count fast-path should not be used here")

    storage = MockStorage()

    class M(DummyModel):
        __storage__ = storage

    q = builder.Query(M, storage=storage)
    q.order_by("age", desc=True).limit(2).offset(1)
    res = q.all()

    assert len(res) == 1
    assert storage.calls == [
        {
            "table_name": "dummy",
            "conditions": [],
            "limit": 2,
            "offset": 1,
            "order_by": "age",
            "order_desc": True,
        }
    ]



def test_query_count_uses_storage_fast_path():
    class MockStorage:
        def __init__(self):
            self.query_calls = 0
            self.count_calls = []

        def query(self, table_name, conditions, limit=None, offset=0, order_by=None, order_desc=False):
            self.query_calls += 1
            return [{"age": 30, "name": "Alice"}]

        def _query_count(self, table_name, conditions, limit=None, offset=0):
            self.count_calls.append(
                {
                    "table_name": table_name,
                    "conditions": conditions,
                    "limit": limit,
                    "offset": offset,
                }
            )
            return 2

    storage = MockStorage()

    class M(DummyModel):
        __storage__ = storage

    q = builder.Query(M, storage=storage)
    col = M.__columns__["age"]
    q.filter(builder.BinaryExpression(col, ">=", 18)).limit(2).offset(1)

    assert q.count() == 2
    assert storage.query_calls == 0
    assert storage.count_calls == [
        {
            "table_name": "dummy",
            "conditions": q._conditions,
            "limit": 2,
            "offset": 1,
        }
    ]
