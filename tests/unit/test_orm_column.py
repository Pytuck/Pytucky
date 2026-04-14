import pytest

from pytucky.core.orm import Column
from pytucky.common.exceptions import ValidationError
from pytucky.query.builder import BinaryExpression


def test_validate_type_conversion_non_strict_int_from_str():
    col = Column(int)
    assert col.validate('42') == 42


def test_validate_type_conversion_non_strict_int_rejects_bool():
    col = Column(int)
    with pytest.raises(ValidationError):
        col.validate(True)


def test_validate_type_conversion_non_strict_float_from_int():
    col = Column(float)
    assert col.validate(5) == 5.0


def test_validate_type_conversion_non_strict_str_from_int():
    col = Column(str)
    assert col.validate(123) == '123'


def test_validate_nullable_allows_none_when_nullable_true():
    col = Column(int, nullable=True)
    assert col.validate(None) is None


def test_validate_nullable_rejects_none_when_nullable_false():
    col = Column(int, nullable=False)
    with pytest.raises(ValidationError):
        col.validate(None)


def test_validate_nullable_primary_key_allows_none_even_if_nullable_false():
    col = Column(int, nullable=False, primary_key=True)
    # primary key may be None for auto-generation
    assert col.validate(None) is None


def test_validate_strict_mode_rejects_conversion():
    col = Column(int, strict=True)
    with pytest.raises(ValidationError):
        col.validate('42')


def test_validate_custom_validator_function_rejects():
    def validator(x):
        return x < 10  # allow only <10

    col = Column(int, validator=validator)
    # valid
    assert col.validate(5) == 5
    # invalid
    with pytest.raises(ValidationError):
        col.validate(20)


def test_validate_custom_validator_list_and_combined_errors():
    def v1(x):
        return x > 0

    def v2(x):
        if x == 3:
            raise ValueError('no three')
        return True

    col = Column(int, validator=[v1, v2])
    assert col.validate(1) == 1
    with pytest.raises(ValidationError):
        col.validate(-1)
    with pytest.raises(ValidationError):
        col.validate(3)


def test_default_and_default_factory_resolution_and_has_default():
    col_static = Column(str, default='hello')
    assert col_static.has_default() is True
    assert col_static.resolve_default() == 'hello'

    col_factory = Column(int, default_factory=lambda: 7)
    assert col_factory.has_default() is True
    assert col_factory.resolve_default() == 7

    col_none = Column(float)
    assert col_none.has_default() is False
    assert col_none.resolve_default() is None


def test_query_expressions_create_binary_expression_and_operator():
    col = Column(int, name='age')
    # equality
    be = col == 5
    assert isinstance(be, BinaryExpression)
    assert be.operator == '=' and be.value == 5

    be = col != 5
    assert isinstance(be, BinaryExpression)
    assert be.operator == '!=' and be.value == 5

    be = col > 5
    assert be.operator == '>' and be.value == 5

    be = col < 5
    assert be.operator == '<' and be.value == 5

    be = col >= 5
    assert be.operator == '>=' and be.value == 5

    be = col <= 5
    assert be.operator == '<=' and be.value == 5

    be = col.in_([1, 2, 3])
    assert be.operator == 'IN' and be.value == [1, 2, 3]

    # string ops on a Column declared as str
    c2 = Column(str, name='name')
    be = c2.contains('test')
    assert be.operator == 'LIKE' and be.value == 'test'

    be = c2.startswith('a')
    assert be.operator == 'STARTSWITH' and be.value == 'a'

    be = c2.endswith('z')
    assert be.operator == 'ENDSWITH' and be.value == 'z'


def test_to_dict_serialization():
    col = Column(int, name='user_id', nullable=False, primary_key=True, index='hash', default=1, comment='id')
    d = col.to_dict()
    assert d['name'] == 'user_id'
    assert d['type'] == 'int'
    assert d['nullable'] is False
    assert d['primary_key'] is True
    assert d['index'] == 'hash'
    assert d['default'] == 1
    assert d['comment'] == 'id'
