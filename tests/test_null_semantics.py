import pytest
from typing import Optional, Union, get_args, get_origin

from pydantable import DataFrame, Schema


def _is_optional_of(dtype, base_type) -> bool:
    return get_origin(dtype) is Union and set(get_args(dtype)) == {base_type, type(None)}


class UserNullable(Schema):
    id: int
    age: Optional[int]


def test_arithmetic_propagates_nulls_and_optional_dtype():
    df = DataFrame[UserNullable]({"id": [1, 2, 3], "age": [20, None, 30]})
    expr = df.age + 1

    assert _is_optional_of(expr.dtype, int)
    df2 = df.with_columns(age2=expr)
    assert df2.collect()["age2"] == [21, None, 31]


def test_comparison_propagates_nulls_optional_bool_dtype():
    df = DataFrame[UserNullable]({"id": [1, 2, 3], "age": [20, None, 30]})
    cond = df.age > 25

    assert _is_optional_of(cond.dtype, bool)
    df2 = df.with_columns(cond=cond)
    assert df2.collect()["cond"] == [False, None, True]


def test_filter_drops_null_rows_and_keeps_true_only():
    df = DataFrame[UserNullable]({"id": [1, 2, 3, 4], "age": [20, None, 30, None]})
    cond = df.age > 25
    df2 = df.filter(cond)

    # keep only condition == True
    assert df2.to_dict() == {"id": [3], "age": [30]}


def test_filter_rejects_non_bool_condition_before_execution():
    df = DataFrame[UserNullable]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match="filter\\(condition\\) expects"):
        df.filter(df.age)


def test_invalid_arithmetic_operand_types_fail_at_ast_build_time():
    df = DataFrame[UserNullable]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match="requires numeric operands"):
        _ = df.age + "x"


def test_with_columns_collision_replaces_schema_and_values():
    df = DataFrame[UserNullable]({"id": [1, 2, 3], "age": [20, None, 30]})
    df2 = df.with_columns(age=df.age + 1)

    assert _is_optional_of(df2.schema_fields()["age"], int)
    assert df2.collect() == {"id": [1, 2, 3], "age": [21, None, 31]}


def test_chained_transformations_preserve_nullable_schema_types():
    df = DataFrame[UserNullable]({"id": [1, 2, 3, 4], "age": [20, None, 30, None]})

    # Derived column should be nullable due to propagate_nulls.
    df2 = df.with_columns(age2=df.age + 1)
    assert _is_optional_of(df2.schema_fields()["age2"], int)

    # Projection should carry nullable types forward.
    df3 = df2.select("id", "age2")
    assert _is_optional_of(df3.schema_fields()["age2"], int)

    # Filter keeps schema, but filters rows: only True survives.
    df4 = df3.filter(df3.age2 > 25)
    assert _is_optional_of(df4.schema_fields()["age2"], int)
    assert df4.to_dict() == {"id": [3], "age2": [31]}

