from pydantable import DataFrame, Schema
from pydantable.expressions import CompareOp, ColumnRef


class User(Schema):
    id: int
    age: int


def test_expression_arithmetic_eval_and_inferred_dtype():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    expr = df.age * 2
    assert expr.dtype is int
    assert expr.referenced_columns() == {"age"}

    df2 = df.with_columns(age2=expr)
    result = df2.collect()
    assert result["age2"] == [40, 60]


def test_expression_comparison_eval():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    cond = df.age > 25
    assert isinstance(cond, CompareOp)
    assert cond.dtype is bool

    df2 = df.with_columns(cond=cond)
    result = df2.collect()
    assert result["cond"] == [False, True]


def test_columnref_is_typed():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    age_col = df.col("age")
    assert isinstance(age_col, ColumnRef)
    assert age_col.dtype is int


def test_reflected_arithmetic_ops_supported():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(a=2 + df.age, b=100 - df.age, c=3 * df.age, d=60 / df.age)
    out = df2.collect()
    assert out["a"] == [22, 32]
    assert out["b"] == [80, 70]
    assert out["c"] == [60, 90]
    assert out["d"] == [3.0, 2.0]

