import pytest

from pydantable import DataFrame, Schema
from pydantable.expressions import ColumnRef


class User(Schema):
    id: int
    age: int


def test_with_columns_and_collect_python():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    assert df2.schema_fields()["age2"] is int

    result = df2.collect(engine="python")
    assert result == {"id": [1, 2], "age": [20, 30], "age2": [40, 60]}


def test_select_and_filter():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": int}

    df4 = df3.filter(df3.age2 > 40)
    result = df4.to_dict()
    assert result == {"id": [2], "age2": [60]}


def test_with_columns_rejects_unknown_referenced_columns():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(ValueError, match="references unknown columns"):
        df.with_columns(
            bad=df.col("age") + 1,
            missing=ColumnRef(name="missing", dtype=int) + 1,
        )

