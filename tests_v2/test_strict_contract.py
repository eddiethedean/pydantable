import pytest
from pydantable import DataFrame, Schema


class Before(Schema):
    id: int
    age: int


class AfterSelect(Schema):
    id: int


class AfterWithCols(Schema):
    id: int
    age2: int


class AfterWithColsFull(Schema):
    id: int
    age: int
    age2: int


def test_df_col_namespace_required() -> None:
    df = DataFrame[Before]({"id": [1], "age": [10]})
    with pytest.raises(AttributeError):
        _ = df.age
    assert df.col.age.referenced_columns() == {"age"}


def test_legacy_schema_changing_apis_removed() -> None:
    df = DataFrame[Before]({"id": [1], "age": [10]})
    with pytest.raises(TypeError):
        df.select("id")
    with pytest.raises(TypeError):
        df.with_columns(age2=df.col.age * 2)
    with pytest.raises(TypeError):
        df.drop("age")
    with pytest.raises(TypeError):
        df.rename({"age": "age2"})


def test_select_as_enforces_schema() -> None:
    df = DataFrame[Before]({"id": [1, 2], "age": [10, 20]})
    out = df.select_as(AfterSelect, df.col.id)
    assert out.schema_fields() == {"id": int}
    assert out.to_dict() == {"id": [1, 2]}


def test_with_columns_as_enforces_schema() -> None:
    df = DataFrame[Before]({"id": [1, 2], "age": [10, 20]})
    out_full = df.with_columns_as(AfterWithColsFull, age2=df.col.age * 2)
    out = out_full.drop_as(AfterWithCols, out_full.col.age)
    assert out.schema_fields() == {"id": int, "age2": int}
    assert out.to_dict() == {"id": [1, 2], "age2": [20, 40]}
