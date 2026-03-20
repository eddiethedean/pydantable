from typing import Optional

from pydantable import DataFrame, DataFrameModel
from pydantable.schema import Schema


class UserSchema(Schema):
    id: int
    age: Optional[int]


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


def test_phase5_collect_preserves_null_filter_semantics():
    df = DataFrame[UserSchema]({"id": [1, 2, 3], "age": [20, None, 30]})
    got = df.filter(df.age > 25).select("id", "age").collect()
    assert got == {"id": [3], "age": [30]}


def test_phase5_row_and_column_inputs_match_under_collect():
    row_df = UserDF(
        [{"id": 1, "age": 10}, {"id": 2, "age": None}, {"id": 3, "age": 30}]
    )
    col_df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})

    row_result = (
        row_df.with_columns(age2=row_df.age + 1)
        .filter(row_df.age > 10)
        .select("id", "age2")
        .collect()
    )
    col_result = (
        col_df.with_columns(age2=col_df.age + 1)
        .filter(col_df.age > 10)
        .select("id", "age2")
        .collect()
    )
    assert row_result == col_result


def test_phase5_collect_matches_derived_schema_fields():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    df2 = df.with_columns(age2=df.age + 1, cond=df.age > 10).select(
        "id", "age2", "cond"
    )
    out = df2.collect()
    assert set(out.keys()) == {"id", "age2", "cond"}
    assert df2.schema_fields()["age2"] == Optional[int]
    assert df2.schema_fields()["cond"] == Optional[bool]
