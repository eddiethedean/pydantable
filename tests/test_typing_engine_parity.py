from __future__ import annotations

from pydantable import DataFrameModel
from pydantable.typing_engine import (
    infer_schema_descriptors_drop,
    infer_schema_descriptors_rename,
    infer_schema_descriptors_select,
    infer_schema_descriptors_with_columns,
)


class Users(DataFrameModel):
    id: int
    age: int
    city: str


def test_typing_engine_select_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.select("id", "age")
    desc = infer_schema_descriptors_select(df.schema_fields(), ["id", "age"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_drop_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.drop("city")
    desc = infer_schema_descriptors_drop(df.schema_fields(), ["city"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_rename_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.rename({"age": "years"})
    desc = infer_schema_descriptors_rename(df.schema_fields(), {"age": "years"})
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_with_columns_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.with_columns(age2=df.age * 2)
    desc = infer_schema_descriptors_with_columns(
        df.schema_fields(), {"age2": df.age * 2}
    )
    assert set(desc) == set(out.schema_fields())
