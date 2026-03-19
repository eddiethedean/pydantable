from __future__ import annotations

from typing import Optional

import pytest
from pydantic import ValidationError

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: Optional[int]


def test_dataframe_model_column_input_happy_path():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    assert df.schema_fields() == {"id": int, "age": Optional[int]}
    assert df.collect() == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_happy_path():
    df = UserDF([{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert df.collect() == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_model_generation_and_validation():
    row_model = UserDF.row_model()
    ok = row_model.model_validate({"id": 1, "age": None})
    assert ok.id == 1

    with pytest.raises(ValidationError):
        row_model.model_validate({"id": "x", "age": 1})


def test_dataframe_model_transformations_return_derived_model():
    df = UserDF({"id": [1, 2, 3], "age": [10, 20, None]})

    df2 = df.with_columns(age2=df.age + 1)
    assert "age2" in df2.schema_fields()
    assert df2.schema_fields()["age2"] == Optional[int]

    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": Optional[int]}

    df4 = df3.filter(df3.age2 > 11)
    assert df4.collect() == {"id": [2], "age2": [21]}


def test_dataframe_model_row_input_rejects_bad_item_type():
    with pytest.raises(TypeError, match="sequence of mapping objects"):
        UserDF([1, 2, 3])  # type: ignore[arg-type]

