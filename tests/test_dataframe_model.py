from __future__ import annotations

import pytest
from pydantable import DataFrameModel
from pydantic import ValidationError


class UserDF(DataFrameModel):
    id: int
    age: int | None


def test_dataframe_model_column_input_happy_path():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    assert df.schema_fields() == {"id": int, "age": int | None}
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
    assert df2.schema_fields()["age2"] == int | None

    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": int | None}

    df4 = df3.filter(df3.age2 > 11)
    assert df4.collect() == {"id": [2], "age2": [21]}


def test_dataframe_model_row_input_rejects_bad_item_type():
    with pytest.raises(TypeError, match="sequence of mapping objects"):
        UserDF([1, 2, 3])  # type: ignore[arg-type]


def test_dataframe_model_parity_with_dataframe_core_expression_behavior():
    # DataFrameModel should expose the same expression typing behavior.
    df = UserDF({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match="requires numeric operands"):
        _ = df.age + "x"


def test_dataframe_model_chained_schema_migration_dtypes():
    df = UserDF({"id": [1, 2, 3], "age": [20, None, 30]})
    df2 = df.with_columns(age2=df.age + 1, flag=df.age > 21)
    schema = df2.schema_fields()
    assert schema["age2"] == int | None
    assert schema["flag"] == bool | None


def test_rust_schema_descriptors_flow_into_derived_model_types():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    df2 = df.with_columns(age2=df.age + 1, flag=df.age > 10)
    # Validate descriptor contract from rust and python mapping.
    desc = df2._df._rust_plan.schema_descriptors()
    assert desc["age2"] == {"base": "int", "nullable": True}
    assert desc["flag"] == {"base": "bool", "nullable": True}
    assert df2.schema_fields()["age2"] == int | None
    assert df2.schema_fields()["flag"] == bool | None


def test_dataframe_model_with_columns_collision_replacement_semantics():
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 20]})
    df2 = df.with_columns(age=df.age + 1)
    assert df2.schema_fields()["age"] == int | None
    assert df2.collect() == {"id": [1, 2, 3], "age": [11, None, 21]}


def test_dataframe_model_filter_preserves_schema_changes_rows_only():
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})
    before = df.schema_fields()
    df2 = df.filter(df.age > 20)
    after = df2.schema_fields()
    assert before == after
    assert df2.collect() == {"id": [3], "age": [30]}


def test_dataframe_model_row_vs_column_input_transformation_parity():
    row_df = UserDF(
        [{"id": 1, "age": 10}, {"id": 2, "age": None}, {"id": 3, "age": 30}]
    )
    col_df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})

    row_df2 = row_df.with_columns(age2=row_df.age + 1)
    row_out = row_df2.filter(row_df2.age2 > 20).select("id", "age2").collect()
    col_df2 = col_df.with_columns(age2=col_df.age + 1)
    col_out = col_df2.filter(col_df2.age2 > 20).select("id", "age2").collect()
    assert row_out == col_out == {"id": [3], "age2": [31]}


def test_rows_materializes_row_models_with_nulls():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    rows = df.rows()

    assert len(rows) == 2
    assert isinstance(rows[0], UserDF.row_model())
    assert rows[0].id == 1
    assert rows[0].age == 20

    assert isinstance(rows[1], UserDF.row_model())
    assert rows[1].id == 2
    assert rows[1].age is None


def test_rows_and_to_dicts_materialize_derived_schema():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    df2 = df.with_columns(age2=df.age + 1)

    rows = df2.rows()
    assert [r.id for r in rows] == [1, 2]
    assert [r.age2 for r in rows] == [21, None]

    got_dicts = df2.to_dicts()
    assert got_dicts == [
        {"id": 1, "age": 20, "age2": 21},
        {"id": 2, "age": None, "age2": None},
    ]


def test_rows_returns_empty_list_for_empty_dataframe():
    df = UserDF({"id": [], "age": []})
    assert df.rows() == []


def test_row_model_rejects_extra_fields():
    row_model = UserDF.row_model()
    with pytest.raises(ValidationError):
        row_model.model_validate({"id": 1, "age": None, "extra": "x"})
