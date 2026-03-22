from __future__ import annotations

import pytest
from pydantable import DataFrameModel
from pydantable.schema import is_supported_scalar_column_annotation
from pydantic import ValidationError


class UserDF(DataFrameModel):
    id: int
    age: int | None


def test_dataframe_model_column_input_happy_path():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    assert df.schema_fields() == {"id": int, "age": int | None}
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_happy_path():
    df = UserDF([{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_sequence_of_pydantic_models():
    rm = UserDF.row_model()
    rows = [rm(id=1, age=20), rm(id=2, age=None)]
    df = UserDF(rows)
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_mixed_dict_and_model():
    rm = UserDF.row_model()
    df = UserDF([{"id": 1, "age": 20}, rm(id=2, age=None)])
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


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
    assert df4.collect(as_lists=True) == {"id": [2], "age2": [21]}


def test_dataframe_model_row_input_rejects_bad_item_type():
    with pytest.raises(TypeError, match="mapping objects or Pydantic models"):
        UserDF([1, 2, 3])  # type: ignore[arg-type]


def test_dataframe_model_rejects_unsupported_list_type_at_class_definition():
    with pytest.raises(TypeError, match="unsupported type") as exc:
        class BadList(DataFrameModel):
            items: list[int]

    assert "BadList" in str(exc.value)
    assert "items" in str(exc.value)
    assert "SUPPORTED_TYPES" in str(exc.value)


def test_dataframe_model_rejects_unsupported_union_of_two_scalars_at_class_definition():
    with pytest.raises(TypeError, match="unsupported type"):
        class BadUnion(DataFrameModel):
            x: int | str


def test_is_supported_scalar_column_annotation_smoke():
    assert is_supported_scalar_column_annotation(int)
    assert is_supported_scalar_column_annotation(int | None)
    assert not is_supported_scalar_column_annotation(list[int])
    assert not is_supported_scalar_column_annotation(dict[str, int])


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
    assert df2.collect(as_lists=True) == {"id": [1, 2, 3], "age": [11, None, 21]}


def test_dataframe_model_filter_preserves_schema_changes_rows_only():
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})
    before = df.schema_fields()
    df2 = df.filter(df.age > 20)
    after = df2.schema_fields()
    assert before == after
    assert df2.collect(as_lists=True) == {"id": [3], "age": [30]}


def test_dataframe_model_row_vs_column_input_transformation_parity():
    row_df = UserDF(
        [{"id": 1, "age": 10}, {"id": 2, "age": None}, {"id": 3, "age": 30}]
    )
    col_df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})

    row_df2 = row_df.with_columns(age2=row_df.age + 1)
    row_out = (
        row_df2.filter(row_df2.age2 > 20).select("id", "age2").collect(as_lists=True)
    )
    col_df2 = col_df.with_columns(age2=col_df.age + 1)
    col_out = (
        col_df2.filter(col_df2.age2 > 20).select("id", "age2").collect(as_lists=True)
    )
    assert row_out == col_out == {"id": [3], "age2": [31]}


def test_rows_materializes_row_models_with_nulls():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    rows = df.rows()

    assert len(rows) == 2
    assert isinstance(rows[0], df.schema_type)
    assert rows[0].id == 1
    assert rows[0].age == 20

    assert isinstance(rows[1], df.schema_type)
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


def test_p1_dataframe_model_methods_and_concat():
    df = UserDF({"id": [3, 1, 2, 2], "age": [30, None, 20, 20]})

    sorted_df = df.sort("id")
    assert sorted_df.collect(as_lists=True)["id"] == [1, 2, 2, 3]

    unique_df = sorted_df.unique(subset=["id", "age"])
    assert unique_df.collect(as_lists=True) == {"id": [1, 2, 3], "age": [None, 20, 30]}

    renamed = unique_df.rename({"age": "years"})
    assert set(renamed.schema_fields().keys()) == {"id", "years"}
    assert renamed.schema_fields()["years"] == int | None
    assert renamed.slice(1, 2).collect(as_lists=True) == {
        "id": [2, 3],
        "years": [20, 30],
    }
    assert renamed.head(1).collect(as_lists=True) == {"id": [1], "years": [None]}
    assert renamed.tail(1).collect(as_lists=True) == {"id": [3], "years": [30]}

    first = renamed.select("id")
    second = renamed.select("id")
    cat = DataFrameModel.concat([first, second], how="vertical")
    assert cat.collect(as_lists=True) == {"id": [1, 2, 3, 1, 2, 3]}


def test_p2_dataframe_model_fill_and_drop_nulls() -> None:
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})
    filled = df.fill_null(0, subset=["age"])
    assert filled.collect(as_lists=True) == {"id": [1, 2, 3], "age": [10, 0, 30]}
    assert filled.schema_fields()["age"] is int

    dropped = df.drop_nulls(subset=["age"])
    assert dropped.collect(as_lists=True) == {"id": [1, 3], "age": [10, 30]}


def test_p4_dataframe_model_groupby_aggregations_schema() -> None:
    df = UserDF({"id": [1, 1, 2], "age": [10, 20, 30]})
    grouped = df.group_by("id").agg(
        age_min=("min", "age"),
        age_max=("max", "age"),
        age_median=("median", "age"),
        age_std=("std", "age"),
        age_var=("var", "age"),
        age_first=("first", "age"),
        age_last=("last", "age"),
        age_n_unique=("n_unique", "age"),
    )
    schema = grouped.schema_fields()
    assert schema["age_min"] == int | None
    assert schema["age_max"] == int | None
    assert schema["age_median"] == float | None
    assert schema["age_std"] == float | None
    assert schema["age_var"] == float | None
    assert schema["age_first"] == int | None
    assert schema["age_last"] == int | None
    assert schema["age_n_unique"] is int


def test_p5_dataframe_model_reshape_methods() -> None:
    class SalesDF(DataFrameModel):
        id: int
        k: str
        v: int | None

    df = SalesDF({"id": [1, 1], "k": ["A", "B"], "v": [10, None]})
    melted = df.melt(
        id_vars=["id"], value_vars=["v"], variable_name="var", value_name="val"
    )
    out = melted.collect(as_lists=True)
    assert out == {"id": [1, 1], "var": ["v", "v"], "val": [10, None]}
    assert melted.schema_fields()["var"] is str
    assert melted.schema_fields()["val"] == int | None

    pivoted = df.pivot(index="id", columns="k", values="v", aggregate_function="first")
    p_out = pivoted.collect(as_lists=True)
    assert p_out["id"] == [1]
    assert p_out["A_first"] == [10]
    assert p_out["B_first"] == [None]


def test_p6_dataframe_model_rolling_and_dynamic() -> None:
    class TSModel(DataFrameModel):
        id: int
        ts: int
        v: int | None

    df = TSModel(
        {
            "id": [1, 1, 1],
            "ts": [0, 3600, 7200],
            "v": [10, None, 30],
        }
    )
    rolled = df.rolling_agg(
        on="ts",
        column="v",
        window_size="2h",
        op="sum",
        out_name="v_roll_sum",
        by=["id"],
    )
    assert rolled.collect(as_lists=True)["v_roll_sum"] == [10, 10, 40]

    grouped = df.group_by_dynamic("ts", every="1h", by=["id"]).agg(
        v_count=("count", "v")
    )
    assert "v_count" in grouped.collect(as_lists=True)
