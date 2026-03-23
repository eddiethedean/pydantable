from __future__ import annotations

import warnings

import pytest
from pydantable import DataFrame, DataFrameModel, Schema
from pydantable.schema import DtypeDriftWarning, is_supported_scalar_column_annotation
from pydantic import ValidationError


class UserDF(DataFrameModel):
    id: int
    age: int | None


class _AddrNested(Schema):
    street: str
    zip_code: int | None


class _PersonWithAddrDF(DataFrameModel):
    id: int
    addr: _AddrNested


def test_dataframe_model_column_input_happy_path():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    assert df.schema_fields() == {"id": int, "age": int | None}
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_happy_path():
    df = UserDF([{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_strict_mode_still_raises() -> None:
    with pytest.raises(ValidationError):
        UserDF([{"id": 1, "age": 20}, {"id": "bad", "age": 30}])


def test_dataframe_model_ignore_errors_row_input_keeps_valid_rows() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [{"id": 1, "age": 20}, {"id": "bad", "age": 30}, {"id": 2, "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"id": "bad", "age": 30}
    assert isinstance(failures[0]["errors"], list)


def test_dataframe_model_ignore_errors_columnar_input_best_effort() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        {"id": [1, "bad", 2], "age": [20, 30, None]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"id": "bad", "age": 30}


def test_dataframe_model_ignore_errors_all_invalid_rows_returns_empty() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [{"id": "bad1", "age": 20}, {"id": "bad2", "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [], "age": []}
    assert len(failures) == 2


def test_dataframe_model_ignore_errors_callback_not_called_when_clean() -> None:
    called = False

    def on_fail(_items: list[dict[str, object]]) -> None:
        nonlocal called
        called = True

    df = UserDF(
        [{"id": 1, "age": 20}, {"id": 2, "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert called is False


def test_dataframe_model_ignore_errors_callback_collects_multiple_row_failures() -> (
    None
):
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [
            {"id": 1, "age": 20},
            {"id": "bad-1", "age": 30},
            {"id": 2, "age": None},
            {"id": "bad-2", "age": None},
        ],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert [f["row_index"] for f in failures] == [1, 3]
    assert failures[0]["row"] == {"id": "bad-1", "age": 30}
    assert failures[1]["row"] == {"id": "bad-2", "age": None}


def test_dataframe_model_ignore_errors_callback_invoked_once_with_all_failures() -> (
    None
):
    invocations = 0
    seen_indices: list[int] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        nonlocal invocations
        invocations += 1
        seen_indices.extend(int(item["row_index"]) for item in items)

    _ = UserDF(
        {"id": ["bad-0", 1, "bad-2"], "age": [10, 20, 30]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert invocations == 1
    assert seen_indices == [0, 2]


def test_dataframe_model_ignore_errors_non_mapping_row_is_reported_and_skipped() -> (
    None
):
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [
            {"id": 1, "age": 20},
            123,  # type: ignore[list-item]
            {"id": 2, "age": None},
        ],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"_raw_row": 123}


def test_dataframe_model_ignore_errors_columnar_multiple_failures_payload() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        {"id": [1, "bad-1", 2, "bad-2"], "age": [20, 30, None, 40]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert [f["row_index"] for f in failures] == [1, 3]
    assert failures[0]["row"] == {"id": "bad-1", "age": 30}
    assert failures[1]["row"] == {"id": "bad-2", "age": 40}


def test_dataframe_model_columnar_strict_mode_raises_without_ignore_errors() -> None:
    with pytest.raises(ValidationError):
        UserDF({"id": [1, "bad", 2], "age": [20, 30, None]})


def test_dataframe_model_ignore_errors_still_checks_column_lengths() -> None:
    with pytest.raises(ValueError, match="same length"):
        UserDF(
            {"id": [1, "bad", 2], "age": [20, 30]},
            ignore_errors=True,
        )


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


def test_dataframe_model_rejects_unsupported_dict_type_at_class_definition():
    with pytest.raises(TypeError, match="unsupported type") as exc:

        class BadDict(DataFrameModel):
            m: dict[int, str]

    assert "BadDict" in str(exc.value)
    assert "m" in str(exc.value)
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


def test_nested_model_column_round_trip() -> None:
    df = _PersonWithAddrDF(
        {
            "id": [1, 2],
            "addr": [
                {"street": "Main", "zip_code": 12345},
                {"street": "Oak", "zip_code": None},
            ],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [
            {"street": "Main", "zip_code": 12345},
            {"street": "Oak", "zip_code": None},
        ],
    }
    desc = df._df._rust_plan.schema_descriptors()["addr"]
    assert desc["kind"] == "struct"
    assert desc["nullable"] is False


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


def test_dataframe_model_accepts_polars_dataframe_when_validate_data_false() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, 2], "age": [20, None]})
    df = UserDF(pdf, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_polars_dataframe_rejects_column_mismatch() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1], "bad": [2]})
    with pytest.raises(ValueError, match="columns exactly"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_polars_dataframe_rejects_null_in_non_nullable_column() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, None], "age": [10, 20]})
    with pytest.raises(ValueError, match="non-nullable"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_trusted_shape_only_allows_dtype_mismatch() -> None:
    with pytest.warns(DtypeDriftWarning, match="shape_only"):
        df = UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="shape_only")
    assert isinstance(df, UserDF)


def test_shape_only_drift_suppressed_by_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS", "1")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DtypeDriftWarning)
        UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="shape_only")


def test_dataframe_model_trusted_strict_rejects_dtype_mismatch() -> None:
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_rejects_null_in_non_nullable_column() -> None:
    with pytest.raises(ValueError, match="non-nullable"):
        UserDF({"id": [1, None], "age": [20, 30]}, trusted_mode="strict")


def test_validate_columns_strict_trusted_mode_conflicts_with_validate_elements() -> (
    None
):
    from pydantable.schema import validate_columns_strict

    with pytest.raises(ValueError, match="conflicts with trusted_mode"):
        validate_columns_strict(
            {"id": [1], "age": [10]},
            UserDF._SchemaModel,
            validate_elements=True,
            trusted_mode="shape_only",
        )


def test_validate_columns_strict_validate_elements_false_and_trusted_off_conflict() -> (
    None
):
    from pydantable.schema import validate_columns_strict

    with pytest.raises(ValueError, match="conflicts with trusted_mode"):
        validate_columns_strict(
            {"id": [1], "age": [10]},
            UserDF._SchemaModel,
            validate_elements=False,
            trusted_mode="off",
        )


def test_dataframe_model_validate_data_false_collect_matches_trusted_shape_only() -> (
    None
):
    data = {"id": [1, 2], "age": [20, None]}
    with pytest.warns(DeprecationWarning, match="trusted_mode"):
        a = UserDF(data, validate_data=False).collect(as_lists=True)
    b = UserDF(data, trusted_mode="shape_only").collect(as_lists=True)
    assert a == b == data


def test_validate_data_kw_deprecation_warning_once() -> None:
    with pytest.warns(DeprecationWarning, match="0\\.16\\.0"):
        UserDF({"id": [1], "age": [10]}, validate_data=True)


def test_default_constructors_no_validate_data_deprecation() -> None:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        UserDF({"id": [1], "age": [10]})
        DataFrame[UserDF._SchemaModel]({"id": [1], "age": [10]})


def test_dataframe_model_polars_shape_only_warns_on_strict_dtype_mismatch() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": ["x", "y"], "age": [1, 2]})
    with pytest.warns(DtypeDriftWarning, match="shape_only"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_polars_trusted_strict_rejects_wrong_scalar_dtype() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": ["x", "y"], "age": [1, 2]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF(pdf, trusted_mode="strict")


def test_dataframe_model_polars_trusted_strict_accepts_int_columns() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, 2], "age": [10, 20]})
    df = UserDF(pdf, trusted_mode="strict")
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [10, 20]}


def test_dataframe_model_numpy_trusted_strict_int_array() -> None:
    np = pytest.importorskip("numpy")
    df = UserDF(
        {"id": np.array([1, 2, 3], dtype=np.int64), "age": [10, 20, 30]},
        trusted_mode="strict",
    )
    assert df.collect(as_lists=True) == {"id": [1, 2, 3], "age": [10, 20, 30]}


def test_dataframe_model_numpy_trusted_strict_rejects_float_for_int_column() -> None:
    np = pytest.importorskip("numpy")
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF(
            {
                "id": np.array([1.0, 2.0], dtype=np.float64),
                "age": [10, 20],
            },
            trusted_mode="strict",
        )


def test_dataframe_model_trusted_strict_nested_list_shape() -> None:
    class S(Schema):
        xs: list[int]

    DataFrame[S]({"xs": [[1, 2], [3]]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[S]({"xs": [[1.0, 2.0]]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_struct_polars() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    pdf = pl.DataFrame({"s": [{"a": 1}, {"a": 2}]})
    DataFrame[Outer](pdf, trusted_mode="strict")
    bad = pl.DataFrame({"s": [{"a": "x"}, {"a": "y"}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_map_entries_polars() -> None:
    pl = pytest.importorskip("polars")

    class M(Schema):
        m: dict[str, int]

    pdf = pl.DataFrame(
        {
            "m": [
                [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
                [{"key": "c", "value": 3}],
            ]
        }
    )
    DataFrame[M](pdf, trusted_mode="strict")
    bad = pl.DataFrame(
        {
            "m": [
                [{"key": "a", "value": 1.5}],
            ]
        }
    )
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[M](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_dict_python_path() -> None:
    class M(Schema):
        m: dict[str, int]

    DataFrame[M]({"m": [{"a": 1}, {"b": 2}]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[M]({"m": [{"a": 1.0}]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_list_of_lists() -> None:
    class S(Schema):
        xss: list[list[int]]

    DataFrame[S]({"xss": [[[1, 2], [3]], [[4]]]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[S]({"xss": [[[1.0]]]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_optional_nested_list_cell() -> None:
    class S(Schema):
        xs: list[int] | None

    DataFrame[S]({"xs": [[1, 2], None]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_polars_struct_extra_field_rejected() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    bad = pl.DataFrame({"s": [{"a": 1, "b": 2}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_polars_struct_missing_field_rejected() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int
        b: int

    class Outer(Schema):
        s: Inner

    bad = pl.DataFrame({"s": [{"a": 1}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_shape_only_allows_polars_dtype_mismatch_nested() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    pdf = pl.DataFrame({"s": [{"a": "x"}, {"a": "y"}]})
    with pytest.warns(DtypeDriftWarning):
        DataFrame[Outer](pdf, trusted_mode="shape_only")
