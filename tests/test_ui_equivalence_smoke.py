from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta

import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrameModel as PolarsDataFrameModel


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_select_with_columns_filter_collect(
    ui_mod: str,
) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserUi(UiDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 2], "age": [20, None]}

    default_df = UserDefault(payload)
    ui_df = UserUi(payload)

    default_df2 = default_df.with_columns(age2=default_df.age * 2).select("id", "age2")
    default_out = default_df2.filter(default_df2.age2 > 10).collect()

    ui_df2 = ui_df.with_columns(age2=ui_df.age * 2).select("id", "age2")
    ui_out = ui_df2.filter(ui_df2.age2 > 10).collect()

    assert_table_eq_sorted(default_out, ui_out, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_join_and_groupby_all_null_semantics(
    ui_mod: str,
) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class LeftDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    class LeftUi(UiDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightUi(UiDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    left_payload = {"id": [1, 1], "age": [None, None], "score": [10, 20]}
    right_payload = {
        "id": [1, 2],
        "age": [None, None],
        "country": ["US", "CA"],
        "score": [100, 200],
    }

    default_left = LeftDefault(left_payload)
    default_right = RightDefault(right_payload)
    ui_left = LeftUi(left_payload)
    ui_right = RightUi(right_payload)

    default_joined = default_left.join(default_right, on="id", how="inner", suffix="_r")
    ui_joined = ui_left.join(ui_right, on="id", how="inner", suffix="_r")

    default_join_out = default_joined.collect()
    ui_join_out = ui_joined.collect()
    assert_table_eq_sorted(default_join_out, ui_join_out, keys=["id"])

    # all-null group semantics: sum/mean -> None, count -> 0
    default_grouped = default_joined.group_by("id").agg(
        age_sum=("sum", "age"),
        age_mean=("mean", "age"),
        age_count=("count", "age"),
    )
    ui_grouped = ui_joined.group_by("id").agg(
        age_sum=("sum", "age"),
        age_mean=("mean", "age"),
        age_count=("count", "age"),
    )

    default_group_out = default_grouped.collect()
    ui_group_out = ui_grouped.collect()
    assert_table_eq_sorted(default_group_out, ui_group_out, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p1_unary_and_concat(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserUi(UiDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [3, 1, 2, 2], "age": [30, None, 20, 20]}
    default_df = UserDefault(payload)
    ui_df = UserUi(payload)

    default_out = (
        default_df.sort("id")
        .unique(subset=["id", "age"])
        .rename({"age": "years"})
        .slice(1, 2)
        .collect()
    )
    ui_out = (
        ui_df.sort("id")
        .unique(subset=["id", "age"])
        .rename({"age": "years"})
        .slice(1, 2)
        .collect()
    )
    assert_table_eq_sorted(default_out, ui_out, keys=["id"])

    default_cat = PolarsDataFrameModel.concat(
        [default_df.select("id"), default_df.select("id")], how="vertical"
    ).collect()
    ui_cat = UiDataFrameModel.concat(
        [ui_df.select("id"), ui_df.select("id")], how="vertical"
    ).collect()
    assert_table_eq_sorted(default_cat, ui_cat, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p2_fill_drop_cast_null_predicates(
    ui_mod: str,
) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserUi(UiDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 2, 3], "age": [10, None, 30]}
    default_df = UserDefault(payload)
    ui_df = UserUi(payload)

    default_out = (
        default_df.fill_null(0, subset=["age"])
        .with_columns(
            age_f=default_df.age.cast(float), age_is_null=default_df.age.is_null()
        )
        .drop_nulls(subset=["age"])
        .collect()
    )
    ui_out = (
        ui_df.fill_null(0, subset=["age"])
        .with_columns(age_f=ui_df.age.cast(float), age_is_null=ui_df.age.is_null())
        .drop_nulls(subset=["age"])
        .collect()
    )
    assert_table_eq_sorted(default_out, ui_out, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p3_join_variants_and_expr_keys(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class LeftDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    class LeftUi(UiDataFrameModel):
        id: int
        age: int | None
        score: int

    class RightUi(UiDataFrameModel):
        id: int
        age: int | None
        country: str
        score: int

    left_payload = {"id": [1, 2], "age": [10, None], "score": [10, 20]}
    right_payload = {
        "id": [2, 3],
        "age": [None, 30],
        "country": ["US", "CA"],
        "score": [200, 300],
    }

    d_left = LeftDefault(left_payload)
    d_right = RightDefault(right_payload)
    b_left = LeftUi(left_payload)
    b_right = RightUi(right_payload)

    for how in ["right", "semi", "anti", "cross"]:
        if how == "cross":
            d_out = d_left.join(d_right, how=how).collect()
            b_out = b_left.join(b_right, how=how).collect()
        else:
            d_out = d_left.join(d_right, on="id", how=how).collect()
            b_out = b_left.join(b_right, on="id", how=how).collect()
        assert_table_eq_sorted(
            d_out, b_out, keys=["id"] if "id" in d_out else list(d_out.keys())[:1]
        )

    d_expr = d_left.join(
        d_right, left_on=d_left.id, right_on=d_right.id, how="inner"
    ).collect()
    b_expr = b_left.join(
        b_right, left_on=b_left.id, right_on=b_right.id, how="inner"
    ).collect()
    assert_table_eq_sorted(d_expr, b_expr, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p4_groupby_aggregations(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None

    class UserUi(UiDataFrameModel):
        id: int
        age: int | None

    payload = {"id": [1, 1, 2, 2], "age": [10, None, 20, 30]}
    d_df = UserDefault(payload)
    b_df = UserUi(payload)

    d_out = (
        d_df.group_by("id")
        .agg(
            age_min=("min", "age"),
            age_max=("max", "age"),
            age_median=("median", "age"),
            age_std=("std", "age"),
            age_var=("var", "age"),
            age_first=("first", "age"),
            age_last=("last", "age"),
            age_n_unique=("n_unique", "age"),
        )
        .collect()
    )
    b_out = (
        b_df.group_by("id")
        .agg(
            age_min=("min", "age"),
            age_max=("max", "age"),
            age_median=("median", "age"),
            age_std=("std", "age"),
            age_var=("var", "age"),
            age_first=("first", "age"),
            age_last=("last", "age"),
            age_n_unique=("n_unique", "age"),
        )
        .collect()
    )
    assert_table_eq_sorted(d_out, b_out, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p5_reshape_ops(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        key: str
        age: int | None

    class UserUi(UiDataFrameModel):
        id: int
        key: str
        age: int | None

    payload = {
        "id": [1, 1, 2, 2],
        "key": ["A", "B", "A", "B"],
        "age": [10, None, 20, 30],
    }
    d_df = UserDefault(payload)
    b_df = UserUi(payload)

    d_melt = d_df.melt(id_vars=["id"], value_vars=["age"]).collect()
    b_melt = b_df.melt(id_vars=["id"], value_vars=["age"]).collect()
    assert_table_eq_sorted(d_melt, b_melt, keys=["id", "variable"])

    d_pivot = d_df.pivot(
        index="id", columns="key", values="age", aggregate_function="sum"
    ).collect()
    b_pivot = b_df.pivot(
        index="id", columns="key", values="age", aggregate_function="sum"
    ).collect()
    assert_table_eq_sorted(d_pivot, b_pivot, keys=["id"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_p6_rolling_and_dynamic(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class TSDefault(PolarsDataFrameModel):
        id: int
        ts: int
        v: int | None

    class TSUi(UiDataFrameModel):
        id: int
        ts: int
        v: int | None

    payload = {
        "id": [1, 1, 1, 2],
        "ts": [0, 3600, 7200, 0],
        "v": [10, None, 30, 5],
    }
    d_df = TSDefault(payload)
    b_df = TSUi(payload)

    d_roll = d_df.rolling_agg(
        on="ts", column="v", window_size="2h", op="sum", out_name="v_roll", by=["id"]
    ).collect()
    b_roll = b_df.rolling_agg(
        on="ts", column="v", window_size="2h", op="sum", out_name="v_roll", by=["id"]
    ).collect()
    assert_table_eq_sorted(d_roll, b_roll, keys=["id", "ts"])

    d_dyn = (
        d_df.group_by_dynamic("ts", every="1h", by=["id"])
        .agg(v_sum=("sum", "v"))
        .collect()
    )
    b_dyn = (
        b_df.group_by_dynamic("ts", every="1h", by=["id"])
        .agg(v_sum=("sum", "v"))
        .collect()
    )
    assert_table_eq_sorted(d_dyn, b_dyn, keys=["id", "ts"])


@pytest.mark.parametrize("ui_mod", ["pydantable.pandas", "pydantable.pyspark"])
def test_ui_equivalence_temporal_columns_and_literals(ui_mod: str) -> None:
    pkg = importlib.import_module(ui_mod)
    UiDataFrameModel = pkg.DataFrameModel

    class TDefault(PolarsDataFrameModel):
        id: int
        ts: datetime
        d: date
        dur: timedelta

    class TUi(UiDataFrameModel):
        id: int
        ts: datetime
        d: date
        dur: timedelta

    payload = {
        "id": [1, 2],
        "ts": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
        "d": [date(2024, 1, 1), date(2024, 1, 2)],
        "dur": [timedelta(hours=1), timedelta(hours=2)],
    }
    d_df = TDefault(payload)
    b_df = TUi(payload)

    d_out = d_df.filter(d_df.ts > datetime(2024, 1, 1, 12, 0, 0)).collect()
    b_out = b_df.filter(b_df.ts > datetime(2024, 1, 1, 12, 0, 0)).collect()
    assert_table_eq_sorted(d_out, b_out, keys=["id"])


def test_pyspark_select_wrapper_equivalence_to_default() -> None:
    pyspark_mod = importlib.import_module("pydantable.pyspark")
    PySparkDataFrameModel = pyspark_mod.DataFrameModel

    class UserDefault(PolarsDataFrameModel):
        id: int
        age: int | None
        name: str

    class UserPySpark(PySparkDataFrameModel):
        id: int
        age: int | None
        name: str

    payload = {"id": [1, 2], "age": [10, None], "name": ["a", "b"]}
    default_df = UserDefault(payload)
    pyspark_df = UserPySpark(payload)

    default_out = (
        default_df.with_columns(age2=default_df.age * 2)
        .rename({"name": "name_new"})
        .select("id", "name_new", "age2")
        .collect()
    )
    pyspark_out = (
        pyspark_df.withColumn("age2", pyspark_df.age * 2)
        .withColumnRenamed("name", "name_new")
        .select_typed("id", "name_new", "age2")
        .collect()
    )
    assert_table_eq_sorted(default_out, pyspark_out, keys=["id"])
