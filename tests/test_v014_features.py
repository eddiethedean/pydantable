"""Regression tests for 0.14.0: trusted ingest, windows, PySpark façade, FastAPI."""

from __future__ import annotations

import warnings
from datetime import date

import pytest
from pydantable import DataFrame
from pydantable.expressions import lag, row_number
from pydantable.pyspark import DataFrame as PSDataFrame
from pydantable.pyspark.sql import functions as F
from pydantable.schema import DtypeDriftWarning, Schema
from pydantable.window_spec import Window


class TwoInt(Schema):
    id: int
    age: int


class TwoCol(Schema):
    a: int
    b: int


def test_dataframe_validate_data_false_emits_deprecation() -> None:
    with pytest.warns(DeprecationWarning, match="trusted_mode"):
        DataFrame[TwoInt]({"id": [1], "age": [2]}, validate_data=False)


def test_dataframe_validate_data_true_emits_deprecation() -> None:
    with pytest.warns(DeprecationWarning, match="0\\.16\\.0"):
        DataFrame[TwoInt]({"id": [1], "age": [2]}, validate_data=True)


def test_dataframe_trusted_mode_with_validate_data_false_no_deprecation() -> None:
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always", DeprecationWarning)
        DataFrame[TwoInt](
            {"id": [1], "age": [2]},
            trusted_mode="strict",
            validate_data=False,
        )
    assert not any(
        issubclass(r.category, DeprecationWarning) and "validate_data" in str(r.message)
        for r in rec
    )


def test_dataframe_trusted_mode_only_no_deprecation() -> None:
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always", DeprecationWarning)
        DataFrame[TwoInt]({"id": [1], "age": [2]}, trusted_mode="shape_only")
    assert not any(issubclass(r.category, DeprecationWarning) for r in rec)


def test_shape_only_float_column_warns_drift_once_per_column() -> None:
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        DataFrame[TwoCol]({"a": [1.0, 2.0], "b": [3.0, 4.0]}, trusted_mode="shape_only")
    drift = [w for w in rec if issubclass(w.category, DtypeDriftWarning)]
    assert len(drift) == 2
    msgs = {str(w.message) for w in drift}
    assert any("'a'" in m for m in msgs)
    assert any("'b'" in m for m in msgs)


def test_shape_only_no_drift_when_values_match_strict() -> None:
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        DataFrame[TwoInt]({"id": [1, 2], "age": [3, 4]}, trusted_mode="shape_only")
    assert not any(issubclass(w.category, DtypeDriftWarning) for w in rec)


class Wg(Schema):
    g: int
    v: int | None


def test_row_number_nulls_order_independent_partitions() -> None:
    df = DataFrame[Wg](
        {
            "g": [1, 1, 2, 2],
            "v": [None, 5, None, 7],
        }
    )
    w = Window.partitionBy("g").orderBy("v", ascending=True, nulls_last=False)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert out["rn"] == [1, 2, 1, 2]


def test_row_number_all_nulls_in_partition_still_dense_1_to_n() -> None:
    df = DataFrame[Wg]({"g": [1, 1, 1], "v": [None, None, None]})
    w = Window.partitionBy("g").orderBy("v", ascending=True, nulls_last=True)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert sorted(out["rn"]) == [1, 2, 3]


def test_order_by_multi_column_nulls_last_list_runs() -> None:
    class WM(Schema):
        g: int
        a: int
        b: int

    df = DataFrame[WM](
        {
            "g": [1, 1, 1],
            "a": [1, 1, 2],
            "b": [10, 20, 30],
        }
    )
    w = Window.partitionBy("g").orderBy("a", "b", nulls_last=[True, False])
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert sorted(out["rn"]) == [1, 2, 3]


def test_lag_respects_nulls_last_in_window_order() -> None:
    """Lag follows partition sort; null placement changes the prior row."""
    df = DataFrame[Wg]({"g": [1, 1, 1], "v": [None, 5, 10]})
    w_first = Window.partitionBy("g").orderBy("v", ascending=True, nulls_last=False)
    w_last = Window.partitionBy("g").orderBy("v", ascending=True, nulls_last=True)
    out_nf = df.with_columns(x=lag(df.v, 1).over(w_first)).collect(as_lists=True)
    out_nl = df.with_columns(x=lag(df.v, 1).over(w_last)).collect(as_lists=True)
    assert out_nf["x"] != out_nl["x"]
    assert out_nf["x"] == [None, None, 5]
    assert out_nl["x"] == [10, None, 5]


class _Dates(Schema):
    d: date
    s: str


def test_pyspark_dayofmonth_lower_upper() -> None:
    df = PSDataFrame[_Dates](
        {
            "d": [date(2024, 3, 15), date(2025, 12, 1)],
            "s": ["HeLLo", "PySpark"],
        }
    )
    out = (
        df.withColumn("dom", F.dayofmonth(df.d))
        .withColumn("sl", F.lower(F.col("s", dtype=str)))
        .withColumn("su", F.upper(F.col("s", dtype=str)))
        .collect(as_lists=True)
    )
    assert out["dom"] == [15, 1]
    assert out["sl"] == ["hello", "pyspark"]
    assert out["su"] == ["HELLO", "PYSPARK"]


class _NumStr(Schema):
    x: int
    y: float
    s: str


def test_pyspark_trim_abs_round_floor_ceil() -> None:
    df = PSDataFrame[_NumStr]({"x": [-1, 2], "y": [-1.4, 1.6], "s": ["  hi ", "x"]})
    out = (
        df.withColumn("ax", F.abs(F.col("x", dtype=int)))
        .withColumn("ay", F.abs(F.col("y", dtype=float)))
        .withColumn("tr", F.trim(F.col("s", dtype=str)))
        .withColumn("rd", F.round(F.col("y", dtype=float), 0))
        .withColumn("fl", F.floor(F.col("y", dtype=float)))
        .withColumn("cl", F.ceil(F.col("y", dtype=float)))
        .collect(as_lists=True)
    )
    assert out["ax"] == [1, 2]
    assert out["ay"] == [1.4, 1.6]
    assert out["tr"] == ["hi", "x"]
    assert out["rd"] == [-1.0, 2.0]
    assert out["fl"] == [-2.0, 1.0]
    assert out["cl"] == [-1.0, 2.0]


pytest.importorskip("fastapi")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from pydantable import DataFrameModel  # noqa: E402
from pydantic import BaseModel  # noqa: E402


class _UserDF(DataFrameModel):
    id: int
    age: int | None


class _UserRow(BaseModel):
    id: int
    age: int | None


def test_fastapi_post_invalid_type_returns_422() -> None:
    app = FastAPI()

    @app.post("/users", response_model=list[_UserRow])
    def create_users(rows: list[_UserDF.RowModel]):
        df = _UserDF(rows)
        return df.collect()

    client = TestClient(app)
    r = client.post("/users", json=[{"id": "not-an-int", "age": 1}])
    assert r.status_code == 422


def test_fastapi_openapi_contains_request_body_schema() -> None:
    app = FastAPI()

    @app.post("/bulk")
    def bulk(body: dict[str, list]):
        return _UserDF(body, trusted_mode="shape_only").to_dict()

    spec = TestClient(app).get("/openapi.json").json()
    post = spec["paths"]["/bulk"]["post"]
    assert "requestBody" in post
