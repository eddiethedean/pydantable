"""Regression and edge-case tests for 0.15.0 (PySpark helpers, cross-feature checks)."""

from __future__ import annotations

from pydantable.pyspark import DataFrame as PSDataFrame
from pydantable.pyspark.sql import functions as F
from pydantable.schema import Schema


class _FloatStr(Schema):
    y: float
    s: str


def test_pyspark_round_fractional_scales() -> None:
    df = PSDataFrame[_FloatStr]({"y": [1.2345, -9.8765], "s": ["a", "b"]})
    out = (
        df.withColumn("r0", F.round(F.col("y", dtype=float), 0))
        .withColumn("r2", F.round(F.col("y", dtype=float), 2))
        .collect(as_lists=True)
    )
    assert out["r0"] == [1.0, -10.0]
    assert out["r2"] == [1.23, -9.88]


def test_pyspark_trim_whitespace_only_and_mixed() -> None:
    df = PSDataFrame[_FloatStr](
        {"y": [0.0, 0.0, 0.0], "s": ["   ", "\tfoo\n", "  bar  "]}
    )
    out = df.withColumn("t", F.trim(F.col("s", dtype=str))).collect(as_lists=True)
    assert out["t"] == ["", "foo", "bar"]


class _OptStr(Schema):
    s: str | None


def test_pyspark_trim_nullable_string_column() -> None:
    df = PSDataFrame[_OptStr]({"s": [None, "  x  "]})
    out = df.withColumn("t", F.trim(F.col("s", dtype=str | None))).collect(
        as_lists=True
    )
    assert out["t"] == [None, "x"]


class _XY(Schema):
    x: int
    y: float


def test_pyspark_abs_float_only_and_zero() -> None:
    df = PSDataFrame[_XY]({"x": [0, -0], "y": [0.0, -2.5]})
    out = (
        df.withColumn("ax", F.abs(F.col("x", dtype=int)))
        .withColumn("ay", F.abs(F.col("y", dtype=float)))
        .collect(as_lists=True)
    )
    assert out["ax"] == [0, 0]
    assert out["ay"] == [0.0, 2.5]


def test_pyspark_floor_ceil_midpoints() -> None:
    df = PSDataFrame[_FloatStr]({"y": [2.5, -2.5, 0.0], "s": ["", "", ""]})
    out = (
        df.withColumn("fl", F.floor(F.col("y", dtype=float)))
        .withColumn("cl", F.ceil(F.col("y", dtype=float)))
        .collect(as_lists=True)
    )
    assert out["fl"] == [2.0, -3.0, 0.0]
    assert out["cl"] == [3.0, -2.0, 0.0]


def test_pyspark_functions_match_core_expr_semantics() -> None:
    """Spark wrappers delegate to Expr; same pipeline via core API should match."""
    from pydantable import DataFrame

    class VT(Schema):
        v: int
        t: str

    data = {"v": [-3, 4], "t": ["  ab ", "  cd "]}
    c = DataFrame[VT](data)
    ps = PSDataFrame[VT](data)
    core_out = c.with_columns(
        av=c.v.abs(),
        tt=c.t.strip(),
    ).to_dict()
    spark_out = (
        ps.withColumn("av", F.abs(F.col("v", dtype=int)))
        .withColumn("tt", F.trim(F.col("t", dtype=str)))
        .to_dict()
    )
    assert core_out["av"] == spark_out["av"] == [3, 4]
    assert core_out["tt"] == spark_out["tt"] == ["ab", "cd"]
