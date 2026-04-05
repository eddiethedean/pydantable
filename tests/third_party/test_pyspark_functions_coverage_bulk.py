"""Bulk coverage for ``pyspark.sql.functions`` error paths and thin wrappers."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pydantable.pyspark import DataFrame
from pydantable.pyspark.sql import functions as F
from pydantable.schema import Schema


class _Mix(Schema):
    s: str
    n: int
    n2: int
    items_str: list[str]
    items_int: list[int]
    ts: datetime


def test_functions_list_mean_join_sort_distinct_numeric_wrappers() -> None:
    df = DataFrame[_Mix](
        {
            "s": ["x"],
            "n": [-3],
            "n2": [4],
            "items_str": [["a", "b"]],
            "items_int": [[3, 1, 2]],
            "ts": [datetime(2024, 1, 1, 12, 0, 0)],
        }
    )
    it = F.col("items_int", dtype=list[int])
    its = F.col("items_str", dtype=list[str])
    fn = F.col("n", dtype=int)
    out = (
        df.withColumn("mn", F.list_mean(it))
        .withColumn("mj", F.list_join(its, ","))
        .withColumn("ms", F.list_sort(it, descending=True))
        .withColumn("ad", F.array_distinct(it, stable=True))
        .withColumn("ab", F.abs(fn))
        .withColumn("rd", F.round(F.col("n2", dtype=int), 1))
        .withColumn("fl", F.floor(F.col("n2", dtype=float)))
        .withColumn("cl", F.ceil(F.col("n2", dtype=float)))
        .collect(as_lists=True)
    )
    assert out["ab"][0] == 3
    assert out["mn"][0] == 2.0


def test_functions_typeerrors_for_non_expr() -> None:
    with pytest.raises(TypeError):
        F.list_mean([1])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.list_join([1], ",")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.list_sort([1])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.map_keys({})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.map_values({})  # type: ignore[arg-type]


def test_functions_global_agg_specs_and_typeerrors() -> None:
    with pytest.raises(TypeError):
        F.sum(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.avg(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.max([])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.min(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.count(())  # type: ignore[arg-type]

    s = F.sum("n")
    assert s._op == "sum" and s._col == "n"  # type: ignore[attr-defined]
    assert F.count() is not None


def test_grouped_agg_spec_alias_errors() -> None:
    s = F.sum("n")
    with pytest.raises(TypeError, match="non-empty"):
        s.alias("")  # type: ignore[union-attr]
    aliased = s.alias("out")
    assert aliased._out_name == "out"  # type: ignore[attr-defined]


def test_functions_greatest_int_columns_and_coalesce_validation() -> None:
    df = DataFrame[_Mix](
        {
            "s": ["a"],
            "n": [1],
            "n2": [5],
            "items_str": [["x"]],
            "items_int": [[1]],
            "ts": [datetime(2024, 1, 1)],
        }
    )
    cn = F.col("n", dtype=int)
    cn2 = F.col("n2", dtype=int)
    with pytest.raises(TypeError, match="at least two"):
        F.greatest(cn)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        F.greatest(cn, 1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        F.coalesce(cn, 1)  # type: ignore[arg-type]

    g = F.greatest(cn, cn2)
    out = df.withColumn("g", g).collect(as_lists=True)
    assert out["g"][0] == 5


def test_functions_explode_rejects_select_style() -> None:
    with pytest.raises(TypeError, match="DataFrame.explode"):
        F.explode(F.col("s", dtype=str))


def test_functions_calendar_on_datetime_column() -> None:
    df = DataFrame[_Mix](
        {
            "s": ["a"],
            "n": [1],
            "n2": [2],
            "items_str": [["x"]],
            "items_int": [[1]],
            "ts": [datetime(2024, 6, 15, 12, 30, 45)],
        }
    )
    cs = F.col("ts", dtype=datetime)
    out = (
        df.withColumn("y", F.year(cs))
        .withColumn("mo", F.month(cs))
        .withColumn("d", F.day(cs))
        .withColumn("dom", F.dayofmonth(cs))
        .withColumn("dw", F.dayofweek(cs))
        .withColumn("q", F.quarter(cs))
        .withColumn("wk", F.weekofyear(cs))
        .withColumn("dy", F.dayofyear(cs))
        .withColumn("h", F.hour(cs))
        .withColumn("mi", F.minute(cs))
        .withColumn("sec", F.second(cs))
        .collect(as_lists=True)
    )
    assert out["y"][0] == 2024
    assert out["mo"][0] == 6
    assert isinstance(out["h"][0], int) and isinstance(out["mi"][0], int)


def test_functions_string_helpers_and_length() -> None:
    df = DataFrame[_Mix](
        {
            "s": [" Ab "],
            "n": [1],
            "n2": [2],
            "items_str": [["a"]],
            "items_int": [[1]],
            "ts": [datetime(2024, 1, 1)],
        }
    )
    c = F.col("s", dtype=str)
    out = (
        df.withColumn("lo", F.lower(c))
        .withColumn("up", F.upper(c))
        .withColumn("tr", F.trim(c))
        .withColumn("sub", F.substring(c, 2, 2))
        .withColumn("ln", F.length(c))
        .collect(as_lists=True)
    )
    assert out["lo"][0] == " ab "
    assert out["tr"][0] == "Ab"
    assert out["ln"][0] == 4


def test_functions_concat() -> None:
    df = DataFrame[_Mix](
        {
            "s": ["a"],
            "n": [1],
            "n2": [2],
            "items_str": [["x"]],
            "items_int": [[1]],
            "ts": [datetime(2024, 1, 1)],
        }
    )
    a = F.col("s", dtype=str)
    b = F.col("n", dtype=int)
    out = df.withColumn("cc", F.concat(a, F.lit("-"), F.cast(b, str))).collect(
        as_lists=True
    )
    assert out["cc"][0] == "a-1"


def test_functions_isnull_coalesce() -> None:
    class N(Schema):
        x: int | None

    df = DataFrame[N]({"x": [None, 1]})
    cx = F.col("x", dtype=int | None)
    out = (
        df.withColumn("a", F.isnull(cx))
        .withColumn("b", F.isnotnull(cx))
        .withColumn("c", F.coalesce(cx, F.lit(0)))
        .collect(as_lists=True)
    )
    assert out["a"] == [True, False]


def test_functions_window_helpers_return_exprs() -> None:
    n = F.col("n", dtype=int)
    assert F.window_sum(n) is not None
    assert F.window_avg(n) is not None
    assert F.window_min(n) is not None
    assert F.window_max(n) is not None
    assert F.lag(n, 1) is not None
    assert F.lead(n, 1) is not None
