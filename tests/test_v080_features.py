"""Stronger coverage for 0.8.0 surfaces: global row count, str→temporal cast, map helpers, window min/max."""

from __future__ import annotations

import calendar
from datetime import date, datetime
from decimal import Decimal

import pytest

from pydantable import DataFrame
from pydantable.expressions import (
    global_count,
    global_row_count,
    global_sum,
    window_max,
    window_min,
)
from pydantable.schema import Schema
from pydantable.window_spec import Window


class T(Schema):
    id: int
    v: int


class Nullable(Schema):
    v: int | None


def test_global_row_count_differs_from_column_count_when_nulls() -> None:
    """``global_row_count`` counts rows; ``global_count(col)`` counts non-null *col* values."""
    df = DataFrame[Nullable]({"v": [1, None, 3]})
    out = df.select(global_row_count(), global_count(df.v)).collect(as_lists=True)
    assert out == {"row_count": [3], "count_v": [2]}


def test_global_row_count_with_other_globals_positional() -> None:
    df = DataFrame[T]({"id": [1, 2], "v": [5, 15]})
    out = df.select(global_row_count(), global_sum(df.v)).collect(as_lists=True)
    assert out == {"row_count": [2], "sum_v": [20]}


def test_global_row_count_after_head() -> None:
    df = DataFrame[T]({"id": [1, 2, 3], "v": [1, 2, 3]})
    out = df.head(2).select(global_row_count()).collect(as_lists=True)
    assert out == {"row_count": [2]}


def test_global_row_count_replicated_in_with_columns() -> None:
    """Global row count lowers to a scalar broadcast across rows (like other global aggs)."""
    df = DataFrame[T]({"id": [1, 2, 3], "v": [10, 20, 30]})
    out = df.with_columns(n=global_row_count()).collect(as_lists=True)
    assert out["n"] == [3, 3, 3]


class StrOpt(Schema):
    s: str | None


def test_cast_nullable_str_to_date_propagates_null() -> None:
    df = DataFrame[StrOpt]({"s": ["2024-06-10", None]})
    out = df.with_columns(d=df.s.cast(date)).collect(as_lists=True)
    assert out["d"] == [date(2024, 6, 10), None]


class StrCol(Schema):
    s: str


def test_cast_str_invalid_iso_to_date_yields_null() -> None:
    df = DataFrame[StrCol]({"s": ["2024-01-01", "not-a-date"]})
    out = df.with_columns(d=df.s.cast(date)).collect(as_lists=True)
    assert out["d"] == [date(2024, 1, 1), None]


def test_cast_str_to_datetime_fractional_seconds() -> None:
    df = DataFrame[StrCol]({"s": ["2024-06-10T00:00:00.5"]})
    out = df.with_columns(ts=df.s.cast(datetime)).collect(as_lists=True)
    exp = calendar.timegm((2024, 6, 10, 0, 0, 0, 0, 0, 0)) + 0.5
    assert abs(out["ts"][0].timestamp() - exp) < 1e-3


class MapStrInt(Schema):
    id: int
    m: dict[str, int]


class MapStrStr(Schema):
    id: int
    m: dict[str, str]


def test_map_get_supports_filtering() -> None:
    df = DataFrame[MapStrInt]({"id": [1, 2], "m": [{"a": 1}, {"a": 5}]})
    out = df.filter(df.m.map_get("a") > 2).collect(as_lists=True)
    assert out == {"id": [2], "m": [{"a": 5}]}


def test_map_get_string_values() -> None:
    df = DataFrame[MapStrStr]({"id": [1], "m": [{"k": "hello"}]})
    out = df.with_columns(v=df.m.map_get("k")).collect(as_lists=True)
    assert out["v"] == ["hello"]


def test_map_get_rejects_non_map_column() -> None:
    df = DataFrame[T]({"id": [1], "v": [42]})
    with pytest.raises(TypeError, match="map_get"):
        df.with_columns(x=df.v.map_get("a"))


def test_map_contains_key_rejects_non_map_column() -> None:
    df = DataFrame[T]({"id": [1], "v": [42]})
    with pytest.raises(TypeError, match="map_contains_key"):
        df.with_columns(x=df.v.map_contains_key("a"))


class W(Schema):
    g: int
    v: int


class WNull(Schema):
    g: int
    v: int | None


class WStr(Schema):
    g: int
    s: str


class WDec(Schema):
    g: int
    v: Decimal


def test_window_min_max_with_order_by_still_partition_extrema() -> None:
    """Polars ``min().over(partition, order_by=...)`` is still the partition min for each row."""
    df = DataFrame[W]({"g": [1, 1, 1], "v": [3, 1, 2]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["lo"] == [1, 1, 1]
    assert out["hi"] == [3, 3, 3]


def test_window_min_max_skip_nulls_in_partition() -> None:
    df = DataFrame[WNull]({"g": [1, 1, 1], "v": [10, None, 30]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["lo"] == [10, 10, 10]
    assert out["hi"] == [30, 30, 30]


def test_window_min_max_decimal_partition() -> None:
    df = DataFrame[WDec](
        {"g": [1, 1], "v": [Decimal("1.5"), Decimal("2.5")]}
    )
    w = Window.partitionBy("g").spec()
    out = df.with_columns(
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["lo"] == [Decimal("1.5"), Decimal("1.5")]
    assert out["hi"] == [Decimal("2.5"), Decimal("2.5")]


def test_window_min_rejects_string_column() -> None:
    df = DataFrame[WStr]({"g": [1, 1], "s": ["a", "b"]})
    w = Window.partitionBy("g").spec()
    with pytest.raises(TypeError, match="min/max"):
        df.with_columns(m=window_min(df.s).over(w))
