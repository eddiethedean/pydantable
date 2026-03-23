"""Global aggregates in select (Phase D) — single-row results."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import (
    global_count,
    global_max,
    global_mean,
    global_min,
    global_sum,
)
from pydantable.schema import Schema


class T(Schema):
    id: int
    v: int


def test_select_global_sum_mean_single_row(
    small_two_int_column_dict: dict[str, list[int]],
) -> None:
    df = DataFrame[T](small_two_int_column_dict)
    out = df.select(global_sum(df.v), global_mean(df.v)).collect(as_lists=True)
    assert out == {"sum_v": [60], "mean_v": [20.0]}


def test_select_global_sum_named() -> None:
    df = DataFrame[T]({"id": [1, 2], "v": [5, 15]})
    out = df.select(total=global_sum(df.v)).collect(as_lists=True)
    assert out == {"total": [20]}


class Nullable(Schema):
    v: int | None


def test_global_sum_and_mean_skip_nulls() -> None:
    df = DataFrame[Nullable]({"v": [2, None, 4]})
    out = df.select(global_sum(df.v), global_mean(df.v)).collect(as_lists=True)
    assert out == {"sum_v": [6], "mean_v": [3.0]}


class X(Schema):
    x: int


def test_global_sum_empty_frame_is_zero() -> None:
    df = DataFrame[X]({"x": []})
    out = df.select(global_sum(df.x)).collect(as_lists=True)
    assert out == {"sum_x": [0]}


def test_global_agg_single_row() -> None:
    df = DataFrame[T]({"id": [7], "v": [42]})
    out = df.select(global_sum(df.v), global_mean(df.v)).collect(as_lists=True)
    assert out == {"sum_v": [42], "mean_v": [42.0]}


def test_global_mean_empty_frame_is_null() -> None:
    """Empty input: mean is undefined (None); matches SQL-ish semantics."""
    df = DataFrame[X]({"x": []})
    out = df.select(global_mean(df.x)).collect(as_lists=True)
    assert out == {"mean_x": [None]}


def test_global_sum_mean_all_nulls() -> None:
    """All-null column: sum is 0, mean is None (see INTERFACE_CONTRACT null rules)."""
    df = DataFrame[Nullable]({"v": [None, None]})
    out = df.select(global_sum(df.v), global_mean(df.v)).collect(as_lists=True)
    assert out == {"sum_v": [0], "mean_v": [None]}


def test_select_global_count_min_max() -> None:
    df = DataFrame[T]({"id": [1, 2, 3], "v": [10, 5, 20]})
    out = df.select(global_count(df.v), global_min(df.v), global_max(df.v)).collect(
        as_lists=True
    )
    assert out == {"count_v": [3], "min_v": [5], "max_v": [20]}


def test_global_count_all_nulls_is_zero() -> None:
    df = DataFrame[Nullable]({"v": [None, None]})
    out = df.select(global_count(df.v)).collect(as_lists=True)
    assert out == {"count_v": [0]}
