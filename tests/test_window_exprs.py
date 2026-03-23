"""Window expressions (row_number, window sum) via Polars lowering."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import (
    dense_rank,
    lag,
    lead,
    rank,
    row_number,
    window_max,
    window_mean,
    window_min,
    window_sum,
)
from pydantable.schema import Schema
from pydantable.window_spec import Window


class W(Schema):
    g: int
    v: int


class W2(Schema):
    g: int
    h: int
    v: int


def test_row_number_over_partition_order() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert out["rn"] == [1, 2, 1]


def test_window_sum_over_partition() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 30, 30]


def test_rank_and_dense_rank_ties_follow_sql_semantics() -> None:
    """Ties: same rank; `rank` leaves gaps, `dense_rank` does not."""
    df = DataFrame[W]({"g": [1, 1, 1, 2], "v": [10, 10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(r=rank().over(w), d=dense_rank().over(w)).collect(
        as_lists=True
    )
    assert out["r"] == [1, 1, 3, 1]
    assert out["d"] == [1, 1, 2, 1]


def test_window_mean_over_partition_without_order() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(m=window_mean(df.v).over(w)).collect(as_lists=True)
    assert out["m"] == [15.0, 15.0, 30.0]


def test_window_min_max_over_partition() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["lo"] == [10, 10, 30]
    assert out["hi"] == [20, 20, 30]


def test_row_number_descending_order_by() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v", ascending=False)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert out["v"] == [10, 20, 30]
    assert out["rn"] == [3, 2, 1]


def test_window_multi_column_partition_by() -> None:
    df = DataFrame[W2]({"g": [1, 1, 2, 2], "h": [1, 2, 1, 1], "v": [1, 2, 3, 4]})
    w = Window.partitionBy("g", "h").orderBy("v", ascending=True)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert out["rn"] == [1, 1, 1, 2]


def test_window_sum_single_row_partition() -> None:
    """One row per partition: window sum equals that row's value."""
    df = DataFrame[W]({"g": [1, 2], "v": [100, 200]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [100, 200]


def test_order_by_per_column_ascending_flags() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [3, 1, 2]})
    w_asc = Window.partitionBy("g").orderBy("v", ascending=[True])
    out_asc = df.with_columns(rn=row_number().over(w_asc)).collect(as_lists=True)
    assert out_asc["rn"] == [3, 1, 2]

    w_desc = Window.partitionBy("g").orderBy("v", ascending=[False])
    out_desc = df.with_columns(rn=row_number().over(w_desc)).collect(as_lists=True)
    assert out_desc["rn"] == [1, 3, 2]


def test_lag_and_lead_shift_within_partition() -> None:
    df = DataFrame[W]({"g": [1, 1, 1, 2], "v": [10, 20, 30, 40]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(
        lg=lag(df.v, 1).over(w),
        ld=lead(df.v, 1).over(w),
    ).collect(as_lists=True)
    assert out["lg"] == [None, 10, 20, None]
    assert out["ld"] == [20, 30, None, None]


def test_lag_and_lead_offset_two_within_partition() -> None:
    """0.7.0 ``lag`` / ``lead`` with default offset 1; larger *n* shifts further."""
    df = DataFrame[W]({"g": [1, 1, 1, 1], "v": [10, 20, 30, 40]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(
        lg2=lag(df.v, 2).over(w),
        ld2=lead(df.v, 2).over(w),
    ).collect(as_lists=True)
    assert out["lg2"] == [None, None, 10, 20]
    assert out["ld2"] == [30, 40, None, None]
