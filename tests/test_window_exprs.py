"""Window expressions (row_number, window sum) via Polars lowering."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
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


class W3(Schema):
    g: int
    v: int | None


class W4(Schema):
    g: int
    o: int
    v: int | None


class W5(Schema):
    g: int
    o1: int
    o2: int
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


def test_window_spec_rows_between_is_threaded_to_expr_ir() -> None:
    spec = Window.partitionBy("g").orderBy("v").rowsBetween(-1, 1)
    expr = window_sum(DataFrame[W]({"g": [1], "v": [1]}).v).over(spec)
    payload = expr._rust_expr.to_serializable()
    assert payload["kind"] == "window"
    assert payload["frame"]["kind"] == "rows"
    assert payload["frame"]["start"] == -1
    assert payload["frame"]["end"] == 1


def test_window_spec_range_between_is_threaded_to_expr_ir() -> None:
    spec = Window.partitionBy("g").orderBy("v").rangeBetween(-2, 0)
    expr = window_sum(DataFrame[W]({"g": [1], "v": [1]}).v).over(spec)
    payload = expr._rust_expr.to_serializable()
    assert payload["kind"] == "window"
    assert payload["frame"]["kind"] == "range"
    assert payload["frame"]["start"] == -2
    assert payload["frame"]["end"] == 0


def test_rows_between_running_sum_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 50]


def test_range_between_running_sum_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 11, 14]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-2, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 21, 14]


def test_range_between_running_mean_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 11, 14]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-2, 0)
    out = df.with_columns(m=window_mean(df.v).over(w)).collect(as_lists=True)
    assert out["m"] == [10.0, 10.5, 14.0]


def test_range_between_includes_boundary_values() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 12, 14]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-2, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 22, 26]


def test_rows_between_respects_partitions() -> None:
    df = DataFrame[W]({"g": [1, 1, 2, 2], "v": [10, 20, 100, 200]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 100, 300]


def test_rows_between_large_bounds_cover_full_partition() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-999, 999)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [60, 60, 60]


def test_rows_between_descending_order_running_sum() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v", ascending=False).rowsBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 50, 30]


def test_rows_between_window_sum_skips_nulls() -> None:
    df = DataFrame[W4]({"g": [1, 1, 1], "o": [1, 2, 3], "v": [10, None, 20]})
    w = Window.partitionBy("g").orderBy("o").rowsBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 10, 20]


def test_range_between_rejects_unsupported_order_column() -> None:
    class R(Schema):
        g: int
        v: str
        x: int

    df = DataFrame[R]({"g": [1, 1], "v": ["a", "b"], "x": [10, 20]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-1, 0)
    with pytest.raises(TypeError, match="numeric/date/datetime/duration"):
        df.with_columns(s=window_sum(df.x).over(w)).collect(as_lists=True)


def test_range_between_running_sum_float_order_contract() -> None:
    class RF(Schema):
        g: int
        o: float
        v: int

    df = DataFrame[RF]({"g": [1, 1, 1], "o": [1.0, 1.5, 3.0], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 30]


def test_range_between_running_sum_date_order_contract() -> None:
    class RD(Schema):
        g: int
        o: date
        v: int

    df = DataFrame[RD](
        {
            "g": [1, 1, 1],
            "o": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 4)],
            "v": [10, 20, 30],
        }
    )
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 30]


def test_range_between_running_sum_datetime_order_contract() -> None:
    class RDT(Schema):
        g: int
        o: datetime
        v: int

    base = datetime(2024, 1, 1, 0, 0, 0)
    df = DataFrame[RDT](
        {
            "g": [1, 1, 1],
            "o": [base, base + timedelta(seconds=1), base + timedelta(seconds=3)],
            "v": [10, 20, 30],
        }
    )
    # Datetime range bounds are interpreted in microseconds.
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1_000_000, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 30]


def test_range_between_duration_order_running_sum_contract() -> None:
    """``rangeBetween`` bounds on ``timedelta`` order keys use microseconds."""

    class RDur(Schema):
        g: int
        o: timedelta
        v: int

    df = DataFrame[RDur](
        {
            "g": [1, 1, 1],
            "o": [
                timedelta(seconds=0),
                timedelta(seconds=1),
                timedelta(seconds=3),
            ],
            "v": [10, 20, 30],
        }
    )
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1_000_000, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [10, 30, 30]


def test_range_between_float_order_mean_min_max_contract() -> None:
    class RF(Schema):
        g: int
        o: float
        v: int

    df = DataFrame[RF]({"g": [1, 1, 1], "o": [1.0, 2.0, 4.0], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1.0, 0.0)
    out = df.with_columns(
        m=window_mean(df.v).over(w),
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["m"] == [10.0, 15.0, 30.0]
    assert out["lo"] == [10, 10, 30]
    assert out["hi"] == [10, 20, 30]


def test_range_between_date_order_respects_partitions() -> None:
    class RD(Schema):
        g: int
        o: date
        v: int

    df = DataFrame[RD](
        {
            "g": [1, 1, 2, 2],
            "o": [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 1),
                date(2024, 1, 3),
            ],
            "v": [100, 200, 1000, 2000],
        }
    )
    w = Window.partitionBy("g").orderBy("o").rangeBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [100, 300, 1000, 2000]


def test_rows_between_mean_min_max_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-1, 0)
    out = df.with_columns(
        m=window_mean(df.v).over(w),
        lo=window_min(df.v).over(w),
        hi=window_max(df.v).over(w),
    ).collect(as_lists=True)
    assert out["m"] == [10.0, 15.0, 25.0]
    assert out["lo"] == [10, 10, 20]
    assert out["hi"] == [10, 20, 30]


def test_rows_between_lag_and_lead_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-1, 1)
    out = df.with_columns(lg=lag(df.v, 1).over(w), ld=lead(df.v, 1).over(w)).collect(
        as_lists=True
    )
    assert out["lg"] == [None, 10, 20]
    assert out["ld"] == [20, 30, None]


def test_rows_between_rank_dense_rank_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1, 1], "v": [10, 10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-2, 0)
    out = df.with_columns(r=rank().over(w), d=dense_rank().over(w)).collect(
        as_lists=True
    )
    assert out["r"] == [1, 1, 3, 4]
    assert out["d"] == [1, 1, 2, 3]


def test_range_between_min_max_contract() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 11, 14]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-2, 0)
    out = df.with_columns(
        lo=window_min(df.v).over(w), hi=window_max(df.v).over(w)
    ).collect(as_lists=True)
    assert out["lo"] == [10, 10, 14]
    assert out["hi"] == [10, 11, 14]


def test_range_between_rejects_rank_and_lag() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-1, 0)
    with pytest.raises(TypeError, match=r"rank\(\) does not support rangeBetween"):
        df.with_columns(r=rank().over(w))
    with pytest.raises(TypeError, match=r"lag\(\) does not support rangeBetween"):
        df.with_columns(l=lag(df.v, 1).over(w))


def test_range_between_rejects_dense_rank_and_lead() -> None:
    df = DataFrame[W]({"g": [1, 1, 1], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v").rangeBetween(-1, 0)
    with pytest.raises(TypeError, match=r"rank\(\) does not support rangeBetween"):
        df.with_columns(d=dense_rank().over(w))
    with pytest.raises(TypeError, match=r"lead\(\) does not support rangeBetween"):
        df.with_columns(l=lead(df.v, 1).over(w))


def test_range_between_multi_order_columns_uses_first_key_for_range() -> None:
    """RANGE bounds use the first order column; extra keys only break ties."""
    df = DataFrame[W5](
        {
            "g": [1, 1, 1, 1],
            "o1": [1, 1, 2, 2],
            "o2": [1, 2, 0, 1],
            "v": [10, 20, 30, 40],
        }
    )
    w = Window.partitionBy("g").orderBy("o1", "o2").rangeBetween(0, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 30, 70, 70]


def test_range_between_multi_order_wide_frame_on_first_key() -> None:
    df = DataFrame[W5](
        {
            "g": [1, 1, 1, 1],
            "o1": [1, 1, 2, 2],
            "o2": [2, 1, 1, 2],
            "v": [100, 200, 300, 400],
        }
    )
    w = Window.partitionBy("g").orderBy("o1", "o2").rangeBetween(-1, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [300, 300, 1000, 1000]


def test_range_between_requires_order_by_when_range_frame() -> None:
    df = DataFrame[W]({"g": [1, 1], "v": [10, 20]})
    w = Window.partitionBy("g").rangeBetween(-1, 0)
    with pytest.raises(ValueError, match="at least one order_by"):
        df.with_columns(s=window_sum(df.v).over(w))


def test_range_between_multi_order_desc_first_key_range_on_that_axis() -> None:
    """Descending first order column: range deltas still use first key values."""
    df = DataFrame[W5](
        {
            "g": [1, 1, 1, 1],
            "o1": [3, 3, 2, 2],
            "o2": [1, 2, 0, 1],
            "v": [10, 20, 30, 40],
        }
    )
    w = (
        Window.partitionBy("g")
        .orderBy("o1", "o2", ascending=[False, True])
        .rangeBetween(0, 0)
    )
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 30, 70, 70]


def test_range_between_multi_order_mixed_asc_desc_second_key() -> None:
    df = DataFrame[W5](
        {
            "g": [1, 1, 1, 1],
            "o1": [1, 1, 2, 2],
            "o2": [2, 1, 5, 0],
            "v": [100, 200, 300, 400],
        }
    )
    w = (
        Window.partitionBy("g")
        .orderBy("o1", "o2", ascending=[True, False])
        .rangeBetween(0, 0)
    )
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [300, 300, 700, 700]


def test_range_between_multi_order_across_partitions_independent() -> None:
    df = DataFrame[W5](
        {
            "g": [1, 1, 2, 2],
            "o1": [1, 1, 10, 10],
            "o2": [1, 2, 1, 2],
            "v": [1, 2, 100, 200],
        }
    )
    w = Window.partitionBy("g").orderBy("o1", "o2").rangeBetween(0, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [3, 3, 300, 300]


def test_range_between_multi_order_window_mean_and_min() -> None:
    df = DataFrame[W5](
        {
            "g": [1, 1, 1],
            "o1": [1, 1, 2],
            "o2": [1, 2, 0],
            "v": [10, 30, 100],
        }
    )
    w = Window.partitionBy("g").orderBy("o1", "o2").rangeBetween(0, 0)
    out = df.with_columns(
        m=window_mean(df.v).over(w),
        lo=window_min(df.v).over(w),
    ).collect(as_lists=True)
    assert out["m"] == [20.0, 20.0, 100.0]
    assert out["lo"] == [10, 10, 100]


class WDate(Schema):
    g: int
    d: date
    tb: int
    v: int


def test_range_between_multi_order_date_first_key() -> None:
    df = DataFrame[WDate](
        {
            "g": [1, 1, 1, 1],
            "d": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 1, 3),
                date(2024, 1, 3),
            ],
            "tb": [1, 2, 0, 1],
            "v": [10, 20, 30, 40],
        }
    )
    w = Window.partitionBy("g").orderBy("d", "tb").rangeBetween(0, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 30, 70, 70]


class WDt(Schema):
    g: int
    ts: datetime
    tb: int
    v: int


def test_range_between_multi_order_datetime_first_key() -> None:
    df = DataFrame[WDt](
        {
            "g": [1, 1, 1, 1],
            "ts": [
                datetime(2024, 1, 1, 12, 0, 0),
                datetime(2024, 1, 1, 12, 0, 0),
                datetime(2024, 1, 1, 14, 0, 0),
                datetime(2024, 1, 1, 14, 0, 0),
            ],
            "tb": [1, 2, 0, 1],
            "v": [1, 2, 4, 8],
        }
    )
    w = Window.partitionBy("g").orderBy("ts", "tb").rangeBetween(0, 0)
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [3, 3, 12, 12]
