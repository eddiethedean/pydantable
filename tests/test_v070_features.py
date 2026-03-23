"""Stronger coverage for 0.7.0 surfaces: globals, strptime/unix_timestamp, map/binary len, temporal parts."""

from __future__ import annotations

import calendar
from datetime import date, datetime, time, timezone

import pytest

from pydantable import DataFrame
from pydantable.expressions import (
    global_count,
    global_max,
    global_min,
)
from pydantable.schema import Schema


class T(Schema):
    id: int
    v: int


class Nullable(Schema):
    v: int | None


class X(Schema):
    x: int


def test_global_count_empty_frame_is_zero() -> None:
    df = DataFrame[X]({"x": []})
    out = df.select(global_count(df.x)).collect(as_lists=True)
    assert out == {"count_x": [0]}


def test_global_min_max_empty_frame_are_null() -> None:
    df = DataFrame[X]({"x": []})
    out = df.select(global_min(df.x), global_max(df.x)).collect(as_lists=True)
    assert out == {"min_x": [None], "max_x": [None]}


def test_global_min_max_all_nulls_are_null() -> None:
    df = DataFrame[Nullable]({"v": [None, None]})
    out = df.select(global_min(df.v), global_max(df.v)).collect(as_lists=True)
    assert out == {"min_v": [None], "max_v": [None]}


def test_global_min_max_single_null_only_column() -> None:
    df = DataFrame[Nullable]({"v": [None]})
    out = df.select(global_min(df.v), global_max(df.v)).collect(as_lists=True)
    assert out == {"min_v": [None], "max_v": [None]}


class StrCol(Schema):
    s: str


def test_strptime_to_datetime_true_returns_datetime_not_date() -> None:
    df = DataFrame[StrCol]({"s": ["2024-01-01"]})
    out = df.with_columns(ts=df.s.strptime("%Y-%m-%d", to_datetime=True)).collect(
        as_lists=True
    )
    assert type(out["ts"][0]) is datetime


def test_strptime_to_date_returns_date() -> None:
    df = DataFrame[StrCol]({"s": ["2024-01-01"]})
    out = df.with_columns(d=df.s.strptime("%Y-%m-%d", to_datetime=False)).collect(
        as_lists=True
    )
    assert out["d"] == [date(2024, 1, 1)]


def test_strptime_invalid_string_raises_on_collect() -> None:
    df = DataFrame[StrCol]({"s": ["not-a-date"]})
    with pytest.raises(ValueError, match="strptime|conversion|str"):
        df.with_columns(d=df.s.strptime("%Y-%m-%d", to_datetime=False)).collect()


def test_strptime_empty_format_raises() -> None:
    df = DataFrame[StrCol]({"s": ["2024-01-01"]})
    with pytest.raises(ValueError, match="non-empty"):
        df.with_columns(d=df.s.strptime("", to_datetime=False))


def test_strptime_requires_string_column() -> None:
    df = DataFrame[T]({"id": [1], "v": [1]})
    with pytest.raises(TypeError, match="string column"):
        df.with_columns(d=df.v.strptime("%Y", to_datetime=False))


class D(Schema):
    d: date


class DT(Schema):
    ts: datetime


def test_unix_timestamp_seconds_matches_utc_instant() -> None:
    ts = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    df = DataFrame[DT]({"ts": [ts]})
    out = df.with_columns(u=df.ts.unix_timestamp("seconds")).collect(as_lists=True)
    exp = int(calendar.timegm((2020, 1, 1, 12, 0, 0, 0, 0, 0)))
    assert out["u"] == [exp]


def test_unix_timestamp_milliseconds_is_seconds_times_1000() -> None:
    ts = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    df = DataFrame[DT]({"ts": [ts]})
    out = df.with_columns(sec=df.ts.unix_timestamp("seconds"), ms=df.ts.unix_timestamp("ms")).collect(
        as_lists=True
    )
    assert out["ms"][0] == out["sec"][0] * 1000


def test_unix_timestamp_invalid_unit_raises() -> None:
    df = DataFrame[D]({"d": [date(2020, 1, 1)]})
    with pytest.raises(ValueError, match="seconds|milliseconds"):
        df.with_columns(u=df.d.unix_timestamp("micros"))


def test_unix_timestamp_requires_date_or_datetime() -> None:
    df = DataFrame[StrCol]({"s": ["x"]})
    with pytest.raises(TypeError, match="date or datetime"):
        df.with_columns(u=df.s.unix_timestamp())


def test_dt_nanosecond_on_datetime_with_microseconds() -> None:
    df = DataFrame[DT]({"ts": [datetime(2024, 1, 1, 12, 0, 0, 500000)]})
    out = df.with_columns(ns=df.ts.dt_nanosecond()).collect(as_lists=True)
    assert out["ns"] == [500_000_000]


class MapOpt(Schema):
    m: dict[str, int] | None


def test_map_len_nullable_map_propagates_null() -> None:
    df = DataFrame[MapOpt]({"m": [{"a": 1}, None]})
    out = df.with_columns(n=df.m.map_len()).collect(as_lists=True)
    assert out["n"] == [1, None]


def test_map_len_requires_map_column() -> None:
    df = DataFrame[T]({"id": [1], "v": [1]})
    with pytest.raises(TypeError, match="map_len"):
        df.with_columns(n=df.v.map_len())


class BytesOpt(Schema):
    data: bytes | None


def test_binary_len_empty_and_nullable() -> None:
    df = DataFrame[BytesOpt]({"data": [b"", None, b"ab"]})
    out = df.with_columns(n=df.data.binary_len()).collect(as_lists=True)
    assert out["n"] == [0, None, 2]


def test_binary_len_requires_bytes_column() -> None:
    df = DataFrame[T]({"id": [1], "v": [1]})
    with pytest.raises(TypeError, match="binary_len|bytes"):
        df.with_columns(n=df.v.binary_len())


class TimeCol(Schema):
    t: time


def test_dt_nanosecond_on_time_column() -> None:
    """0.7.0 extended nanosecond extractor to ``time`` (and ``datetime``)."""
    df = DataFrame[TimeCol]({"t": [time(0, 0, 0, 123000)]})
    out = df.with_columns(ns=df.t.dt_nanosecond()).collect(as_lists=True)
    assert out["ns"] == [123_000_000]
