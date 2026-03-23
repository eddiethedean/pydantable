"""Expression surfaces added in 0.7.0 (strptime, unix_timestamp, map/binary helpers)."""

from __future__ import annotations

from datetime import date, datetime, time

from pydantable import DataFrame, Schema


def test_map_len_counts_entries() -> None:
    class M(Schema):
        id: int
        m: dict[str, int]

    df = DataFrame[M]({"id": [1, 2], "m": [{"a": 1, "b": 2}, {}]})
    out = df.with_columns(n=df.m.map_len()).collect(as_lists=True)
    assert out["n"] == [2, 0]


def test_map_get_and_contains_key() -> None:
    class M(Schema):
        id: int
        m: dict[str, int] | None

    df = DataFrame[M](
        {
            "id": [1, 2, 3],
            "m": [{"a": 1, "b": 2}, {}, None],
        }
    )
    out = df.with_columns(
        av=df.m.map_get("a"),
        has_b=df.m.map_contains_key("b"),
    ).collect(as_lists=True)
    assert out["av"] == [1, None, None]
    assert out["has_b"] == [True, False, None]


def test_binary_len_byte_count() -> None:
    class B(Schema):
        id: int
        data: bytes

    df = DataFrame[B]({"id": [1], "data": [b"hello"]})
    out = df.with_columns(n=df.data.binary_len()).collect(as_lists=True)
    assert out["n"] == [5]


def test_time_part_extractors() -> None:
    class T(Schema):
        id: int
        t: time

    df = DataFrame[T]({"id": [1], "t": [time(14, 7, 3)]})
    out = df.with_columns(
        h=df.t.dt_hour(),
        mi=df.t.dt_minute(),
        s=df.t.dt_second(),
        ns=df.t.dt_nanosecond(),
    ).collect(as_lists=True)
    assert out["h"] == [14]
    assert out["mi"] == [7]
    assert out["s"] == [3]
    assert out["ns"] == [0]


def test_strptime_string_to_date() -> None:
    class S(Schema):
        s: str

    df = DataFrame[S]({"s": ["2024-06-01"]})
    out = df.with_columns(d=df.s.strptime("%Y-%m-%d", to_datetime=False)).collect(
        as_lists=True
    )
    assert out["d"] == [date(2024, 6, 1)]


def test_unix_timestamp_date_seconds() -> None:
    class D(Schema):
        d: date

    df = DataFrame[D]({"d": [date(1970, 1, 2)]})
    sec = df.with_columns(u=df.d.unix_timestamp("seconds")).collect(as_lists=True)
    assert sec["u"] == [86400]


def test_unix_timestamp_datetime_milliseconds_is_int() -> None:
    """Milliseconds since epoch; naive datetimes follow Polars/local interpretation."""

    class DT(Schema):
        ts: datetime

    df = DataFrame[DT]({"ts": [datetime(2020, 1, 1, 12, 0, 0)]})
    ms = df.with_columns(u=df.ts.unix_timestamp("ms")).collect(as_lists=True)
    assert isinstance(ms["u"][0], int)
