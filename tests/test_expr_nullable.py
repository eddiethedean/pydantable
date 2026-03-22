"""Nullable propagation for typed expressions (Polars execution path)."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantable import DataFrame, Schema


class _OptNum(Schema):
    x: int | None


class _OptStr(Schema):
    s: str | None


class _OptBool(Schema):
    a: bool | None
    b: bool | None


class _OptList(Schema):
    items: list[int] | None


class _OptTs(Schema):
    ts: datetime | None


def test_numeric_unary_null_propagates() -> None:
    df = DataFrame[_OptNum]({"x": [-3, None, 0]})
    out = df.with_columns(ax=df.x.abs()).collect(as_lists=True)
    assert out["ax"] == [3, None, 0]


def test_string_strip_null_propagates() -> None:
    df = DataFrame[_OptStr]({"s": ["  hi  ", None, "x"]})
    out = df.with_columns(t=df.s.strip()).collect(as_lists=True)
    assert out["t"] == ["hi", None, "x"]


def test_logical_and_null_kleene() -> None:
    df = DataFrame[_OptBool]({"a": [True, False, None], "b": [True, True, True]})
    out = df.with_columns(x=df.a & df.b).collect(as_lists=True)
    assert out["x"][0] is True
    assert out["x"][1] is False
    assert out["x"][2] is None


def test_list_len_null_list_cell() -> None:
    df = DataFrame[_OptList]({"items": [[1, 2], None, []]})
    out = df.with_columns(n=df.items.list_len()).collect(as_lists=True)
    assert out["n"] == [2, None, 0]


def test_dt_year_null_datetime() -> None:
    df = DataFrame[_OptTs]({"ts": [datetime(2024, 1, 1, tzinfo=timezone.utc), None]})
    out = df.with_columns(y=df.ts.dt_year()).collect(as_lists=True)
    assert out["y"] == [2024, None]
