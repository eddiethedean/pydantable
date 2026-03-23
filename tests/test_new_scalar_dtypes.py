"""Tests for time, bytes, and dict[str, V] map columns (0.6.0)."""

from __future__ import annotations

from datetime import time

import pytest
from pydantable import DataFrame, DataFrameModel, Schema


def test_time_column_roundtrip() -> None:
    class T(Schema):
        id: int
        t: time

    df = DataFrame[T](
        {
            "id": [1, 2],
            "t": [time(12, 30, 45), time(0, 0, 1)],
        }
    )
    out = df.collect(as_lists=True)
    assert out["t"][0] == time(12, 30, 45)
    assert out["t"][1] == time(0, 0, 1)


def test_bytes_column_roundtrip() -> None:
    class B(Schema):
        id: int
        data: bytes

    df = DataFrame[B]({"id": [1], "data": [b"hello"]})
    out = df.collect(as_lists=True)
    assert out["data"][0] == b"hello"


def test_map_column_roundtrip() -> None:
    class M(Schema):
        id: int
        m: dict[str, int]

    df = DataFrame[M](
        {
            "id": [1, 2],
            "m": [{"a": 1, "b": 2}, {}],
        }
    )
    out = df.collect(as_lists=True)
    assert out["m"][0] == {"a": 1, "b": 2}
    assert out["m"][1] == {}


def test_map_rejects_non_string_keys() -> None:
    class Bad(Schema):
        id: int
        m: dict[int, int]  # type: ignore[valid-type]

    with pytest.raises(TypeError):
        DataFrame[Bad]({"id": [1], "m": [{1: 2}]})


def test_nullable_time_and_bytes_roundtrip() -> None:
    class T(Schema):
        id: int
        t: time | None
        data: bytes | None

    df = DataFrame[T](
        {
            "id": [1, 2],
            "t": [time(1, 2, 3), None],
            "data": [b"a", None],
        }
    )
    out = df.collect(as_lists=True)
    assert out["t"] == [time(1, 2, 3), None]
    assert out["data"] == [b"a", None]


def test_time_filter_is_not_null_and_compare() -> None:
    class T(Schema):
        id: int
        t: time | None

    df = DataFrame[T]({"id": [1, 2, 3], "t": [time(12, 0, 0), None, time(12, 0, 0)]})
    non_null = df.filter(df.t.is_not_null()).collect(as_lists=True)
    assert non_null == {"id": [1, 3], "t": [time(12, 0, 0), time(12, 0, 0)]}

    noon = df.filter(df.t == time(12, 0, 0)).collect(as_lists=True)
    assert noon == {"id": [1, 3], "t": [time(12, 0, 0), time(12, 0, 0)]}


def test_bytes_filter_equality() -> None:
    class B(Schema):
        id: int
        data: bytes

    df = DataFrame[B]({"id": [1, 2], "data": [b"x", b"y"]})
    out = df.filter(df.data == b"x").collect(as_lists=True)
    assert out == {"id": [1], "data": [b"x"]}


def test_map_str_values_roundtrip() -> None:
    class M(Schema):
        id: int
        labels: dict[str, str]

    df = DataFrame[M](
        {
            "id": [1, 2],
            "labels": [{"en": "a", "de": "b"}, {}],
        }
    )
    out = df.collect(as_lists=True)
    assert out["labels"][0] == {"en": "a", "de": "b"}
    assert out["labels"][1] == {}


def test_map_preserved_through_select_and_filter_on_other_column() -> None:
    class M(Schema):
        id: int
        m: dict[str, int]

    df = DataFrame[M](
        {
            "id": [1, 2],
            "m": [{"k": 1}, {"k": 2}],
        }
    )
    picked = df.filter(df.id == 2).select("m").collect(as_lists=True)
    assert picked == {"m": [{"k": 2}]}


def test_dataframe_model_scalar_060_types_column_input() -> None:
    class Row(DataFrameModel):
        id: int
        t: time
        blob: bytes
        meta: dict[str, int]

    df = Row(
        {
            "id": [1],
            "t": [time(9, 30, 0)],
            "blob": [b"ok"],
            "meta": [{"a": 1}],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1],
        "t": [time(9, 30, 0)],
        "blob": [b"ok"],
        "meta": [{"a": 1}],
    }
