"""Tests for time, bytes, and dict[str, V] map columns (0.6.0)."""

from __future__ import annotations

from datetime import time

import pytest
from pydantable import DataFrame, Schema


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
