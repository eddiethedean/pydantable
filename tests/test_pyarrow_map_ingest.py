"""Arrow-native ``map<utf8, …>`` ingest for ``dict[str, T]`` columns (0.15.0)."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema

pa = pytest.importorskip("pyarrow")


class MapInt(Schema):
    m: dict[str, int]


class MapIntNullableCell(Schema):
    """Map column nullable per-cell (Arrow null map)."""

    m: dict[str, int] | None


def test_trusted_strict_arrow_map_to_dict_int() -> None:
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array(
        [[("a", 1), ("b", 2)], None, []],
        type=mt,
    )
    df = DataFrame[MapIntNullableCell]({"m": arr}, trusted_mode="strict")
    assert df.to_dict()["m"] == [{"a": 1, "b": 2}, None, {}]


def test_shape_only_arrow_map() -> None:
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("k", 10)]], type=mt)
    df = DataFrame[MapInt]({"m": arr}, trusted_mode="shape_only")
    assert df.to_dict()["m"] == [{"k": 10}]


def test_arrow_map_chunked() -> None:
    mt = pa.map_(pa.string(), pa.int64())
    c1 = pa.array([[("a", 1)]], type=mt)
    c2 = pa.array([[("b", 2)]], type=mt)
    ca = pa.chunked_array([c1, c2])
    df = DataFrame[MapInt]({"m": ca}, trusted_mode="strict")
    assert df.to_dict()["m"] == [{"a": 1}, {"b": 2}]


def test_strict_arrow_map_rejects_float_values() -> None:
    mt = pa.map_(pa.string(), pa.float64())
    arr = pa.array([[("a", 1.0)]], type=mt)
    with pytest.raises(ValueError, match="strict trusted"):
        DataFrame[MapInt]({"m": arr}, trusted_mode="strict")


def test_arrow_map_non_string_keys_rejected() -> None:
    mt = pa.map_(pa.int64(), pa.string())
    arr = pa.array([[(1, "x")]], type=mt)
    with pytest.raises(TypeError, match="string"):
        DataFrame[MapInt]({"m": arr}, trusted_mode="shape_only")


def test_off_mode_arrow_map_converts() -> None:
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("a", 1)]], type=mt)
    df = DataFrame[MapInt]({"m": arr})
    assert df.to_dict()["m"] == [{"a": 1}]
