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


class MapStr(Schema):
    m: dict[str, str]


def test_arrow_map_large_string_keys() -> None:
    mt = pa.map_(pa.large_string(), pa.int64())
    arr = pa.array([[("wide", 42)]], type=mt)
    df = DataFrame[MapInt]({"m": arr}, trusted_mode="strict")
    assert df.to_dict()["m"] == [{"wide": 42}]


def test_arrow_map_duplicate_string_keys_last_wins() -> None:
    """Aligned with map_from_entries / Polars duplicate-key policy."""
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("a", 1), ("a", 2), ("b", 3)]], type=mt)
    df = DataFrame[MapInt]({"m": arr}, trusted_mode="shape_only")
    assert df.to_dict()["m"] == [{"a": 2, "b": 3}]


def test_arrow_map_string_values_strict() -> None:
    mt = pa.map_(pa.string(), pa.string())
    arr = pa.array([[("k", "v"), ("x", "y")]], type=mt)
    df = DataFrame[MapStr]({"m": arr}, trusted_mode="strict")
    assert df.to_dict()["m"] == [{"k": "v", "x": "y"}]


def test_arrow_map_dataframe_model_trusted_strict() -> None:
    from pydantable import DataFrameModel

    class MDF(DataFrameModel):
        m: dict[str, int]

    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("p", 9)]], type=mt)
    dfm = MDF({"m": arr}, trusted_mode="strict")
    assert dfm.to_dict() == {"m": [{"p": 9}]}


def test_arrow_map_zero_rows_with_int_column() -> None:
    class IMap(Schema):
        id: int
        m: dict[str, int]

    mt = pa.map_(pa.string(), pa.int64())
    df = DataFrame[IMap](
        {"id": pa.array([], type=pa.int64()), "m": pa.array([], type=mt)},
        trusted_mode="strict",
    )
    assert df.to_dict() == {"id": [], "m": []}


def test_arrow_map_strict_rejects_string_column_for_int_values() -> None:
    mt = pa.map_(pa.string(), pa.string())
    arr = pa.array([[("k", "not-int")]], type=mt)
    with pytest.raises(ValueError, match="strict trusted"):
        DataFrame[MapInt]({"m": arr}, trusted_mode="strict")


def test_arrow_map_ingest_then_map_get_and_contains() -> None:
    """map_get / map_contains_key after Arrow map ingest (string-keyed maps)."""
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("a", 1), ("b", 2)]], type=mt)
    df = DataFrame[MapInt]({"m": arr}, trusted_mode="strict")
    out = df.with_columns(
        av=df.m.map_get("a"),
        miss=df.m.map_get("missing"),
        has_a=df.m.map_contains_key("a"),
        has_z=df.m.map_contains_key("z"),
    ).collect(as_lists=True)
    assert out["av"] == [1]
    assert out["miss"] == [None]
    assert out["has_a"] == [True]
    assert out["has_z"] == [False]
