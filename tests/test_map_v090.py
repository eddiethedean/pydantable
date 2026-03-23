"""Map v0.9.0 contract tests for JSON-like values."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.schema import Schema


class MapList(Schema):
    payload: dict[str, list[int] | None]


class MapStruct(Schema):
    payload: dict[str, dict[str, int] | None]


class MapNullable(Schema):
    payload: dict[str, list[int] | None] | None


def test_map_list_values_roundtrip_and_map_len() -> None:
    df = DataFrame[MapList](
        {
            "payload": [
                {"a": [1, 2], "b": [3]},
                {"a": [], "b": None},
            ]
        }
    )
    out = df.with_columns(n=df.payload.map_len()).collect(as_lists=True)
    assert out["n"] == [2, 2]


def test_map_struct_values_map_get_preserves_nested_shape() -> None:
    df = DataFrame[MapStruct](
        {
            "payload": [
                {"a": {"x": 7}, "b": {"x": 1}},
                {"a": None, "b": {"x": 2}},
            ]
        }
    )
    out = df.with_columns(a=df.payload.map_get("a"), b=df.payload.map_get("b")).collect(
        as_lists=True
    )
    assert out["a"] == [{"x": 7}, None]
    assert out["b"] == [{"x": 1}, {"x": 2}]


def test_map_get_missing_key_returns_null() -> None:
    df = DataFrame[MapList]({"payload": [{"a": [1]}, {"b": [2]}]})
    out = df.with_columns(m=df.payload.map_get("missing")).collect(as_lists=True)
    assert out["m"] == [None, None]


def test_map_contains_key_and_nullable_map_behavior() -> None:
    df = DataFrame[MapNullable]({"payload": [{"a": [1]}, None, {"b": None}]})
    out = df.with_columns(
        has_a=df.payload.map_contains_key("a"),
        len_=df.payload.map_len(),
    ).collect(as_lists=True)
    assert out["has_a"] == [True, None, False]
    assert out["len_"] == [1, None, 1]
