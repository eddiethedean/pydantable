"""Map v0.9.0 contract tests for JSON-like values."""

from __future__ import annotations

import pytest
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


def test_map_keys_and_map_values_roundtrip() -> None:
    df = DataFrame[MapStruct]({"payload": [{"a": {"x": 1}, "b": None}]})
    out = df.with_columns(k=df.payload.map_keys(), v=df.payload.map_values()).collect(
        as_lists=True
    )
    assert out["k"] == [["a", "b"]]
    assert out["v"] == [[{"x": 1}, None]]


def test_map_keys_and_values_nullable_map_propagates_null() -> None:
    df = DataFrame[MapNullable]({"payload": [{"a": [1]}, None]})
    out = df.with_columns(k=df.payload.map_keys(), v=df.payload.map_values()).collect(
        as_lists=True
    )
    assert out["k"] == [["a"], None]
    assert out["v"] == [[[1]], None]


def test_map_keys_requires_map_column() -> None:
    class NotMap(Schema):
        payload: list[int]

    df = DataFrame[NotMap]({"payload": [[1, 2]]})
    with pytest.raises(TypeError, match=r"map_keys\(\) requires a map column"):
        df.with_columns(k=df.payload.map_keys())


def test_map_entries_roundtrip_shape() -> None:
    df = DataFrame[MapStruct]({"payload": [{"a": {"x": 1}, "b": None}]})
    out = df.with_columns(e=df.payload.map_entries()).collect(as_lists=True)
    assert out["e"] == [[{"key": "a", "value": {"x": 1}}, {"key": "b", "value": None}]]


def test_map_keys_values_entries_on_empty_map() -> None:
    df = DataFrame[MapStruct]({"payload": [{}]})
    out = df.with_columns(
        k=df.payload.map_keys(),
        v=df.payload.map_values(),
        e=df.payload.map_entries(),
    ).collect(as_lists=True)
    assert out["k"] == [[]]
    assert out["v"] == [[]]
    assert out["e"] == [[]]


def test_map_from_entries_roundtrip() -> None:
    df = DataFrame[MapStruct]({"payload": [{"a": {"x": 1}, "b": None}, {}]})
    out = df.with_columns(rebuilt=df.payload.map_entries().map_from_entries()).collect(
        as_lists=True
    )
    assert out["rebuilt"] == [{"a": {"x": 1}, "b": None}, {}]


def test_map_from_entries_requires_entry_struct_shape() -> None:
    class BadEntries(Schema):
        entries: list[dict[str, int]]

    df = DataFrame[BadEntries]({"entries": [[{"x": 1}]]})
    with pytest.raises(TypeError, match=r"list of entry structs"):
        df.with_columns(m=df.entries.map_from_entries())


def test_map_from_entries_nullable_map_roundtrip() -> None:
    df = DataFrame[MapNullable]({"payload": [{"a": [1]}, None]})
    out = df.with_columns(r=df.payload.map_entries().map_from_entries()).collect(
        as_lists=True
    )
    assert out["r"] == [{"a": [1]}, None]


def test_expr_element_at_matches_map_get() -> None:
    df = DataFrame[MapStruct]({"payload": [{"a": {"x": 1}, "b": None}]})
    out = df.with_columns(
        g1=df.payload.map_get("a"),
        g2=df.payload.element_at("a"),
    ).collect(as_lists=True)
    assert out["g1"] == out["g2"] == [{"x": 1}]


def test_expr_element_at_missing_key_returns_null() -> None:
    df = DataFrame[MapList]({"payload": [{"a": [1]}, {"b": [2]}]})
    out = df.with_columns(m=df.payload.element_at("missing")).collect(as_lists=True)
    assert out["m"] == [None, None]


def test_expr_element_at_requires_map_column() -> None:
    class NotMap(Schema):
        x: int

    df = DataFrame[NotMap]({"x": [1]})
    with pytest.raises(TypeError, match=r"map_get\(\) requires a map column"):
        df.with_columns(y=df.x.element_at("k"))


def test_map_from_entries_expr_serializable_kind() -> None:
    df = DataFrame[MapStruct]({"payload": [{"a": {"x": 1}}]})
    expr = df.payload.map_entries().map_from_entries()
    payload = expr._rust_expr.to_serializable()
    assert payload["kind"] == "map_from_entries"
    assert payload["inner"]["kind"] == "map_entries"
