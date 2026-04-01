"""Phase C: str_json_decode (Polars json_decode) to struct / map."""

from __future__ import annotations

import pytest

pytest.importorskip("pydantable._core")

from pydantable import DataFrame, Schema


class _Inner(Schema):
    x: int
    y: str | None


class _Row(Schema):
    id: int
    s: _Inner


class _JsonTextRow(Schema):
    id: int
    raw: str


def test_struct_json_encode_round_trip_str_json_decode() -> None:
    df = DataFrame[_Row]({"id": [1, 2], "s": [{"x": 7, "y": "hi"}, {"x": 0, "y": None}]})
    enc = df.with_columns(js=df.s.struct_json_encode())
    out = enc.with_columns(s2=enc.js.str_json_decode(_Inner)).collect(as_lists=True)
    assert out["s2"] == out["s"]


def test_str_json_decode_map_from_entries_json() -> None:
    """Polars decodes ``dict[str, T]`` as list-of-{{key,value}}; JSON must match that shape."""
    df = DataFrame[_JsonTextRow](
        {
            "id": [1],
            "raw": [
                '[{"key":"a","value":10},{"key":"b","value":20}]',
            ],
        }
    )
    got = df.with_columns(m=df.raw.str_json_decode(dict[str, int])).collect(as_lists=True)
    assert got["m"] == [{"a": 10, "b": 20}]


def test_str_json_decode_invalid_json_raises_at_collect() -> None:
    """Polars ``json_decode`` errors the evaluation when any row is not valid JSON (v0.53)."""
    df = DataFrame[_JsonTextRow](
        {
            "id": [1, 2],
            "raw": ['{"x": 1, "y": "ok"}', "not-json{"],
        }
    )
    bad = df.with_columns(parsed=df.raw.str_json_decode(_Inner))
    with pytest.raises(ValueError, match="json|JSON"):
        bad.collect(as_lists=True)


def test_str_json_decode_rejects_non_string_column() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": None}]})
    with pytest.raises(TypeError, match="str_json_decode"):
        _ = df.with_columns(bad=df.id.str_json_decode(_Inner))


def test_str_json_decode_rejects_scalar_target() -> None:
    df = DataFrame[_JsonTextRow]({"id": [1], "raw": ['"x"']})
    with pytest.raises(TypeError, match="struct"):
        _ = df.with_columns(bad=df.raw.str_json_decode(int))
