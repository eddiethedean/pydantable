"""Phase B: struct JSON encode / JSONPath / rename / with_fields expressions."""

from __future__ import annotations

import json

import pytest
from pydantable import DataFrame, Schema
from pydantable.expressions import Literal


class _Inner(Schema):
    x: int
    y: str | None


class _Row(Schema):
    id: int
    s: _Inner


def test_struct_json_encode_rowwise_json_text() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 7, "y": "hi"}]})
    out = df.with_columns(js=df.s.struct_json_encode()).collect(as_lists=True)
    parsed = [json.loads(x) for x in out["js"]]
    assert parsed == [{"x": 7, "y": "hi"}]


def test_struct_json_path_match_reads_field() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 99, "y": None}]})
    out = df.with_columns(v=df.s.struct_json_path_match("$.x")).collect(as_lists=True)
    assert out["v"] == ["99"]


def test_struct_rename_fields_and_unnest_uses_new_names() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": "a"}]})
    renamed = df.with_columns(t=df.s.struct_rename_fields(["xx", "yy"]))
    flat = renamed.unnest("t")
    got = flat.collect(as_lists=True)
    assert "t_xx" in got and "t_yy" in got
    assert got["t_xx"] == [1]
    assert got["t_yy"] == ["a"]


def test_struct_with_fields_adds_literal() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 2, "y": None}]})
    out = df.with_columns(
        s2=df.s.struct_with_fields(z=Literal(value=3)),
    ).collect(as_lists=True)
    assert out["s2"] == [{"x": 2, "y": None, "z": 3}]


def test_struct_rename_fields_wrong_count_raises() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": "z"}]})
    with pytest.raises(ValueError, match="expected 2 names"):
        _ = df.with_columns(bad=df.s.struct_rename_fields(["only_one"]))


def test_struct_json_path_match_empty_path_raises() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": None}]})
    with pytest.raises(ValueError, match="JSONPath"):
        _ = df.with_columns(bad=df.s.struct_json_path_match(""))


def test_struct_json_encode_rejects_non_struct() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": None}]})
    with pytest.raises(TypeError, match="struct_json_encode"):
        _ = df.with_columns(bad=df.id.struct_json_encode())


def test_struct_with_fields_requires_fields_and_expr_values() -> None:
    df = DataFrame[_Row]({"id": [1], "s": [{"x": 1, "y": None}]})
    with pytest.raises(TypeError, match="at least one keyword"):
        df.with_columns(bad=df.s.struct_with_fields())
    with pytest.raises(TypeError, match="expects Expr"):
        df.with_columns(bad=df.s.struct_with_fields(z=3))  # type: ignore[arg-type]
