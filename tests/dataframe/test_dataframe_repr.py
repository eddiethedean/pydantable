"""Smoke tests for :meth:`DataFrame.__repr__` and related wrappers."""

from __future__ import annotations

from pydantable import DataFrame, DataFrameModel
from pydantable.dataframe._repr_display import _REPR_HTML_MAX_COLS, _REPR_MAX_COLUMNS
from pydantable.schema import Schema
from pydantic import BaseModel, create_model


class _User(BaseModel):
    id: int
    name: str


class _Nullable(BaseModel):
    id: int
    age: int | None


def test_dataframe_repr_includes_schema_and_columns() -> None:
    df = DataFrame[_User]({"id": [1], "name": ["a"]})
    r = repr(df)
    assert "DataFrame[_User]" in r
    assert "schema: _User" in r
    assert "columns (2):" in r
    assert "id" in r and "name" in r
    assert "int" in r and "str" in r


def test_dataframe_repr_optional_union_style() -> None:
    df = DataFrame[_Nullable]({"id": [1], "age": [None]})
    r = repr(df)
    assert "age" in r
    assert "int | None" in r


def test_dataframe_repr_truncates_many_columns() -> None:
    n = _REPR_MAX_COLUMNS + 4
    field_defs = {f"c{i}": (int, ...) for i in range(n)}
    Wide = create_model("Wide", __base__=Schema, **field_defs)
    data = {f"c{i}": [1] for i in range(n)}
    df = DataFrame[Wide](data)
    r = repr(df)
    assert f"columns ({n}):" in r
    assert "… and 4 more" in r


def test_dataframe_model_repr_delegates() -> None:
    class Users(DataFrameModel):
        id: int
        name: str

    m = Users({"id": [1], "name": ["x"]})
    r = repr(m)
    assert r.startswith("Users\n")
    assert "DataFrame[UsersSchema]" in r
    assert "id" in r and "name" in r


def test_grouped_dataframe_repr() -> None:
    df = DataFrame[_User]({"id": [1, 1], "name": ["a", "b"]})
    g = df.group_by("id")
    r = repr(g)
    assert "GroupedDataFrame(by=['id'])" in r
    assert "DataFrame[_User]" in r


def test_dataframe_repr_html_table_and_escape() -> None:
    df = DataFrame[_User]({"id": [1, 2], "name": ["<script>x</script>", "ok"]})
    h = df._repr_html_()
    assert "<table" in h
    assert "</table>" in h
    assert "<thead>" in h and "<tbody>" in h
    assert "&lt;script&gt;" in h
    assert "<script>" not in h


def test_dataframe_repr_html_truncates_many_columns() -> None:
    n = _REPR_HTML_MAX_COLS + 3
    field_defs = {f"c{i}": (int, ...) for i in range(n)}
    Wide = create_model("Wide", __base__=Schema, **field_defs)
    data = {f"c{i}": [i] for i in range(n)}
    df = DataFrame[Wide](data)
    h = df._repr_html_()
    assert "omitted" in h.lower() or "…" in h


def test_dataframe_model_repr_html() -> None:
    class Users(DataFrameModel):
        id: int
        name: str

    m = Users({"id": [1], "name": ["n"]})
    h = m._repr_html_()
    assert "Users" in h
    assert "<table" in h
