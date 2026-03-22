"""Integration tests for nested Pydantic model (struct) columns."""

from __future__ import annotations

import typing

import pytest
from pydantable import DataFrame, Schema
from pydantable.schema import is_supported_column_annotation
from pydantic import ValidationError


class _Street(Schema):
    name: str


class _Addr(Schema):
    street: str
    line2: _Street


class _Customer(Schema):
    id: int
    addr: _Addr


class _AddrFlat(Schema):
    """Single-field struct for clearer Polars null round-trips."""

    street: str


class _PersonOpt(Schema):
    id: int
    addr: _Addr | None


class _PersonOptFlat(Schema):
    id: int
    addr: _AddrFlat | None


class ForwardB(Schema):
    x: int


class ForwardA(Schema):
    """Uses a forward reference to another Schema in the same module."""

    b: ForwardB


def test_deep_nested_struct_roundtrip_and_chained_struct_field() -> None:
    df = DataFrame[_Customer](
        {
            "id": [1],
            "addr": [
                {
                    "street": "Main",
                    "line2": {"name": "Unit 2"},
                }
            ],
        }
    )
    assert df.schema_fields()["addr"] is _Addr
    got = df.collect(as_lists=True)
    assert got["addr"] == [{"street": "Main", "line2": {"name": "Unit 2"}}]

    lat_expr = df.addr.struct_field("line2").struct_field("name")
    out = df.with_columns(unit=lat_expr)
    assert out.schema_fields()["unit"] is str
    assert out.collect(as_lists=True) == {
        "id": [1],
        "addr": got["addr"],
        "unit": ["Unit 2"],
    }


def test_unnest_struct_promotes_fields_with_separator() -> None:
    df = DataFrame[_PersonOptFlat](
        {
            "id": [1, 2],
            "addr": [{"street": "Main"}, None],
        }
    )
    flat = df.unnest("addr")
    out = flat.collect(as_lists=True)
    assert "addr_street" in out
    assert out["id"] == [1, 2]
    assert out["addr_street"] == ["Main", None]
    st_ann = flat.schema_fields()["addr_street"]
    origin = typing.get_origin(st_ann)
    assert st_ann is str or (origin is not None and str in typing.get_args(st_ann))


def test_unnest_nested_struct_keeps_inner_struct_column() -> None:
    df = DataFrame[_Customer](
        {
            "id": [1],
            "addr": [{"street": "Main", "line2": {"name": "U"}}],
        }
    )
    u = df.unnest("addr")
    got = u.collect(as_lists=True)
    assert got["id"] == [1]
    assert "addr_street" in got and got["addr_street"] == ["Main"]
    assert "addr_line2" in got
    assert got["addr_line2"] == [{"name": "U"}]


def test_struct_field_unknown_name_raises() -> None:
    addr = {"street": "a", "line2": {"name": "n"}}
    df = DataFrame[_PersonOpt]({"id": [1], "addr": [addr]})
    with pytest.raises(TypeError, match="Unknown struct field"):
        df.with_columns(bad=df.addr.struct_field("missing_field"))


def test_filter_on_struct_field_scalar() -> None:
    df = DataFrame[_PersonOpt](
        {
            "id": [1, 2, 3],
            "addr": [
                {"street": "oak", "line2": {"name": "a"}},
                {"street": "elm", "line2": {"name": "b"}},
                {"street": "oak", "line2": {"name": "c"}},
            ],
        }
    )
    f = df.filter(df.addr.struct_field("street") == "oak")
    assert f.collect(as_lists=True) == {
        "id": [1, 3],
        "addr": [
            {"street": "oak", "line2": {"name": "a"}},
            {"street": "oak", "line2": {"name": "c"}},
        ],
    }


def test_sort_preserves_struct_column() -> None:
    df = DataFrame[_PersonOpt](
        {
            "id": [3, 1, 2],
            "addr": [
                {"street": "c", "line2": {"name": "x"}},
                {"street": "a", "line2": {"name": "y"}},
                {"street": "b", "line2": {"name": "z"}},
            ],
        }
    )
    s = df.sort("id")
    assert s.schema_fields()["addr"] == _Addr | None
    assert s.collect(as_lists=True) == {
        "id": [1, 2, 3],
        "addr": [
            {"street": "a", "line2": {"name": "y"}},
            {"street": "b", "line2": {"name": "z"}},
            {"street": "c", "line2": {"name": "x"}},
        ],
    }


def test_nullable_struct_column_none_cells() -> None:
    """Polars materializes a missing struct row as null fields, not Python ``None``."""
    df = DataFrame[_PersonOptFlat](
        {
            "id": [1, 2],
            "addr": [{"street": "a"}, None],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [{"street": "a"}, {"street": None}],
    }


def test_struct_field_propagates_null_when_outer_struct_missing() -> None:
    df = DataFrame[_PersonOptFlat](
        {
            "id": [1, 2],
            "addr": [{"street": "a"}, None],
        }
    )
    out = df.with_columns(s=df.addr.struct_field("street")).collect(as_lists=True)
    assert out["s"] == ["a", None]


def test_is_null_on_struct_column_not_like_python_none() -> None:
    # Expr.is_null() on struct columns does not treat ingested None as SQL NULL here.
    df = DataFrame[_PersonOptFlat]({"id": [1, 2], "addr": [{"street": "a"}, None]})
    out = df.with_columns(m=df.addr.is_null()).collect(as_lists=True)
    assert out["m"] == [False, False]


def test_struct_field_derived_column_is_optional() -> None:
    df = DataFrame[_PersonOptFlat]({"id": [1, 2], "addr": [{"street": "a"}, None]})
    w = df.with_columns(s=df.addr.struct_field("street"))
    assert w.schema_fields()["s"] == str | None


def test_struct_equality_same_column() -> None:
    df = DataFrame[_PersonOpt](
        {
            "id": [1],
            "addr": [{"street": "x", "line2": {"name": "y"}}],
        }
    )
    out = df.with_columns(self_eq=(df.addr == df.addr)).collect(as_lists=True)
    assert out["self_eq"] == [True]


def test_struct_ordering_comparison_raises() -> None:
    df = DataFrame[_PersonOpt](
        {"id": [1], "addr": [{"street": "a", "line2": {"name": "n"}}]},
    )
    with pytest.raises(TypeError, match="Ordering comparisons"):
        _ = df.addr < df.addr


def test_chained_rename_preserves_nested_schema_class() -> None:
    df = DataFrame[_PersonOpt](
        {"id": [1], "addr": [{"street": "s", "line2": {"name": "n"}}]},
    )
    r1 = df.rename({"addr": "location"})
    r2 = r1.rename({"location": "mailing"})
    assert r2.schema_fields()["mailing"] == _Addr | None


def test_rename_required_nested_struct_preserves_user_class() -> None:
    df = DataFrame[_Customer](
        {
            "id": [1],
            "addr": [{"street": "s", "line2": {"name": "n"}}],
        }
    )
    r = df.rename({"addr": "mailing"})
    assert r.schema_fields()["mailing"] is _Addr


def test_forward_ref_nested_models_supported() -> None:
    assert is_supported_column_annotation(ForwardA)
    df = DataFrame[ForwardA]({"b": [{"x": 1}, {"x": 2}]})
    assert df.collect(as_lists=True) == {"b": [{"x": 1}, {"x": 2}]}


def test_validation_rejects_wrong_struct_shape() -> None:
    with pytest.raises(ValidationError):
        DataFrame[_PersonOpt](
            {
                "id": [1],
                "addr": [{"street": "only"}],
            }
        )
