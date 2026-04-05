"""Extra selector DSL: unions with str/iterable, dtype aliases, rename_map errors."""

from __future__ import annotations

import re

import pytest
from pydantable import DataFrame
from pydantable import selectors as s
from pydantable.schema import Schema


def test_selector_union_intersection_sub_with_str_and_tuple() -> None:
    class S(Schema):
        a: int
        b: int
        c: int

    df = DataFrame[S]({"a": [1], "b": [2], "c": [3]})
    u = s.by_name("a") | "b"
    assert set(df.select_schema(u).to_dict()) == {"a", "b"}
    u2 = s.by_name("a") | ("b", "c")
    assert set(df.select_schema(u2).to_dict()) == {"a", "b", "c"}
    inter = s.by_name("a", "b") & "a"
    assert set(df.select_schema(inter).to_dict()) == {"a"}
    diff = s.everything() - "c"
    assert set(df.select_schema(diff).to_dict()) == {"a", "b"}


def test_matches_compiled_regex_pattern() -> None:
    class S(Schema):
        col_x: int
        y: int

    df = DataFrame[S]({"col_x": [1], "y": [2]})
    out = df.select_schema(s.matches(re.compile(r"^col_"))).to_dict()
    assert set(out) == {"col_x"}


def test_everything_alias_all() -> None:
    class S(Schema):
        z: int

    df = DataFrame[S]({"z": [1]})
    a = df.select_schema(s.all()).to_dict()
    b = df.select_schema(s.everything()).to_dict()
    assert a == b


def test_by_dtype_mixed_type_and_group() -> None:
    class S(Schema):
        n: int
        t: str

    df = DataFrame[S]({"n": [1], "t": ["x"]})
    sel = s.by_dtype(int, s.STRING)
    assert set(df.select_schema(sel).to_dict()) == {"n", "t"}


def test_dtype_aliases_integer_float_struct_decimal() -> None:
    from decimal import Decimal

    class Inner(Schema):
        x: int

    class Mixed(Schema):
        i: int
        f: float
        st: Inner

    df = DataFrame[Mixed]({"i": [1], "f": [1.0], "st": [{"x": 1}]})
    assert set(df.select_schema(s.integer()).to_dict()) == {"i"}
    assert set(df.select_schema(s.float()).to_dict()) == {"f"}
    assert set(df.select_schema(s.struct()).to_dict()) == {"st"}

    class Dec(Schema):
        dec: Decimal

    dfd = DataFrame[Dec]({"dec": [Decimal("1.5")]})
    assert set(dfd.select_schema(s.decimal()).to_dict()) == {"dec"}
    assert set(dfd.select_schema(s.decimals()).to_dict()) == {"dec"}


def test_rename_map_type_errors_and_duplicate_target() -> None:
    class S(Schema):
        a: int
        b: int

    df = DataFrame[S]({"a": [1], "b": [2]})
    with pytest.raises(TypeError, match="expects a Selector"):
        s.rename_map("bad", lambda c: c)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="expects a callable"):
        s.rename_map(s.by_name("a"), "not_callable")  # type: ignore[arg-type]

    mk_dup = s.rename_map(s.everything(), lambda _c: "same")
    with pytest.raises(ValueError, match="duplicate output column"):
        mk_dup(df.schema_fields())

    mk_empty = s.rename_map(s.starts_with("zzz"), lambda c: c)
    with pytest.raises(ValueError, match="matched no columns"):
        mk_empty(df.schema_fields())
