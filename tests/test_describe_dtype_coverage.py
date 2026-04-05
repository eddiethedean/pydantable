"""Cover ``dataframe/_describe_dtype`` helpers used by info/describe/repr."""

from __future__ import annotations

import datetime
from typing import Literal, Union

import pytest

from pydantable.dataframe import _describe_dtype as dd


def test_is_describe_flags() -> None:
    assert dd._is_describe_numeric(int)
    assert dd._is_describe_numeric(float)
    assert dd._is_describe_numeric(int | None)
    assert not dd._is_describe_numeric(str)
    assert dd._is_describe_bool(bool)
    assert dd._is_describe_bool(bool | None)
    assert not dd._is_describe_bool(int)
    assert dd._is_describe_str(str)
    assert not dd._is_describe_str(bytes)
    assert dd._is_describe_temporal(datetime.date)
    assert dd._is_describe_temporal(datetime.datetime)
    assert dd._is_describe_temporal(datetime.datetime | None)
    assert not dd._is_describe_temporal(int)


def test_dtype_repr_none_and_types() -> None:
    assert dd._dtype_repr(None) == "Any"
    assert dd._dtype_repr(type(None)) == "None"
    assert dd._dtype_repr(int) == "int"


def test_dtype_repr_literal() -> None:
    assert dd._dtype_repr(Literal["a", "b"]) == "Literal['a', 'b']"


def test_dtype_repr_optional_union() -> None:
    assert dd._dtype_repr(int | None) == "int | None"
    assert dd._dtype_repr(Union[int, str, None]) == "int | str | None"  # noqa: UP007


def test_dtype_repr_union_no_none() -> None:
    assert dd._dtype_repr(int | str) == "int | str"


def test_dtype_repr_generic() -> None:
    assert dd._dtype_repr(list[int]) == "list[int]"
    assert dd._dtype_repr(dict[str, int]) == "dict[str, int]"


def test_dtype_repr_origin_no_args() -> None:
    class Bare:
        pass

    assert dd._dtype_repr(Bare).endswith(".Bare")


def test_dtype_repr_long_fallback() -> None:
    class LongRepr:
        def __repr__(self) -> str:
            return "x" * 100

    out = dd._dtype_repr(LongRepr())
    assert out.endswith("…")
    assert len(out) <= 72


def test_dtype_repr_non_type_annotation_fallback() -> None:
    assert dd._dtype_repr("not a type") == "'not a type'"
