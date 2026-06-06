"""Cover ``dataframe/_describe_dtype`` helpers used by info/describe/repr."""

from __future__ import annotations

import datetime
from typing import Literal, Union

from pydantable.dataframe import _describe_dtype as dd


def test_describe_dtype_flags_and_repr() -> None:
    assert dd._is_describe_numeric(int)
    assert dd._is_describe_numeric(float)
    assert not dd._is_describe_numeric(str)
    assert dd._is_describe_bool(bool)
    assert not dd._is_describe_bool(int)
    assert dd._is_describe_str(str)
    assert dd._is_describe_temporal(datetime.date)
    assert dd._is_describe_temporal(datetime.datetime | None)

    assert dd._dtype_repr(None) == "Any"
    assert dd._dtype_repr(int) == "int"
    assert dd._dtype_repr(Literal["a", "b"]) == "Literal['a', 'b']"
    assert dd._dtype_repr(int | None) == "int | None"
    assert dd._dtype_repr(Union[int, str, None]) == "int | str | None"  # noqa: UP007
    assert dd._dtype_repr(int | str) == "int | str"
    assert dd._dtype_repr(list[int]) == "list[int]"
    assert dd._dtype_repr(dict[str, int]) == "dict[str, int]"
    assert dd._dtype_repr("not a type") == "'not a type'"

    class LongRepr:
        def __repr__(self) -> str:
            return "x" * 100

    out = dd._dtype_repr(LongRepr())
    assert out.endswith("…")
    assert len(out) <= 72
