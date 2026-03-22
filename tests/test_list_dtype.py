"""Integration tests for homogeneous list columns (list[T]) and explode()."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema
from pydantic import ValidationError


class _TagsInt(Schema):
    id: int
    tags: list[int]


class _Words(Schema):
    id: int
    parts: list[str]


class _TagsOpt(Schema):
    id: int
    tags: list[int] | None


class _Addr(Schema):
    street: str


class _RowWithStructList(Schema):
    id: int
    items: list[_Addr]


class _TwoLists(Schema):
    id: int
    a: list[int]
    b: list[str]


def test_list_str_roundtrip_filter_select() -> None:
    df = DataFrame[_Words](
        {
            "id": [1, 2, 3],
            "parts": [["a", "b"], ["c"], []],
        }
    )
    assert df.schema_fields()["parts"].__origin__ is list
    got = df.collect(as_lists=True)
    assert got == {"id": [1, 2, 3], "parts": [["a", "b"], ["c"], []]}
    f = df.filter(df.id > 1)
    assert f.collect(as_lists=True) == {"id": [2, 3], "parts": [["c"], []]}
    s = df.select("parts", "id")
    assert set(s.collect(as_lists=True).keys()) == {"parts", "id"}


def test_list_int_nullable_column_cells() -> None:
    df = DataFrame[_TagsOpt](
        {
            "id": [1, 2, 3],
            "tags": [[1, 2], None, []],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1, 2, 3],
        "tags": [[1, 2], None, []],
    }


def test_list_int_validation_rejects_bad_element_type() -> None:
    with pytest.raises(ValidationError):
        DataFrame[_TagsInt]({"id": [1], "tags": [["not", "ints"]]})


def test_list_struct_roundtrip() -> None:
    df = DataFrame[_RowWithStructList](
        {
            "id": [1, 2],
            "items": [
                [{"street": "a"}, {"street": "b"}],
                [{"street": "c"}],
            ],
        }
    )
    assert df.schema_fields()["items"].__origin__ is list
    assert df.collect(as_lists=True) == {
        "id": [1, 2],
        "items": [
            [{"street": "a"}, {"street": "b"}],
            [{"street": "c"}],
        ],
    }


def test_explode_empty_list_rows() -> None:
    df = DataFrame[_TagsInt](
        {
            "id": [1, 2],
            "tags": [[], [1, 2]],
        }
    )
    ex = df.explode("tags")
    # Empty list contributes no rows for that id in Polars explode.
    assert ex.collect(as_lists=True) == {"id": [2, 2], "tags": [1, 2]}


def test_explode_null_list_cell_keeps_row() -> None:
    """Nullable list column: None cell survives as null inner with keep_nulls."""
    df = DataFrame[_TagsOpt](
        {
            "id": [1, 2],
            "tags": [None, [1]],
        }
    )
    ex = df.explode("tags")
    out = ex.collect(as_lists=True)
    assert out["id"] == [1, 2]
    assert out["tags"][0] is None
    assert out["tags"][1] == 1


def test_explode_multi_column_requires_matching_lengths() -> None:
    df = DataFrame[_TwoLists](
        {
            "id": [1, 2],
            "a": [[10, 20], [30]],
            "b": [["x", "y"], ["z"]],
        }
    )
    out = df.explode(["a", "b"]).collect(as_lists=True)
    assert out == {
        "id": [1, 1, 2],
        "a": [10, 20, 30],
        "b": ["x", "y", "z"],
    }


def test_explode_mismatched_list_lengths_errors() -> None:
    df = DataFrame[_TwoLists](
        {
            "id": [1],
            "a": [[1, 2]],
            "b": [["only"]],
        }
    )
    with pytest.raises((ValueError, RuntimeError, OSError)):
        df.explode(["a", "b"]).collect(as_lists=True)


def test_arithmetic_rejects_list_column() -> None:
    df = DataFrame[_TagsInt]({"id": [1], "tags": [[1, 2]]})
    with pytest.raises(TypeError, match="list-typed"):
        _ = df.tags + 1


def test_compare_equality_rejects_list_column() -> None:
    df = DataFrame[_TagsInt]({"id": [1], "tags": [[1, 2]]})
    with pytest.raises(TypeError, match="list columns"):
        _ = df.tags == df.tags


def test_join_preserves_list_column() -> None:
    class L(Schema):
        k: int
        tags: list[int]

    class R(Schema):
        k: int
        v: int

    left = DataFrame[L]({"k": [1], "tags": [[1, 2]]})
    right = DataFrame[R]({"k": [1], "v": [10]})
    j = left.join(right, on="k", how="inner")
    assert j.schema_fields()["tags"].__origin__ is list
    assert j.collect(as_lists=True) == {"k": [1], "tags": [[1, 2]], "v": [10]}


def test_concat_vertical_list_columns() -> None:
    df1 = DataFrame[_TagsInt]({"id": [1], "tags": [[1]]})
    df2 = DataFrame[_TagsInt]({"id": [2], "tags": [[2, 3]]})
    cat = DataFrame.concat([df1, df2], how="vertical")
    assert cat.schema_fields()["tags"].__origin__ is list
    assert cat.collect(as_lists=True) == {
        "id": [1, 2],
        "tags": [[1], [2, 3]],
    }


def test_nested_list_int_roundtrip() -> None:
    class Nested(Schema):
        id: int
        grid: list[list[int]]

    df = DataFrame[Nested](
        {
            "id": [1],
            "grid": [[[1, 2], [3]]],
        }
    )
    assert df.collect(as_lists=True) == {"id": [1], "grid": [[[1, 2], [3]]]}


def test_schema_fields_optional_list_matches_annotation() -> None:
    fields = DataFrame[_TagsOpt]({"id": [1], "tags": [None]}).schema_fields()
    assert fields["tags"] == list[int] | None
