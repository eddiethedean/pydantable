"""Error paths on grouped ``agg`` (column / Expr validation)."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema


class _G(Schema):
    id: int
    a: int
    b: int


def test_grouped_agg_expr_must_reference_exactly_one_column() -> None:
    df = DataFrame[_G]({"id": [1, 1], "a": [10, 20], "b": [1, 2]})
    g = df.group_by("id")
    bad = df.a + df.b
    with pytest.raises(TypeError, match="exactly one column"):
        g.agg(s=("sum", bad))


def test_grouped_sum_requires_column_name() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    with pytest.raises(ValueError, match="at least one column"):
        g.sum()


def test_grouped_dataframe_repr_html_includes_keys() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    html = g._repr_html_()
    assert "GroupedDataFrame" in html and "id" in html


def test_grouped_convenience_methods_with_streaming_flag() -> None:
    df = DataFrame[_G]({"id": [1, 1], "a": [10, 20], "b": [1, 2]})
    g = df.group_by("id")
    g.sum("a", streaming=True).collect(as_lists=True)
    g.mean("a", streaming=True).collect(as_lists=True)
    g.min("a", streaming=True).collect(as_lists=True)
    g.max("a", streaming=True).collect(as_lists=True)
    g.count("a", streaming=True).collect(as_lists=True)
    g.len(streaming=True).collect(as_lists=True)
