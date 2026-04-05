"""Error paths on grouped ``agg`` (column / Expr validation)."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema
from pydantable.expressions import Literal


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


def test_grouped_agg_spec_must_be_length_two_tuple() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    with pytest.raises(TypeError, match="agg\\(\\) expects specs like"):
        g.agg(s=("sum",))  # type: ignore[arg-type]


def test_grouped_agg_operator_must_be_string() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    with pytest.raises(TypeError, match="operator must be a string"):
        g.agg(s=(123, "a"))  # type: ignore[arg-type]


def test_grouped_agg_column_must_be_name_or_single_column_expr() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    with pytest.raises(TypeError, match="column name or Expr"):
        g.agg(s=("sum", 99))  # type: ignore[arg-type]


def test_grouped_agg_expr_must_reference_one_column_not_zero() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    with pytest.raises(TypeError, match="exactly one column"):
        g.agg(s=("sum", Literal(value=1)))


def test_grouped_dataframe_text_repr_contains_keys() -> None:
    df = DataFrame[_G]({"id": [1], "a": [1], "b": [2]})
    g = df.group_by("id")
    assert "GroupedDataFrame" in repr(g) and "'id'" in repr(g)


class _TS(Schema):
    id: int
    ts: int
    v: int | None


def test_dynamic_grouped_repr_and_html() -> None:
    df = DataFrame[_TS]({"id": [1, 1], "ts": [0, 3600], "v": [10, 20]})
    dg = df.group_by_dynamic("ts", every="1h", by=["id"])
    r = repr(dg)
    assert "DynamicGroupedDataFrame" in r and "ts" in r and "1h" in r
    html = dg._repr_html_()
    assert "DynamicGroupedDataFrame" in html and "ts" in html


def test_dynamic_grouped_agg_validation() -> None:
    df = DataFrame[_TS]({"id": [1], "ts": [0], "v": [1]})
    dg = df.group_by_dynamic("ts", every="1h", by=["id"])
    with pytest.raises(TypeError, match="agg\\(\\) expects specs like"):
        dg.agg(bad=("sum",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be strings"):
        dg.agg(s=(1, "v"))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be strings"):
        dg.agg(s=("sum", 1))  # type: ignore[arg-type]
