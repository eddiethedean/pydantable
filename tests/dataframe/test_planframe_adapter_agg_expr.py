"""PlanFrame ``AggExpr`` lowering for group-by aggregations (issue #6)."""

from __future__ import annotations

from planframe.expr import api as pf
from pydantable import DataFrameModel


class _Grp(DataFrameModel):
    g: int
    x: float


def test_planframe_group_by_agg_sum_agg_expr() -> None:
    """``agg_sum(col(...))`` in named aggregations (not ``(op, name)`` tuple)."""
    m = _Grp({"g": [1, 1, 2], "x": [10.0, 20.0, 5.0]})
    out = m.group_by("g").agg(s=pf.agg_sum(pf.col("x")))
    d = out.to_dict()
    assert sorted(zip(d["g"], d["s"], strict=True)) == [(1, 30.0), (2, 5.0)]


def test_planframe_group_by_agg_count_agg_expr() -> None:
    m = _Grp({"g": [1, 1, 1, 2], "x": [1.0, 2.0, 3.0, 4.0]})
    out = m.group_by("g").agg(n=pf.agg_count(pf.col("x")))
    d = out.to_dict()
    assert sorted(zip(d["g"], d["n"], strict=True)) == [(1, 3), (2, 1)]
