"""PlanFrame ``Over`` lowering via ``planframe_adapter.expr`` (issue #5)."""

from __future__ import annotations

import pytest
from planframe.backend.errors import PlanFrameBackendError
from planframe.expr import api as pf
from pydantable import DataFrameModel
from pydantable.planframe_adapter.execute import execute_frame


class _Part(DataFrameModel):
    k: int
    t: int
    x: float


def test_planframe_over_agg_sum_partition_and_order_by() -> None:
    """``over(..., partition_by=..., order_by=...)`` with inner ``AggExpr`` sum."""
    m = _Part({"k": [1, 1, 2], "t": [1, 2, 1], "x": [10.0, 20.0, 5.0]})
    expr = pf.over(
        pf.agg_sum(pf.col("x")),
        partition_by=("k",),
        order_by=("t",),
    )
    out = execute_frame(m._pf.with_columns(win_sum=expr))
    # Unframed window sum: partition total of ``x`` for each ``k`` (repeated per row).
    assert out.to_dict()["win_sum"] == [30.0, 30.0, 5.0]


def test_planframe_over_agg_mean_partition_only() -> None:
    m = _Part({"k": [1, 1, 2], "t": [1, 2, 1], "x": [10.0, 20.0, 6.0]})
    expr = pf.over(pf.agg_mean(pf.col("x")), partition_by=("k",), order_by=None)
    out = execute_frame(m._pf.with_columns(win_mean=expr))
    assert out.to_dict()["win_mean"] == [15.0, 15.0, 6.0]


def test_planframe_over_unknown_partition_column() -> None:
    m = _Part({"k": [1], "t": [1], "x": [1.0]})
    expr = pf.over(pf.agg_sum(pf.col("x")), partition_by=("missing",), order_by=None)
    with pytest.raises(PlanFrameBackendError) as ei:
        execute_frame(m._pf.with_columns(s=expr))
    assert isinstance(ei.value.__cause__, KeyError)
    assert "missing" in str(ei.value.__cause__)
