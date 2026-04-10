"""PlanFrame expression keys on DataFrameModel (Phase 3 surface)."""

from __future__ import annotations

import pytest
from planframe.backend.errors import PlanFrameBackendError
from planframe.expr import api as pf
from pydantable import DataFrameModel


class _SortDF(DataFrameModel):
    a: int
    b: int


def test_dataframe_model_sort_planframe_col_expr() -> None:
    df = _SortDF({"a": [3, 1, 2], "b": [10, 20, 30]})
    out = df.sort(pf.col("a")).to_dict()
    assert out == {"a": [1, 2, 3], "b": [20, 30, 10]}


class _JoinL(DataFrameModel):
    a: int
    x: int


class _JoinR(DataFrameModel):
    a: int
    y: int


def test_dataframe_model_join_planframe_expr_keys() -> None:
    left = _JoinL({"a": [1, 2], "x": [10, 20]})
    right = _JoinR({"a": [1, 3], "y": [100, 300]})
    out = left.join(
        right,
        left_on=(pf.col("a"),),
        right_on=(pf.col("a"),),
        how="inner",
    ).to_dict()
    assert out == {"y": [100], "a": [1], "x": [10]}


class _GrpDF(DataFrameModel):
    g: int
    x: float


def test_dataframe_model_group_by_planframe_col_normalizes_to_str() -> None:
    """``pf.Col(name)`` group keys normalize to str for PlanFrame agg compilation."""

    m = _GrpDF({"g": [1, 1, 2], "x": [10.0, 20.0, 5.0]})
    out_str = m.group_by("g").agg(s=pf.agg_sum(pf.col("x"))).to_dict()
    out_col = m.group_by(pf.col("g")).agg(s=pf.agg_sum(pf.col("x"))).to_dict()
    assert sorted(zip(out_str["g"], out_str["s"], strict=True)) == sorted(
        zip(out_col["g"], out_col["s"], strict=True)
    )
    assert sorted(zip(out_col["g"], out_col["s"], strict=True)) == [(1, 30.0), (2, 5.0)]


def test_dataframe_model_group_by_non_trivial_expr_agg_not_supported_yet() -> None:
    """Composite expr group keys may fail at agg until schema/compile follow-up."""

    m = _GrpDF({"g": [1, 2], "x": [1.0, 1.0]})
    key = pf.Add(pf.col("g"), pf.lit(0))
    with pytest.raises((PlanFrameBackendError, KeyError)):
        m.group_by(key).agg(s=pf.agg_sum(pf.col("x"))).to_dict()


class _PivotDF(DataFrameModel):
    id: int
    k: str
    v: int


def test_dataframe_model_pivot_longer_planframe_roundtrip() -> None:
    """Milestone 3C: reshape helpers already delegate to PlanFrame; lock behavior."""

    df = _PivotDF({"id": [1, 1], "k": ["A", "B"], "v": [10, 20]})
    long_df = df.pivot_longer(
        id_vars=["id"], value_vars=["v"], names_to="var", values_to="val"
    )
    assert long_df.collect(as_lists=True) == {
        "id": [1, 1],
        "var": ["v", "v"],
        "val": [10, 20],
    }
