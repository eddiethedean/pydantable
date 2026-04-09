"""PlanFrame numeric/math lowering via ``planframe_adapter.expr`` (issue #4)."""

from __future__ import annotations

import math

from planframe.expr import api as pf
from pydantable import DataFrameModel
from pydantable.planframe_adapter.execute import execute_frame


class _Floats(DataFrameModel):
    x: float


class _Ints(DataFrameModel):
    n: int


def _run_with_column(m: DataFrameModel, pf_expr: object) -> dict[str, list[object]]:
    out = execute_frame(m._pf.with_column("out", pf_expr))
    return out.to_dict()


def test_planframe_sqrt_float() -> None:
    d = _run_with_column(
        _Floats({"x": [4.0, 0.0]}),
        pf.Sqrt(pf.col("x")),
    )
    assert d["out"] == [2.0, 0.0]


def test_planframe_sqrt_non_finite_inputs() -> None:
    d = _run_with_column(
        _Floats({"x": [4.0, float("inf"), float("nan"), 0.0]}),
        pf.Sqrt(pf.col("x")),
    )
    s = d["out"]
    assert s[0] == 2.0
    assert math.isinf(s[1])
    assert isinstance(s[2], float) and math.isnan(s[2])
    assert s[3] == 0.0


def test_planframe_sqrt_int_promotes_to_float() -> None:
    d = _run_with_column(
        _Ints({"n": [4, 9]}),
        pf.Sqrt(pf.col("n")),
    )
    assert d["out"] == [2.0, 3.0]


def test_planframe_is_finite() -> None:
    d = _run_with_column(
        _Floats({"x": [1.0, float("inf"), float("nan"), 0.0]}),
        pf.IsFinite(pf.col("x")),
    )
    assert d["out"] == [True, False, False, True]
