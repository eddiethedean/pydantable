"""Smoke tests for :meth:`Expr.__repr__` and related types."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import (
    _WindowAggPending,
    _WindowFnPending,
    _WindowShiftPending,
    when,
)
from pydantable.window_spec import Window
from pydantic import BaseModel


class _S(BaseModel):
    x: int


def test_column_ref_repr_name() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    r = repr(df.x)
    assert "ColumnRef" in r
    assert "'x'" in r


def test_expr_repr_contains_ast() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    e = df.x + 1
    r = repr(e)
    assert "BinaryOp" in r
    assert "dtype" in r
    assert "ast=" in r


def test_when_chain_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    w = when(df.x > 0, df.x).when(df.x < 0, df.x * -1)
    r = repr(w)
    assert "WhenChain" in r
    assert "2 branches" in r


def test_window_pending_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    assert "_WindowFnPending" in repr(_WindowFnPending("row_number"))
    p = _WindowAggPending(df.x, "sum")
    assert "sum" in repr(p)
    s = _WindowShiftPending(df.x, "lag", 2)
    assert "lag" in repr(s) and "n=2" in repr(s)


def test_window_over_repr_roundtrip_smoke() -> None:
    w = Window.partitionBy("x").orderBy("x")
    e = _WindowFnPending("row_number").over(w)
    assert "Expr" in repr(e)
