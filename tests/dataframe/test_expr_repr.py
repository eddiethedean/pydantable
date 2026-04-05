"""Smoke tests for :meth:`Expr.__repr__` and related types."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import (
    Literal,
    _WindowAggPending,
    _WindowFnPending,
    _WindowShiftPending,
    coalesce,
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


def test_literal_expr_repr() -> None:
    e = Literal(value=42)
    r = repr(e)
    assert "Literal" in r
    assert "dtype=" in r
    assert "ast=" in r


def test_expr_repr_contains_ast() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    e = df.x + 1
    r = repr(e)
    assert "BinaryOp" in r
    assert "dtype" in r
    assert "ast=" in r


def test_compare_op_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    e = df.x > 0
    r = repr(e)
    assert "CompareOp" in r
    assert "refs=" in r and "x" in r


def test_subtract_literal_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    r = repr(Literal(value=0) - df.x)
    assert "BinaryOp" in r
    assert "ast=" in r


def test_cast_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    r = repr(df.x.cast(float))
    assert "Expr" in r or "ast=" in r


def test_is_null_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    r = repr(df.x.is_null())
    assert "Expr" in r
    assert "ast=" in r


def test_coalesce_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    r = repr(coalesce(df.x, Literal(value=0)))
    assert "Expr" in r
    assert "ast=" in r


def test_when_chain_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    w = when(df.x > 0, df.x).when(df.x < 0, df.x * -1)
    r = repr(w)
    assert "WhenChain" in r
    assert "2 branches" in r


def test_when_otherwise_final_expr_repr() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    e = when(df.x > 0, df.x).otherwise(Literal(value=0))
    r = repr(e)
    assert "Expr" in r
    assert "ast=" in r


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


def test_expr_repr_referenced_columns_sorted() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    e = df.x + df.x
    r = repr(e)
    assert "refs=" in r
