from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Union


class Expr:
    """
    Typed expression AST.

    The skeleton supports basic arithmetic/comparison so we can infer result
    dtypes and (optionally) execute expressions in pure Python.
    """

    dtype: Any

    def referenced_columns(self) -> Set[str]:
        raise NotImplementedError

    def eval(self, context: Dict[str, list[Any]]) -> list[Any]:
        raise NotImplementedError

    # Arithmetic
    def __add__(self, other: Any) -> "Expr":
        return _binary_op(self, other, "+", op=operator.add)

    def __sub__(self, other: Any) -> "Expr":
        return _binary_op(self, other, "-", op=operator.sub)

    def __mul__(self, other: Any) -> "Expr":
        return _binary_op(self, other, "*", op=operator.mul)

    def __truediv__(self, other: Any) -> "Expr":
        return _binary_op(self, other, "/", op=operator.truediv)

    # Comparisons
    def __eq__(self, other: Any) -> "Expr":  # type: ignore[override]
        return _compare_op(self, other, "==", op=operator.eq)

    def __ne__(self, other: Any) -> "Expr":  # type: ignore[override]
        return _compare_op(self, other, "!=", op=operator.ne)

    def __lt__(self, other: Any) -> "Expr":
        return _compare_op(self, other, "<", op=operator.lt)

    def __le__(self, other: Any) -> "Expr":
        return _compare_op(self, other, "<=", op=operator.le)

    def __gt__(self, other: Any) -> "Expr":
        return _compare_op(self, other, ">", op=operator.gt)

    def __ge__(self, other: Any) -> "Expr":
        return _compare_op(self, other, ">=", op=operator.ge)


@dataclass(frozen=True, eq=False)
class ColumnRef(Expr):
    name: str
    dtype: Any

    def referenced_columns(self) -> Set[str]:
        return {self.name}

    def eval(self, context: Dict[str, list[Any]]) -> list[Any]:
        return context[self.name]


@dataclass(frozen=True, eq=False)
class Literal(Expr):
    value: Any
    dtype: Any

    def referenced_columns(self) -> Set[str]:
        return set()

    def eval(self, context: Dict[str, list[Any]]) -> list[Any]:
        # Determine length from any one column.
        if not context:
            raise ValueError("Cannot evaluate literal without any context columns")
        n = len(next(iter(context.values())))
        return [self.value] * n


@dataclass(frozen=True, eq=False)
class BinaryOp(Expr):
    op_symbol: str
    left: Expr
    right: Expr
    dtype: Any

    def referenced_columns(self) -> Set[str]:
        return self.left.referenced_columns() | self.right.referenced_columns()

    def eval(self, context: Dict[str, list[Any]]) -> list[Any]:
        lvals = self.left.eval(context)
        rvals = self.right.eval(context)
        if len(lvals) != len(rvals):
            raise ValueError("Mismatched expression lengths")
        op = _ARITH_OPS[self.op_symbol]
        return [op(a, b) for a, b in zip(lvals, rvals)]


@dataclass(frozen=True, eq=False)
class CompareOp(Expr):
    op_symbol: str
    left: Expr
    right: Expr
    dtype: Any = bool

    def referenced_columns(self) -> Set[str]:
        return self.left.referenced_columns() | self.right.referenced_columns()

    def eval(self, context: Dict[str, list[Any]]) -> list[Any]:
        lvals = self.left.eval(context)
        rvals = self.right.eval(context)
        if len(lvals) != len(rvals):
            raise ValueError("Mismatched expression lengths")
        op = _CMP_OPS[self.op_symbol]
        return [op(a, b) for a, b in zip(lvals, rvals)]


_ARITH_OPS = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
}

_CMP_OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}


def _binary_op(left: Expr, right: Any, op_symbol: str, op: Any) -> Expr:
    right_expr = _coerce_to_expr(right, dtype_for_literal=type(right))
    dtype = _infer_arithmetic_dtype(left.dtype, right_expr.dtype, op_symbol)
    # `op` is passed for possible future extension; we use symbol lookup at eval time.
    _ = op
    return BinaryOp(op_symbol=op_symbol, left=left, right=right_expr, dtype=dtype)


def _compare_op(left: Expr, right: Any, op_symbol: str, op: Any) -> Expr:
    right_expr = _coerce_to_expr(right, dtype_for_literal=type(right))
    _ = op
    return CompareOp(op_symbol=op_symbol, left=left, right=right_expr)


def _coerce_to_expr(value: Any, dtype_for_literal: Optional[Any] = None) -> Expr:
    if isinstance(value, Expr):
        return value
    # Treat scalars as literals.
    dtype = dtype_for_literal if dtype_for_literal is not None else type(value)
    return Literal(value=value, dtype=dtype)


def _infer_arithmetic_dtype(left_dtype: Any, right_dtype: Any, _op_symbol: str) -> Any:
    # Skeleton heuristic:
    # - if either side is float -> float
    # - else if both sides are int-like -> int
    # - else fallback to `object`
    float_types = (float,)
    int_types = (int,)

    if left_dtype in float_types or right_dtype in float_types:
        return float
    if left_dtype in int_types and right_dtype in int_types:
        return int
    return object

