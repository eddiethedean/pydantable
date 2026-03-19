from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Union, get_args, get_origin


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


_NoneType = type(None)

# Supported base types for expression typing.
#
# Notes:
# - `bool` is intentionally excluded from arithmetic (even though it's a subclass
#   of `int` in Python).
# - `str` is allowed for equality, and for ordering comparisons (<, <=, >, >=).
_SUPPORTED_BASE_TYPES = (int, float, bool, str)
_NUMERIC_BASE_TYPES = (int, float)


def _split_nullable(dtype: Any) -> tuple[Any, bool]:
    """
    Split a dtype annotation into (base_type, nullable).

    Supported inputs:
    - `int` / `float` / `bool` / `str`
    - `Optional[T]` / `Union[T, None]` where T is one of supported base types
    - `NoneType` for an "unknown base but nullable" literal (`Literal(None)`).
    """
    if dtype is _NoneType:
        # `Literal(None)` - the base type is unknown at this point.
        return (None, True)

    origin = get_origin(dtype)
    if origin is Union:
        args = tuple(get_args(dtype))
        # Only support the simple Optional[T] form for the skeleton.
        if len(args) == 2 and _NoneType in args:
            other = args[0] if args[1] is _NoneType else args[1]
            if other not in _SUPPORTED_BASE_TYPES:
                raise TypeError(f"Unsupported Optional base type: {other!r}")
            return (other, True)

        raise TypeError(f"Unsupported nullable/union dtype: {dtype!r}")

    if dtype not in _SUPPORTED_BASE_TYPES:
        raise TypeError(f"Unsupported dtype: {dtype!r}")

    return (dtype, False)


def _optional_if(nullable: bool, base_type: Any) -> Any:
    if not nullable:
        return base_type
    # Use `Optional[T]` so downstream schema generation keeps a stable annotation.
    return Optional[base_type]  # type: ignore[return-value]


def _infer_arithmetic_result_dtype(
    op_symbol: str, left_dtype: Any, right_dtype: Any
) -> Any:
    left_base, left_nullable = _split_nullable(left_dtype)
    right_base, right_nullable = _split_nullable(right_dtype)

    result_nullable = left_nullable or right_nullable

    # Resolve "unknown base" from Literal(None) on either side.
    if left_base is None and right_base is None:
        raise TypeError("Cannot infer arithmetic result type from Literal(None) alone.")
    if left_base is None:
        left_base = right_base
    if right_base is None:
        right_base = left_base

    if left_base not in _NUMERIC_BASE_TYPES or right_base not in _NUMERIC_BASE_TYPES:
        raise TypeError(
            f"Arithmetic '{op_symbol}' requires numeric operands; got {left_base!r} and {right_base!r}."
        )

    # Division always promotes to float in this skeleton.
    if op_symbol == "/":
        return _optional_if(result_nullable or float in (left_base, right_base), float)

    # +, -, *:
    # - int op int -> int
    # - otherwise -> float
    if left_base is int and right_base is int:
        return _optional_if(result_nullable, int)
    return _optional_if(result_nullable, float)


def _infer_ordering_result_dtype(
    op_symbol: str, left_dtype: Any, right_dtype: Any
) -> Any:
    left_base, left_nullable = _split_nullable(left_dtype)
    right_base, right_nullable = _split_nullable(right_dtype)
    result_nullable = left_nullable or right_nullable

    if left_base is None and right_base is None:
        raise TypeError(
            f"Cannot infer ordering result type from Literal(None) alone for operator {op_symbol!r}."
        )
    if left_base is None:
        left_base = right_base
    if right_base is None:
        right_base = left_base

    allowed_numeric = left_base in _NUMERIC_BASE_TYPES and right_base in _NUMERIC_BASE_TYPES
    allowed_str = left_base is str and right_base is str
    if not (allowed_numeric or allowed_str):
        raise TypeError(
            f"Ordering '{op_symbol}' requires numeric or str operands; got {left_base!r} and {right_base!r}."
        )

    # Ordering comparisons always return bool/Optional[bool].
    return _optional_if(result_nullable, bool)


def _infer_equality_result_dtype(
    op_symbol: str, left_dtype: Any, right_dtype: Any
) -> Any:
    _ = op_symbol
    left_base, left_nullable = _split_nullable(left_dtype)
    right_base, right_nullable = _split_nullable(right_dtype)
    result_nullable = left_nullable or right_nullable

    if left_base is None and right_base is None:
        raise TypeError(f"Cannot infer equality result type from Literal(None) alone.")
    if left_base is None:
        left_base = right_base
    if right_base is None:
        right_base = left_base

    allowed_numeric = left_base in _NUMERIC_BASE_TYPES and right_base in _NUMERIC_BASE_TYPES
    allowed_bool = left_base is bool and right_base is bool
    allowed_str = left_base is str and right_base is str
    if not (allowed_numeric or allowed_bool or allowed_str):
        raise TypeError(
            f"Equality requires both operands to be numeric, both bool, or both str; got {left_base!r} and {right_base!r}."
        )

    return _optional_if(result_nullable, bool)


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
        out: list[Any] = []
        for a, b in zip(lvals, rvals):
            if a is None or b is None:
                out.append(None)
            else:
                out.append(op(a, b))
        return out


@dataclass(frozen=True, eq=False)
class CompareOp(Expr):
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
        op = _CMP_OPS[self.op_symbol]
        out: list[Any] = []
        for a, b in zip(lvals, rvals):
            if a is None or b is None:
                out.append(None)
            else:
                out.append(op(a, b))
        return out


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
    dtype = _infer_arithmetic_result_dtype(op_symbol, left.dtype, right_expr.dtype)
    # `op` is passed for possible future extension; we use symbol lookup at eval time.
    _ = op
    return BinaryOp(op_symbol=op_symbol, left=left, right=right_expr, dtype=dtype)


def _compare_op(left: Expr, right: Any, op_symbol: str, op: Any) -> Expr:
    right_expr = _coerce_to_expr(right, dtype_for_literal=type(right))
    if op_symbol in ("<", "<=", ">", ">="):
        dtype = _infer_ordering_result_dtype(op_symbol, left.dtype, right_expr.dtype)
    else:
        dtype = _infer_equality_result_dtype(op_symbol, left.dtype, right_expr.dtype)
    _ = op
    return CompareOp(op_symbol=op_symbol, left=left, right=right_expr, dtype=dtype)


def _coerce_to_expr(value: Any, dtype_for_literal: Optional[Any] = None) -> Expr:
    if isinstance(value, Expr):
        return value

    # Treat scalars as literals, with strict typing for the skeleton.
    if value is None:
        return Literal(value=value, dtype=_NoneType)

    dtype = dtype_for_literal if dtype_for_literal is not None else type(value)
    if dtype not in _SUPPORTED_BASE_TYPES:
        raise TypeError(
            f"Unsupported literal type for expression typing: {dtype!r}. Supported: {', '.join(t.__name__ for t in _SUPPORTED_BASE_TYPES)}."
        )

    return Literal(value=value, dtype=dtype)


# =============================================================================
# Rust-first wrapper layer
# =============================================================================


def _load_rust_core() -> Any:
    try:
        from . import _core as rust_core  # type: ignore

        return rust_core
    except ImportError:
        return None


_RUST_CORE = _load_rust_core()


def _require_rust_core() -> Any:
    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust extension is required for typed expression building."
        )
    return _RUST_CORE


class Expr:  # type: ignore[override]
    """
    Thin wrapper around Rust-typed expression nodes.

    Operator/type validation happens in Rust at AST-build time.
    """

    def __init__(self, *, rust_expr: Any):
        self._rust_expr = rust_expr

    @property
    def dtype(self) -> Any:
        return self._rust_expr.dtype

    def referenced_columns(self) -> Set[str]:
        return set(self._rust_expr.referenced_columns())

    def _coerce_other(self, other: Any) -> "Expr":
        if isinstance(other, Expr):
            return other
        return Literal(value=other)

    # Arithmetic
    def __add__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            "+", self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def __sub__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            "-", self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def __mul__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            "*", self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def __truediv__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            "/", self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    # Comparisons
    def __eq__(self, other: Any) -> "Expr":  # type: ignore[override]
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            "==", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def __ne__(self, other: Any) -> "Expr":  # type: ignore[override]
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            "!=", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def __lt__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            "<", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def __le__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            "<=", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def __gt__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            ">", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def __ge__(self, other: Any) -> "Expr":
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            ">=", self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)


class ColumnRef(Expr):  # type: ignore[override]
    def __init__(self, *, name: str, dtype: Any):
        rust_expr = _require_rust_core().make_column_ref(
            name=name, dtype_annotation=dtype
        )
        super().__init__(rust_expr=rust_expr)


class Literal(Expr):  # type: ignore[override]
    def __init__(self, *, value: Any, dtype: Any = None):
        # `dtype` is accepted for backwards compatibility with the old skeleton.
        # Rust derives the actual dtype from the provided scalar value.
        _ = dtype
        rust_expr = _require_rust_core().make_literal(value=value)
        super().__init__(rust_expr=rust_expr)


class BinaryOp(Expr):  # type: ignore[override]
    def __init__(self, *, rust_expr: Any):
        super().__init__(rust_expr=rust_expr)


class CompareOp(Expr):  # type: ignore[override]
    def __init__(self, *, rust_expr: Any):
        super().__init__(rust_expr=rust_expr)

