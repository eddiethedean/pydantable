from __future__ import annotations

import warnings
from typing import Any

from .rust_engine import _require_rust_core


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

    def referenced_columns(self) -> set[str]:
        return set(self._rust_expr.referenced_columns())

    def _coerce_other(self, other: Any) -> Expr:
        if isinstance(other, Expr):
            return other
        return Literal(value=other)

    def _binary(self, op_symbol: str, other: Any) -> Expr:
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            op_symbol, self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def _binary_reflected(self, op_symbol: str, other: Any) -> Expr:
        # `other <op> self`
        left_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            op_symbol, left_expr._rust_expr, self._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def _compare(self, op_symbol: str, other: Any) -> Expr:
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            op_symbol, self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def cast(self, dtype: Any) -> Expr:
        rust_expr = _require_rust_core().cast_expr(self._rust_expr, dtype)
        return Expr(rust_expr=rust_expr)

    def is_null(self) -> Expr:
        rust_expr = _require_rust_core().is_null_expr(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def is_not_null(self) -> Expr:
        rust_expr = _require_rust_core().is_not_null_expr(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def over(
        self,
        partition_by: str | list[str] | tuple[str, ...] | None = None,
        order_by: str | list[str] | tuple[str, ...] | None = None,
    ) -> Expr:
        if partition_by is not None or order_by is not None:
            warnings.warn(
                "Expr.over(partition_by=..., order_by=...) is not yet implemented; "
                "arguments are ignored and the expression is evaluated without "
                "window framing.",
                UserWarning,
                stacklevel=2,
            )
        return self

    # Arithmetic
    def __add__(self, other: Any) -> Expr:
        return self._binary("+", other)

    def __sub__(self, other: Any) -> Expr:
        return self._binary("-", other)

    def __mul__(self, other: Any) -> Expr:
        return self._binary("*", other)

    def __truediv__(self, other: Any) -> Expr:
        return self._binary("/", other)

    def __radd__(self, other: Any) -> Expr:
        return self._binary_reflected("+", other)

    def __rsub__(self, other: Any) -> Expr:
        return self._binary_reflected("-", other)

    def __rmul__(self, other: Any) -> Expr:
        return self._binary_reflected("*", other)

    def __rtruediv__(self, other: Any) -> Expr:
        return self._binary_reflected("/", other)

    # Comparisons
    def __eq__(self, other: Any) -> Expr:  # type: ignore[override]
        return self._compare("==", other)

    def __ne__(self, other: Any) -> Expr:  # type: ignore[override]
        return self._compare("!=", other)

    def __lt__(self, other: Any) -> Expr:
        return self._compare("<", other)

    def __le__(self, other: Any) -> Expr:
        return self._compare("<=", other)

    def __gt__(self, other: Any) -> Expr:
        return self._compare(">", other)

    def __ge__(self, other: Any) -> Expr:
        return self._compare(">=", other)

    def isin(self, *values: Any) -> Expr:
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            vals = list(values[0])
        else:
            vals = list(values)
        rust_expr = _require_rust_core().expr_in_list(self._rust_expr, vals)
        return Expr(rust_expr=rust_expr)

    def between(self, low: Any, high: Any) -> Expr:
        lo = self._coerce_other(low)
        hi = self._coerce_other(high)
        rust_expr = _require_rust_core().expr_between(
            self._rust_expr, lo._rust_expr, hi._rust_expr
        )
        return Expr(rust_expr=rust_expr)

    def substr(self, start: Any, length: Any | None = None) -> Expr:
        st = self._coerce_other(start)
        rust = _require_rust_core()
        if length is None:
            rust_expr = rust.expr_substring(self._rust_expr, st._rust_expr, None)
        else:
            ln = self._coerce_other(length)
            rust_expr = rust.expr_substring(
                self._rust_expr, st._rust_expr, ln._rust_expr
            )
        return Expr(rust_expr=rust_expr)

    def char_length(self) -> Expr:
        rust_expr = _require_rust_core().expr_string_length(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def struct_field(self, name: str) -> Expr:
        rust_expr = _require_rust_core().expr_struct_field(self._rust_expr, name)
        return Expr(rust_expr=rust_expr)

    # Numeric
    def abs(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_abs(self._rust_expr))

    def round(self, decimals: int = 0) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_round(self._rust_expr, int(decimals)))

    def floor(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_floor(self._rust_expr))

    def ceil(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_ceil(self._rust_expr))

    # Strings
    def strip(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "strip"))

    def upper(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "upper"))

    def lower(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "lower"))

    def str_replace(self, pattern: str, replacement: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_replace(
                self._rust_expr, str(pattern), str(replacement)
            )
        )

    def strip_prefix(self, prefix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(
                self._rust_expr, "strip_prefix", str(prefix)
            )
        )

    def strip_suffix(self, suffix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(
                self._rust_expr, "strip_suffix", str(suffix)
            )
        )

    def strip_chars(self, chars: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(self._rust_expr, "strip_chars", str(chars))
        )

    # Boolean logic (typed; operands must be boolean expressions)
    def __and__(self, other: Any) -> Expr:
        right = other if isinstance(other, Expr) else self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_and(self._rust_expr, right._rust_expr))

    def __rand__(self, other: Any) -> Expr:
        left = self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_and(left._rust_expr, self._rust_expr))

    def __or__(self, other: Any) -> Expr:
        right = other if isinstance(other, Expr) else self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_or(self._rust_expr, right._rust_expr))

    def __ror__(self, other: Any) -> Expr:
        left = self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_or(left._rust_expr, self._rust_expr))

    def __invert__(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_not(self._rust_expr))

    # Datetime / date parts (Rust validates column type)
    def dt_year(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "year"))

    def dt_month(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "month"))

    def dt_day(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "day"))

    def dt_hour(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "hour"))

    def dt_minute(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "minute"))

    def dt_second(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "second"))

    def dt_date(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_datetime_to_date(self._rust_expr))

    # List columns
    def list_len(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_len(self._rust_expr))

    def list_get(self, index: Any) -> Expr:
        rust = _require_rust_core()
        idx = index if isinstance(index, Expr) else Literal(value=index)
        return Expr(
            rust_expr=rust.expr_list_get(self._rust_expr, idx._rust_expr),
        )

    def list_contains(self, value: Any) -> Expr:
        rust = _require_rust_core()
        v = value if isinstance(value, Expr) else Literal(value=value)
        return Expr(
            rust_expr=rust.expr_list_contains(self._rust_expr, v._rust_expr),
        )

    def list_min(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_min(self._rust_expr))

    def list_max(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_max(self._rust_expr))

    def list_sum(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_sum(self._rust_expr))


class WhenChain:
    """Chained ``when`` / ``otherwise`` (Spark-style)."""

    def __init__(self, condition: Expr, value: Expr):
        if not isinstance(condition, Expr) or not isinstance(value, Expr):
            raise TypeError("when() expects Expr arguments.")
        self._branches: list[tuple[Expr, Expr]] = [(condition, value)]

    def when(self, condition: Expr, value: Expr) -> WhenChain:
        if not isinstance(condition, Expr) or not isinstance(value, Expr):
            raise TypeError("when().when(...) expects Expr arguments.")
        self._branches.append((condition, value))
        return self

    def otherwise(self, value: Expr) -> Expr:
        if not isinstance(value, Expr):
            raise TypeError("otherwise() expects an Expr.")
        rust = _require_rust_core()
        conds = [c._rust_expr for c, _ in self._branches]
        thens = [v._rust_expr for _, v in self._branches]
        return Expr(rust_expr=rust.expr_case_when(conds, thens, value._rust_expr))


def when(condition: Expr, value: Expr) -> WhenChain:
    """First branch of a ``CASE WHEN`` (chain ``.when(...).otherwise(...)``)."""
    return WhenChain(condition, value)


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


def coalesce(*exprs: Expr) -> Expr:
    """SQL ``coalesce``: first non-null among compatible typed expressions."""
    if not exprs:
        raise TypeError("coalesce() requires at least one expression.")
    rust = _require_rust_core()
    return Expr(
        rust_expr=rust.coalesce_exprs([e._rust_expr for e in exprs]),
    )


def concat(*exprs: Expr) -> Expr:
    """Concatenate string expressions."""
    if len(exprs) < 2:
        raise TypeError("concat() requires at least two expressions.")
    for e in exprs:
        if not isinstance(e, Expr):
            raise TypeError("concat() arguments must be Expr instances.")
    rust = _require_rust_core()
    return Expr(rust_expr=rust.expr_string_concat([e._rust_expr for e in exprs]))
