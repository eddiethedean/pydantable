from __future__ import annotations

from typing import Any


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
        # Placeholder API surface for phase P6. Current execution paths do not
        # yet model full window-expression AST lowering.
        _ = partition_by
        _ = order_by
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
