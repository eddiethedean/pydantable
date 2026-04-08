from __future__ import annotations

from typing import Any

from pydantable.planframe_adapter.errors import require_planframe


def compile_expr(expr: Any, *, schema_fields: dict[str, Any]) -> Any:
    """
    Lower a PlanFrame Expr to a pydantable Expr using known column dtypes.

    PlanFrame 0.3+ passes a Schema into `BaseAdapter.compile_expr(...)` so adapters can
    lower dtype-aware column references without requiring a concrete backend frame.
    """

    require_planframe()
    return _to_pyd_expr(expr, schema_fields=schema_fields)


def _to_pyd_expr(expr: Any, *, schema_fields: dict[str, Any]) -> Any:
    require_planframe()
    from planframe.expr import api as pf

    from pydantable.expressions import ColumnRef, Literal
    from pydantable.expressions import Expr as PydExpr

    if isinstance(expr, PydExpr):
        return expr

    if isinstance(expr, pf.Expr):
        pass
    else:
        raise TypeError("Expected a PlanFrame Expr.")

    if isinstance(expr, pf.Col):
        if expr.name not in schema_fields:
            raise KeyError(f"Unknown column {expr.name!r} for PlanFrame expression.")
        return ColumnRef(name=expr.name, dtype=schema_fields[expr.name])
    if isinstance(expr, pf.Lit):
        return Literal(value=expr.value)

    # Binary arithmetic
    if isinstance(expr, pf.Add):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) + _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Sub):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) - _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Mul):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) * _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.TrueDiv):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) / _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )

    # Comparisons
    if isinstance(expr, pf.Eq):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) == _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Ne):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) != _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Lt):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) < _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Le):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) <= _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Gt):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) > _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Ge):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) >= _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )

    # Null + membership
    if isinstance(expr, pf.IsNull):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).is_null()
    if isinstance(expr, pf.IsNotNull):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).is_not_null()
    if isinstance(expr, pf.IsIn):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).is_in(
            list(expr.options)
        )

    # Boolean ops
    if isinstance(expr, pf.And):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) & _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Or):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) | _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Not):
        return ~_to_pyd_expr(expr.value, schema_fields=schema_fields)
    if isinstance(expr, pf.Xor):
        return _to_pyd_expr(expr.left, schema_fields=schema_fields) ^ _to_pyd_expr(
            expr.right, schema_fields=schema_fields
        )

    # Common scalar functions
    if isinstance(expr, pf.Abs):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).abs()
    if isinstance(expr, pf.Round):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).round(expr.ndigits)
    if isinstance(expr, pf.Floor):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).floor()
    if isinstance(expr, pf.Ceil):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).ceil()
    if isinstance(expr, pf.Coalesce):
        from pydantable.expressions import coalesce

        return coalesce(
            *[_to_pyd_expr(v, schema_fields=schema_fields) for v in expr.values]
        )

    if isinstance(expr, pf.IfElse):
        from pydantable.expressions import when

        return when(
            _to_pyd_expr(expr.cond, schema_fields=schema_fields),
            _to_pyd_expr(expr.then_value, schema_fields=schema_fields),
        ).otherwise(_to_pyd_expr(expr.else_value, schema_fields=schema_fields))

    if isinstance(expr, pf.Between):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).is_between(
            _to_pyd_expr(expr.low, schema_fields=schema_fields),
            _to_pyd_expr(expr.high, schema_fields=schema_fields),
            closed=expr.closed,
        )

    if isinstance(expr, pf.Clip):
        # pydantable's `case_when` requires else dtype == then dtype.
        # Coerce literal bounds to the column dtype when possible.
        clip_dtype = None
        if isinstance(expr.value, pf.Col):
            field = schema_fields.get(expr.value.name)
            if field is not None:
                clip_dtype = field

        lower = (
            None
            if expr.lower is None
            else _to_pyd_expr(expr.lower, schema_fields=schema_fields)
        )
        upper = (
            None
            if expr.upper is None
            else _to_pyd_expr(expr.upper, schema_fields=schema_fields)
        )
        if clip_dtype is not None:
            if lower is not None:
                lower = lower.cast(clip_dtype)
            if upper is not None:
                upper = upper.cast(clip_dtype)
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).clip(
            lower=lower, upper=upper
        )

    if isinstance(expr, pf.Pow):
        return _to_pyd_expr(expr.base, schema_fields=schema_fields) ** _to_pyd_expr(
            expr.exponent, schema_fields=schema_fields
        )
    if isinstance(expr, pf.Exp):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).exp()
    if isinstance(expr, pf.Log):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).log()

    if isinstance(expr, pf.StrContains):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).str_contains(
            expr.pattern, literal=expr.literal
        )
    if isinstance(expr, pf.StrStartsWith):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).str_starts_with(
            expr.prefix
        )
    if isinstance(expr, pf.StrEndsWith):
        return _to_pyd_expr(expr.value, schema_fields=schema_fields).str_ends_with(
            expr.suffix
        )

    raise NotImplementedError(
        f"Unsupported PlanFrame expression node: {type(expr).__name__}"
    )
