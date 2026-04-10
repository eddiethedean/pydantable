"""Lower PlanFrame expression IR to pydantable ``Expr`` objects.

**Supported surface** (keep in sync with adapter tests and
``docs/PLANFRAME_ADAPTER_ROADMAP.md`` §1.1):

- **Refs / literals:** ``Col``, ``Lit``
- **Arithmetic / compare / boolean / null / membership:** binary ops, ``IsNull``,
  ``IsIn``, ``And``, ``Or``, ``Not``, ``Xor``
- **Scalars:** ``Abs``, ``Round``, ``Floor``, ``Ceil``, ``Coalesce``, ``IfElse``,
  ``Between``, ``Clip``, ``Pow``, ``Exp``, ``Log``, ``Sqrt``, ``IsFinite``
- **Strings:** ``StrContains``, ``StrStartsWith``, ``StrEndsWith``, ``StrLower``,
  ``StrUpper``, ``StrLen``, ``StrStrip``, ``StrReplace``, ``StrSplit``
- **Datetime parts:** ``DtYear``, ``DtMonth``, ``DtDay``
- **AggExpr** (ops): ``count``, ``sum``, ``mean``, ``min``, ``max``, ``median``,
  ``std``, ``var``, ``first``, ``last``, ``n_unique`` (must match
  :meth:`pydantable.dataframe.grouped.GroupedDataFrame.agg`)
- **Over:** inner node must be ``AggExpr`` with ops ``sum``, ``mean``, ``min``, ``max``

Any other node raises ``NotImplementedError`` with a stable message.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantable.planframe_adapter.errors import require_planframe


def _planframe_window_spec(
    partition_by: tuple[str, ...],
    order_by: tuple[str, ...] | None,
) -> Any:
    """Build :class:`~pydantable.window_spec.WindowSpec` from PlanFrame ``Over``."""

    from pydantable.window_spec import Window

    if not partition_by:
        raise ValueError("PlanFrame Over requires non-empty partition_by.")
    if order_by is not None and not order_by:
        raise ValueError(
            "PlanFrame Over order_by must be non-empty when provided "
            "(use None for partition-only windows)."
        )
    if order_by is None:
        return Window.partitionBy(*partition_by).spec()
    return Window.partitionBy(*partition_by).orderBy(*order_by)


def _validate_planframe_window_columns(
    partition_by: tuple[str, ...],
    order_by: tuple[str, ...] | None,
    *,
    schema_fields: dict[str, Any],
) -> None:
    for name in partition_by:
        if name not in schema_fields:
            raise KeyError(f"Unknown column {name!r} in PlanFrame Over partition_by.")
    if order_by is not None:
        for name in order_by:
            if name not in schema_fields:
                raise KeyError(f"Unknown column {name!r} in PlanFrame Over order_by.")


def compile_expr(
    expr: Any,
    *,
    schema_fields: dict[str, Any],
    allow_unknown_cols: bool = False,
    resolve_col: Callable[[str], Any] | None = None,
) -> Any:
    """
    Lower a PlanFrame Expr to a pydantable Expr using known column dtypes.

    PlanFrame 0.3+ passes a Schema into `BaseAdapter.compile_expr(...)` so adapters can
    lower dtype-aware column references without requiring a concrete backend frame.

    When *resolve_col* is provided (typically wired to ``BaseAdapter.resolve_dtype``),
    it is used for column names missing from *schema_fields* (e.g. projected schemas).
    """

    require_planframe()
    return _to_pyd_expr(
        expr,
        schema_fields=schema_fields,
        allow_unknown_cols=allow_unknown_cols,
        resolve_col=resolve_col,
    )


def _to_pyd_expr(
    expr: Any,
    *,
    schema_fields: dict[str, Any],
    allow_unknown_cols: bool = False,
    resolve_col: Callable[[str], Any] | None = None,
) -> Any:
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
            if resolve_col is not None:
                resolved = resolve_col(expr.name)
                if resolved is not None:
                    return ColumnRef(name=expr.name, dtype=resolved)
            if allow_unknown_cols:
                # Group-by ``AggExpr`` compilation uses the *output* Frame schema, which
                # omits non-key source columns; aggregation still references them on the
                # pre-group frame. Use a permissive scalar dtype so :class:`ColumnRef`
                # can be built; the group-by engine still resolves the column on the
                # input frame by name.
                return ColumnRef(name=expr.name, dtype=float)
            raise KeyError(f"Unknown column {expr.name!r} for PlanFrame expression.")
        return ColumnRef(name=expr.name, dtype=schema_fields[expr.name])
    if isinstance(expr, pf.Lit):
        return Literal(value=expr.value)

    def _rec(e: Any) -> Any:
        return _to_pyd_expr(
            e,
            schema_fields=schema_fields,
            allow_unknown_cols=allow_unknown_cols,
            resolve_col=resolve_col,
        )

    # Binary arithmetic
    if isinstance(expr, pf.Add):
        return _rec(expr.left) + _rec(expr.right)
    if isinstance(expr, pf.Sub):
        return _rec(expr.left) - _rec(expr.right)
    if isinstance(expr, pf.Mul):
        return _rec(expr.left) * _rec(expr.right)
    if isinstance(expr, pf.TrueDiv):
        return _rec(expr.left) / _rec(expr.right)

    # Comparisons
    if isinstance(expr, pf.Eq):
        return _rec(expr.left) == _rec(expr.right)
    if isinstance(expr, pf.Ne):
        return _rec(expr.left) != _rec(expr.right)
    if isinstance(expr, pf.Lt):
        return _rec(expr.left) < _rec(expr.right)
    if isinstance(expr, pf.Le):
        return _rec(expr.left) <= _rec(expr.right)
    if isinstance(expr, pf.Gt):
        return _rec(expr.left) > _rec(expr.right)
    if isinstance(expr, pf.Ge):
        return _rec(expr.left) >= _rec(expr.right)

    # Null + membership
    if isinstance(expr, pf.IsNull):
        return _rec(expr.value).is_null()
    if isinstance(expr, pf.IsNotNull):
        return _rec(expr.value).is_not_null()
    if isinstance(expr, pf.IsIn):
        return _rec(expr.value).is_in(list(expr.options))

    # Boolean ops
    if isinstance(expr, pf.And):
        return _rec(expr.left) & _rec(expr.right)
    if isinstance(expr, pf.Or):
        return _rec(expr.left) | _rec(expr.right)
    if isinstance(expr, pf.Not):
        return ~_rec(expr.value)
    if isinstance(expr, pf.Xor):
        return _rec(expr.left) ^ _rec(expr.right)

    # Common scalar functions
    if isinstance(expr, pf.Abs):
        return _rec(expr.value).abs()
    if isinstance(expr, pf.Round):
        return _rec(expr.value).round(expr.ndigits)
    if isinstance(expr, pf.Floor):
        return _rec(expr.value).floor()
    if isinstance(expr, pf.Ceil):
        return _rec(expr.value).ceil()
    if isinstance(expr, pf.Coalesce):
        from pydantable.expressions import coalesce

        return coalesce(*[_rec(v) for v in expr.values])

    if isinstance(expr, pf.IfElse):
        from pydantable.expressions import when

        return when(
            _rec(expr.cond),
            _rec(expr.then_value),
        ).otherwise(_rec(expr.else_value))

    if isinstance(expr, pf.Between):
        return _rec(expr.value).is_between(
            _rec(expr.low),
            _rec(expr.high),
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

        lower = None if expr.lower is None else _rec(expr.lower)
        upper = None if expr.upper is None else _rec(expr.upper)
        if clip_dtype is not None:
            if lower is not None:
                lower = lower.cast(clip_dtype)
            if upper is not None:
                upper = upper.cast(clip_dtype)
        return _rec(expr.value).clip(lower=lower, upper=upper)

    if isinstance(expr, pf.Pow):
        return _rec(expr.base) ** _rec(expr.exponent)
    if isinstance(expr, pf.Exp):
        return _rec(expr.value).exp()
    if isinstance(expr, pf.Log):
        return _rec(expr.value).log()

    if isinstance(expr, pf.Sqrt):
        return _rec(expr.value).sqrt()
    if isinstance(expr, pf.IsFinite):
        return _rec(expr.value).is_finite()

    # Group-by: PlanFrame ``AggExpr`` lowers to ``(op, inner_expr)`` for
    # :meth:`pydantable.dataframe.grouped.GroupedDataFrame.agg`.
    if isinstance(expr, pf.AggExpr):
        inner_e = _to_pyd_expr(
            expr.inner,
            schema_fields=schema_fields,
            allow_unknown_cols=True,
            resolve_col=resolve_col,
        )
        op = expr.op
        _AGG_OPS = frozenset(
            {
                "count",
                "sum",
                "mean",
                "min",
                "max",
                "median",
                "std",
                "var",
                "first",
                "last",
                "n_unique",
            }
        )
        if op not in _AGG_OPS:
            raise NotImplementedError(
                f"Unsupported PlanFrame AggExpr op: {op!r} "
                f"(supported: {', '.join(sorted(_AGG_OPS))})."
            )
        return (op, inner_e)

    if isinstance(expr, pf.StrContains):
        v = _rec(expr.value)
        if expr.literal:
            return v.str_contains(expr.pattern)
        return v.str_contains_pat(expr.pattern, literal=False)
    if isinstance(expr, pf.StrStartsWith):
        return _rec(expr.value).starts_with(expr.prefix)
    if isinstance(expr, pf.StrEndsWith):
        return _rec(expr.value).ends_with(expr.suffix)

    if isinstance(expr, pf.StrLower):
        return _rec(expr.value).lower()
    if isinstance(expr, pf.StrUpper):
        return _rec(expr.value).upper()
    if isinstance(expr, pf.StrLen):
        return _rec(expr.value).char_length()
    if isinstance(expr, pf.StrStrip):
        return _rec(expr.value).strip()
    if isinstance(expr, pf.StrReplace):
        return _rec(expr.value).str_replace(
            expr.pattern,
            expr.replacement,
            literal=expr.literal,
        )
    if isinstance(expr, pf.StrSplit):
        return _rec(expr.value).str_split(expr.by)

    # Datetime / date parts
    if isinstance(expr, pf.DtYear):
        return _rec(expr.value).dt_year()
    if isinstance(expr, pf.DtMonth):
        return _rec(expr.value).dt_month()
    if isinstance(expr, pf.DtDay):
        return _rec(expr.value).dt_day()

    if isinstance(expr, pf.Over):
        _validate_planframe_window_columns(
            expr.partition_by, expr.order_by, schema_fields=schema_fields
        )
        ws = _planframe_window_spec(expr.partition_by, expr.order_by)
        inner_pf = expr.value
        if isinstance(inner_pf, pf.AggExpr):
            from pydantable.expressions import (
                window_max,
                window_mean,
                window_min,
                window_sum,
            )

            col_e = _rec(inner_pf.inner)
            op = inner_pf.op
            if op == "sum":
                return window_sum(col_e).over(ws)
            if op == "mean":
                return window_mean(col_e).over(ws)
            if op == "min":
                return window_min(col_e).over(ws)
            if op == "max":
                return window_max(col_e).over(ws)
            raise NotImplementedError(
                f"Unsupported AggExpr op inside PlanFrame Over: {op!r} "
                "(supported: sum, mean, min, max)."
            )
        raise NotImplementedError(
            f"Unsupported PlanFrame expression inside Over: {type(inner_pf).__name__} "
            "(only AggExpr is supported)."
        )

    raise NotImplementedError(
        f"Unsupported PlanFrame expression node: {type(expr).__name__}"
    )
