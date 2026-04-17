from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.expressions import AliasedExpr, Expr
from pydantable.schema import make_derived_schema_type

if TYPE_CHECKING:
    from pydantable.dataframe._impl import DataFrame


def plan_with_columns(
    df: DataFrame[Any],
    *exprs: Any,
    **new_columns: Expr | Any,
) -> DataFrame[Any]:
    rust_columns: dict[str, Any] = {}

    for item in exprs:
        if not isinstance(item, AliasedExpr):
            raise TypeError(
                "with_columns() positional args must be Expr.alias('name') "
                "(AliasedExpr)."
            )
        if item.name in rust_columns or item.name in new_columns:
            raise ValueError(f"with_columns() duplicate output column {item.name!r}.")
        rust_columns[item.name] = item.expr._rust_expr

    for name, value in new_columns.items():
        if isinstance(value, Expr):
            rust_columns[name] = value._rust_expr
        else:
            rust_columns[name] = df._engine.make_literal(value=value)

    rust_plan = df._engine.plan_with_columns(df._rust_plan, rust_columns)
    desc = rust_plan.schema_descriptors()
    derived_fields = df._field_types_from_descriptors(desc)
    derived_schema_type = make_derived_schema_type(
        df._current_schema_type, derived_fields
    )

    return df._from_plan(
        root_data=df._root_data,
        root_schema_type=df._root_schema_type,
        current_schema_type=derived_schema_type,
        rust_plan=rust_plan,
        engine=df._engine,
    )
