from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.expressions import Expr

if TYPE_CHECKING:
    from pydantable.dataframe._impl import DataFrame


def plan_filter(df: DataFrame[Any], condition: Expr) -> DataFrame[Any]:
    if not isinstance(condition, Expr):
        raise TypeError("filter(condition) expects an Expr.")

    rust_plan = df._engine.plan_filter(df._rust_plan, condition._rust_expr)
    return df._from_plan(
        root_data=df._root_data,
        root_schema_type=df._root_schema_type,
        current_schema_type=df._current_schema_type,
        rust_plan=rust_plan,
        engine=df._engine,
    )
