from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from pydantable.backend import _require_rust_core

from .base import Backend


class PolarsBackend(Backend):
    """
    Polars-style execution backed by the existing Rust/Polars core.
    """

    name = "polars"

    def execute_plan(self, plan: Any, data: Any) -> Any:
        rust = _require_rust_core()
        return rust.execute_plan(plan, data)

    def execute_join(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        left_on: Sequence[str],
        right_on: Sequence[str],
        how: str,
        suffix: str,
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_join(
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            list(left_on),
            list(right_on),
            how,
            suffix,
        )

    def execute_groupby_agg(
        self,
        plan: Any,
        root_data: Any,
        by: Sequence[str],
        aggregations: Any,
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_groupby_agg(
            plan,
            root_data,
            list(by),
            aggregations,
        )

    def execute_concat(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        how: str,
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_concat(
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            how,
        )

    def execute_melt(
        self,
        plan: Any,
        root_data: Any,
        id_vars: Sequence[str],
        value_vars: Sequence[str] | None,
        variable_name: str,
        value_name: str,
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_melt(
            plan,
            root_data,
            list(id_vars),
            None if value_vars is None else list(value_vars),
            variable_name,
            value_name,
        )

    def execute_pivot(
        self,
        plan: Any,
        root_data: Any,
        index: Sequence[str],
        columns: str,
        values: Sequence[str],
        aggregate_function: str,
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_pivot(
            plan,
            root_data,
            list(index),
            columns,
            list(values),
            aggregate_function,
        )

    def execute_explode(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_explode(plan, root_data, list(columns))

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
    ) -> tuple[Any, Any]:
        rust = _require_rust_core()
        return rust.execute_unnest(plan, root_data, list(columns))
