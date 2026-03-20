from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence


class Backend(Protocol):
    """
    Backend executor interface.

    The typed logical plan (and expression AST) is produced by Rust; backends
    control how the plan is materialized into Python values.
    """

    name: str

    def execute_plan(self, plan: Any, data: Any) -> Any: ...

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
    ) -> tuple[Any, Any]: ...

    def execute_groupby_agg(
        self,
        plan: Any,
        root_data: Any,
        by: Sequence[str],
        aggregations: Any,
    ) -> tuple[Any, Any]: ...

    def execute_concat(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        how: str,
    ) -> tuple[Any, Any]: ...

    def execute_melt(
        self,
        plan: Any,
        root_data: Any,
        id_vars: Sequence[str],
        value_vars: Sequence[str] | None,
        variable_name: str,
        value_name: str,
    ) -> tuple[Any, Any]: ...

    def execute_pivot(
        self,
        plan: Any,
        root_data: Any,
        index: Sequence[str],
        columns: str,
        values: Sequence[str],
        aggregate_function: str,
    ) -> tuple[Any, Any]: ...

    def execute_explode(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
    ) -> tuple[Any, Any]: ...

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
    ) -> tuple[Any, Any]: ...

    def execute_rolling_agg(
        self,
        plan: Any,
        root_data: Any,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None,
        min_periods: int,
    ) -> tuple[Any, Any]: ...

    def execute_groupby_dynamic_agg(
        self,
        plan: Any,
        root_data: Any,
        index_column: str,
        every: str,
        period: str | None,
        by: Sequence[str] | None,
        aggregations: Any,
    ) -> tuple[Any, Any]: ...
