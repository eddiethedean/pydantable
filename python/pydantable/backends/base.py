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
        on: Sequence[str],
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

