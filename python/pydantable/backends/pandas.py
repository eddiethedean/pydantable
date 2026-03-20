from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from .base import Backend
from .pandas_executor import (
    execute_groupby_agg_pandas,
    execute_join_pandas,
    execute_plan_pandas,
)


class PandasBackend(Backend):
    """
    Execute logical plans using pandas.

    Requires the optional ``pandas`` dependency (``pip install pydantable[pandas]``).
    """

    name = "pandas"

    def execute_plan(self, plan: Any, data: Any) -> Any:
        return execute_plan_pandas(plan, dict(data))

    def execute_join(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        on: Sequence[str],
        how: str,
        suffix: str,
    ) -> tuple[Any, Any]:
        return execute_join_pandas(
            left_plan,
            dict(left_root_data),
            right_plan,
            dict(right_root_data),
            list(on),
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
        return execute_groupby_agg_pandas(
            plan,
            dict(root_data),
            list(by),
            dict(aggregations),
        )
