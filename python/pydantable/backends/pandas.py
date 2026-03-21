from __future__ import annotations

from typing import Any

from .pandas_executor import execute_plan_pandas
from .polars import PolarsBackend


class PandasBackend(PolarsBackend):
    """
    Backend tag for the ``pandas`` interface module.

    ``execute_plan`` materializes the logical plan using the optional pandas
    runtime; joins, reshape, and other operations use the Rust core via
    ``PolarsBackend``.
    """

    name = "pandas"

    def execute_plan(self, plan: Any, data: Any) -> Any:
        return execute_plan_pandas(plan, dict(data))
