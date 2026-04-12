"""PlanFrame's PySpark-shaped API on :class:`planframe.frame.Frame`.

This is **not** Apache Spark: names mirror PySpark for PlanFrame lazy plans executed
via a :class:`~planframe.backend.adapter.BaseAdapter` (for pydantable, typically
:class:`~pydantable.planframe_adapter.PydantableAdapter`).

Re-exports :mod:`planframe.spark` unchanged so you can combine ``SparkFrame`` /
``Column`` / ``functions`` with ``Frame.source(...)`` per upstream docs.
"""

from __future__ import annotations

from planframe.spark import (
    Column,
    GroupedData,
    SparkFrame,
    functions,
    lit_value,
    unwrap_expr,
)

__all__ = [
    "Column",
    "GroupedData",
    "SparkFrame",
    "functions",
    "lit_value",
    "unwrap_expr",
]
