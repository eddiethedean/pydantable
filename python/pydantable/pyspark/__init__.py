"""PySpark-shaped :class:`DataFrame` and :class:`DataFrameModel` (same engine as core).

This is a **facade** for familiar names (``withColumn``, ``orderBy``, …), not a
Spark cluster client. See :mod:`pydantable.pyspark.sql` for ``functions``, types,
and :class:`~pydantable.window_spec.Window`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.expressions import Expr
from pydantable.schema import Schema

from . import sql
from .dataframe import DataFrame, DataFrameModel

if TYPE_CHECKING:
    from pydantable.pyspark.sql_dataframe import SqlDataFrame, SqlDataFrameModel


def __getattr__(name: str) -> Any:
    if name == "SqlDataFrame":
        from pydantable.pyspark.sql_dataframe import SqlDataFrame as _SqlDataFrame

        return _SqlDataFrame
    if name == "SqlDataFrameModel":
        from pydantable.pyspark.sql_dataframe import (
            SqlDataFrameModel as _SqlDataFrameModel,
        )

        return _SqlDataFrameModel
    if name == "sparkdantic":
        # Lazy: optional ``sparkdantic`` is only in ``pydantable[spark]``; the PySpark
        # façade (``DataFrame``, ``sql``, …) must import without it.
        from . import sparkdantic as _sparkdantic

        return _sparkdantic
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DataFrame",
    "DataFrameModel",
    "Expr",
    "Schema",
    "SqlDataFrame",
    "SqlDataFrameModel",
    "sparkdantic",
    "sql",
]
