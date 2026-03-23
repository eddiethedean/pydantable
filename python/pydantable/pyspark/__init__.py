"""PySpark-shaped :class:`DataFrame` and :class:`DataFrameModel` (same engine as core).

This is a **facade** for familiar names (``withColumn``, ``orderBy``, …), not a
Spark cluster client. See :mod:`pydantable.pyspark.sql` for ``functions``, types,
and :class:`~pydantable.window_spec.Window`.
"""

from __future__ import annotations

from pydantable.expressions import Expr
from pydantable.schema import Schema

from . import sql
from .dataframe import DataFrame, DataFrameModel

__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema", "sql"]
