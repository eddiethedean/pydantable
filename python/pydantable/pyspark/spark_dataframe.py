"""Spark execution with :mod:`pydantable.pyspark`-style method names.

Install with ``pip install "pydantable[spark]"``.
"""

from __future__ import annotations

from typing import Any, cast

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel
from pydantable.expressions import AliasedExpr, Expr
from pydantable.pyspark.dataframe import DataFrame as PySparkDataFrame
from pydantable.pyspark.dataframe import DataFrameModel as PySparkDataFrameModel
from pydantable.spark_dataframe import (
    SparkDataFrame as CoreSparkDataFrame,
)
from pydantable.spark_dataframe import (
    SparkDataFrameModel as CoreSparkDataFrameModel,
)


class SparkDataFrame(CoreSparkDataFrame, PySparkDataFrame):
    """Spark backend plus PySpark-shaped API (``withColumn``, ``orderBy``, …)."""

    @staticmethod
    def _as_pyspark_df(df: CoreDataFrame) -> SparkDataFrame:
        return cast(
            "SparkDataFrame",
            SparkDataFrame._from_plan(
                root_data=df._root_data,
                root_schema_type=df._root_schema_type,
                current_schema_type=df._current_schema_type,
                rust_plan=df._rust_plan,
                engine=df._engine,
            ),
        )

    def filter(self, condition: Any) -> SparkDataFrame:  # type: ignore[override]
        # Prefer typed Expr when the caller uses the pyspark-shaped facade.
        if isinstance(condition, Expr):
            return self._as_pyspark_df(CoreDataFrame.filter(self, condition))
        return self._as_pyspark_df(CoreSparkDataFrame.filter(self, condition))

    def with_columns(  # type: ignore[override]
        self, *exprs: Any, **columns: Any
    ) -> SparkDataFrame:
        # If the caller uses typed expressions, route to the core typed implementation.
        if exprs or any(
            isinstance(v, (Expr, AliasedExpr)) for v in columns.values()
        ):
            return self._as_pyspark_df(
                CoreDataFrame.with_columns(self, *exprs, **columns)
            )

        if exprs:
            raise TypeError(
                "SparkDataFrame.with_columns(*exprs, **columns) positional expressions "
                "require typed Exprs (use df.withColumn(...) / df.withColumns(...))."
            )
        return self._as_pyspark_df(CoreSparkDataFrame.with_columns(self, **columns))


class SparkDataFrameModel(CoreSparkDataFrameModel, PySparkDataFrameModel):
    """Spark backend plus PySpark-shaped ``DataFrameModel`` methods."""

    _dataframe_cls = SparkDataFrame

    @classmethod
    def concat(
        cls,
        dfs: Any,
        *,
        how: str = "vertical",
    ) -> CoreDataFrameModel:
        # Defer to the PySpark facade's signature/behavior; unwrap/rewrap.
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, CoreDataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        out = SparkDataFrame.concat([df._df for df in dfs], how=how)
        return cls._from_dataframe(out)


__all__ = ["SparkDataFrame", "SparkDataFrameModel"]

