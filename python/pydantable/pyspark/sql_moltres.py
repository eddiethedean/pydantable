"""Moltres SQL execution with :mod:`pydantable.pyspark`-style method names.

Install with ``pip install "pydantable[moltres]"``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe_model import DataFrameModel
from pydantable.pyspark.dataframe import DataFrame as PySparkDataFrame
from pydantable.pyspark.dataframe import DataFrameModel as PySparkDataFrameModel
from pydantable.schema import make_derived_schema_type
from pydantable.sql_moltres import (
    SqlDataFrame as CoreSqlDataFrame,
)
from pydantable.sql_moltres import (
    SqlDataFrameModel as CoreSqlDataFrameModel,
)


class SqlDataFrame(CoreSqlDataFrame, PySparkDataFrame):
    """Moltres SQL backend plus PySpark-shaped API (``withColumn``, ``orderBy``, …)."""

    @staticmethod
    def _as_pyspark_df(df: CoreDataFrame) -> SqlDataFrame:
        return cast(
            "SqlDataFrame",
            SqlDataFrame._from_plan(
                root_data=df._root_data,
                root_schema_type=df._root_schema_type,
                current_schema_type=df._current_schema_type,
                rust_plan=df._rust_plan,
                engine=df._engine,
            ),
        )

    def toDF(self, *cols: str) -> SqlDataFrame:
        current = list(self.schema_fields().keys())
        if len(cols) != len(current):
            raise ValueError(f"toDF() expects {len(current)} names, got {len(cols)}.")
        mapping = dict(zip(current, cols, strict=True))
        renamed = self.rename(mapping)
        ordered_types = {name: renamed._current_field_types[name] for name in cols}
        ordered_schema = make_derived_schema_type(
            renamed._root_schema_type, ordered_types
        )
        return self._as_pyspark_df(
            type(self)._from_plan(
                root_data=renamed._root_data,
                root_schema_type=renamed._root_schema_type,
                current_schema_type=ordered_schema,
                rust_plan=renamed._rust_plan,
                engine=self._engine,
            ),
        )


class SqlDataFrameModel(CoreSqlDataFrameModel, PySparkDataFrameModel):
    """Moltres SQL backend plus PySpark-shaped :class:`DataFrameModel` methods."""

    _dataframe_cls = SqlDataFrame

    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrameModel[Any]],
        *,
        how: str = "vertical",
    ) -> DataFrameModel[Any]:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, DataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        return cls._from_dataframe(
            SqlDataFrame.concat([df._df for df in dfs], how=how),
        )


__all__ = ["SqlDataFrame", "SqlDataFrameModel"]
