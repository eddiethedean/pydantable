"""Mongo execution with :mod:`pydantable.pyspark`-style method names.

Install with ``pip install "pydantable[mongo]"``.
"""

from __future__ import annotations

from typing import Any, cast

from pydantable.mongo_dataframe import (
    MongoDataFrame as CoreMongoDataFrame,
)
from pydantable.mongo_dataframe import (
    MongoDataFrameModel as CoreMongoDataFrameModel,
)
from pydantable.pyspark.dataframe import DataFrame as PySparkDataFrame
from pydantable.pyspark.dataframe import DataFrameModel as PySparkDataFrameModel


class MongoDataFrame(CoreMongoDataFrame, PySparkDataFrame):
    """Mongo backend plus PySpark-shaped API (``withColumn``, ``orderBy``, …)."""

    @staticmethod
    def _as_pyspark_df(df: Any) -> MongoDataFrame:
        return cast(
            "MongoDataFrame",
            MongoDataFrame._from_plan(
                root_data=df._root_data,
                root_schema_type=df._root_schema_type,
                current_schema_type=df._current_schema_type,
                rust_plan=df._rust_plan,
                engine=df._engine,
            ),
        )


class MongoDataFrameModel(CoreMongoDataFrameModel, PySparkDataFrameModel):
    """Mongo backend plus PySpark-shaped ``DataFrameModel`` methods."""

    _dataframe_cls = MongoDataFrame

    @classmethod
    def concat(
        cls,
        dfs: Any,
        *,
        how: str = "vertical",
    ) -> Any:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, CoreMongoDataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        out = MongoDataFrame.concat([df._df for df in dfs], how=how)
        return cls._from_dataframe(out)


__all__ = ["MongoDataFrame", "MongoDataFrameModel"]

