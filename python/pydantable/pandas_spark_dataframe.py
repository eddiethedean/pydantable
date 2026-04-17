"""Spark execution with :mod:`pydantable.pandas`-style method names.

Install with ``pip install "pydantable[spark]"``.
"""

from __future__ import annotations

from pydantable.pandas import PandasDataFrame, PandasDataFrameModel
from pydantable.spark_dataframe import (
    SparkDataFrame as CoreSparkDataFrame,
)
from pydantable.spark_dataframe import (
    SparkDataFrameModel as CoreSparkDataFrameModel,
)


class SparkDataFrame(CoreSparkDataFrame, PandasDataFrame):
    """Spark backend plus pandas-shaped API (``merge``, ``assign``, …)."""


class SparkDataFrameModel(CoreSparkDataFrameModel, PandasDataFrameModel):
    """Spark backend plus pandas-shaped ``DataFrameModel`` methods."""

    _dataframe_cls = SparkDataFrame


__all__ = ["SparkDataFrame", "SparkDataFrameModel"]
