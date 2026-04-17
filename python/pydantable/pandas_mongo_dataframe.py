"""Mongo execution with :mod:`pydantable.pandas`-style method names.

Install with ``pip install "pydantable[mongo]"``.
"""

from __future__ import annotations

from pydantable.mongo_dataframe import (
    MongoDataFrame as CoreMongoDataFrame,
)
from pydantable.mongo_dataframe import (
    MongoDataFrameModel as CoreMongoDataFrameModel,
)
from pydantable.pandas import PandasDataFrame, PandasDataFrameModel


class MongoDataFrame(CoreMongoDataFrame, PandasDataFrame):
    """Mongo backend plus pandas-shaped API (``merge``, ``assign``, …)."""


class MongoDataFrameModel(CoreMongoDataFrameModel, PandasDataFrameModel):
    """Mongo backend plus pandas-shaped ``DataFrameModel`` methods."""

    _dataframe_cls = MongoDataFrame


__all__ = ["MongoDataFrame", "MongoDataFrameModel"]

