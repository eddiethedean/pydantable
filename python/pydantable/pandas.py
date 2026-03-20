from __future__ import annotations

from .dataframe import DataFrame as _BaseDataFrame
from .dataframe_model import DataFrameModel as _BaseDataFrameModel
from .expressions import Expr
from .schema import Schema


class DataFrame(_BaseDataFrame):
    """pandas-flavored interface class; execution uses the shared Rust core."""

    _backend = "pandas"


class DataFrameModel(_BaseDataFrameModel):
    """pandas-flavored typed model wrapper; execution uses the shared Rust core."""

    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
