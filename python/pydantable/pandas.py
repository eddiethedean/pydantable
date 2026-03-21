from __future__ import annotations

from .expressions import Expr
from .pandas_ui import PandasDataFrame, PandasDataFrameModel
from .schema import Schema


class DataFrame(PandasDataFrame):
    """pandas-flavored interface; core execution uses the Rust engine."""

    _backend = "pandas"


class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
