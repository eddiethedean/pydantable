from __future__ import annotations

from .expressions import Expr
from .pandas_ui import PandasDataFrame, PandasDataFrameModel
from .schema import Schema


class DataFrame(PandasDataFrame):
    _backend = "pandas"


class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
