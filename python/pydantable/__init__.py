from __future__ import annotations

import os

from . import pandas as pandas
from . import pyspark as pyspark
from .expressions import Expr
from .schema import Schema

_backend = os.getenv("PYDANTABLE_BACKEND", "polars").lower()
if _backend == "pandas":
    from .pandas import DataFrame, DataFrameModel
elif _backend == "pyspark":
    from .pyspark import DataFrame, DataFrameModel
else:
    from .dataframe import DataFrame
    from .dataframe_model import DataFrameModel

__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema", "pandas", "pyspark"]
__version__ = "0.4.0"
