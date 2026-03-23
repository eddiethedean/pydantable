"""PydanTable: typed, Pydantic-backed tables executed by a Rust (Polars) core.

Exports the primary types: :class:`DataFrame`, :class:`Schema`, :class:`Expr`, and
:class:`DataFrameModel`. Use ``DataFrame[YourSchema](data)`` after defining
``YourSchema`` as a Pydantic model subclassing :class:`Schema`.

Alternate facades live in :mod:`pydantable.pandas` (pandas-like names) and
:mod:`pydantable.pyspark` (PySpark-like names); they share the same engine.
"""

from __future__ import annotations

from . import pandas as pandas
from . import pyspark as pyspark
from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .expressions import Expr
from .schema import Schema

__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema", "pandas", "pyspark"]
__version__ = "0.8.0"
