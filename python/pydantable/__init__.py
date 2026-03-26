"""PydanTable: typed, Pydantic-backed tables executed by a Rust (Polars) core.

Exports the primary types: :class:`DataFrame`, :class:`Schema`, :class:`Expr`, and
:class:`DataFrameModel`. Use ``DataFrame[YourSchema](data)`` after defining
``YourSchema`` as a Pydantic model subclassing :class:`Schema`.

Alternate facades live in :mod:`pydantable.pandas` (pandas-like names) and
:mod:`pydantable.pyspark` (PySpark-like names); they share the same engine.
"""

from __future__ import annotations

from . import pandas as pandas
from . import plugins as plugins
from . import pyspark as pyspark
from ._extension import MissingRustExtensionError
from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .display import get_repr_html_limits, reset_display_options, set_display_options
from .expressions import Expr
from .io import (
    afetch_sql,
    amaterialize_parquet,
    aread_parquet,
    aread_parquet_url,
    export_parquet,
    fetch_sql,
    materialize_ipc,
    materialize_ndjson,
    materialize_parquet,
    read_parquet,
    read_parquet_url,
)
from .observe import get_observer, set_observer
from .schema import DtypeDriftWarning, Schema

__all__ = [
    "DataFrame",
    "DataFrameModel",
    "DtypeDriftWarning",
    "Expr",
    "MissingRustExtensionError",
    "Schema",
    "afetch_sql",
    "amaterialize_parquet",
    "aread_parquet",
    "aread_parquet_url",
    "export_parquet",
    "fetch_sql",
    "get_observer",
    "get_repr_html_limits",
    "materialize_ipc",
    "materialize_ndjson",
    "materialize_parquet",
    "pandas",
    "plugins",
    "pyspark",
    "read_parquet",
    "read_parquet_url",
    "reset_display_options",
    "set_display_options",
    "set_observer",
]
__version__ = "1.0.0"
