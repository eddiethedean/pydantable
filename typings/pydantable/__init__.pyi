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
from .types import WKB

__version__ = "1.3.0"

__all__ = [
    "WKB",
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
