"""PydanTable: strongly typed, Pydantic-backed tables with a Rust execution core.

**Primary exports:** :class:`DataFrame`, :class:`Schema`, :class:`Expr`,
:class:`DataFrameModel`. Build ``DataFrame[YourSchema](data)`` where ``YourSchema``
subclasses :class:`Schema`.

**Facades:** :mod:`pydantable.pandas` (pandas-like names) and
:mod:`pydantable.pyspark` (PySpark-like names) wrap the same engine.

**I/O:** many helpers are re-exported from :mod:`pydantable.io` for convenience.
"""

from __future__ import annotations

from . import pandas as pandas
from . import plugins as plugins
from . import pyspark as pyspark
from . import selectors as selectors
from ._extension import MissingRustExtensionError
from .awaitable_dataframe_model import AwaitableDataFrameModel
from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .display import get_repr_html_limits, reset_display_options, set_display_options
from .errors import ColumnLengthMismatchError, PydantableUserError
from .expressions import Expr
from .io import (
    afetch_sql,
    afetch_sql_raw,
    afetch_sqlmodel,
    aiter_csv,
    aiter_ipc,
    aiter_json_array,
    aiter_json_lines,
    aiter_ndjson,
    aiter_parquet,
    aiter_sql,
    aiter_sql_raw,
    aiter_sqlmodel,
    amaterialize_parquet,
    aread_parquet,
    aread_parquet_url,
    awrite_sql_raw,
    awrite_sqlmodel,
    awrite_sqlmodel_batches,
    export_parquet,
    fetch_sql,
    fetch_sql_raw,
    fetch_sqlmodel,
    iter_avro,
    iter_bigquery,
    iter_csv,
    iter_delta,
    iter_excel,
    iter_ipc,
    iter_json_array,
    iter_json_lines,
    iter_kafka_json,
    iter_ndjson,
    iter_orc,
    iter_parquet,
    iter_snowflake,
    iter_sqlmodel,
    materialize_ipc,
    materialize_ndjson,
    materialize_parquet,
    read_parquet,
    read_parquet_url,
    sqlmodel_columns,
    write_csv_batches,
    write_ipc_batches,
    write_ndjson_batches,
    write_parquet_batches,
    write_sqlmodel,
    write_sqlmodel_batches,
)
from .materialization import PlanMaterialization, plan_materialization_summary
from .observe import get_observer, set_observer
from .schema import DtypeDriftWarning, Schema
from .types import WKB

__all__ = [
    "WKB",
    "AwaitableDataFrameModel",
    "ColumnLengthMismatchError",
    "DataFrame",
    "DataFrameModel",
    "DtypeDriftWarning",
    "Expr",
    "MissingRustExtensionError",
    "PlanMaterialization",
    "PydantableUserError",
    "Schema",
    "afetch_sql",
    "afetch_sql_raw",
    "afetch_sqlmodel",
    "aiter_csv",
    "aiter_ipc",
    "aiter_json_array",
    "aiter_json_lines",
    "aiter_ndjson",
    "aiter_parquet",
    "aiter_sql",
    "aiter_sql_raw",
    "aiter_sqlmodel",
    "amaterialize_parquet",
    "aread_parquet",
    "aread_parquet_url",
    "awrite_sql_raw",
    "awrite_sqlmodel",
    "awrite_sqlmodel_batches",
    "export_parquet",
    "fetch_sql",
    "fetch_sql_raw",
    "fetch_sqlmodel",
    "get_observer",
    "get_repr_html_limits",
    "iter_avro",
    "iter_bigquery",
    "iter_csv",
    "iter_delta",
    "iter_excel",
    "iter_ipc",
    "iter_json_array",
    "iter_json_lines",
    "iter_kafka_json",
    "iter_ndjson",
    "iter_orc",
    "iter_parquet",
    "iter_snowflake",
    "iter_sqlmodel",
    "materialize_ipc",
    "materialize_ndjson",
    "materialize_parquet",
    "pandas",
    "plan_materialization_summary",
    "plugins",
    "pyspark",
    "read_parquet",
    "read_parquet_url",
    "reset_display_options",
    "selectors",
    "set_display_options",
    "set_observer",
    "sqlmodel_columns",
    "write_csv_batches",
    "write_ipc_batches",
    "write_ndjson_batches",
    "write_parquet_batches",
    "write_sqlmodel",
    "write_sqlmodel_batches",
]
__version__ = "1.14.0"
