"""PydanTable: strongly typed, Pydantic-backed tables with a Rust execution core.

**Primary exports:** :class:`DataFrame`, :class:`Schema`, :class:`Expr`,
:class:`DataFrameModel`. Build ``DataFrame[YourSchema](data)`` where ``YourSchema``
subclasses :class:`Schema`.

**Facades:** :mod:`pydantable.pandas` (pandas-like names) and
:mod:`pydantable.pyspark` (PySpark-like names) wrap the same engine.

**I/O:** eager loaders, iterators, SQL helpers, and optional Mongo column-dict helpers
are imported from this package (``from pydantable import fetch_sqlmodel, fetch_mongo,
materialize_parquet, iter_sql_raw, write_mongo, …``); :mod:`pydantable.io` is the
implementation module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    BeanieWriteOptions,
    afetch_beanie,
    afetch_mongo,
    afetch_mongo_async,
    afetch_sql,
    afetch_sql_raw,
    afetch_sqlmodel,
    aiter_beanie,
    aiter_csv,
    aiter_ipc,
    aiter_json_array,
    aiter_json_lines,
    aiter_mongo,
    aiter_mongo_async,
    aiter_ndjson,
    aiter_parquet,
    aiter_sql,
    aiter_sql_raw,
    aiter_sqlmodel,
    amaterialize_json,
    amaterialize_parquet,
    aread_parquet,
    aread_parquet_url,
    awrite_beanie,
    awrite_mongo,
    awrite_mongo_async,
    awrite_sql_raw,
    awrite_sqlmodel,
    awrite_sqlmodel_batches,
    export_parquet,
    fetch_mongo,
    fetch_sql,
    fetch_sql_raw,
    fetch_sqlmodel,
    is_async_mongo_collection,
    iter_avro,
    iter_bigquery,
    iter_csv,
    iter_delta,
    iter_excel,
    iter_ipc,
    iter_json_array,
    iter_json_lines,
    iter_kafka_json,
    iter_mongo,
    iter_ndjson,
    iter_orc,
    iter_parquet,
    iter_snowflake,
    iter_sql_raw,
    iter_sqlmodel,
    materialize_ipc,
    materialize_json,
    materialize_ndjson,
    materialize_parquet,
    read_parquet,
    read_parquet_url,
    sqlmodel_columns,
    write_csv_batches,
    write_ipc_batches,
    write_mongo,
    write_ndjson_batches,
    write_parquet_batches,
    write_sql_raw,
    write_sqlmodel,
    write_sqlmodel_batches,
)
from .materialization import PlanMaterialization, plan_materialization_summary
from .observe import get_observer, set_observer
from .schema import DtypeDriftWarning, Schema
from .types import WKB

if TYPE_CHECKING:
    from pydantable.mongo_beanie import sync_pymongo_collection
    from pydantable.mongo_dataframe import (
        MongoDataFrame,
        MongoDataFrameModel,
        MongoPydantableEngine,
        MongoRoot,
    )
    from pydantable.sql_dataframe import SqlDataFrame, SqlDataFrameModel


def __getattr__(name: str) -> Any:
    import warnings

    if name == "SqlDataFrame":
        from pydantable.sql_dataframe import SqlDataFrame

        return SqlDataFrame
    if name == "SqlDataFrameModel":
        from pydantable.sql_dataframe import SqlDataFrameModel

        return SqlDataFrameModel
    if name == "sync_pymongo_collection":
        from pydantable.mongo_beanie import sync_pymongo_collection

        return sync_pymongo_collection
    if name in (
        "MongoDataFrame",
        "MongoDataFrameModel",
        "MongoPydantableEngine",
        "MongoRoot",
    ):
        import pydantable.mongo_dataframe as mongo_df

        return getattr(mongo_df, name)
    if name in (
        "EnteiDataFrame",
        "EnteiDataFrameModel",
        "EnteiPydantableEngine",
    ):
        from pydantable import mongo_entei

        _mongo_rename = {
            "EnteiDataFrame": "MongoDataFrame",
            "EnteiDataFrameModel": "MongoDataFrameModel",
            "EnteiPydantableEngine": "MongoPydantableEngine",
        }
        warnings.warn(
            f"{name} is deprecated; use {_mongo_rename[name]} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(mongo_entei, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "WKB",
    "AwaitableDataFrameModel",
    "BeanieWriteOptions",
    "ColumnLengthMismatchError",
    "DataFrame",
    "DataFrameModel",
    "DtypeDriftWarning",
    "Expr",
    "MissingRustExtensionError",
    "MongoDataFrame",
    "MongoDataFrameModel",
    "MongoPydantableEngine",
    "MongoRoot",
    "PlanMaterialization",
    "PydantableUserError",
    "Schema",
    "SqlDataFrame",
    "SqlDataFrameModel",
    "afetch_beanie",
    "afetch_mongo",
    "afetch_mongo_async",
    "afetch_sql",
    "afetch_sql_raw",
    "afetch_sqlmodel",
    "aiter_beanie",
    "aiter_csv",
    "aiter_ipc",
    "aiter_json_array",
    "aiter_json_lines",
    "aiter_mongo",
    "aiter_mongo_async",
    "aiter_ndjson",
    "aiter_parquet",
    "aiter_sql",
    "aiter_sql_raw",
    "aiter_sqlmodel",
    "amaterialize_json",
    "amaterialize_parquet",
    "aread_parquet",
    "aread_parquet_url",
    "awrite_beanie",
    "awrite_mongo",
    "awrite_mongo_async",
    "awrite_sql_raw",
    "awrite_sqlmodel",
    "awrite_sqlmodel_batches",
    "export_parquet",
    "fetch_mongo",
    "fetch_sql",
    "fetch_sql_raw",
    "fetch_sqlmodel",
    "get_observer",
    "get_repr_html_limits",
    "is_async_mongo_collection",
    "iter_avro",
    "iter_bigquery",
    "iter_csv",
    "iter_delta",
    "iter_excel",
    "iter_ipc",
    "iter_json_array",
    "iter_json_lines",
    "iter_kafka_json",
    "iter_mongo",
    "iter_ndjson",
    "iter_orc",
    "iter_parquet",
    "iter_snowflake",
    "iter_sql_raw",
    "iter_sqlmodel",
    "materialize_ipc",
    "materialize_json",
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
    "sync_pymongo_collection",
    "write_csv_batches",
    "write_ipc_batches",
    "write_mongo",
    "write_ndjson_batches",
    "write_parquet_batches",
    "write_sql_raw",
    "write_sqlmodel",
    "write_sqlmodel_batches",
]
__version__ = "1.17.0"
