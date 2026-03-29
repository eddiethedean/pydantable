from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Executor
from pathlib import Path
from typing import Any, BinaryIO

from pydantable._extension import MissingRustExtensionError

from . import extras as extras
from . import http as http
from .arrow import (
    arrow_table_to_column_dict,
    record_batch_to_column_dict,
)
from .extras import (
    read_avro,
    read_bigquery,
    read_csv_stdin,
    read_delta,
    read_excel,
    read_kafka_json_batch,
    read_orc,
    read_snowflake,
    write_csv_stdout,
)
from .http import (
    fetch_bytes,
    fetch_csv_url,
    fetch_ndjson_url,
    fetch_parquet_url,
    read_from_object_store,
)
from .rap_support import aread_csv_rap, rap_csv_available
from .sql import StreamingColumns, fetch_sql, iter_sql, write_sql

# Streaming batch I/O helpers (iterators)
def iter_parquet(
    path: str | Path, *, batch_size: int = 65536, columns: list[str] | None = None
) -> Any: ...
def iter_ipc(
    source: _Source, *, batch_size: int = 65536, as_stream: bool = False
) -> Any: ...
def iter_csv(
    path: str | Path, *, batch_size: int = 65536, encoding: str = "utf-8"
) -> Any: ...
def iter_ndjson(
    path: str | Path, *, batch_size: int = 65536, encoding: str = "utf-8"
) -> Any: ...
def iter_json_lines(
    path: str | Path, *, batch_size: int = 65536, encoding: str = "utf-8"
) -> Any: ...
def iter_json_array(
    path: str | Path, *, batch_size: int = 65536, encoding: str = "utf-8"
) -> Any: ...

async def aiter_parquet(
    path: str | Path,
    *,
    batch_size: int = 65536,
    columns: list[str] | None = None,
    executor: Executor | None = None,
): ...
async def aiter_ipc(
    source: _Source,
    *,
    batch_size: int = 65536,
    as_stream: bool = False,
    executor: Executor | None = None,
): ...
async def aiter_csv(
    path: str | Path,
    *,
    batch_size: int = 65536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
): ...
async def aiter_ndjson(
    path: str | Path,
    *,
    batch_size: int = 65536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
): ...
async def aiter_json_lines(
    path: str | Path,
    *,
    batch_size: int = 65536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
): ...
async def aiter_json_array(
    path: str | Path,
    *,
    batch_size: int = 65536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
): ...

# Batch writers
def write_parquet_batches(
    path: str | Path | BinaryIO,
    batches: Any,
    *,
    compression: str | None = None,
) -> None: ...
def write_ipc_batches(
    path: str | Path | BinaryIO,
    batches: Any,
    *,
    as_stream: bool = True,
) -> None: ...
def write_csv_batches(
    path: str | Path,
    batches: Any,
    *,
    mode: str = "w",
    encoding: str = "utf-8",
    write_header: bool = True,
) -> None: ...
def write_ndjson_batches(
    path: str | Path,
    batches: Any,
    *,
    mode: str = "w",
    encoding: str = "utf-8",
) -> None: ...

# Extras iterators
def iter_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    batch_size: int = 65536,
    experimental: bool = True,
) -> Any: ...
def iter_delta(
    path: str | Path,
    *,
    batch_size: int = 65536,
    experimental: bool = True,
) -> Any: ...
def iter_avro(
    path: str | Path,
    *,
    batch_size: int = 65536,
    experimental: bool = True,
) -> Any: ...
def iter_orc(
    path: str | Path,
    *,
    batch_size: int = 65536,
    experimental: bool = True,
) -> Any: ...
def iter_bigquery(
    query: str,
    *,
    project: str | None = None,
    batch_size: int = 65536,
    experimental: bool = True,
    **kwargs: Any,
) -> Any: ...
def iter_snowflake(
    sql: str,
    *,
    batch_size: int = 65536,
    experimental: bool = True,
    **connect_kwargs: Any,
) -> Any: ...
def iter_kafka_json(
    topic: str,
    *,
    bootstrap_servers: str,
    max_messages: int | None = None,
    batch_size: int = 1000,
    experimental: bool = True,
    **consumer_config: Any,
) -> Any: ...

_Source = str | Path | BinaryIO | bytes

_ENGINE_ENV = "PYDANTABLE_IO_ENGINE"

def _default_engine() -> str: ...
def _is_local_path(source: _Source) -> bool: ...
async def _run_io(
    fn: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any] | None = None,
    *,
    executor: Executor | None = None,
) -> Any: ...
def _scan_file_root(
    path: str | Path,
    fmt: str,
    *,
    columns: list[str] | None = None,
    scan_kwargs: dict[str, Any] | None = None,
) -> Any: ...
def read_parquet(
    path: str | Path, *, columns: list[str] | None = None, **scan_kwargs: Any
) -> Any: ...
def read_csv(
    path: str | Path, *, columns: list[str] | None = None, **scan_kwargs: Any
) -> Any: ...
def read_ndjson(
    path: str | Path, *, columns: list[str] | None = None, **scan_kwargs: Any
) -> Any: ...
def read_ipc(
    path: str | Path, *, columns: list[str] | None = None, **scan_kwargs: Any
) -> Any: ...
def read_json(
    path: str | Path, *, columns: list[str] | None = None, **scan_kwargs: Any
) -> Any: ...
def read_parquet_url(
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    **kwargs: Any,
) -> Any: ...
def read_parquet_url_ctx(
    dataframe_cls: Any,
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    **kwargs: Any,
): ...
async def aread_parquet_url_ctx(
    dataframe_cls: Any,
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    executor: Any = None,
    **kwargs: Any,
): ...
def materialize_parquet(
    source: _Source, *, columns: list[str] | None = None, engine: str | None = None
) -> dict[str, list[Any]]: ...
def materialize_ipc(
    source: _Source, *, as_stream: bool = False, engine: str | None = None
) -> dict[str, list[Any]]: ...
def materialize_csv(
    path: str | Path, *, engine: str | None = None, use_rap: bool = False
) -> dict[str, list[Any]]: ...
def materialize_ndjson(
    path: str | Path, *, engine: str | None = None
) -> dict[str, list[Any]]: ...
def _json_rows_to_columns(rows: list[dict[str, Any]]) -> dict[str, list[Any]]: ...
def materialize_json(
    path: str | Path, *, engine: str | None = None
) -> dict[str, list[Any]]: ...
def export_json(
    path: str | Path, data: dict[str, list[Any]], *, indent: int | None = None
) -> None: ...
def export_parquet(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None: ...
def export_csv(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None: ...
def export_ndjson(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None: ...
def export_ipc(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None: ...
async def aread_parquet(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any: ...
async def aread_parquet_url(
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **kwargs: Any,
) -> Any: ...
async def aread_csv(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any: ...
async def aread_ndjson(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any: ...
async def aread_ipc(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any: ...
async def aread_json(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any: ...
async def amaterialize_parquet(
    source: _Source,
    *,
    columns: list[str] | None = None,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]: ...
async def amaterialize_ipc(
    source: _Source,
    *,
    as_stream: bool = False,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]: ...
async def amaterialize_csv(
    path: str | Path,
    *,
    engine: str | None = None,
    use_rap: bool = False,
    executor: Executor | None = None,
) -> dict[str, list[Any]]: ...
async def amaterialize_ndjson(
    path: str | Path, *, engine: str | None = None, executor: Executor | None = None
) -> dict[str, list[Any]]: ...
async def amaterialize_json(
    path: str | Path, *, engine: str | None = None, executor: Executor | None = None
) -> dict[str, list[Any]]: ...
async def aexport_parquet(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None: ...
async def aexport_csv(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None: ...
async def aexport_ndjson(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None: ...
async def aexport_ipc(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None: ...
async def aexport_json(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    indent: int | None = None,
    executor: Executor | None = None,
) -> None: ...
async def afetch_sql(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]] | StreamingColumns: ...
async def aiter_sql(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int = 65536,
    executor: Executor | None = None,
): ...
async def awrite_sql(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> None: ...
def write_sql_batches(
    batches: Any,
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
) -> None: ...
async def awrite_sql_batches(
    batches: Any,
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> None: ...

__all__ = [
    "MissingRustExtensionError",
    "aexport_csv",
    "aexport_ipc",
    "aexport_json",
    "aexport_ndjson",
    "aexport_parquet",
    "afetch_sql",
    "aiter_sql",
    "amaterialize_csv",
    "amaterialize_ipc",
    "amaterialize_json",
    "amaterialize_ndjson",
    "amaterialize_parquet",
    "aread_csv",
    "aread_csv_rap",
    "aread_ipc",
    "aread_json",
    "aread_ndjson",
    "aread_parquet",
    "aread_parquet_url",
    "aread_parquet_url_ctx",
    "arrow_table_to_column_dict",
    "awrite_sql",
    "awrite_sql_batches",
    "export_csv",
    "export_ipc",
    "export_json",
    "export_ndjson",
    "export_parquet",
    "extras",
    "fetch_bytes",
    "fetch_csv_url",
    "fetch_ndjson_url",
    "fetch_parquet_url",
    "fetch_sql",
    "http",
    "iter_sql",
    "materialize_csv",
    "materialize_ipc",
    "materialize_json",
    "materialize_ndjson",
    "materialize_parquet",
    "rap_csv_available",
    "read_avro",
    "read_bigquery",
    "read_csv",
    "read_csv_stdin",
    "read_delta",
    "read_excel",
    "read_from_object_store",
    "read_ipc",
    "read_json",
    "read_kafka_json_batch",
    "read_ndjson",
    "read_orc",
    "read_parquet",
    "read_parquet_url",
    "read_parquet_url_ctx",
    "read_snowflake",
    "record_batch_to_column_dict",
    "write_csv_stdout",
    "write_sql",
    "write_sql_batches",
]

__all__ = [
    "MissingRustExtensionError",
    "aexport_csv",
    "aexport_ipc",
    "aexport_json",
    "aexport_ndjson",
    "aexport_parquet",
    "afetch_sql",
    "aiter_sql",
    "amaterialize_csv",
    "amaterialize_ipc",
    "amaterialize_json",
    "amaterialize_ndjson",
    "amaterialize_parquet",
    "aread_csv",
    "aread_csv_rap",
    "aread_ipc",
    "aread_json",
    "aread_ndjson",
    "aread_parquet",
    "aread_parquet_url",
    "aread_parquet_url_ctx",
    "arrow_table_to_column_dict",
    "awrite_sql",
    "awrite_sql_batches",
    "export_csv",
    "export_ipc",
    "export_json",
    "export_ndjson",
    "export_parquet",
    "extras",
    "fetch_bytes",
    "fetch_csv_url",
    "fetch_ndjson_url",
    "fetch_parquet_url",
    "fetch_sql",
    "http",
    "iter_sql",
    "materialize_csv",
    "materialize_ipc",
    "materialize_json",
    "materialize_ndjson",
    "materialize_parquet",
    "rap_csv_available",
    "read_avro",
    "read_bigquery",
    "read_csv",
    "read_csv_stdin",
    "read_delta",
    "read_excel",
    "read_from_object_store",
    "read_ipc",
    "read_json",
    "read_kafka_json_batch",
    "read_ndjson",
    "read_orc",
    "read_parquet",
    "read_parquet_url",
    "read_parquet_url_ctx",
    "read_snowflake",
    "record_batch_to_column_dict",
    "write_csv_stdout",
    "write_sql",
    "write_sql_batches",
]
