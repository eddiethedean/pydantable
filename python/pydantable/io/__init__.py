"""Unified data I/O: lazy ``read_*`` roots, ``materialize_*`` column dicts, and ``export_*``.

* **``read_*`` / ``aread_*``** return :class:`pydantable_native._core.ScanFileRoot` for lazy Polars scans
  (no full Python column lists).
* **``materialize_*``** (and **``fetch_sql_raw``** / **``fetch_sql``** / **``fetch_*_url``** / **``fetch_mongo``**) return ``dict[str, list]``.
* **``export_*`` / ``aexport_*``** write column dicts to files; **``write_mongo``** inserts into a PyMongo collection. **``amaterialize_*``** / **``afetch_*``** / **``awrite_mongo``** use :class:`asyncio.to_thread` (or an optional executor).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import tempfile
import warnings
from contextlib import asynccontextmanager, contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from pydantable._extension import MissingRustExtensionError
from pydantable.observe import span

from . import extras as extras
from . import http as http
from .arrow import (
    arrow_table_to_column_dict,
    read_ipc_pyarrow,
    read_parquet_pyarrow,
    record_batch_to_column_dict,
)
from .batches import iter_chain_batches
from .extras import (
    iter_avro,
    iter_bigquery,
    iter_delta,
    iter_excel,
    iter_kafka_json,
    iter_orc,
    iter_snowflake,
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
from .iter_file import (
    iter_csv,
    iter_ipc,
    iter_json_array,
    iter_json_lines,
    iter_ndjson,
    iter_parquet,
)
from .rap_support import aread_csv_rap, rap_csv_available
from .sql import (
    StreamingColumns,
    fetch_sql,
    fetch_sql_raw,
    iter_sql,
    iter_sql_raw,
    write_sql,
    write_sql_raw,
)
from .sqlmodel_read import fetch_sqlmodel, iter_sqlmodel
from .sqlmodel_schema import sqlmodel_columns
from .mongo import fetch_mongo, iter_mongo, write_mongo
from .sqlmodel_write import write_sqlmodel
from .write_batches import (
    write_csv_batches,
    write_ipc_batches,
    write_ndjson_batches,
    write_parquet_batches,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from concurrent.futures import Executor

from pydantable.plugins import register_reader, register_writer

_IO_LOG = logging.getLogger(__name__)

_Source = str | Path | BinaryIO | bytes

_ENGINE_ENV = "PYDANTABLE_IO_ENGINE"


def _default_engine() -> str:
    return os.environ.get(_ENGINE_ENV, "auto").lower()


def _is_local_path(source: _Source) -> bool:
    return isinstance(source, (str, Path))


async def _run_io(
    fn: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any] | None = None,
    *,
    executor: Executor | None = None,
) -> Any:
    kw = dict(kwargs or {})
    if executor is not None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, lambda: fn(*args, **kw))
    return await asyncio.to_thread(fn, *args, **kw)


def _scan_file_root(
    path: str | Path,
    fmt: str,
    *,
    columns: list[str] | None = None,
    scan_kwargs: dict[str, Any] | None = None,
) -> Any:
    try:
        from pydantable_native import (  # type: ignore[import-not-found]
            require_rust_core,
        )
    except ImportError as e:
        raise MissingRustExtensionError() from e
    rust = require_rust_core()
    if not hasattr(rust, "ScanFileRoot"):
        raise MissingRustExtensionError(
            "The native extension does not export ScanFileRoot. Reinstall or rebuild pydantable. "
            "See docs/DEVELOPER.md."
        )
    sk = scan_kwargs if scan_kwargs else None
    return rust.ScanFileRoot(str(path), fmt, columns, sk)


def read_parquet(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy Parquet read (local path); returns ``ScanFileRoot``. Use ``DataFrame[Schema].read_parquet``.

    Extra keyword arguments are forwarded as Polars scan options (e.g. ``low_memory``, ``n_rows``,
    ``parallel``, ``glob``, ``hive_partitioning``, ``hive_start_idx``, ``try_parse_hive_dates``,
    ``include_file_paths``, ``row_index_name``, ``row_index_offset``). Unknown keys raise
    ``ValueError`` from the Rust layer. Per-scan details: ``IO_PARQUET`` on the doc site; kwargs matrix: ``DATA_IO_SOURCES`` (**Audit: Polars 0.53.x vs pydantable**).
    """
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "parquet", columns=columns, scan_kwargs=sk)


def read_csv(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy CSV read (local path); returns ``ScanFileRoot``. Use ``DataFrame[Schema].read_csv``.

    Extra keyword arguments are forwarded as Polars ``LazyCsvReader`` options (e.g. ``has_header``,
    ``separator``, ``skip_rows``, ``skip_lines``, ``n_rows``, ``infer_schema_length``,
    ``ignore_errors``, ``low_memory``, ``rechunk``, ``glob``, ``cache``, ``quote_char``, ``eol_char``,
    ``include_file_paths``, ``row_index_name``, ``row_index_offset``, ``raise_if_empty``,
    ``truncate_ragged_lines``, ``decimal_comma``, ``try_parse_dates``). Unknown keys raise
    ``ValueError`` from the Rust layer. Per-scan details: ``IO_CSV`` on the doc site; kwargs matrix:
    ``DATA_IO_SOURCES`` (**Audit: Polars 0.53.x vs pydantable**).
    """
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "csv", columns=columns, scan_kwargs=sk)


def read_ndjson(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy newline-delimited JSON read (local path); returns ``ScanFileRoot``.

    Extra keyword arguments are forwarded as Polars ``LazyJsonLineReader`` options (e.g.
    ``low_memory``, ``rechunk``, ``ignore_errors``, ``n_rows``, ``infer_schema_length``,
    ``glob``, ``include_file_paths``, ``row_index_name``, ``row_index_offset``). ``glob=False``
    raises ``ValueError`` (Polars 0.53 NDJSON scans always expand paths). Unknown keys raise
    ``ValueError`` from the Rust layer. Per-scan details: ``IO_NDJSON`` on the doc site; kwargs
    matrix: ``DATA_IO_SOURCES`` (**Audit: Polars 0.53.x vs pydantable**).
    """
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "ndjson", columns=columns, scan_kwargs=sk)


def read_ipc(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy Arrow IPC **file** read (local path); returns ``ScanFileRoot``.

    Extra keyword arguments are forwarded to the Rust layer: **``IpcScanOptions``**
    (**``record_batch_statistics``**) and **``UnifiedScanArgs``** (**``glob``**, **``cache``**,
    **``rechunk``**, **``n_rows``**, **``hive_partitioning``**, **``hive_start_idx``**,
    **``try_parse_hive_dates``**, **``include_file_paths``**, **``row_index_name``**,
    **``row_index_offset``**). Unknown keys raise ``ValueError``. Per-scan details: ``IO_IPC`` on
    the doc site; kwargs matrix: ``DATA_IO_SOURCES`` (**Audit: Polars 0.53.x vs pydantable**).
    """
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "ipc", columns=columns, scan_kwargs=sk)


def read_json(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy **JSON Lines** read (local path); alias of :func:`read_ndjson` (same ``ScanFileRoot``).

    **Not** a lazy reader for a single-file JSON **array** ``[{...}, ...]`` — use
    :func:`materialize_json` or :func:`iter_json_array` for array layout.

    **Paths:** directory, glob, or a single file behave like :func:`read_ndjson` (Polars
    ``LazyJsonLineReader``). Pass ``glob=True`` when using a directory or ``*.jsonl``-style
    pattern so kwargs match other ``read_*`` APIs. **``scan_kwargs``** are the same as NDJSON
    (e.g. ``low_memory``, ``rechunk``, ``ignore_errors``, ``n_rows``, ``infer_schema_length``,
    ``glob``, ``include_file_paths``, ``row_index_name``, ``row_index_offset``); ``glob=False``
    raises ``ValueError``. Unknown keys raise from the Rust layer. See ``IO_JSON`` and
    ``DATA_IO_SOURCES`` (**Audit**).
    """
    return read_ndjson(path, columns=columns, **scan_kwargs)


def read_parquet_url(
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    **kwargs: Any,
) -> Any:
    """Download Parquet from ``url`` (HTTP(S)) to a temp file; return ``ScanFileRoot``.

    The file is **not** removed automatically: delete it when the pipeline finishes
    (see {doc}`DATA_IO_SOURCES`).
    """
    data = fetch_bytes(url, experimental=experimental, **kwargs)
    fd, name = tempfile.mkstemp(suffix=".parquet")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
    except (OSError, MemoryError):
        with suppress(OSError):
            os.unlink(name)
        raise
    try:
        return _scan_file_root(name, "parquet", columns=columns, scan_kwargs=None)
    except Exception:
        # Scan setup can fail for many reasons; always remove the temp file.
        with suppress(OSError):
            os.unlink(name)
        raise


@contextmanager
def read_parquet_url_ctx(
    dataframe_cls: Any,
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    **kwargs: Any,
):
    """Download Parquet from ``url`` to a temp file, yield ``DataFrame[Schema]``, delete the file after.

    Pass the parametrized frame class (e.g. ``DataFrame[MySchema]``). The lazy plan must not
    be used after the context exits (the backing file is removed).
    """
    root = read_parquet_url(url, experimental=experimental, columns=columns, **kwargs)
    path = str(getattr(root, "path", "") or "")
    if not path:
        raise RuntimeError(
            "ScanFileRoot.path is empty; cannot manage temp file lifecycle"
        )
    try:
        yield dataframe_cls._from_scan_root(root)
    finally:
        with suppress(OSError):
            os.unlink(path)


@asynccontextmanager
async def aread_parquet_url_ctx(
    dataframe_cls: Any,
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    executor: Any = None,
    **kwargs: Any,
):
    """Async variant of :func:`read_parquet_url_ctx` (uses :func:`aread_parquet_url`)."""
    root = await aread_parquet_url(
        url,
        experimental=experimental,
        columns=columns,
        executor=executor,
        **kwargs,
    )
    path = str(getattr(root, "path", "") or "")
    if not path:
        raise RuntimeError(
            "ScanFileRoot.path is empty; cannot manage temp file lifecycle"
        )
    try:
        yield dataframe_cls._from_scan_root(root)
    finally:
        with suppress(OSError):
            os.unlink(path)


def materialize_parquet(
    source: _Source,
    *,
    columns: list[str] | None = None,
    engine: str | None = None,
) -> dict[str, list[Any]]:
    """
    Eagerly read Parquet into ``dict[str, list]`` (loads full data into Python).

    **Single file:** one local path, buffer, or file-like per call. For **multiple** Parquet
    files, use :func:`read_parquet` with ``glob=True`` / a directory and materialize via
    :meth:`~pydantable.dataframe.DataFrame.to_dict`, or call ``materialize_parquet`` per file
    and merge (mind schema alignment).

    * ``engine="auto"`` (default): Rust for local file paths when ``columns`` is ``None``;
      otherwise PyArrow.
    * ``engine="rust"`` / ``"pyarrow"``: force that implementation.

    For out-of-core pipelines prefer :func:`read_parquet` + :meth:`~pydantable.dataframe.DataFrame.write_parquet`.
    """
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_parquet", engine=eng, columns=columns is not None):
        use_rust = (
            eng in ("auto", "rust") and columns is None and _is_local_path(source)
        )
        if use_rust and eng != "pyarrow":
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_read_parquet_path,
            )

            path = str(source)
            if os.path.isfile(path):
                try:
                    return rust_read_parquet_path(path)
                except Exception:
                    # PyO3/native may wrap diverse failures; fall back to PyArrow when auto.
                    _IO_LOG.debug(
                        "rust_read_parquet_path failed; trying PyArrow",
                        exc_info=True,
                    )
                    if eng == "rust":
                        raise
        if eng == "rust" and not use_rust:
            raise ValueError(
                "Rust Parquet read needs a local file path and columns=None"
            )
        return read_parquet_pyarrow(source, columns=columns)


def materialize_ipc(
    source: _Source,
    *,
    as_stream: bool = False,
    engine: str | None = None,
) -> dict[str, list[Any]]:
    """Read Arrow IPC (file or stream) into ``dict[str, list]``.

    **Single file** per call. For multiple IPC files, prefer lazy :func:`read_ipc` + ``glob`` /
    ``to_dict``, or iterate :func:`materialize_ipc` per path.
    """
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_ipc", engine=eng, as_stream=bool(as_stream)):
        if (
            eng in ("auto", "rust")
            and not as_stream
            and _is_local_path(source)
            and os.path.isfile(str(source))
        ):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_read_ipc_path,
            )

            try:
                return rust_read_ipc_path(str(source))
            except Exception:
                _IO_LOG.debug(
                    "rust_read_ipc_path failed; trying PyArrow", exc_info=True
                )
                if eng == "rust":
                    raise
        if eng == "rust" and (as_stream or not _is_local_path(source)):
            raise ValueError(
                "Rust IPC read supports on-disk file format only (as_stream=False)"
            )
        return read_ipc_pyarrow(source, as_stream=as_stream)


def materialize_csv(
    path: str | Path,
    *,
    engine: str | None = None,
    use_rap: bool = False,
) -> dict[str, list[Any]]:
    """
    Read CSV from a **local path** into ``dict[str, list]``.

    **Single file** per call. For **multiple** CSVs, use :func:`read_csv` with ``glob=True`` /
    ``to_dict``, or call ``materialize_csv`` per file and merge.

    * ``engine="auto"``: try Rust, then fall back to stdlib ``csv`` on failure.
    * ``use_rap=True``: load via :func:`aread_csv_rap` (only when no running event loop).
    """
    with span(
        "io.materialize_csv",
        engine=(engine or _default_engine()).lower(),
        use_rap=bool(use_rap),
    ):
        if use_rap:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(aread_csv_rap(str(path)))
            raise RuntimeError(
                "in an async context, await aread_csv_rap(path) instead of use_rap=True"
            )

        eng = (engine or _default_engine()).lower()
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_read_csv_path,
            )

            try:
                return rust_read_csv_path(str(path))
            except Exception:
                _IO_LOG.debug(
                    "rust_read_csv_path failed; using stdlib csv", exc_info=True
                )
                if eng == "rust":
                    raise
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            cols: dict[str, list[Any]] = {h: [] for h in header}
            for row in reader:
                for i, h in enumerate(header):
                    cols[h].append(row[i] if i < len(row) else None)
            return cols


def materialize_ndjson(
    path: str | Path, *, engine: str | None = None
) -> dict[str, list[Any]]:
    """Read newline-delimited JSON from a **single local path** into ``dict[str, list]``.

    For **multiple** NDJSON files, use :func:`read_ndjson` with ``glob=True`` / ``to_dict``, or
    call ``materialize_ndjson`` per file and merge.
    """
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_ndjson", engine=eng):
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_read_ndjson_path,
            )

            try:
                return rust_read_ndjson_path(str(path))
            except Exception:
                _IO_LOG.debug(
                    "rust_read_ndjson_path failed; using pure Python JSON lines",
                    exc_info=True,
                )
                if eng == "rust":
                    raise
        rows: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        if not rows:
            return {}
        keys = sorted({k for r in rows for k in r})
        return {k: [r.get(k) for r in rows] for k in keys}


def _json_rows_to_columns(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {}
    keys = sorted({k for row in rows for k in row})
    return {k: [row.get(k) for row in rows] for k in keys}


def materialize_json(
    path: str | Path, *, engine: str | None = None
) -> dict[str, list[Any]]:
    """Load a JSON file into ``dict[str, list]``: either a JSON array of objects or JSON Lines.

    **Single file** per call. For **multiple** JSON files, use lazy :func:`read_json` /
    ``read_ndjson`` with ``glob=True`` and ``to_dict``, or call ``materialize_json`` per path.

    If the first non-whitespace character is ``[``, the file is parsed as one JSON array.
    Otherwise the file is read as newline-delimited JSON (same as :func:`materialize_ndjson`).
    """
    p = Path(path)
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_json", engine=eng):
        with p.open(encoding="utf-8") as f:
            while True:
                ch = f.read(1)
                if not ch:
                    return {}
                if not ch.isspace():
                    break
            if ch == "[":
                f.seek(0)
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError(
                        "materialize_json: expected a JSON array of objects when file starts with '['"
                    )
                if not data:
                    return {}
                if not all(isinstance(x, dict) for x in data):
                    raise ValueError(
                        "materialize_json: array elements must be JSON objects"
                    )
                return _json_rows_to_columns(data)
            f.seek(0)
        return materialize_ndjson(p, engine=eng)


def export_json(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    indent: int | None = None,
) -> None:
    """Write ``dict[str, list]`` as one JSON array of row objects.

    Uses :func:`json.dump` with ``default=str``. Nested ``dict``/``list`` cells
    serialize as JSON objects/arrays; non-JSON-native scalars (e.g. ``datetime``,
    ``Decimal``, ``UUID``) become ``str(value)``, not necessarily ISO-8601. For
    stable JSON output, prefer normalizing rows or using Pydantic
    ``model_dump(mode="json")`` after materialization.
    """
    keys = list(data.keys())
    n = len(data[keys[0]]) if keys else 0
    rows = [{k: data[k][i] for k in keys} for i in range(n)]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=indent, default=str)


def export_parquet(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None:
    """Write ``dict[str, list]`` to Parquet (eager). For lazy plan output use :meth:`DataFrame.write_parquet`."""
    eng = (engine or _default_engine()).lower()
    with span("io.export_parquet", engine=eng, path=str(path)):
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_write_parquet_path,
            )

            try:
                rust_write_parquet_path(str(path), data)
                return
            except ImportError:
                if eng == "rust":
                    raise
        try:
            import pyarrow as pa  # type: ignore[import-not-found, import-untyped]
            import pyarrow.parquet as pq  # type: ignore[import-not-found, import-untyped]
        except ImportError as e:
            raise ImportError(
                "export_parquet fallback requires pyarrow (pip install 'pydantable[arrow]')."
            ) from e
        pq.write_table(pa.Table.from_pydict(data), str(path))


def export_csv(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None:
    """Write ``dict[str, list]`` to CSV (eager)."""
    eng = (engine or _default_engine()).lower()
    with span("io.export_csv", engine=eng, path=str(path)):
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_write_csv_path,
            )

            try:
                rust_write_csv_path(str(path), data)
                return
            except ImportError:
                if eng == "rust":
                    raise
        headers = list(data.keys())
        n = len(data[headers[0]]) if headers else 0
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(headers)
            for i in range(n):
                w.writerow([data[h][i] for h in headers])


def export_ndjson(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None:
    """Write ``dict[str, list]`` as newline-delimited JSON (eager)."""
    eng = (engine or _default_engine()).lower()
    with span("io.export_ndjson", engine=eng, path=str(path)):
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_write_ndjson_path,
            )

            try:
                rust_write_ndjson_path(str(path), data)
                return
            except ImportError:
                if eng == "rust":
                    raise
        headers = list(data.keys())
        n = len(data[headers[0]]) if headers else 0
        with open(path, "w", encoding="utf-8") as fh:
            for i in range(n):
                fh.write(
                    json.dumps({h: data[h][i] for h in headers}, default=str) + "\n"
                )


def export_ipc(
    path: str | Path, data: dict[str, list[Any]], *, engine: str | None = None
) -> None:
    """Write ``dict[str, list]`` to Arrow IPC file (eager)."""
    eng = (engine or _default_engine()).lower()
    with span("io.export_ipc", engine=eng, path=str(path)):
        if eng in ("auto", "rust"):
            from pydantable_native.io_core import (  # type: ignore[import-not-found]
                rust_write_ipc_path,
            )

            try:
                rust_write_ipc_path(str(path), data)
                return
            except ImportError:
                if eng == "rust":
                    raise
        try:
            import pyarrow as pa  # type: ignore[import-not-found, import-untyped]
        except ImportError as e:
            raise ImportError(
                "export_ipc fallback requires pyarrow (pip install 'pydantable[arrow]')."
            ) from e
        table = pa.Table.from_pydict(data)
        with open(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


async def aread_parquet(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any:
    return await _run_io(
        read_parquet, (path,), {"columns": columns, **scan_kwargs}, executor=executor
    )


async def aread_parquet_url(
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **kwargs: Any,
) -> Any:
    return await _run_io(
        read_parquet_url,
        (url,),
        {"experimental": experimental, "columns": columns, **kwargs},
        executor=executor,
    )


async def aread_csv(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any:
    return await _run_io(
        read_csv, (path,), {"columns": columns, **scan_kwargs}, executor=executor
    )


async def aread_ndjson(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any:
    return await _run_io(
        read_ndjson, (path,), {"columns": columns, **scan_kwargs}, executor=executor
    )


async def aread_ipc(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any:
    return await _run_io(
        read_ipc, (path,), {"columns": columns, **scan_kwargs}, executor=executor
    )


async def aread_json(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    executor: Executor | None = None,
    **scan_kwargs: Any,
) -> Any:
    return await _run_io(
        read_json, (path,), {"columns": columns, **scan_kwargs}, executor=executor
    )


async def amaterialize_parquet(
    source: _Source,
    *,
    columns: list[str] | None = None,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    return await _run_io(
        materialize_parquet,
        (source,),
        {"columns": columns, "engine": engine},
        executor=executor,
    )


async def amaterialize_ipc(
    source: _Source,
    *,
    as_stream: bool = False,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    return await _run_io(
        materialize_ipc,
        (source,),
        {"as_stream": as_stream, "engine": engine},
        executor=executor,
    )


async def amaterialize_csv(
    path: str | Path,
    *,
    engine: str | None = None,
    use_rap: bool = False,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    if use_rap:
        return await aread_csv_rap(str(path))
    return await _run_io(
        materialize_csv,
        (path,),
        {"engine": engine, "use_rap": False},
        executor=executor,
    )


async def amaterialize_ndjson(
    path: str | Path,
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    return await _run_io(
        materialize_ndjson, (path,), {"engine": engine}, executor=executor
    )


async def amaterialize_json(
    path: str | Path,
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    return await _run_io(
        materialize_json, (path,), {"engine": engine}, executor=executor
    )


async def aexport_parquet(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None:
    await _run_io(export_parquet, (path, data), {"engine": engine}, executor=executor)


async def aexport_csv(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None:
    await _run_io(export_csv, (path, data), {"engine": engine}, executor=executor)


async def aexport_ndjson(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None:
    await _run_io(export_ndjson, (path, data), {"engine": engine}, executor=executor)


async def aexport_ipc(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    engine: str | None = None,
    executor: Executor | None = None,
) -> None:
    await _run_io(export_ipc, (path, data), {"engine": engine}, executor=executor)


async def aexport_json(
    path: str | Path,
    data: dict[str, list[Any]],
    *,
    indent: int | None = None,
    executor: Executor | None = None,
) -> None:
    await _run_io(export_json, (path, data), {"indent": indent}, executor=executor)


async def afetch_sql(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]] | StreamingColumns:
    warnings.warn(
        "afetch_sql is deprecated and will be removed in a future major version; "
        "for mapped tables use afetch_sqlmodel(...), for string SQL use afetch_sql_raw(...).",
        DeprecationWarning,
        stacklevel=2,
    )
    return await _run_io(
        fetch_sql_raw,
        (sql, bind),
        {
            "parameters": parameters,
            "batch_size": batch_size,
            "auto_stream": auto_stream,
            "auto_stream_threshold_rows": auto_stream_threshold_rows,
        },
        executor=executor,
    )


async def afetch_sql_raw(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]] | StreamingColumns:
    """Async :func:`fetch_sql_raw` via :func:`asyncio.to_thread` (optional ``Executor``)."""
    return await _run_io(
        fetch_sql_raw,
        (sql, bind),
        {
            "parameters": parameters,
            "batch_size": batch_size,
            "auto_stream": auto_stream,
            "auto_stream_threshold_rows": auto_stream_threshold_rows,
        },
        executor=executor,
    )


async def aiter_sql_raw(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int = 65_536,
    executor: Executor | None = None,
):
    """
    Async generator yielding batches from :func:`iter_sql_raw` without blocking the loop.

    This runs the synchronous SQLAlchemy cursor in a background thread and streams
    batch dicts through an ``asyncio.Queue``.
    """
    import asyncio
    import threading

    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    q: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
    sentinel = object()
    stop = threading.Event()

    loop = asyncio.get_running_loop()

    def _put(item: object) -> None:
        # Backpressure: never drop batches if the async consumer is slow.
        # We block the producer thread until the event loop enqueues the item.
        if stop.is_set():
            return
        fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
        try:
            # Avoid deadlocking the producer thread if the consumer stops early
            # (queue fills and q.put never completes).
            while not stop.is_set():
                try:
                    fut.result(timeout=0.25)
                    return
                except TimeoutError:
                    continue
        except BaseException:
            return
        finally:
            if stop.is_set():
                with suppress(BaseException):
                    fut.cancel()

    def _runner() -> None:
        try:
            for batch in iter_sql_raw(
                sql,
                bind,
                parameters=parameters,
                batch_size=batch_size,
            ):
                if stop.is_set():
                    return
                _put(batch)
        except BaseException as e:  # propagate exceptions to async consumer
            _put(e)
        finally:
            _put(sentinel)

    if executor is not None:
        loop.run_in_executor(executor, _runner)
    else:
        threading.Thread(target=_runner, daemon=True).start()

    try:
        while True:
            item = await q.get()
            if item is sentinel:
                return
            if isinstance(item, BaseException):
                raise item
            yield item  # dict[str, list[Any]]
    finally:
        stop.set()
        with suppress(BaseException):
            loop.call_soon_threadsafe(q.put_nowait, sentinel)


async def aiter_sql(
    sql: str,
    bind: str | Any,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int = 65_536,
    executor: Executor | None = None,
):
    """Deprecated: use :func:`aiter_sql_raw` or :func:`aiter_sqlmodel`."""
    warnings.warn(
        "aiter_sql is deprecated and will be removed in a future major version; "
        "for mapped tables use aiter_sqlmodel(...), for string SQL use aiter_sql_raw(...).",
        DeprecationWarning,
        stacklevel=2,
    )
    async for batch in aiter_sql_raw(
        sql,
        bind,
        parameters=parameters,
        batch_size=batch_size,
        executor=executor,
    ):
        yield batch


async def afetch_sqlmodel(
    model: Any,
    bind: str | Any,
    *,
    where: Any | None = None,
    parameters: Mapping[str, Any] | None = None,
    columns: Sequence[Any] | None = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]] | StreamingColumns:
    return await _run_io(
        fetch_sqlmodel,
        (model, bind),
        {
            "where": where,
            "parameters": parameters,
            "columns": columns,
            "order_by": order_by,
            "limit": limit,
            "batch_size": batch_size,
            "auto_stream": auto_stream,
            "auto_stream_threshold_rows": auto_stream_threshold_rows,
        },
        executor=executor,
    )


async def aiter_sqlmodel(
    model: Any,
    bind: str | Any,
    *,
    where: Any | None = None,
    parameters: Mapping[str, Any] | None = None,
    columns: Sequence[Any] | None = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
    batch_size: int = 65_536,
    executor: Executor | None = None,
):
    """
    Async generator yielding batches from :func:`iter_sqlmodel` without blocking the loop.
    """
    import asyncio
    import threading

    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    q: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
    sentinel = object()
    stop = threading.Event()
    loop = asyncio.get_running_loop()

    def _put(item: object) -> None:
        if stop.is_set():
            return
        fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
        try:
            while not stop.is_set():
                try:
                    fut.result(timeout=0.25)
                    return
                except TimeoutError:
                    continue
        except BaseException:
            return
        finally:
            if stop.is_set():
                with suppress(BaseException):
                    fut.cancel()

    def _runner() -> None:
        try:
            for batch in iter_sqlmodel(
                model,
                bind,
                where=where,
                parameters=parameters,
                columns=columns,
                order_by=order_by,
                limit=limit,
                batch_size=batch_size,
            ):
                if stop.is_set():
                    return
                _put(batch)
        except BaseException as e:
            _put(e)
        finally:
            _put(sentinel)

    if executor is not None:
        loop.run_in_executor(executor, _runner)
    else:
        threading.Thread(target=_runner, daemon=True).start()

    try:
        while True:
            item = await q.get()
            if item is sentinel:
                return
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()
        with suppress(BaseException):
            loop.call_soon_threadsafe(q.put_nowait, sentinel)


async def _aiter_from_iter(
    it: Any,
    *,
    executor: Executor | None,
):
    """
    Convert a synchronous iterator yielding batches into an async generator.

    Uses the same queue/backpressure approach as :func:`aiter_sql`.
    """
    import asyncio
    import threading

    q: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
    sentinel = object()
    stop = threading.Event()
    loop = asyncio.get_running_loop()

    def _put(item: object) -> None:
        if stop.is_set():
            return
        fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
        try:
            while not stop.is_set():
                try:
                    fut.result(timeout=0.25)
                    return
                except TimeoutError:
                    continue
        except BaseException:
            return
        finally:
            if stop.is_set():
                with suppress(BaseException):
                    fut.cancel()

    def _runner() -> None:
        try:
            for batch in it:
                if stop.is_set():
                    return
                _put(batch)
        except BaseException as e:
            _put(e)
        finally:
            _put(sentinel)

    if executor is not None:
        loop.run_in_executor(executor, _runner)
    else:
        threading.Thread(target=_runner, daemon=True).start()

    try:
        while True:
            item = await q.get()
            if item is sentinel:
                return
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()
        with suppress(BaseException):
            loop.call_soon_threadsafe(q.put_nowait, sentinel)


async def aiter_parquet(
    path: str | Path,
    *,
    batch_size: int = 65_536,
    columns: list[str] | None = None,
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_parquet`."""
    it = iter_parquet(path, batch_size=batch_size, columns=columns)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def aiter_ipc(
    source: _Source,
    *,
    batch_size: int = 65_536,
    as_stream: bool = False,
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_ipc`."""
    it = iter_ipc(source, batch_size=batch_size, as_stream=as_stream)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def aiter_csv(
    path: str | Path,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_csv`."""
    it = iter_csv(path, batch_size=batch_size, encoding=encoding)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def aiter_ndjson(
    path: str | Path,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_ndjson`."""
    it = iter_ndjson(path, batch_size=batch_size, encoding=encoding)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def aiter_json_lines(
    path: str | Path,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_json_lines`."""
    it = iter_json_lines(path, batch_size=batch_size, encoding=encoding)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def aiter_json_array(
    path: str | Path,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_json_array`."""
    it = iter_json_array(path, batch_size=batch_size, encoding=encoding)
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def afetch_mongo(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Any = None,
    sort: Sequence[tuple[str, int]] | None = None,
    limit: int | None = None,
    fields: Sequence[str] | None = None,
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    """Async :func:`fetch_mongo` via :func:`asyncio.to_thread` (optional ``Executor``)."""
    return await _run_io(
        fetch_mongo,
        (collection,),
        {
            "match": match,
            "projection": projection,
            "sort": sort,
            "limit": limit,
            "fields": fields,
        },
        executor=executor,
    )


async def aiter_mongo(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Any = None,
    sort: Sequence[tuple[str, int]] | None = None,
    limit: int | None = None,
    batch_size: int = 1000,
    fields: Sequence[str] | None = None,
    executor: Executor | None = None,
):
    """Async batches from :func:`iter_mongo` without blocking the event loop."""
    it = iter_mongo(
        collection,
        match=match,
        projection=projection,
        sort=sort,
        limit=limit,
        batch_size=batch_size,
        fields=fields,
    )
    async for batch in _aiter_from_iter(it, executor=executor):
        yield batch


async def awrite_mongo(
    collection: Any,
    data: dict[str, list[Any]],
    *,
    ordered: bool = True,
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> int:
    """Async :func:`write_mongo` via :func:`asyncio.to_thread` (optional ``Executor``)."""
    return await _run_io(
        write_mongo,
        (collection, data),
        {"ordered": ordered, "chunk_size": chunk_size},
        executor=executor,
    )


async def awrite_sql(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> None:
    warnings.warn(
        "awrite_sql is deprecated and will be removed in a future major version; "
        "for mapped tables use awrite_sqlmodel(...), for string SQL use awrite_sql_raw(...).",
        DeprecationWarning,
        stacklevel=2,
    )
    await _run_io(
        write_sql_raw,
        (data, table_name, bind),
        {"schema": schema, "if_exists": if_exists, "chunk_size": chunk_size},
        executor=executor,
    )


async def awrite_sql_raw(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> None:
    """Async :func:`write_sql_raw` via :func:`asyncio.to_thread` (optional ``Executor``)."""
    await _run_io(
        write_sql_raw,
        (data, table_name, bind),
        {"schema": schema, "if_exists": if_exists, "chunk_size": chunk_size},
        executor=executor,
    )


def write_sql_batches(
    batches: Any,
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
) -> None:
    """
    Write an iterator of batch column dicts to SQL.

    Deprecated: prefer calling :func:`write_sql_raw` per batch.

    Each batch is a ``dict[str, list]`` (e.g. from :func:`iter_sql_raw` or a
    :class:`~pydantable.DataFrameModel` batch via ``.to_dict()``).
    """
    warnings.warn(
        "write_sql_batches is deprecated and will be removed in a future major version; "
        "call write_sql_raw once per batch or use write_sqlmodel_batches for table models.",
        DeprecationWarning,
        stacklevel=2,
    )
    first = True
    for batch in batches:
        cols = batch.to_dict() if hasattr(batch, "to_dict") else batch
        mode = if_exists if first else "append"
        write_sql_raw(
            cols,
            table_name,
            bind,
            schema=schema,
            if_exists=mode,
            chunk_size=chunk_size,
        )
        first = False


async def awrite_sql_batches(
    batches: Any,
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    executor: Executor | None = None,
) -> None:
    warnings.warn(
        "awrite_sql_batches is deprecated and will be removed in a future major version; "
        "call awrite_sql_raw per batch or use awrite_sqlmodel_batches for table models.",
        DeprecationWarning,
        stacklevel=2,
    )
    first = True
    async for batch in batches:
        cols = batch.to_dict() if hasattr(batch, "to_dict") else batch
        mode = if_exists if first else "append"
        await _run_io(
            write_sql_raw,
            (cols, table_name, bind),
            {"schema": schema, "if_exists": mode, "chunk_size": chunk_size},
            executor=executor,
        )
        first = False


async def awrite_sqlmodel(
    data: dict[str, list[Any]],
    model: Any,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    validate_rows: bool = False,
    replace_ok: bool = False,
    executor: Executor | None = None,
) -> None:
    await _run_io(
        write_sqlmodel,
        (data, model, bind),
        {
            "schema": schema,
            "if_exists": if_exists,
            "chunk_size": chunk_size,
            "validate_rows": validate_rows,
            "replace_ok": replace_ok,
        },
        executor=executor,
    )


def write_sqlmodel_batches(
    batches: Any,
    model: Any,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    validate_rows: bool = False,
    replace_ok: bool = False,
) -> None:
    """
    Write an iterator of batch column dicts via :func:`write_sqlmodel`.

    The first batch uses ``if_exists``; later batches always append.
    """
    first = True
    for batch in batches:
        cols = batch.to_dict() if hasattr(batch, "to_dict") else batch
        mode = if_exists if first else "append"
        write_sqlmodel(
            cols,
            model,
            bind,
            schema=schema,
            if_exists=mode,
            chunk_size=chunk_size,
            validate_rows=validate_rows,
            replace_ok=replace_ok if mode == "replace" else False,
        )
        first = False


async def awrite_sqlmodel_batches(
    batches: Any,
    model: Any,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    validate_rows: bool = False,
    replace_ok: bool = False,
    executor: Executor | None = None,
) -> None:
    first = True
    async for batch in batches:
        cols = batch.to_dict() if hasattr(batch, "to_dict") else batch
        mode = if_exists if first else "append"
        await _run_io(
            write_sqlmodel,
            (cols, model, bind),
            {
                "schema": schema,
                "if_exists": mode,
                "chunk_size": chunk_size,
                "validate_rows": validate_rows,
                "replace_ok": replace_ok if mode == "replace" else False,
            },
            executor=executor,
        )
        first = False


__all__ = [
    "MissingRustExtensionError",
    "aexport_csv",
    "aexport_ipc",
    "aexport_json",
    "aexport_ndjson",
    "aexport_parquet",
    "afetch_mongo",
    "afetch_sql",
    "afetch_sql_raw",
    "afetch_sqlmodel",
    "aiter_csv",
    "aiter_ipc",
    "aiter_json_array",
    "aiter_json_lines",
    "aiter_mongo",
    "aiter_ndjson",
    "aiter_parquet",
    "aiter_sql",
    "aiter_sql_raw",
    "aiter_sqlmodel",
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
    "awrite_mongo",
    "awrite_sql",
    "awrite_sql_batches",
    "awrite_sql_raw",
    "awrite_sqlmodel",
    "awrite_sqlmodel_batches",
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
    "fetch_mongo",
    "fetch_sql",
    "fetch_sql_raw",
    "fetch_sqlmodel",
    "http",
    "iter_avro",
    "iter_bigquery",
    "iter_chain_batches",
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
    "iter_sql",
    "iter_sql_raw",
    "iter_sqlmodel",
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
    "sqlmodel_columns",
    "write_csv_batches",
    "write_csv_stdout",
    "write_ipc_batches",
    "write_ndjson_batches",
    "write_parquet_batches",
    "write_mongo",
    "write_sql",
    "write_sql_batches",
    "write_sql_raw",
    "write_sqlmodel",
    "write_sqlmodel_batches",
]

# Built-in plugin registrations (additive)
register_reader("read_parquet", read_parquet, stable=True)
register_reader("read_csv", read_csv, stable=True)
register_reader("read_ndjson", read_ndjson, stable=True)
register_reader("read_ipc", read_ipc, stable=True)
register_reader("read_json", read_json, stable=True)
register_reader("materialize_parquet", materialize_parquet, stable=True)
register_reader("materialize_csv", materialize_csv, stable=True)
register_reader("materialize_ndjson", materialize_ndjson, stable=True)
register_reader("materialize_ipc", materialize_ipc, stable=True)
register_reader("materialize_json", materialize_json, stable=True)
register_reader("fetch_sql", fetch_sql, requires_extra="sql", stable=False)
register_reader("fetch_sql_raw", fetch_sql_raw, requires_extra="sql", stable=True)
register_reader("fetch_sqlmodel", fetch_sqlmodel, requires_extra="sql", stable=True)
register_reader("fetch_mongo", fetch_mongo, requires_extra="mongo", stable=True)
register_reader("iter_sql", iter_sql, requires_extra="sql", stable=False)
register_reader("iter_sql_raw", iter_sql_raw, requires_extra="sql", stable=True)
register_reader("iter_sqlmodel", iter_sqlmodel, requires_extra="sql", stable=True)
register_reader("iter_mongo", iter_mongo, requires_extra="mongo", stable=True)

register_writer("export_parquet", export_parquet, stable=True)
register_writer("export_csv", export_csv, stable=True)
register_writer("export_ndjson", export_ndjson, stable=True)
register_writer("export_ipc", export_ipc, stable=True)
register_writer("export_json", export_json, stable=True)
register_writer("write_sql", write_sql, requires_extra="sql", stable=False)
register_writer("write_sql_raw", write_sql_raw, requires_extra="sql", stable=True)
register_writer("write_sqlmodel", write_sqlmodel, requires_extra="sql", stable=True)
register_writer("write_mongo", write_mongo, requires_extra="mongo", stable=True)
