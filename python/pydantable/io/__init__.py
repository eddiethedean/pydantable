"""Unified data I/O: lazy ``read_*`` roots, ``materialize_*`` column dicts, and ``export_*``.

* **``read_*`` / ``aread_*``** return :class:`pydantable._core.ScanFileRoot` for lazy Polars scans
  (no full Python column lists).
* **``materialize_*``** (and **``fetch_sql``** / **``fetch_*_url``**) return ``dict[str, list]``.
* **``export_*`` / ``aexport_*``** write column dicts to files. **``amaterialize_*``** uses :class:`asyncio.to_thread`.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import tempfile
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
from .sql import fetch_sql, write_sql

if TYPE_CHECKING:
    from collections.abc import Mapping
    from concurrent.futures import Executor

from pydantable.plugins import register_reader, register_writer

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
        from pydantable import _core as rust
    except ImportError as e:
        raise MissingRustExtensionError() from e
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
    ``parallel``, ``glob``). Unknown keys raise ``ValueError`` from the Rust layer.
    """
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "parquet", columns=columns, scan_kwargs=sk)


def read_csv(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy CSV read (local path); returns ``ScanFileRoot``. See ``read_parquet`` for ``**scan_kwargs``."""
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "csv", columns=columns, scan_kwargs=sk)


def read_ndjson(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy newline-delimited JSON read (local path); returns ``ScanFileRoot``."""
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "ndjson", columns=columns, scan_kwargs=sk)


def read_ipc(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy Arrow IPC **file** read (local path); returns ``ScanFileRoot``."""
    sk = scan_kwargs if scan_kwargs else None
    return _scan_file_root(path, "ipc", columns=columns, scan_kwargs=sk)


def read_json(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    **scan_kwargs: Any,
) -> Any:
    """Lazy JSON Lines read (local path); same engine as :func:`read_ndjson`.

    For a JSON **array** of objects in one file, use :func:`materialize_json` and construct
    a :class:`~pydantable.dataframe.DataFrame` from the column dict.
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
    except Exception:
        os.unlink(name)
        raise
    return _scan_file_root(name, "parquet", columns=columns, scan_kwargs=None)


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

    * ``engine="auto"`` (default): Rust for local file paths when ``columns`` is ``None``;
      otherwise PyArrow.
    * ``engine="rust"`` / ``"pyarrow"``: force that implementation.

    For out-of-core pipelines prefer :func:`read_parquet` + :meth:`~pydantable.dataframe.DataFrame.write_parquet`.
    """
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_parquet", engine=eng, columns=columns is not None):
        use_rust = eng in ("auto", "rust") and columns is None and _is_local_path(source)
        if use_rust and eng != "pyarrow":
            from ._core_io import rust_read_parquet_path

            path = str(source)
            if os.path.isfile(path):
                try:
                    return rust_read_parquet_path(path)
                except Exception:
                    if eng == "rust":
                        raise
        if eng == "rust" and not use_rust:
            raise ValueError("Rust Parquet read needs a local file path and columns=None")
        return read_parquet_pyarrow(source, columns=columns)


def materialize_ipc(
    source: _Source,
    *,
    as_stream: bool = False,
    engine: str | None = None,
) -> dict[str, list[Any]]:
    """Read Arrow IPC (file or stream) into ``dict[str, list]``."""
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_ipc", engine=eng, as_stream=bool(as_stream)):
        if (
            eng in ("auto", "rust")
            and not as_stream
            and _is_local_path(source)
            and os.path.isfile(str(source))
        ):
            from ._core_io import rust_read_ipc_path

            try:
                return rust_read_ipc_path(str(source))
            except Exception:
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

    * ``engine="auto"``: try Rust, then fall back to stdlib ``csv`` on failure.
    * ``use_rap=True``: load via :func:`aread_csv_rap` (only when no running event loop).
    """
    with span("io.materialize_csv", engine=(engine or _default_engine()).lower(), use_rap=bool(use_rap)):
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
            from ._core_io import rust_read_csv_path

            try:
                return rust_read_csv_path(str(path))
            except Exception:
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
    eng = (engine or _default_engine()).lower()
    with span("io.materialize_ndjson", engine=eng):
        if eng in ("auto", "rust"):
            from ._core_io import rust_read_ndjson_path

            try:
                return rust_read_ndjson_path(str(path))
            except Exception:
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
    """Write ``dict[str, list]`` as one JSON array of row objects."""
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
            from ._core_io import rust_write_parquet_path

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
            from ._core_io import rust_write_csv_path

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
            from ._core_io import rust_write_ndjson_path

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
            from ._core_io import rust_write_ipc_path

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
    executor: Executor | None = None,
) -> dict[str, list[Any]]:
    return await _run_io(
        fetch_sql,
        (sql, bind),
        {"parameters": parameters},
        executor=executor,
    )


async def awrite_sql(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Any,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    executor: Executor | None = None,
) -> None:
    await _run_io(
        write_sql,
        (data, table_name, bind),
        {"schema": schema, "if_exists": if_exists},
        executor=executor,
    )


__all__ = [
    "MissingRustExtensionError",
    "aexport_csv",
    "aexport_ipc",
    "aexport_json",
    "aexport_ndjson",
    "aexport_parquet",
    "afetch_sql",
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
register_reader("fetch_sql", fetch_sql, requires_extra="sql", stable=True)

register_writer("export_parquet", export_parquet, stable=True)
register_writer("export_csv", export_csv, stable=True)
register_writer("export_ndjson", export_ndjson, stable=True)
register_writer("export_ipc", export_ipc, stable=True)
register_writer("export_json", export_json, stable=True)
register_writer("write_sql", write_sql, requires_extra="sql", stable=True)
