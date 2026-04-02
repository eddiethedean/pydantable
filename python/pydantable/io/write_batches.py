"""Streaming writers that accept iterators of ``dict[str, list]`` batches."""

from __future__ import annotations

import csv
import json
from contextlib import suppress
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, BinaryIO, TextIO

from .batches import ensure_rectangular

if TYPE_CHECKING:
    from _csv import _writer as _CsvWriter
    from collections.abc import Iterable

_PathLike = str | Path
_TextStream = TextIO | IO[str]


def _reject_directory_file_path(path: _PathLike, api: str) -> None:
    """Batch writers target one file; reject an existing directory path early."""
    p = Path(path)
    if p.exists() and p.is_dir():
        raise ValueError(
            f"{api} expects a single output file path, not a directory ({path!r}). "
            "For hive-style partitioned Parquet output, use "
            "``DataFrame.write_parquet(..., partition_by=[...])``."
        )


def write_csv_batches(
    path: _PathLike | _TextStream,
    batches: Iterable[dict[str, list[Any]]],
    *,
    mode: str = "w",
    encoding: str = "utf-8",
    newline: str = "",
    write_header: bool = True,
) -> None:
    """
    Write an iterator of rectangular column dict batches to a **single CSV file**.

    ``path`` must be a file path (or open text stream), not a directory. ``mode="w"``
    truncates or creates the file; ``mode="a"`` appends rows. With ``mode="a"``, a header
    row is written only when the file is new and ``write_header`` is true (first batch).
    """
    if mode not in ("w", "a"):
        raise ValueError("mode must be 'w' or 'a'")
    if isinstance(path, (str, Path)):
        _reject_directory_file_path(path, "write_csv_batches")
        with open(path, mode, newline=newline, encoding=encoding) as fh:
            write_csv_batches(
                fh,
                batches,
                mode=mode,
                encoding=encoding,
                newline=newline,
                write_header=write_header,
            )
        return

    writer: _CsvWriter | None = None
    header: list[str] | None = None
    first = True
    for batch in batches:
        if not batch:
            continue
        ensure_rectangular(batch)
        if header is None:
            header = list(batch.keys())
            writer = csv.writer(path)
        assert header is not None and writer is not None
        if first and write_header and mode == "w":
            writer.writerow(header)
        n = len(next(iter(batch.values())))
        for i in range(n):
            writer.writerow([batch[h][i] for h in header])
        first = False


def write_ndjson_batches(
    path: _PathLike | _TextStream,
    batches: Iterable[dict[str, list[Any]]],
    *,
    mode: str = "w",
    encoding: str = "utf-8",
) -> None:
    """
    Write an iterator of rectangular column dict batches to a **single NDJSON file**.

    ``path`` must be a file path (or open text stream), not a directory. ``mode="w"``
    truncates or creates the file; ``mode="a"`` appends one JSON object per line.
    """
    if mode not in ("w", "a"):
        raise ValueError("mode must be 'w' or 'a'")
    if isinstance(path, (str, Path)):
        _reject_directory_file_path(path, "write_ndjson_batches")
        with open(path, mode, encoding=encoding) as fh:
            write_ndjson_batches(fh, batches, mode=mode, encoding=encoding)
        return

    for batch in batches:
        if not batch:
            continue
        ensure_rectangular(batch)
        keys = list(batch.keys())
        n = len(next(iter(batch.values())))
        for i in range(n):
            row = {k: batch[k][i] for k in keys}
            path.write(json.dumps(row, default=str) + "\n")


def write_ipc_batches(
    path: _PathLike | BinaryIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    as_stream: bool = True,
) -> None:
    """
    Write batches to a **single Arrow IPC file or stream** (not a dataset directory).

    ``path`` must be a file path (or open binary stream). Requires `pyarrow` (install
    ``pydantable[arrow]``).
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        from pyarrow import ipc  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "write_ipc_batches requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    if isinstance(path, (str, Path)):
        _reject_directory_file_path(path, "write_ipc_batches")
        with open(path, "wb") as fh:
            write_ipc_batches(fh, batches, as_stream=as_stream)
        return

    writer = None
    try:
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            table = pa.Table.from_pydict(batch)
            if writer is None:
                writer = (
                    ipc.new_stream(path, table.schema)
                    if as_stream
                    else ipc.new_file(path, table.schema)
                )
            writer.write_table(table)
    finally:
        if writer is not None:
            with suppress(Exception):
                writer.close()


def write_parquet_batches(
    path: _PathLike | BinaryIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    compression: str | None = None,
) -> None:
    """
    Write batches to a **single Parquet file** (one row group per non-empty batch).

    ``path`` must be a file path (or open binary stream), not a directory or hive
    dataset root. For partitioned Parquet on disk, use :meth:`~pydantable.dataframe.DataFrame.write_parquet`
    with ``partition_by``. Requires `pyarrow` (install ``pydantable[arrow]``).
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found,import-untyped]
    except ImportError as e:
        raise ImportError(
            "write_parquet_batches requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    if isinstance(path, (str, Path)):
        _reject_directory_file_path(path, "write_parquet_batches")
        with open(path, "wb") as fh:
            write_parquet_batches(fh, batches, compression=compression)
        return

    writer = None
    try:
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            table = pa.Table.from_pydict(batch)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression=compression)
            writer.write_table(table)
    finally:
        if writer is not None:
            with suppress(Exception):
                writer.close()
