from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO, TextIO

from .batches import ensure_rectangular

_PathLike = str | Path


def write_csv_batches(
    path: _PathLike | TextIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    mode: str = "w",
    encoding: str = "utf-8",
    newline: str = "",
    write_header: bool = True,
) -> None:
    """
    Write an iterator of rectangular column dict batches to a single CSV.
    """
    if mode not in ("w", "a"):
        raise ValueError("mode must be 'w' or 'a'")
    close_after = False
    if isinstance(path, (str, Path)):
        fh = open(path, mode, newline=newline, encoding=encoding)
        close_after = True
    else:
        fh = path

    try:
        writer: csv.writer | None = None
        header: list[str] | None = None
        first = True
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            if header is None:
                header = list(batch.keys())
                writer = csv.writer(fh)
            assert header is not None and writer is not None
            if first and write_header and mode == "w":
                writer.writerow(header)
            n = len(next(iter(batch.values())))
            for i in range(n):
                writer.writerow([batch[h][i] for h in header])
            first = False
    finally:
        if close_after:
            fh.close()


def write_ndjson_batches(
    path: _PathLike | TextIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    mode: str = "w",
    encoding: str = "utf-8",
) -> None:
    """
    Write an iterator of rectangular column dict batches to a single NDJSON file.
    """
    if mode not in ("w", "a"):
        raise ValueError("mode must be 'w' or 'a'")
    close_after = False
    if isinstance(path, (str, Path)):
        fh = open(path, mode, encoding=encoding)
        close_after = True
    else:
        fh = path

    try:
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            keys = list(batch.keys())
            n = len(next(iter(batch.values())))
            for i in range(n):
                row = {k: batch[k][i] for k in keys}
                fh.write(json.dumps(row, default=str) + "\n")
    finally:
        if close_after:
            fh.close()


def write_ipc_batches(
    path: _PathLike | BinaryIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    as_stream: bool = True,
) -> None:
    """
    Write batches to Arrow IPC.

    Requires `pyarrow` (install `pydantable[arrow]`).
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        from pyarrow import ipc  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "write_ipc_batches requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    close_after = False
    if isinstance(path, (str, Path)):
        fh = open(path, "wb")
        close_after = True
    else:
        fh = path

    writer = None
    try:
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            table = pa.Table.from_pydict(batch)
            if writer is None:
                if as_stream:
                    writer = ipc.new_stream(fh, table.schema)
                else:
                    writer = ipc.new_file(fh, table.schema)
            writer.write_table(table)
        if writer is not None:
            writer.close()
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if close_after:
            fh.close()


def write_parquet_batches(
    path: _PathLike | BinaryIO,
    batches: Iterable[dict[str, list[Any]]],
    *,
    compression: str | None = None,
) -> None:
    """
    Write batches to a single Parquet file (row groups per batch).

    Requires `pyarrow` (install `pydantable[arrow]`).
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "write_parquet_batches requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    close_after = False
    if isinstance(path, (str, Path)):
        fh = open(path, "wb")
        close_after = True
    else:
        fh = path

    writer = None
    try:
        for batch in batches:
            if not batch:
                continue
            ensure_rectangular(batch)
            table = pa.Table.from_pydict(batch)
            if writer is None:
                writer = pq.ParquetWriter(fh, table.schema, compression=compression)
            writer.write_table(table)
        if writer is not None:
            writer.close()
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if close_after:
            fh.close()

