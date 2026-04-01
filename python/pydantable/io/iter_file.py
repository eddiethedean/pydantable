"""Chunked, iterator-based file readers yielding ``dict[str, list]`` batches.

Used by :class:`~pydantable.dataframe_model.DataFrameModel.iter_*` classmethods and
standalone scripts. Optional dependencies (``pyarrow``, etc.) apply per format; see
each function's docstring.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, TextIO

from .batches import ensure_rectangular

if TYPE_CHECKING:
    from collections.abc import Iterator

_PathLike = str | Path


def iter_parquet(
    path: _PathLike,
    *,
    batch_size: int = 65_536,
    columns: list[str] | None = None,
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield Parquet data in batches as ``dict[str, list]``.

    Requires `pyarrow` (install `pydantable[arrow]`).
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found,import-untyped]
    except ImportError as e:
        raise ImportError(
            "iter_parquet requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    pf = pq.ParquetFile(str(path))
    for record_batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        d = record_batch.to_pydict()
        out = {k: list(v) for k, v in d.items()}
        ensure_rectangular(out)
        yield out


def iter_ipc(
    source: _PathLike | BinaryIO | bytes,
    *,
    batch_size: int = 65_536,
    as_stream: bool = False,
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield Arrow IPC in batches as ``dict[str, list]``.

    - ``as_stream=False`` reads IPC file format.
    - ``as_stream=True`` reads IPC stream format.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        from pyarrow import ipc  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "iter_ipc requires pyarrow (pip install 'pydantable[arrow]')."
        ) from e

    if isinstance(source, (str, Path)):
        if as_stream:
            reader = ipc.open_stream(str(source))
        else:
            reader = ipc.open_file(str(source))
    else:
        buf = pa.py_buffer(source)
        reader = ipc.open_stream(buf) if as_stream else ipc.open_file(buf)

    def _batches() -> Iterator[Any]:  # RecordBatch
        if as_stream:
            yield from reader
            return
        n = reader.num_record_batches
        for i in range(n):
            yield reader.get_batch(i)

    with reader:
        for batch in _batches():
            d = batch.to_pydict()
            out = {k: list(v) for k, v in d.items()}
            ensure_rectangular(out)
            yield out


def iter_csv(
    path: _PathLike | TextIO,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    newline: str = "",
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield CSV rows in batches as ``dict[str, list]``.

    Values are yielded as strings (or ``None`` for missing cells); downstream typed
    constructors can validate/coerce as needed.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(path, (str, Path)):
        with open(path, newline=newline, encoding=encoding) as fh:
            yield from iter_csv(
                fh,
                batch_size=batch_size,
                encoding=encoding,
                newline=newline,
            )
        return

    reader = csv.reader(path)
    try:
        header = next(reader)
    except StopIteration:
        return
    header = [str(h) for h in header]
    cols: dict[str, list[Any]] = {h: [] for h in header}
    n = 0
    for row in reader:
        for i, h in enumerate(header):
            cols[h].append(row[i] if i < len(row) else None)
        n += 1
        if n >= batch_size:
            ensure_rectangular(cols)
            yield cols
            cols = {h: [] for h in header}
            n = 0
    if n:
        ensure_rectangular(cols)
        yield cols


def iter_ndjson(
    path: _PathLike | TextIO,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield NDJSON (JSON Lines) as ``dict[str, list]`` batches.

    Each line must be a JSON object; keys are unioned within each batch.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(path, (str, Path)):
        with open(path, encoding=encoding) as fh:
            yield from iter_ndjson(fh, batch_size=batch_size, encoding=encoding)
        return

    rows: list[dict[str, Any]] = []
    for line in path:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise ValueError("NDJSON lines must be JSON objects")
        rows.append(obj)
        if len(rows) >= batch_size:
            yield _rows_to_columns(rows)
            rows = []
    if rows:
        yield _rows_to_columns(rows)


def iter_json_lines(
    path: _PathLike | TextIO,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
) -> Iterator[dict[str, list[Any]]]:
    """Alias of :func:`iter_ndjson`."""
    yield from iter_ndjson(path, batch_size=batch_size, encoding=encoding)


def iter_json_array(
    path: _PathLike | TextIO,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield a JSON array-of-objects file in batches.

    This is not incremental JSON parsing: the full file is currently loaded then
    chunked. Provided for API uniformity.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(path, (str, Path)):
        with open(path, encoding=encoding) as fh:
            yield from iter_json_array(fh, batch_size=batch_size, encoding=encoding)
        return

    data = json.load(path)
    if not isinstance(data, list):
        raise ValueError("JSON array reader expects a top-level array")
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("JSON array elements must be objects")
        rows.append(item)
        if len(rows) >= batch_size:
            yield _rows_to_columns(rows)
            rows = []
    if rows:
        yield _rows_to_columns(rows)


def _rows_to_columns(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {}
    keys = sorted({k for r in rows for k in r})
    out = {k: [r.get(k) for r in rows] for k in keys}
    ensure_rectangular(out)
    return out
