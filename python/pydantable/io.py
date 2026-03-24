"""File and Arrow IPC helpers that return columnar ``dict[str, list]`` for constructors.

Requires the optional ``pyarrow`` package (``pip install 'pydantable[arrow]'`` or
``pip install pyarrow``). Results are plain Python lists per column, suitable for
:class:`~pydantable.DataFrame` / :class:`~pydantable.DataFrameModel` and normal
validation rules.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO

_Source = str | Path | BinaryIO | bytes


def _require_pyarrow():
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "pyarrow is required for pydantable.io helpers. Install with: "
            "pip install 'pydantable[arrow]'"
        ) from e
    return pa


def _column_to_pylist(col: Any) -> list[Any]:
    if hasattr(col, "combine_chunks"):
        col = col.combine_chunks()
    return col.to_pylist()


def arrow_table_to_column_dict(table: Any) -> dict[str, list[Any]]:
    """Convert a PyArrow ``Table`` to ``dict[str, list]`` (copies into Python lists)."""
    pa = _require_pyarrow()
    if not isinstance(table, pa.Table):
        raise TypeError(f"expected pyarrow.Table, got {type(table)!r}")
    return {name: _column_to_pylist(table.column(name)) for name in table.column_names}


def record_batch_to_column_dict(batch: Any) -> dict[str, list[Any]]:
    """Convert a PyArrow ``RecordBatch`` to ``dict[str, list]``."""
    pa = _require_pyarrow()
    if not isinstance(batch, pa.RecordBatch):
        raise TypeError(f"expected pyarrow.RecordBatch, got {type(batch)!r}")
    return {
        batch.schema.field(i).name: _column_to_pylist(batch.column(i))
        for i in range(batch.num_columns)
    }


def read_parquet(
    source: _Source,
    *,
    columns: list[str] | None = None,
) -> dict[str, list[Any]]:
    """
    Read a Parquet file or buffer into ``dict[str, list]``.

    ``source`` may be a path, ``pathlib.Path``, ``bytes``, or binary file-like object.
    This uses PyArrow only (no Polars dependency on the read path).
    """
    _require_pyarrow()
    import pyarrow.parquet as pq  # type: ignore[import-not-found,import-untyped]

    if isinstance(source, bytes):
        source = BytesIO(source)
    table = pq.read_table(source, columns=columns)
    return arrow_table_to_column_dict(table)


def read_ipc(
    source: _Source,
    *,
    as_stream: bool = False,
) -> dict[str, list[Any]]:
    """
    Read Arrow IPC into ``dict[str, list]``.

    Use ``as_stream=False`` (default) for the *file* format (``.arrow`` / feather-style
    container). Use ``as_stream=True`` for the *streaming* IPC format (sequence of
    record batches).

    ``source`` may be a path, ``pathlib.Path``, ``bytes``, or binary file-like object.
    """
    _require_pyarrow()
    import pyarrow.ipc as ipc  # type: ignore[import-not-found,import-untyped]

    if isinstance(source, bytes):
        source = BytesIO(source)
    if as_stream:
        with ipc.open_stream(source) as reader:
            table = reader.read_all()
    else:
        with ipc.open_file(source) as reader:
            table = reader.read_all()
    return arrow_table_to_column_dict(table)
