"""SQLAlchemy-backed raw string SQL I/O (optional ``[sql]`` extra).

Use :func:`fetch_sql_raw`, :func:`iter_sql_raw`, and :func:`write_sql_raw` for explicit
string-SQL access.

Works with **any database URL and dialect** SQLAlchemy supports (PostgreSQL, MySQL,
SQLite, SQL Server, Oracle, etc.). Install the matching **DBAPI driver** for your URL
(``psycopg``, ``pymysql``, ``pyodbc``, …); see SQLAlchemy's "Supported Databases" docs.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine


def mappings_rows_to_column_dict(rows: Sequence[Any]) -> dict[str, list[Any]]:
    """Turn SQLAlchemy ``RowMapping`` rows into ``dict[column_name, list]``."""
    if not rows:
        return {}
    first = rows[0]
    keys = list(first.keys())
    return {k: [row[k] for row in rows] for k in keys}


_ENV_FETCH_BATCH_SIZE = "PYDANTABLE_SQL_FETCH_BATCH_SIZE"
_ENV_WRITE_CHUNK_SIZE = "PYDANTABLE_SQL_WRITE_CHUNK_SIZE"
_ENV_AUTO_STREAM_THRESHOLD_ROWS = "PYDANTABLE_SQL_AUTO_STREAM_THRESHOLD_ROWS"

_DEFAULT_FETCH_BATCH_SIZE = 65_536
_DEFAULT_WRITE_CHUNK_SIZE = 10_000
# Heuristic: above this, return a streaming container by default.
_DEFAULT_AUTO_STREAM_THRESHOLD_ROWS = 200_000


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        val = int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an int, got {raw!r}") from e
    if val <= 0:
        raise ValueError(f"{name} must be positive, got {val}")
    return val


def _fetch_batch_size(batch_size: int | None) -> int:
    if batch_size is not None:
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        return int(batch_size)
    return _env_int(_ENV_FETCH_BATCH_SIZE, _DEFAULT_FETCH_BATCH_SIZE)


def _write_chunk_size(chunk_size: int | None) -> int:
    if chunk_size is not None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        return int(chunk_size)
    return _env_int(_ENV_WRITE_CHUNK_SIZE, _DEFAULT_WRITE_CHUNK_SIZE)


def _auto_stream_threshold_rows(threshold: int | None) -> int:
    if threshold is not None:
        if threshold <= 0:
            raise ValueError("auto_stream_threshold_rows must be a positive integer")
        return int(threshold)
    return _env_int(
        _ENV_AUTO_STREAM_THRESHOLD_ROWS, _DEFAULT_AUTO_STREAM_THRESHOLD_ROWS
    )


class StreamingColumns(Mapping[str, list[Any]]):
    """
    Large SQL result container that can be materialized on demand.

    Behaves like a mapping of ``{column_name: list}`` (lists are materialized per
    column on access). Use :meth:`to_dict` to materialize the full column dict.
    """

    def __init__(self, batches: list[dict[str, list[Any]]]) -> None:
        self._batches = batches
        self._cache: dict[str, list[Any]] = {}
        self._keys: list[str] = []
        for b in batches:
            if b:
                self._keys = list(b.keys())
                break

    def __iter__(self):
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, key: str) -> list[Any]:
        if key in self._cache:
            return self._cache[key]
        out: list[Any] = []
        for b in self._batches:
            if not b:
                continue
            out.extend(b.get(key, []))
        self._cache[key] = out
        return out

    def batches(self) -> list[dict[str, list[Any]]]:
        """Return the underlying list of batch dicts."""
        return self._batches

    def to_dict(self) -> dict[str, list[Any]]:
        return {k: self[k] for k in self._keys}


def _to_engine(bind: str | Engine | Connection) -> Engine:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    if isinstance(bind, SAEngine):
        return bind
    if isinstance(bind, SAConnection):
        return bind.engine
    return create_engine(bind)


def iter_sql_raw(
    sql: str,
    bind: str | Engine | Connection,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int | None = None,
) -> Iterator[dict[str, list[Any]]]:
    """
    Execute ``sql`` and yield results in batches as ``dict[column_name, list]``.

    Streaming alternative to :func:`fetch_sql_raw` for large result sets.

    Notes:
    - ``sql`` should be a ``SELECT`` (or other statement returning rows).
    - Use **bound parameters** only — never interpolate untrusted input into ``sql``.
    - ``bind`` may be a SQLAlchemy URL string, ``Engine``, or ``Connection``.
    """
    bs = _fetch_batch_size(batch_size)

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    params = dict(parameters or {})

    if isinstance(bind, SAConnection):
        result = bind.execution_options(stream_results=True).execute(text(sql), params)
        while True:
            chunk = result.mappings().fetchmany(bs)
            if not chunk:
                break
            yield mappings_rows_to_column_dict(chunk)
        return

    eng = bind if isinstance(bind, SAEngine) else create_engine(bind)
    with eng.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(sql), params)
        while True:
            chunk = result.mappings().fetchmany(bs)
            if not chunk:
                break
            yield mappings_rows_to_column_dict(chunk)


def fetch_sql_raw(
    sql: str,
    bind: str | Engine | Connection,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
) -> dict[str, list[Any]] | StreamingColumns:
    """
    Execute ``sql`` and return rows as ``dict[column_name, list]`` (materialized).

    ``bind`` may be any SQLAlchemy **URL** your environment has drivers for, or a
    :class:`~sqlalchemy.engine.Engine` / :class:`~sqlalchemy.engine.Connection`.
    Use **bound parameters** only — never interpolate untrusted input into ``sql``.
    """
    bs = _fetch_batch_size(batch_size)
    thresh = _auto_stream_threshold_rows(auto_stream_threshold_rows)

    batches: list[dict[str, list[Any]]] = []
    total = 0
    streaming = False
    for b in iter_sql_raw(sql, bind, parameters=parameters, batch_size=bs):
        if not b:
            continue
        batches.append(b)
        # any column length works; iter_sql batches are rectangular
        any_col = next(iter(b.values()))
        total += len(any_col)
        if auto_stream and total > thresh:
            streaming = True

    if not batches:
        return {}
    if streaming:
        return StreamingColumns(batches)
    if len(batches) == 1:
        return batches[0]
    keys = list(batches[0].keys())
    out: dict[str, list[Any]] = {k: [] for k in keys}
    for b in batches:
        for k in keys:
            out[k].extend(b.get(k, []))
    return out


def _infer_columns(data: dict[str, list[Any]]) -> list[Any]:
    from sqlalchemy import Column
    from sqlalchemy import types as sat

    cols: list[Any] = []
    for name, col in data.items():
        sample = next((x for x in col if x is not None), None)
        if isinstance(sample, bool):
            typ: Any = sat.Boolean()
        elif isinstance(sample, int):
            typ = sat.BigInteger()
        elif isinstance(sample, float):
            typ = sat.Float()
        else:
            typ = sat.Text()
        cols.append(Column(name, typ))
    return cols


def write_sql_raw(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Engine | Connection,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
) -> None:
    """
    Insert ``data`` (column dict) into ``table_name``.

    * ``append``: table must already exist; rows are appended.
    * ``replace``: drops the table if it exists, recreates it with inferred column types, then inserts.
      ``table_name`` / ``schema`` must be **trusted** identifiers (not user-controlled).

    ``bind`` is any SQLAlchemy-supported **URL** or **Engine** (same driver rules as ``fetch_sql_raw``).
    ``if_exists="replace"`` uses generic DDL; exotic dialects may need app-specific migrations instead.
    """
    from sqlalchemy import MetaData, Table, insert, inspect
    from sqlalchemy.schema import CreateTable, DropTable

    if if_exists not in ("append", "replace"):
        raise ValueError("if_exists must be 'append' or 'replace'")
    if not data:
        return
    lengths = {len(v) for v in data.values()}
    if len(lengths) != 1:
        raise ValueError("all columns in data must have the same length")
    n = lengths.pop()
    keys = list(data.keys())
    chunk_n = _write_chunk_size(chunk_size)

    def _row_chunks():
        for start in range(0, n, chunk_n):
            end = min(start + chunk_n, n)
            chunk = []
            for i in range(start, end):
                chunk.append({k: data[k][i] for k in keys})
            yield chunk

    eng = _to_engine(bind)
    insp = inspect(eng)
    exists = insp.has_table(table_name, schema=schema)

    with eng.begin() as conn:
        if if_exists == "replace":
            if exists:
                old_md = MetaData()
                old_tbl = Table(table_name, old_md, schema=schema)
                conn.execute(DropTable(old_tbl))
            md = MetaData()
            tbl = Table(
                table_name,
                md,
                *_infer_columns(data),
                schema=schema,
            )
            conn.execute(CreateTable(tbl))
            for chunk in _row_chunks():
                conn.execute(insert(tbl), chunk)
            return

        if not exists:
            raise ValueError(
                f"table {table_name!r} does not exist (if_exists='append')"
            )
        md = MetaData()
        tbl = Table(table_name, md, schema=schema, autoload_with=conn)
        for chunk in _row_chunks():
            conn.execute(insert(tbl), chunk)
