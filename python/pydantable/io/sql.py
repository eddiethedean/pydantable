"""SQLAlchemy-backed ``fetch_sql`` / ``write_sql`` (optional ``[sql]`` extra).

Works with **any database URL and dialect** SQLAlchemy supports (PostgreSQL, MySQL,
SQLite, SQL Server, Oracle, etc.). Install the matching **DBAPI driver** for your URL
(``psycopg``, ``pymysql``, ``pyodbc``, …); see SQLAlchemy's "Supported Databases" docs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from sqlalchemy.engine import Connection, Engine


def _to_engine(bind: str | Engine | Connection) -> Engine:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    if isinstance(bind, SAEngine):
        return bind
    if isinstance(bind, SAConnection):
        return bind.engine
    return create_engine(bind)


def fetch_sql(
    sql: str,
    bind: str | Engine | Connection,
    *,
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, list[Any]]:
    """
    Execute ``sql`` and return rows as ``dict[column_name, list]`` (materialized).

    ``bind`` may be any SQLAlchemy **URL** your environment has drivers for, or a
    :class:`~sqlalchemy.engine.Engine` / :class:`~sqlalchemy.engine.Connection`.
    Use **bound parameters** only — never interpolate untrusted input into ``sql``.
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    params = dict(parameters or {})
    # Stream under the hood to avoid materializing the full row-mapping list
    # (still returns one fully-materialized column dict).
    cols: dict[str, list[Any]] = {}
    keys: list[str] | None = None

    def _consume(result: Any) -> None:
        nonlocal cols, keys
        # Use a moderate batch size to keep overhead low while bounding peak memory.
        batch_size = 65_536
        while True:
            chunk = result.mappings().fetchmany(batch_size)
            if not chunk:
                break
            if keys is None:
                keys = list(chunk[0].keys())
                cols = {k: [] for k in keys}
            for row in chunk:
                for k in keys:  # type: ignore[union-attr]
                    cols[k].append(row[k])

    if isinstance(bind, SAConnection):
        result = bind.execution_options(stream_results=True).execute(text(sql), params)
        _consume(result)
        return cols

    eng = bind if isinstance(bind, SAEngine) else create_engine(bind)
    with eng.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(sql), params)
        _consume(result)
    return cols


def iter_sql(
    sql: str,
    bind: str | Engine | Connection,
    *,
    parameters: Mapping[str, Any] | None = None,
    batch_size: int = 65_536,
) -> Iterator[dict[str, list[Any]]]:
    """
    Execute ``sql`` and yield results in batches as ``dict[column_name, list]``.

    This is a streaming alternative to :func:`fetch_sql` for large result sets.
    Each yielded batch is fully materialized in Python, but the full result set
    is never loaded at once.

    Notes:
    - ``sql`` should be a ``SELECT`` (or other statement returning rows).
    - Use **bound parameters** only — never interpolate untrusted input into ``sql``.
    - ``bind`` may be a SQLAlchemy URL string, ``Engine``, or ``Connection``.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    params = dict(parameters or {})

    def _rows_to_cols(rows: Sequence[Any]) -> dict[str, list[Any]]:
        if not rows:
            return {}
        first = rows[0]
        keys = list(first.keys())
        return {k: [row[k] for row in rows] for k in keys}

    if isinstance(bind, SAConnection):
        result = bind.execution_options(stream_results=True).execute(text(sql), params)
        while True:
            chunk = result.mappings().fetchmany(batch_size)
            if not chunk:
                break
            yield _rows_to_cols(chunk)
        return

    eng = bind if isinstance(bind, SAEngine) else create_engine(bind)
    with eng.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(sql), params)
        while True:
            chunk = result.mappings().fetchmany(batch_size)
            if not chunk:
                break
            yield _rows_to_cols(chunk)


def _infer_columns(data: dict[str, list[Any]]):
    from sqlalchemy import Column
    from sqlalchemy import types as sat

    cols = []
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


def write_sql(
    data: dict[str, list[Any]],
    table_name: str,
    bind: str | Engine | Connection,
    *,
    schema: str | None = None,
    if_exists: str = "append",
) -> None:
    """
    Insert ``data`` (column dict) into ``table_name``.

    * ``append``: table must already exist; rows are appended.
    * ``replace``: drops the table if it exists, recreates it with inferred column types, then inserts.
      ``table_name`` / ``schema`` must be **trusted** identifiers (not user-controlled).

    ``bind`` is any SQLAlchemy-supported **URL** or **Engine** (same driver rules as ``fetch_sql``).
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

    def _row_chunks(chunk_size: int = 10_000):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        for start in range(0, n, chunk_size):
            end = min(start + chunk_size, n)
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
