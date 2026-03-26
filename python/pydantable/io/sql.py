"""SQLAlchemy-backed ``fetch_sql`` / ``write_sql`` (optional ``[sql]`` extra).

Works with **any database URL and dialect** SQLAlchemy supports (PostgreSQL, MySQL,
SQLite, SQL Server, Oracle, etc.). Install the matching **DBAPI driver** for your URL
(``psycopg``, ``pymysql``, ``pyodbc``, …); see SQLAlchemy's "Supported Databases" docs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

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
    if isinstance(bind, SAConnection):
        result = bind.execute(text(sql), params)
        rows = result.mappings().all()
        if not rows:
            return {}
        keys = list(rows[0].keys())
        return {k: [row[k] for row in rows] for k in keys}

    eng = bind if isinstance(bind, SAEngine) else create_engine(bind)
    with eng.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.mappings().all()
    if not rows:
        return {}
    keys = list(rows[0].keys())
    return {k: [row[k] for row in rows] for k in keys}


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
    rows = [{k: data[k][i] for k in data} for i in range(n)]

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
            conn.execute(insert(tbl), rows)
            return

        if not exists:
            raise ValueError(
                f"table {table_name!r} does not exist (if_exists='append')"
            )
        md = MetaData()
        tbl = Table(table_name, md, schema=schema, autoload_with=conn)
        conn.execute(insert(tbl), rows)
