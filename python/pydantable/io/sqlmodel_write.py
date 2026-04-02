"""SQLModel-first SQL writes (optional ``sqlmodel`` / ``pydantable[sql]``).

Uses :class:`sqlmodel.SQLModel` ``__table__`` for DDL on **replace** (no type inference).
Mirrors :func:`pydantable.io.write_sql` chunking and transactions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from .sql import _to_engine, _write_chunk_size
from .sqlmodel_read import _ensure_table_model, _require_sqlmodel
from .sqlmodel_schema import table_column_key_set

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.schema import Table


def _validate_if_exists(if_exists: str) -> None:
    if if_exists not in ("append", "replace"):
        raise ValueError("if_exists must be 'append' or 'replace'")


def _align_data_keys(data: dict[str, list[Any]], table: Table) -> None:
    data_keys = set(data.keys())
    expected = table_column_key_set(table)
    if data_keys == expected:
        return
    missing = sorted(expected - data_keys)
    extra = sorted(data_keys - expected)
    parts: list[str] = []
    if missing:
        parts.append(f"missing columns: {missing}")
    if extra:
        parts.append(f"extra columns: {extra}")
    raise ValueError(
        "data keys must match SQLModel table columns exactly. " + "; ".join(parts)
    )


def _insert_row_dict(row: dict[str, Any], table: Table) -> dict[str, Any]:
    """Omit None only for primary-key columns so autoincrement can apply."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            col = table.c.get(k)
            if col is not None and col.primary_key:
                continue
        out[k] = v
    return out


def write_sqlmodel(
    data: dict[str, list[Any]],
    model: type[Any],
    bind: str | Engine | Connection,
    *,
    schema: str | None = None,
    if_exists: str = "append",
    chunk_size: int | None = None,
    validate_rows: bool = False,
    replace_ok: bool = False,
) -> None:
    """
    Insert ``data`` into the table defined by ``model`` (``table=True`` SQLModel).

    * ``append``: table must already exist.
    * ``replace``: drops the table if present, recreates from ``model.__table__``, inserts.
      Requires ``replace_ok=True`` (destructive).

    ``validate_rows=True`` runs ``model.model_validate`` per row; failures include the
    row index in the error.
    """
    _require_sqlmodel()
    _ensure_table_model(model)
    _validate_if_exists(if_exists)
    if if_exists == "replace" and not replace_ok:
        raise ValueError(
            "if_exists='replace' is destructive (DROP + CREATE). "
            "Pass replace_ok=True after confirming the table name/schema are trusted."
        )
    if not data:
        return

    lengths = {len(v) for v in data.values()}
    if len(lengths) != 1:
        raise ValueError("all columns in data must have the same length")
    n = next(iter(lengths))

    table = model.__table__
    tbl_schema = table.schema
    if schema is not None and schema != tbl_schema:
        raise ValueError(
            f"schema={schema!r} does not match model.__table__.schema ({tbl_schema!r})"
        )

    _align_data_keys(data, table)

    keys = list(data.keys())
    if validate_rows:
        for i in range(n):
            row = {k: data[k][i] for k in keys}
            try:
                model.model_validate(row)
            except ValidationError as e:
                raise ValueError(f"row {i} failed validation for {model!r}: {e}") from e

    chunk_n = _write_chunk_size(chunk_size)

    def _row_chunks():
        for start in range(0, n, chunk_n):
            end = min(start + chunk_n, n)
            chunk = []
            for i in range(start, end):
                raw = {k: data[k][i] for k in keys}
                chunk.append(_insert_row_dict(raw, table))
            yield chunk

    from sqlalchemy import insert, inspect

    eng = _to_engine(bind)

    with eng.begin() as conn:
        if if_exists == "replace":
            table.drop(conn, checkfirst=True)
            table.create(bind=conn)
        else:
            if not inspect(eng).has_table(table.name, schema=tbl_schema):
                raise ValueError(
                    f"table {table.name!r} does not exist (if_exists='append')"
                )

        for chunk in _row_chunks():
            conn.execute(insert(table), chunk)
