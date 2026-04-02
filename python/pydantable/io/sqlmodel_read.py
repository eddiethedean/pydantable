"""SQLModel-first SQL reads (optional ``sqlmodel`` / ``pydantable[sql]``).

Builds a :func:`sqlmodel.select` for a table model and returns column dicts like
:func:`pydantable.io.fetch_sql` / :func:`pydantable.io.iter_sql`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.errors import MissingOptionalDependency

from .sql import (
    StreamingColumns,
    _auto_stream_threshold_rows,
    _fetch_batch_size,
    mappings_rows_to_column_dict,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from sqlalchemy.engine import Connection, Engine


def _require_sqlmodel() -> Any:
    try:
        import sqlmodel
    except ImportError as e:
        raise MissingOptionalDependency(
            "sqlmodel is required for fetch_sqlmodel / iter_sqlmodel. "
            "Install with: pip install 'pydantable[sql]'"
        ) from e
    return sqlmodel


def _ensure_table_model(model: type[Any]) -> None:
    if getattr(model, "__table__", None) is None:
        raise TypeError(
            f"{model!r} must be a SQLModel class declared with table=True "
            "(a mapped SQL table)."
        )


def _build_select(
    model: type[Any],
    *,
    where: Any | None = None,
    columns: Sequence[Any] | None = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
) -> Any:
    sm = _require_sqlmodel()
    select = sm.select
    stmt = select(model) if columns is None else select(*columns)
    if where is not None:
        stmt = stmt.where(where)
    for key in order_by or ():
        stmt = stmt.order_by(key)
    if limit is not None:
        stmt = stmt.limit(limit)
    return stmt


def iter_sqlmodel(
    model: type[Any],
    bind: str | Engine | Connection,
    *,
    where: Any | None = None,
    parameters: Mapping[str, Any] | None = None,
    columns: Sequence[Any] | None = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
) -> Iterator[dict[str, list[Any]]]:
    """
    Stream rows for ``model`` as ``dict[column_name, list]`` batches.

    ``model`` must be a :class:`sqlmodel.SQLModel` subclass with ``table=True``.
    """
    _ensure_table_model(model)
    stmt = _build_select(
        model,
        where=where,
        columns=columns,
        order_by=order_by,
        limit=limit,
    )
    bs = _fetch_batch_size(batch_size)
    params = dict(parameters or {})

    from sqlalchemy import create_engine
    from sqlalchemy.engine import Connection as SAConnection
    from sqlalchemy.engine import Engine as SAEngine

    if isinstance(bind, SAConnection):
        result = bind.execution_options(stream_results=True).execute(stmt, params)
        while True:
            chunk = result.mappings().fetchmany(bs)
            if not chunk:
                break
            yield mappings_rows_to_column_dict(chunk)
        return

    eng = bind if isinstance(bind, SAEngine) else create_engine(bind)
    with eng.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(stmt, params)
        while True:
            chunk = result.mappings().fetchmany(bs)
            if not chunk:
                break
            yield mappings_rows_to_column_dict(chunk)


def fetch_sqlmodel(
    model: type[Any],
    bind: str | Engine | Connection,
    *,
    where: Any | None = None,
    parameters: Mapping[str, Any] | None = None,
    columns: Sequence[Any] | None = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    auto_stream: bool = True,
    auto_stream_threshold_rows: int | None = None,
) -> dict[str, list[Any]] | StreamingColumns:
    """
    Load rows for ``model`` into ``dict[column_name, list]`` (or :class:`StreamingColumns`).

    Semantics match :func:`pydantable.io.fetch_sql` for ``batch_size``,
    ``auto_stream``, and ``auto_stream_threshold_rows``.
    """
    bs = _fetch_batch_size(batch_size)
    thresh = _auto_stream_threshold_rows(auto_stream_threshold_rows)

    batches: list[dict[str, list[Any]]] = []
    total = 0
    streaming = False
    for b in iter_sqlmodel(
        model,
        bind,
        where=where,
        parameters=parameters,
        columns=columns,
        order_by=order_by,
        limit=limit,
        batch_size=bs,
    ):
        if not b:
            continue
        batches.append(b)
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
