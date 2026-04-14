"""PyMongo column-dict I/O (optional ``pymongo`` / ``pydantable[mongo]``).

Eager reads return ``dict[column_name, list]`` like :func:`pydantable.io.fetch_sqlmodel`.
Writes use :meth:`pymongo.collection.Collection.insert_many` from a rectangular column dict.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.errors import MissingOptionalDependency

from .batches import ensure_rectangular
from .sql import _write_chunk_size

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence


def _require_pymongo() -> None:
    try:
        __import__("pymongo")
    except ImportError as e:
        raise MissingOptionalDependency(
            "pymongo is required for MongoDB I/O (fetch_mongo, iter_mongo, write_mongo, …). "
            'Install with: pip install pymongo or pip install "pydantable[mongo]"'
        ) from e


def _docs_to_column_dict(
    docs: list[dict[str, Any]],
    *,
    fields: Sequence[str] | None,
) -> dict[str, list[Any]]:
    if not docs:
        return {}
    if fields is not None:
        keys = list(fields)
    else:
        keys_set: set[str] = set()
        for d in docs:
            keys_set.update(d.keys())
        keys = sorted(keys_set)
    n = len(docs)
    out: dict[str, list[Any]] = {k: [None] * n for k in keys}
    for i, d in enumerate(docs):
        for k in keys:
            if k in d:
                out[k][i] = d[k]
    return out


def _build_cursor(
    collection: Any,
    *,
    match: Mapping[str, Any] | None,
    projection: Mapping[str, Any] | int | bool | None,
    sort: Sequence[tuple[str, int]] | None,
    limit: int | None,
    batch_size: int,
) -> Any:
    _require_pymongo()
    cur = collection.find(match or {}, projection)
    if sort is not None:
        cur = cur.sort(list(sort))
    if limit is not None:
        cur = cur.limit(int(limit))
    if batch_size > 0:
        cur = cur.batch_size(batch_size)
    return cur


def iter_mongo(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | int | bool | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    limit: int | None = None,
    batch_size: int = 1000,
    fields: Sequence[str] | None = None,
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield ``dict[column_name, list]`` batches from a PyMongo ``Collection.find``.

    Each batch is rectangular. Document keys are merged per batch (sorted union unless
    ``fields`` fixes the column order). Install **pymongo** (or ``pydantable[mongo]``).
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    cur = _build_cursor(
        collection,
        match=match,
        projection=projection,
        sort=sort,
        limit=limit,
        batch_size=batch_size,
    )
    batch: list[dict[str, Any]] = []
    for doc in cur:
        batch.append(dict(doc))
        if len(batch) >= batch_size:
            yield _docs_to_column_dict(batch, fields=fields)
            batch = []
    if batch:
        yield _docs_to_column_dict(batch, fields=fields)


def fetch_mongo(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | int | bool | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    limit: int | None = None,
    fields: Sequence[str] | None = None,
) -> dict[str, list[Any]]:
    """
    Load all matching documents into a single ``dict[column_name, list]``.

    Materializes the full cursor in memory; for large scans prefer :func:`iter_mongo`.
    """
    _require_pymongo()
    cur = _build_cursor(
        collection,
        match=match,
        projection=projection,
        sort=sort,
        limit=limit,
        batch_size=0,
    )
    docs = [dict(d) for d in cur]
    return _docs_to_column_dict(docs, fields=fields)


def write_mongo(
    collection: Any,
    data: dict[str, list[Any]],
    *,
    ordered: bool = True,
    chunk_size: int | None = None,
) -> int:
    """
    Insert rows from a rectangular column ``dict`` via ``insert_many``.

    Returns the number of inserted document ids. Empty ``data`` or zero rows is a no-op
    (returns ``0``).
    """
    _require_pymongo()
    if not data:
        return 0
    ensure_rectangular(data)
    n = len(next(iter(data.values())))
    if n == 0:
        return 0
    keys = list(data.keys())
    chunk_n = _write_chunk_size(chunk_size)
    total = 0
    for start in range(0, n, chunk_n):
        end = min(start + chunk_n, n)
        rows = [{k: data[k][i] for k in keys} for i in range(start, end)]
        res = collection.insert_many(rows, ordered=ordered)
        total += len(res.inserted_ids)
    return total


__all__ = [
    "fetch_mongo",
    "iter_mongo",
    "write_mongo",
]
