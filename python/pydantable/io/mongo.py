"""PyMongo column-dict I/O (optional ``pymongo`` / ``pydantable[mongo]``).

Eager reads return ``dict[column_name, list]`` like :func:`pydantable.io.fetch_sqlmodel`.
Writes use :meth:`pymongo.collection.Collection.insert_many` from a rectangular column dict.

Async :class:`pymongo.asynchronous.collection.AsyncCollection` is supported by
:func:`afetch_mongo` / :func:`aiter_mongo` / :func:`awrite_mongo` in :mod:`pydantable.io`
via dedicated awaitable helpers (native PyMongo async API), avoiding thread offloading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantable.errors import MissingOptionalDependency

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping, Sequence

from .batches import ensure_rectangular
from .sql import _write_chunk_size


def _require_pymongo() -> None:
    try:
        __import__("pymongo")
    except ImportError as e:
        raise MissingOptionalDependency(
            "pymongo is required for MongoDB I/O (fetch_mongo, iter_mongo, write_mongo, …). "
            'Install with: pip install pymongo or pip install "pydantable[mongo]"'
        ) from e


def is_async_mongo_collection(collection: Any) -> bool:
    """True for :class:`pymongo.asynchronous.collection.AsyncCollection` instances."""
    try:
        return type(collection).__module__.startswith("pymongo.asynchronous")
    except Exception:
        # Broad: tolerate pathological proxies or broken __class__ / __module__.
        return False


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


def _apply_cursor_modifiers(
    cur: Any,
    *,
    sort: Sequence[tuple[str, int]] | None,
    skip: int | None,
    limit: int | None,
    batch_size: int,
    max_time_ms: int | None,
) -> Any:
    if sort is not None:
        cur = cur.sort(list(sort))
    if skip is not None:
        cur = cur.skip(int(skip))
    if limit is not None:
        cur = cur.limit(int(limit))
    if max_time_ms is not None:
        cur = cur.max_time_ms(max_time_ms)
    if batch_size > 0:
        cur = cur.batch_size(int(batch_size))
    return cur


def _build_cursor(
    collection: Any,
    *,
    match: Mapping[str, Any] | None,
    projection: Mapping[str, Any] | int | bool | None,
    sort: Sequence[tuple[str, int]] | None,
    skip: int | None,
    limit: int | None,
    batch_size: int,
    session: Any | None,
    max_time_ms: int | None,
) -> Any:
    _require_pymongo()
    find_kw: dict[str, Any] = {}
    if session is not None:
        find_kw["session"] = session
    cur = collection.find(match or {}, projection, **find_kw)
    return _apply_cursor_modifiers(
        cur,
        sort=sort,
        skip=skip,
        limit=limit,
        batch_size=batch_size,
        max_time_ms=max_time_ms,
    )


def iter_mongo(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | int | bool | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    skip: int | None = None,
    limit: int | None = None,
    batch_size: int = 1000,
    fields: Sequence[str] | None = None,
    session: Any | None = None,
    max_time_ms: int | None = None,
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
        skip=skip,
        limit=limit,
        batch_size=batch_size,
        session=session,
        max_time_ms=max_time_ms,
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
    skip: int | None = None,
    limit: int | None = None,
    fields: Sequence[str] | None = None,
    session: Any | None = None,
    max_time_ms: int | None = None,
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
        skip=skip,
        limit=limit,
        batch_size=0,
        session=session,
        max_time_ms=max_time_ms,
    )
    docs = [dict(d) for d in cur]
    return _docs_to_column_dict(docs, fields=fields)


def write_mongo(
    collection: Any,
    data: dict[str, list[Any]],
    *,
    ordered: bool = True,
    chunk_size: int | None = None,
    session: Any | None = None,
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
    insert_kw: dict[str, Any] = {}
    if session is not None:
        insert_kw["session"] = session
    for start in range(0, n, chunk_n):
        end = min(start + chunk_n, n)
        rows = [{k: data[k][i] for k in keys} for i in range(start, end)]
        res = collection.insert_many(rows, ordered=ordered, **insert_kw)
        total += len(res.inserted_ids)
    return total


def _build_async_cursor(
    collection: Any,
    *,
    match: Mapping[str, Any] | None,
    projection: Mapping[str, Any] | int | bool | None,
    sort: Sequence[tuple[str, int]] | None,
    skip: int | None,
    limit: int | None,
    batch_size: int,
    session: Any | None,
    max_time_ms: int | None,
) -> Any:
    _require_pymongo()
    find_kw: dict[str, Any] = {}
    if session is not None:
        find_kw["session"] = session
    cur = collection.find(match or {}, projection, **find_kw)
    return _apply_cursor_modifiers(
        cur,
        sort=sort,
        skip=skip,
        limit=limit,
        batch_size=batch_size,
        max_time_ms=max_time_ms,
    )


async def afetch_mongo_async(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | int | bool | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    skip: int | None = None,
    limit: int | None = None,
    fields: Sequence[str] | None = None,
    session: Any | None = None,
    max_time_ms: int | None = None,
) -> dict[str, list[Any]]:
    """Async :func:`fetch_mongo` for :class:`~pymongo.asynchronous.collection.AsyncCollection`."""
    _require_pymongo()
    cur = _build_async_cursor(
        collection,
        match=match,
        projection=projection,
        sort=sort,
        skip=skip,
        limit=limit,
        batch_size=0,
        session=session,
        max_time_ms=max_time_ms,
    )
    docs: list[dict[str, Any]] = []
    async for d in cur:
        docs.append(dict(d))
    return _docs_to_column_dict(docs, fields=fields)


async def aiter_mongo_async(
    collection: Any,
    *,
    match: Mapping[str, Any] | None = None,
    projection: Mapping[str, Any] | int | bool | None = None,
    sort: Sequence[tuple[str, int]] | None = None,
    skip: int | None = None,
    limit: int | None = None,
    batch_size: int = 1000,
    fields: Sequence[str] | None = None,
    session: Any | None = None,
    max_time_ms: int | None = None,
) -> AsyncIterator[dict[str, list[Any]]]:
    """Async :func:`iter_mongo` for :class:`~pymongo.asynchronous.collection.AsyncCollection`."""
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    _require_pymongo()
    cur = _build_async_cursor(
        collection,
        match=match,
        projection=projection,
        sort=sort,
        skip=skip,
        limit=limit,
        batch_size=batch_size,
        session=session,
        max_time_ms=max_time_ms,
    )
    batch: list[dict[str, Any]] = []
    async for doc in cur:
        batch.append(dict(doc))
        if len(batch) >= batch_size:
            yield _docs_to_column_dict(batch, fields=fields)
            batch = []
    if batch:
        yield _docs_to_column_dict(batch, fields=fields)


async def awrite_mongo_async(
    collection: Any,
    data: dict[str, list[Any]],
    *,
    ordered: bool = True,
    chunk_size: int | None = None,
    session: Any | None = None,
) -> int:
    """Async :func:`write_mongo` for :class:`~pymongo.asynchronous.collection.AsyncCollection`."""
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
    insert_kw: dict[str, Any] = {}
    if session is not None:
        insert_kw["session"] = session
    for start in range(0, n, chunk_n):
        end = min(start + chunk_n, n)
        rows = [{k: data[k][i] for k in keys} for i in range(start, end)]
        res = await collection.insert_many(rows, ordered=ordered, **insert_kw)
        total += len(res.inserted_ids)
    return total


__all__ = [
    "afetch_mongo_async",
    "aiter_mongo_async",
    "awrite_mongo_async",
    "fetch_mongo",
    "is_async_mongo_collection",
    "iter_mongo",
    "write_mongo",
]
