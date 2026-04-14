"""Async Beanie ODM integration (optional ``beanie`` / ``pydantable[mongo]``).

These helpers intentionally operate at the **Beanie query/document** level so callers
can fully leverage Beanie features (operators, projections, links, hooks, migrations).

They return/accept the same column-dict shape as other pydantable eager I/O:
``dict[str, list]``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantable.errors import MissingOptionalDependency


def _require_beanie() -> None:
    try:
        __import__("beanie")
    except ImportError as e:
        raise MissingOptionalDependency(
            "beanie is required for Beanie ODM I/O (afetch_beanie, aiter_beanie, awrite_beanie, …). "
            'Install with: pip install beanie or pip install "pydantable[mongo]"'
        ) from e


def _pydantic_model_to_dict(obj: Any) -> dict[str, Any]:
    # Beanie Document is a Pydantic model. Prefer `model_dump()` when present.
    if hasattr(obj, "model_dump"):
        return obj.model_dump(by_alias=True)  # type: ignore[no-any-return]
    if isinstance(obj, Mapping):
        return dict(obj)
    return dict(obj)  # best effort; may raise


def _flatten_dict(
    d: Mapping[str, Any],
    *,
    prefix: str = "",
    out: dict[str, Any] | None = None,
    sep: str = ".",
) -> dict[str, Any]:
    out = {} if out is None else out
    for k, v in d.items():
        key = f"{prefix}{sep}{k}" if prefix else str(k)
        if isinstance(v, Mapping):
            _flatten_dict(v, prefix=key, out=out, sep=sep)
        else:
            out[key] = v
    return out


def _normalize_id_keys(
    row: dict[str, Any], *, id_column: Literal["id", "_id"]
) -> dict[str, Any]:
    # Beanie exposes `id` for Mongo `_id`. When dumping `by_alias=True`, `_id` may appear.
    if id_column == "id":
        if "id" in row:
            return row
        if "_id" in row and "id" not in row:
            row = dict(row)
            row["id"] = row.pop("_id")
            return row
        return row
    # id_column == "_id"
    if "_id" in row:
        return row
    if "id" in row and "_id" not in row:
        row = dict(row)
        row["_id"] = row.pop("id")
        return row
    return row


def _rows_to_column_dict(
    rows: Sequence[dict[str, Any]],
    *,
    fields: Sequence[str] | None,
) -> dict[str, list[Any]]:
    if not rows:
        return {}
    if fields is not None:
        keys = list(fields)
    else:
        keys_set: set[str] = set()
        for r in rows:
            keys_set.update(r.keys())
        keys = sorted(keys_set)
    out: dict[str, list[Any]] = {k: [] for k in keys}
    for r in rows:
        for k in keys:
            out[k].append(r.get(k))
    return out


def _projection_model_for_fields(
    document_cls: type[Any], fields: Sequence[str]
) -> type[Any]:
    """Build a Pydantic projection model using the Document's field annotations."""
    from pydantic import BaseModel, ConfigDict, create_model

    doc_fields = getattr(document_cls, "model_fields", {}) or {}
    defs: dict[str, tuple[Any, Any]] = {}
    for name in fields:
        field = doc_fields.get(name)
        ann = getattr(field, "annotation", Any) if field is not None else Any
        defs[name] = (ann, None)

    cfg = ConfigDict(populate_by_name=True)
    return create_model(  # type: ignore[call-overload]
        f"{document_cls.__name__}Projection",
        __base__=BaseModel,
        __config__=cfg,
        **defs,
    )


async def _query_to_list(query: Any) -> list[Any]:
    # Beanie FindMany supports `to_list()`. Also accept async iterables.
    to_list = getattr(query, "to_list", None)
    if callable(to_list):
        return await to_list()
    out: list[Any] = []
    async for item in query:
        out.append(item)
    return out


async def afetch_beanie(
    document_or_query: Any,
    *,
    criteria: Any | None = None,
    projection_model: type[Any] | None = None,
    fields: Sequence[str] | None = None,
    fetch_links: bool = False,
    nesting_depth: int | None = None,
    nesting_depths_per_field: Mapping[str, int] | None = None,
    flatten: bool = True,
    id_column: Literal["id", "_id"] = "id",
) -> dict[str, list[Any]]:
    """Fetch Beanie documents (or a Beanie query) into ``dict[str, list]``.

    - **document_or_query**: a Beanie ``Document`` class (preferred) or a query object
      returned from ``Document.find(...)``.
    - **fields**: convenience projection (builds a temporary projection model).
    - **projection_model**: passed to Beanie ``.project(...)``.
    - **fetch_links / nesting_depth***: forwarded to Beanie find call (relations).
    - **flatten**: if True, nested objects are flattened into dot-path keys.
    - **id_column**: normalize Mongo id to ``id`` (default) or ``_id``.
    """
    _require_beanie()

    query: Any
    doc_cls: type[Any] | None = None
    if isinstance(document_or_query, type) and hasattr(document_or_query, "find"):
        doc_cls = document_or_query
        if criteria is None:
            # Prefer find_all when present; else find({}).
            find_all = getattr(doc_cls, "find_all", None)
            if callable(find_all):
                query = find_all(fetch_links=fetch_links)
            else:
                query = doc_cls.find({}, fetch_links=fetch_links)
        else:
            query = doc_cls.find(criteria, fetch_links=fetch_links)

        # Beanie supports nesting depth controls as kwargs on `find(...)` in current docs.
        # Keep method-based calls best-effort for older/newer versions.
        if nesting_depth is not None:
            nd = getattr(query, "nesting_depth", None)
            if callable(nd):
                query = nd(nesting_depth)
        if nesting_depths_per_field is not None:
            ndpf = getattr(query, "nesting_depths_per_field", None)
            if callable(ndpf):
                query = ndpf(dict(nesting_depths_per_field))
    else:
        query = document_or_query

    if projection_model is not None and fields is not None:
        raise TypeError("Pass only one of projection_model= or fields=, not both.")
    if fields is not None:
        projection_model = _projection_model_for_fields(
            doc_cls or type("Doc", (), {}), fields
        )
    if projection_model is not None:
        project = getattr(query, "project", None)
        if not callable(project):
            raise TypeError("Query object does not support Beanie-style .project(...).")
        query = project(projection_model)

    items = await _query_to_list(query)

    norm_rows: list[dict[str, Any]] = []
    for obj in items:
        row = _pydantic_model_to_dict(obj)
        row = _normalize_id_keys(row, id_column=id_column)
        if flatten:
            row = _flatten_dict(row)
        norm_rows.append(row)

    # Column order: preserve `fields` when supplied; else sorted union.
    fixed_fields = list(fields) if fields is not None else None
    return _rows_to_column_dict(norm_rows, fields=fixed_fields)


async def aiter_beanie(
    document_or_query: Any,
    *,
    criteria: Any | None = None,
    batch_size: int = 1000,
    projection_model: type[Any] | None = None,
    fields: Sequence[str] | None = None,
    fetch_links: bool = False,
    nesting_depth: int | None = None,
    nesting_depths_per_field: Mapping[str, int] | None = None,
    flatten: bool = True,
    id_column: Literal["id", "_id"] = "id",
) -> AsyncIterator[dict[str, list[Any]]]:
    """Yield rectangular column-dict batches from Beanie results."""
    _require_beanie()
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    # Build query first, then iterate.
    query = document_or_query
    doc_cls: type[Any] | None = None
    if isinstance(document_or_query, type) and hasattr(document_or_query, "find"):
        doc_cls = document_or_query
        if criteria is None:
            find_all = getattr(doc_cls, "find_all", None)
            if callable(find_all):
                query = find_all(fetch_links=fetch_links)
            else:
                query = doc_cls.find({}, fetch_links=fetch_links)
        else:
            query = doc_cls.find(criteria, fetch_links=fetch_links)

        if nesting_depth is not None:
            nd = getattr(query, "nesting_depth", None)
            if callable(nd):
                query = nd(nesting_depth)
        if nesting_depths_per_field is not None:
            ndpf = getattr(query, "nesting_depths_per_field", None)
            if callable(ndpf):
                query = ndpf(dict(nesting_depths_per_field))

    if projection_model is not None and fields is not None:
        raise TypeError("Pass only one of projection_model= or fields=, not both.")
    if fields is not None:
        projection_model = _projection_model_for_fields(
            doc_cls or type("Doc", (), {}), fields
        )
    if projection_model is not None:
        project = getattr(query, "project", None)
        if not callable(project):
            raise TypeError("Query object does not support Beanie-style .project(...).")
        query = project(projection_model)

    batch: list[dict[str, Any]] = []
    async for obj in query:
        row = _pydantic_model_to_dict(obj)
        row = _normalize_id_keys(row, id_column=id_column)
        if flatten:
            row = _flatten_dict(row)
        batch.append(row)
        if len(batch) >= batch_size:
            fixed_fields = list(fields) if fields is not None else None
            yield _rows_to_column_dict(batch, fields=fixed_fields)
            batch = []
    if batch:
        fixed_fields = list(fields) if fields is not None else None
        yield _rows_to_column_dict(batch, fields=fixed_fields)


@dataclass(frozen=True, slots=True)
class BeanieWriteOptions:
    """Options that control ODM-aware write behavior."""

    validate_on_save: bool | None = None
    skip_actions: Sequence[Any] | None = None
    link_rule: Any | None = None


async def awrite_beanie(
    document_cls: type[Any],
    data: dict[str, list[Any]],
    *,
    ordered: bool = True,
    chunk_size: int | None = None,
    options: BeanieWriteOptions | None = None,
) -> int:
    """Insert documents via Beanie so validate_on_save/actions can run.

    This is intentionally **not** a high-throughput bulk insert helper; if you want raw
    speed and are ok bypassing ODM hooks, use :func:`pydantable.write_mongo` /
    :func:`pydantable.io.write_mongo` instead.
    """
    _require_beanie()
    if not data:
        return 0

    # Local import to avoid a cycle.
    from pydantable.io.batches import ensure_rectangular
    from pydantable.io.sql import _write_chunk_size

    ensure_rectangular(data)
    n = len(next(iter(data.values())))
    if n == 0:
        return 0

    keys = list(data.keys())
    chunk_n = _write_chunk_size(chunk_size)
    total = 0
    opt = options or BeanieWriteOptions()

    # ODM-aware, per-document inserts. `ordered` is best-effort here: we stop on first
    # error when ordered=True; continue when ordered=False.
    for start in range(0, n, chunk_n):
        end = min(start + chunk_n, n)
        for i in range(start, end):
            row = {k: data[k][i] for k in keys}
            try:
                doc = document_cls(**row)
                insert = getattr(doc, "insert", None)
                if not callable(insert):
                    raise TypeError("Beanie document instances must support .insert().")
                kw: dict[str, Any] = {}
                if opt.skip_actions is not None:
                    kw["skip_actions"] = list(opt.skip_actions)
                if opt.link_rule is not None:
                    kw["link_rule"] = opt.link_rule
                # validate_on_save is a document Settings flag; keep this knob for
                # future expansion, but do not attempt to override Settings today.
                _ = opt.validate_on_save
                await insert(**kw)
                total += 1
            except Exception:
                if ordered:
                    raise
                continue
    return total


__all__ = [
    "BeanieWriteOptions",
    "afetch_beanie",
    "aiter_beanie",
    "awrite_beanie",
]
