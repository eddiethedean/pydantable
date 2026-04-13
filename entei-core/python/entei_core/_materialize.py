"""Materialize MongoDB collections to pydantable column dicts."""

from __future__ import annotations

from typing import Any

from entei_core.mongo_root import MongoRoot


def mongo_root_to_column_dict(root: MongoRoot) -> dict[str, list[Any]]:
    coll = root.collection
    cursor = coll.find()
    docs: list[dict[str, Any]] = list(cursor)
    if not docs:
        keys = list(root.fields) if root.fields else []
        return {k: [] for k in keys}

    if root.fields:
        keys = list(root.fields)
    else:
        key_set: set[str] = set()
        for d in docs:
            key_set.update(d.keys())
        keys = sorted(key_set)

    out: dict[str, list[Any]] = {k: [] for k in keys}
    for d in docs:
        for k in keys:
            out[k].append(d.get(k))
    return out


def materialize_root_data(data: Any) -> Any:
    """If ``data`` is :class:`MongoRoot`, return columnar dict; else pass through."""
    if isinstance(data, MongoRoot):
        return mongo_root_to_column_dict(data)
    return data
