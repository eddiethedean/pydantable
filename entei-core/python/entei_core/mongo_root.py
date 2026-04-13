"""Typed carrier for pymongo collection roots for ``EnteiPydantableEngine``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MongoRoot:
    """Reference to a MongoDB collection as lazy root data for pydantable.

    The engine materializes documents to columnar ``dict[str, list]`` (top-level
    fields only in this version) before delegating plan execution to the native
    Polars/Rust core.

    Parameters
    ----------
    collection:
        A :class:`pymongo.collection.Collection` (or compatible, e.g. mongomock).
    fields:
        Optional ordered column list. If omitted, keys are the union of top-level
        field names across all documents (sorted for stability). Empty collection
        requires ``fields`` to produce empty columns.
    """

    collection: Any
    fields: tuple[str, ...] | None = None
