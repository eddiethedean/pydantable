from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def ensure_rectangular(batch: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """
    Validate that `batch` is a rectangular column dict (all columns same length).

    Returns the input batch for convenience.
    """
    if not batch:
        return batch
    lengths = {len(v) for v in batch.values()}
    if len(lengths) != 1:
        raise ValueError("batch columns must have the same length")
    return batch


def iter_concat_batches(batches: Iterator[dict[str, list[Any]]]) -> dict[str, list[Any]]:
    """
    Concatenate an iterator of rectangular column batches into one column dict.

    Missing columns in later batches are treated as empty for that batch.
    """
    out: dict[str, list[Any]] = {}
    keys: list[str] | None = None
    for batch in batches:
        if not batch:
            continue
        ensure_rectangular(batch)
        if keys is None:
            keys = list(batch.keys())
            out = {k: [] for k in keys}
        for k in keys:
            out[k].extend(batch.get(k, []))
    return out

