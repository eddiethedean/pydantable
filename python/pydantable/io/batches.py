"""Helpers for validating and concatenating rectangular column batches."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator


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


def iter_chain_batches(
    paths: Iterable[Path | str],
    iter_one: Callable[[Path], Iterator[dict[str, list[Any]]]],
) -> Iterator[dict[str, list[Any]]]:
    """
    Yield batches from several files by chaining per-file iterators.

    ``iter_one`` is typically ``lambda p: iter_parquet(p)`` (or another ``iter_*``).
    This does not merge schemas across files; callers must ensure compatible layouts.
    """
    for p in paths:
        yield from iter_one(Path(p))


def iter_concat_batches(
    batches: Iterator[dict[str, list[Any]]],
) -> dict[str, list[Any]]:
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
