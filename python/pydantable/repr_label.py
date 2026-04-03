"""Compact single-line labels for lazy/async DataFrameModel reprs."""

from __future__ import annotations

_REPR_LABEL_MAX = 200


def short_repr_label(text: str, *, max_len: int = _REPR_LABEL_MAX) -> str:
    """Collapse whitespace and truncate for one-line ``repr`` labels."""

    collapsed = " ".join(text.split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3] + "..."
