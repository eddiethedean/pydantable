"""Structured ingest error payloads (Phase 2).

These are intended for service boundaries where batch ingest may accept partial
success (e.g. `ignore_errors=True`) and the caller needs machine-readable details.
"""

from __future__ import annotations

from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field


class IngestRowFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_index: int = Field(..., description="Index of the failing row in the input.")
    row: dict[str, Any] = Field(..., description="Best-effort row dict for debugging.")
    errors: list[dict[str, Any]] = Field(
        ..., description="Pydantic-style error entries (ValidationError.errors())."
    )


class IngestValidationErrorDetail(BaseModel):
    """Top-level error payload for a batch ingest operation."""

    model_config = ConfigDict(extra="forbid")

    title: str = "Ingest validation failed"
    failures: list[IngestRowFailure]


def coerce_validation_failures(obj: Any) -> list[IngestRowFailure]:
    """
    Coerce a failures payload to a list of `IngestRowFailure`.

    Accepts:
    - list[dict] shaped like `{row_index, row, errors}`
    - list[IngestRowFailure]
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        out: list[IngestRowFailure] = []
        for item in obj:
            if isinstance(item, IngestRowFailure):
                out.append(item)
            else:
                out.append(IngestRowFailure.model_validate(item))
        return out
    raise TypeError(
        "validation failures must be a list of dicts or IngestRowFailure instances"
    )


__all__ = [
    "IngestRowFailure",
    "IngestValidationErrorDetail",
    "coerce_validation_failures",
]

