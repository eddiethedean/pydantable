"""Columnar rehydration: enum coercion and row model materialization."""

from __future__ import annotations

import enum
from typing import Any, get_origin

from pydantic import BaseModel, TypeAdapter

from pydantable.schema import _annotation_nullable_inner


def _coerce_enum_columns(
    data: dict[str, list[Any]],
    field_types: dict[str, Any],
) -> dict[str, list[Any]]:
    """Rehydrate Rust Utf8 enum cells into concrete ``enum.Enum`` field types."""
    if not data or not field_types:
        return data
    out = dict(data)
    for name, ann in field_types.items():
        if name not in out:
            continue
        inner, _nullable = _annotation_nullable_inner(ann)
        origin = get_origin(inner)
        if origin is list:
            continue
        if not isinstance(inner, type):
            continue
        if issubclass(inner, BaseModel):
            continue
        if not (issubclass(inner, enum.Enum) and inner is not enum.Enum):
            continue
        adapter = TypeAdapter(ann)
        out[name] = [adapter.validate_python(v) for v in out[name]]
    return out


def _rows_from_column_dict(
    data: dict[str, list[Any]], row_type: type[BaseModel]
) -> list[BaseModel]:
    """Build validated row models from aligned column lists (same length per column)."""
    if not data:
        return []
    n = len(next(iter(data.values())))
    out: list[BaseModel] = []
    for i in range(n):
        row_dict = {name: col[i] for name, col in data.items()}
        out.append(row_type.model_validate(row_dict))
    return out
