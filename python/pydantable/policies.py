"""Helpers for reading pydantable-namespaced field metadata from Pydantic models.

Phase 1 scope (see docs/PYDANTIC_ROADMAP.md):
- Shallow, top-level field policies only.
- Policies are read from ``FieldInfo.json_schema_extra["pydantable"]`` when present.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def _pydantable_extra(field: Any) -> dict[str, Any]:
    extra = getattr(field, "json_schema_extra", None)
    if not isinstance(extra, dict):
        return {}
    v = extra.get("pydantable")
    return dict(v) if isinstance(v, dict) else {}


def column_policies(model: type[BaseModel]) -> dict[str, dict[str, Any]]:
    """Return per-field policy dicts for ``model`` (top-level only)."""
    out: dict[str, dict[str, Any]] = {}
    for name, finfo in model.model_fields.items():
        p = _pydantable_extra(finfo)
        if p:
            out[name] = p
    return out


def column_policy(model: type[BaseModel], name: str) -> dict[str, Any]:
    """Return the policy dict for one field name (empty if absent)."""
    finfo = model.model_fields.get(name)
    if finfo is None:
        raise KeyError(name)
    return _pydantable_extra(finfo)


__all__ = ["column_policies", "column_policy"]

