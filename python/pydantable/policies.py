"""Helpers for reading pydantable-namespaced field metadata from Pydantic models.

Phase 1 scope (see docs/PYDANTIC_ROADMAP.md):
- Shallow, top-level field policies only.
- Policies are read from ``FieldInfo.json_schema_extra["pydantable"]`` when present.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from pydantic import BaseModel

Strictness = Literal["inherit", "coerce", "strict", "off"]


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


def _as_strictness(v: Any) -> Strictness | None:
    if v in ("inherit", "coerce", "strict", "off"):
        return v
    return None


def resolve_column_strictness(
    model: type[BaseModel],
    name: str,
    *,
    column_default: Strictness = "coerce",
    nested_default: Strictness = "inherit",
) -> tuple[Strictness, Strictness]:
    """
    Resolve (strictness, nested_strictness) for one top-level field.

    - Field policy overrides win.
    - `inherit` falls back to provided defaults.
    """
    p = column_policy(model, name)
    s = _as_strictness(p.get("strictness")) or "inherit"
    ns = _as_strictness(p.get("nested_strictness")) or "inherit"

    if s == "inherit":
        s = column_default
    if ns == "inherit":
        ns = nested_default
    return s, ns


__all__ = [
    "Strictness",
    "column_policies",
    "column_policy",
    "resolve_column_strictness",
]
