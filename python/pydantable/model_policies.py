"""Model-level pydantable policy helpers (Phase 2).

Policies are configured via an optional class attribute ``__pydantable__`` on
`DataFrameModel` subclasses. Values are treated as **data**, not code:
- Unknown keys are tolerated (forward compatible).
- Policies are merged by walking the MRO from base to subclass.
"""

from __future__ import annotations

from typing import Any


def _as_policy_dict(v: object) -> dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    return {}


def merged_model_policy(model_cls: type) -> dict[str, Any]:
    """
    Return merged ``__pydantable__`` policy dict for ``model_cls``.

    Merge semantics:
    - Later (more-derived) classes override earlier keys.
    - Values are treated as opaque JSON-like data; we do not deep-merge nested dicts
      in Phase 2 (callers can do that for specific keys if desired).
    """
    out: dict[str, Any] = {}
    for base in reversed(getattr(model_cls, "__mro__", ())):
        if base is object:
            continue
        v = getattr(base, "__pydantable__", None)
        out.update(_as_policy_dict(v))
    return out


def model_policy_value(model_cls: type, key: str, default: Any = None) -> Any:
    return merged_model_policy(model_cls).get(key, default)


__all__ = ["merged_model_policy", "model_policy_value"]
