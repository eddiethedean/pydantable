"""Polars engine streaming flag resolution (env + per-call overrides)."""

from __future__ import annotations

import os
from typing import Union, get_args, get_origin

_ENGINE_STREAMING_ENV = "PYDANTABLE_ENGINE_STREAMING"

_NoneType = type(None)


def _resolve_engine_streaming(
    *,
    streaming: bool | None = None,
    engine_streaming: bool | None = None,
    default: bool | None = None,
) -> bool:
    """Resolve Polars collect engine streaming flag.

    Resolution order:
    - explicit `engine_streaming=` (preferred alias)
    - explicit `streaming=` (legacy name used throughout the API)
    - `default=` (per-object default, e.g. set on scan roots)
    - env `PYDANTABLE_ENGINE_STREAMING` (truthy: 1/true/yes)
    """
    if streaming is not None and engine_streaming is not None:
        raise TypeError("Pass either streaming= or engine_streaming=, not both.")
    explicit = engine_streaming if engine_streaming is not None else streaming
    if explicit is not None:
        return bool(explicit)
    if default is not None:
        return bool(default)
    v = os.environ.get(_ENGINE_STREAMING_ENV, "").strip().lower()
    return v in ("1", "true", "yes")


def _is_bool_or_nullable_bool(dtype: object) -> bool:
    """True if ``dtype`` is ``bool`` or optional bool (``| None`` / ``Union``)."""
    if dtype is bool:
        return True
    origin = get_origin(dtype)
    if origin is Union:
        args = tuple(get_args(dtype))
        if len(args) == 2 and _NoneType in args and bool in args:
            return True
    return False
