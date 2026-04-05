"""Schema annotation helpers for :meth:`DataFrame.info`, :meth:`describe`, and repr."""

from __future__ import annotations

import types
from typing import Any, Literal, Union, get_args, get_origin

from pydantable.schema import _annotation_nullable_inner

_NoneType = type(None)
_UNION_ORIGINS = (types.UnionType, Union)


def _is_describe_numeric(annotation: Any) -> bool:
    inner, _ = _annotation_nullable_inner(annotation)
    return inner is int or inner is float


def _is_describe_bool(annotation: Any) -> bool:
    inner, _ = _annotation_nullable_inner(annotation)
    return inner is bool


def _is_describe_str(annotation: Any) -> bool:
    inner, _ = _annotation_nullable_inner(annotation)
    return inner is str


def _is_describe_temporal(annotation: Any) -> bool:
    from datetime import date, datetime

    inner, _ = _annotation_nullable_inner(annotation)
    return inner is date or inner is datetime


def _dtype_repr(annotation: Any) -> str:
    """Stable, readable dtype string for schema annotations (repr / logging)."""
    from ._repr_display import _REPR_DTYPE_MAX_LEN

    if annotation is None:
        return "Any"
    if isinstance(annotation, type):
        if annotation is _NoneType:
            return "None"
        # Py 3.9–3.10: ``list[int]`` is both ``isinstance(..., type)`` and a
        # ``types.GenericAlias``; it must use get_origin/get_args below, not ``__qualname__``.
        if get_origin(annotation) is None and not get_args(annotation):
            return getattr(annotation, "__qualname__", annotation.__name__)

    args = get_args(annotation)
    origin = get_origin(annotation)

    if origin is Literal:
        inner = ", ".join(repr(a) for a in args)
        return f"Literal[{inner}]"

    if origin is not None and origin in _UNION_ORIGINS:
        if (
            len(args) == 2
            and _NoneType in args
            and not all(a is _NoneType for a in args)
        ):
            other = args[0] if args[1] is _NoneType else args[1]
            return f"{_dtype_repr(other)} | None"
        return " | ".join(_dtype_repr(a) for a in args)

    if origin is not None:
        oname = getattr(
            origin, "__qualname__", getattr(origin, "__name__", repr(origin))
        )
        if args:
            inner = ", ".join(_dtype_repr(a) for a in args)
            return f"{oname}[{inner}]"
        return oname

    s = repr(annotation)
    if len(s) > _REPR_DTYPE_MAX_LEN:
        return f"{s[: _REPR_DTYPE_MAX_LEN - 1]}…"
    return s
