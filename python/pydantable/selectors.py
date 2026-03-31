from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

_PY_INT = int
_PY_FLOAT = float


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    # Python 3.10+: Optional[T] is Union[T, NoneType]
    if origin is type(None):  # pragma: no cover
        return annotation
    if origin is getattr(__import__("types"), "UnionType", object()):  # pragma: no cover
        return annotation
    if origin is getattr(__import__("typing"), "Union", object()) or str(origin).endswith("types.UnionType"):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_list_annotation(annotation: Any) -> bool:
    origin = get_origin(annotation)
    return origin in (list, tuple, set)  # schema-first containers we expose


def _is_struct_annotation(annotation: Any) -> bool:
    ann = _unwrap_optional(annotation)
    if not isinstance(ann, type):
        return False
    try:
        return issubclass(ann, BaseModel)
    except TypeError:  # pragma: no cover
        return False


@dataclass(frozen=True, slots=True)
class Selector:
    """
    Schema-driven column selector.

    A Selector resolves *only* against a DataFrame's current schema. This keeps the
    DSL deterministic and compatible with schema-first typing.
    """

    _resolver: Callable[[Mapping[str, Any]], set[str]]
    _repr: str | None = None

    def __repr__(self) -> str:  # pragma: no cover
        return self._repr or "Selector(<resolver>)"

    def resolve(self, schema_field_types: Mapping[str, Any]) -> list[str]:
        selected = self._resolver(schema_field_types)
        return [name for name in schema_field_types.keys() if name in selected]

    def exclude(self, other: Selector | str | Iterable[str]) -> Selector:
        return self - other

    def __or__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        rep = (
            f"({self!r} | {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(lambda schema: self._resolver(schema) | o._resolver(schema), rep)

    def __and__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        rep = (
            f"({self!r} & {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(lambda schema: self._resolver(schema) & o._resolver(schema), rep)

    def __sub__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        rep = (
            f"({self!r} - {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(lambda schema: self._resolver(schema) - o._resolver(schema), rep)

    def __invert__(self) -> Selector:
        rep = f"(~{self!r})" if self._repr is not None else None
        return Selector(lambda schema: set(schema.keys()) - self._resolver(schema), rep)


def _as_selector(obj: Selector | str | Iterable[str]) -> Selector:
    if isinstance(obj, Selector):
        return obj
    if isinstance(obj, str):
        names = (obj,)
    else:
        names = tuple(obj)
    return by_name(*names)


def everything() -> Selector:
    return Selector(lambda schema: set(schema.keys()), "everything()")


def all() -> Selector:  # noqa: A001 - intentional parity name
    return everything()


def by_name(*names: str) -> Selector:
    wanted = {str(n) for n in names}
    rep = f"by_name({', '.join(repr(n) for n in names)})"
    return Selector(lambda schema: {n for n in schema.keys() if n in wanted}, rep)


def starts_with(prefix: str) -> Selector:
    p = str(prefix)
    return Selector(
        lambda schema: {n for n in schema.keys() if n.startswith(p)},
        f"starts_with({p!r})",
    )


def ends_with(suffix: str) -> Selector:
    s = str(suffix)
    return Selector(
        lambda schema: {n for n in schema.keys() if n.endswith(s)},
        f"ends_with({s!r})",
    )


def contains(substr: str) -> Selector:
    sub = str(substr)
    return Selector(
        lambda schema: {n for n in schema.keys() if sub in n},
        f"contains({sub!r})",
    )


def matches(pattern: str | re.Pattern[str]) -> Selector:
    rx = re.compile(pattern) if isinstance(pattern, str) else pattern
    return Selector(
        lambda schema: {n for n in schema.keys() if rx.search(n) is not None},
        f"matches({rx.pattern!r})",
    )


class _DTypeGroup:
    def __init__(self, name: str, predicate: Callable[[Any], bool]) -> None:
        self._name = name
        self._predicate = predicate

    def __repr__(self) -> str:  # pragma: no cover
        return f"<selectors.{self._name}>"

    def match(self, annotation: Any) -> bool:
        return self._predicate(annotation)


NUMERIC = _DTypeGroup(
    "NUMERIC",
    lambda ann: _unwrap_optional(ann) in (_PY_INT, _PY_FLOAT, Decimal),
)
INTEGERS = _DTypeGroup("INTEGERS", lambda ann: _unwrap_optional(ann) is _PY_INT)
FLOATS = _DTypeGroup("FLOATS", lambda ann: _unwrap_optional(ann) is _PY_FLOAT)
DECIMALS = _DTypeGroup("DECIMALS", lambda ann: _unwrap_optional(ann) is Decimal)
STRING = _DTypeGroup("STRING", lambda ann: _unwrap_optional(ann) is str)
BOOLEAN = _DTypeGroup("BOOLEAN", lambda ann: _unwrap_optional(ann) is bool)
TEMPORAL = _DTypeGroup(
    "TEMPORAL",
    lambda ann: _unwrap_optional(ann) in (date, datetime, time, timedelta),
)
LIST = _DTypeGroup("LIST", lambda ann: _is_list_annotation(_unwrap_optional(ann)))
STRUCT = _DTypeGroup("STRUCT", lambda ann: _is_struct_annotation(ann))
UUIDS = _DTypeGroup("UUID", lambda ann: _unwrap_optional(ann) is UUID)


def by_dtype(*dtypes: Any) -> Selector:
    requested = tuple(dtypes)

    def _matches_any(annotation: Any) -> bool:
        for d in requested:
            if isinstance(d, _DTypeGroup):
                if d.match(annotation):
                    return True
            else:
                if _unwrap_optional(annotation) is d:
                    return True
        return False

    rep_parts: list[str] = []
    for d in requested:
        if isinstance(d, _DTypeGroup):
            rep_parts.append(repr(d))
        else:
            rep_parts.append(getattr(d, "__name__", repr(d)))
    rep = f"by_dtype({', '.join(rep_parts)})"
    return Selector(
        lambda schema: {name for name, ann in schema.items() if _matches_any(ann)},
        rep,
    )


def numeric() -> Selector:
    return by_dtype(NUMERIC)


def integers() -> Selector:
    return by_dtype(INTEGERS)


def integer() -> Selector:
    return integers()


def floats() -> Selector:
    return by_dtype(FLOATS)


def float() -> Selector:  # noqa: A001 - intentional parity name
    return floats()


def decimals() -> Selector:
    return by_dtype(DECIMALS)


def decimal() -> Selector:
    return decimals()


def string() -> Selector:
    return by_dtype(STRING)


def boolean() -> Selector:
    return by_dtype(BOOLEAN)


def temporal() -> Selector:
    return by_dtype(TEMPORAL)


def lists() -> Selector:
    return by_dtype(LIST)


def structs() -> Selector:
    return by_dtype(STRUCT)


def struct() -> Selector:
    return structs()


def uuids() -> Selector:
    return by_dtype(UUIDS)

