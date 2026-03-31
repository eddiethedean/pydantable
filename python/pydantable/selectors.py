from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, get_args, get_origin
from uuid import UUID


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


@dataclass(frozen=True, slots=True)
class Selector:
    """
    Schema-driven column selector.

    A Selector resolves *only* against a DataFrame's current schema. This keeps the
    DSL deterministic and compatible with schema-first typing.
    """

    _resolver: Callable[[Mapping[str, Any]], set[str]]

    def resolve(self, schema_field_types: Mapping[str, Any]) -> list[str]:
        selected = self._resolver(schema_field_types)
        return [name for name in schema_field_types.keys() if name in selected]

    def exclude(self, other: Selector | str | Iterable[str]) -> Selector:
        return self - other

    def __or__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        return Selector(lambda schema: self._resolver(schema) | o._resolver(schema))

    def __and__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        return Selector(lambda schema: self._resolver(schema) & o._resolver(schema))

    def __sub__(self, other: Selector | str | Iterable[str]) -> Selector:
        o = _as_selector(other)
        return Selector(lambda schema: self._resolver(schema) - o._resolver(schema))

    def __invert__(self) -> Selector:
        return Selector(lambda schema: set(schema.keys()) - self._resolver(schema))


def _as_selector(obj: Selector | str | Iterable[str]) -> Selector:
    if isinstance(obj, Selector):
        return obj
    if isinstance(obj, str):
        names = (obj,)
    else:
        names = tuple(obj)
    return by_name(*names)


def everything() -> Selector:
    return Selector(lambda schema: set(schema.keys()))


def all() -> Selector:  # noqa: A001 - intentional parity name
    return everything()


def by_name(*names: str) -> Selector:
    wanted = {str(n) for n in names}
    return Selector(lambda schema: {n for n in schema.keys() if n in wanted})


def starts_with(prefix: str) -> Selector:
    p = str(prefix)
    return Selector(lambda schema: {n for n in schema.keys() if n.startswith(p)})


def ends_with(suffix: str) -> Selector:
    s = str(suffix)
    return Selector(lambda schema: {n for n in schema.keys() if n.endswith(s)})


def contains(substr: str) -> Selector:
    sub = str(substr)
    return Selector(lambda schema: {n for n in schema.keys() if sub in n})


def matches(pattern: str | re.Pattern[str]) -> Selector:
    rx = re.compile(pattern) if isinstance(pattern, str) else pattern
    return Selector(lambda schema: {n for n in schema.keys() if rx.search(n) is not None})


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
    lambda ann: _unwrap_optional(ann) in (int, float, Decimal),
)
STRING = _DTypeGroup("STRING", lambda ann: _unwrap_optional(ann) is str)
BOOLEAN = _DTypeGroup("BOOLEAN", lambda ann: _unwrap_optional(ann) is bool)
TEMPORAL = _DTypeGroup(
    "TEMPORAL",
    lambda ann: _unwrap_optional(ann) in (date, datetime, time, timedelta),
)
LIST = _DTypeGroup("LIST", lambda ann: _is_list_annotation(_unwrap_optional(ann)))
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

    return Selector(
        lambda schema: {name for name, ann in schema.items() if _matches_any(ann)}
    )


def numeric() -> Selector:
    return by_dtype(NUMERIC)


def string() -> Selector:
    return by_dtype(STRING)


def boolean() -> Selector:
    return by_dtype(BOOLEAN)


def temporal() -> Selector:
    return by_dtype(TEMPORAL)


def lists() -> Selector:
    return by_dtype(LIST)


def uuids() -> Selector:
    return by_dtype(UUIDS)

