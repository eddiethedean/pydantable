"""Schema-driven column selection for :meth:`DataFrame.select_schema`.

Construct :class:`Selector` values with factories such as :func:`everything`,
:func:`by_name`, :func:`by_dtype`, and dtype groups (:data:`NUMERIC`,
:data:`STRUCT`, ...). Combine selectors with ``|`` (union), ``&`` (intersection),
``-`` (difference), and ``~`` (complement). Resolution uses only the current
column name → annotation mapping.

See the **SELECTORS** documentation page.
"""

from __future__ import annotations

import enum
import ipaddress
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

_PY_INT = int
_PY_FLOAT = float


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    # Python 3.10+: Optional[T] is Union[T, NoneType]
    if origin is type(None):  # pragma: no cover
        return annotation
    if origin is getattr(
        __import__("types"), "UnionType", object()
    ):  # pragma: no cover
        return annotation
    if origin is getattr(__import__("typing"), "Union", object()) or str(
        origin
    ).endswith("types.UnionType"):
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


def _is_map_annotation(annotation: Any) -> bool:
    ann = _unwrap_optional(annotation)
    origin = get_origin(ann)
    if origin is not dict:
        return False
    args = get_args(ann)
    return len(args) == 2 and args[0] is str


def _is_enum_annotation(annotation: Any) -> bool:
    ann = _unwrap_optional(annotation)
    if not isinstance(ann, type):
        return False
    try:
        return issubclass(ann, enum.Enum) and ann is not enum.Enum
    except TypeError:  # pragma: no cover
        return False


def _is_wkb_annotation(annotation: Any) -> bool:
    ann = _unwrap_optional(annotation)
    return (
        isinstance(ann, type)
        and ann.__name__ == "WKB"
        and getattr(ann, "__module__", "") == "pydantable.types"
    )


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
        """Return matching column names in *schema* iteration order."""
        selected = self._resolver(schema_field_types)
        return [name for name in schema_field_types if name in selected]

    def exclude(self, other: Selector | str | Iterable[str]) -> Selector:
        """Equivalent to ``self - other`` (remove columns matched by ``other``)."""
        return self - other

    def __or__(self, other: Selector | str | Iterable[str]) -> Selector:
        """Union of the two selectors' column sets."""
        o = _as_selector(other)
        rep = (
            f"({self!r} | {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(
            lambda schema: self._resolver(schema) | o._resolver(schema), rep
        )

    def __and__(self, other: Selector | str | Iterable[str]) -> Selector:
        """Intersection of the two selectors' column sets."""
        o = _as_selector(other)
        rep = (
            f"({self!r} & {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(
            lambda schema: self._resolver(schema) & o._resolver(schema), rep
        )

    def __sub__(self, other: Selector | str | Iterable[str]) -> Selector:
        """Set difference: columns in ``self`` that are not in ``other``."""
        o = _as_selector(other)
        rep = (
            f"({self!r} - {o!r})"
            if self._repr is not None and o._repr is not None
            else None
        )
        return Selector(
            lambda schema: self._resolver(schema) - o._resolver(schema), rep
        )

    def __invert__(self) -> Selector:
        """Complement: all schema columns not matched by ``self``."""
        rep = f"(~{self!r})" if self._repr is not None else None
        return Selector(lambda schema: set(schema) - self._resolver(schema), rep)


def _as_selector(obj: Selector | str | Iterable[str]) -> Selector:
    if isinstance(obj, Selector):
        return obj
    names: tuple[str, ...]
    names = (obj,) if isinstance(obj, str) else tuple(obj)
    return by_name(*names)


def everything() -> Selector:
    """Select every column present in the schema mapping."""
    return Selector(lambda schema: set(schema), "everything()")


def all() -> Selector:
    """Alias for :func:`everything` (spelled ``all`` for readability in pipelines)."""
    return everything()


def by_name(*names: str) -> Selector:
    """Select columns whose names appear in ``names``."""
    wanted = {str(n) for n in names}
    rep = f"by_name({', '.join(repr(n) for n in names)})"
    return Selector(lambda schema: {n for n in schema if n in wanted}, rep)


def starts_with(prefix: str) -> Selector:
    """Select columns whose names start with ``prefix``."""
    p = str(prefix)
    return Selector(
        lambda schema: {n for n in schema if n.startswith(p)},
        f"starts_with({p!r})",
    )


def ends_with(suffix: str) -> Selector:
    """Select columns whose names end with ``suffix``."""
    s = str(suffix)
    return Selector(
        lambda schema: {n for n in schema if n.endswith(s)},
        f"ends_with({s!r})",
    )


def contains(substr: str) -> Selector:
    """Select columns whose names contain the substring ``substr``."""
    sub = str(substr)
    return Selector(
        lambda schema: {n for n in schema if sub in n},
        f"contains({sub!r})",
    )


def matches(pattern: str | re.Pattern[str]) -> Selector:
    """Select columns whose names match the regex ``pattern`` (``search`` semantics)."""
    rx = re.compile(pattern) if isinstance(pattern, str) else pattern
    return Selector(
        lambda schema: {n for n in schema if rx.search(n) is not None},
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
BINARIES = _DTypeGroup("BINARIES", lambda ann: _unwrap_optional(ann) is bytes)
MAPS = _DTypeGroup("MAPS", lambda ann: _is_map_annotation(ann))
ENUMS = _DTypeGroup("ENUMS", lambda ann: _is_enum_annotation(ann))
IPV4S = _DTypeGroup("IPV4S", lambda ann: _unwrap_optional(ann) is ipaddress.IPv4Address)
IPV6S = _DTypeGroup("IPV6S", lambda ann: _unwrap_optional(ann) is ipaddress.IPv6Address)
WKBS = _DTypeGroup("WKBS", lambda ann: _is_wkb_annotation(ann))


def by_dtype(*dtypes: Any) -> Selector:
    """Select columns whose annotations match any of ``dtypes``.

    Pass concrete Python types (``int``, ``str``, …) or dtype groups such as
    :data:`NUMERIC`, :data:`STRUCT`, :data:`MAPS`.
    """
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
    """Select int, float, and :class:`decimal.Decimal` columns."""
    return by_dtype(NUMERIC)


def integers() -> Selector:
    """Select ``int`` columns."""
    return by_dtype(INTEGERS)


def integer() -> Selector:
    """Alias for :func:`integers`."""
    return integers()


def floats() -> Selector:
    """Select ``float`` columns."""
    return by_dtype(FLOATS)


def float() -> Selector:
    """Alias for :func:`floats`."""
    return floats()


def decimals() -> Selector:
    """Select :class:`decimal.Decimal` columns."""
    return by_dtype(DECIMALS)


def decimal() -> Selector:
    """Alias for :func:`decimals`."""
    return decimals()


def string() -> Selector:
    """Select ``str`` columns."""
    return by_dtype(STRING)


def boolean() -> Selector:
    """Select ``bool`` columns."""
    return by_dtype(BOOLEAN)


def temporal() -> Selector:
    """Select ``date``, ``datetime``, ``time``, or ``timedelta`` columns."""
    return by_dtype(TEMPORAL)


def lists() -> Selector:
    """Select list/tuple/set-typed columns."""
    return by_dtype(LIST)


def structs() -> Selector:
    """Select nested Pydantic :class:`~pydantic.BaseModel` columns."""
    return by_dtype(STRUCT)


def struct() -> Selector:
    """Alias for :func:`structs`."""
    return structs()


def uuids() -> Selector:
    """Select :class:`uuid.UUID` columns."""
    return by_dtype(UUIDS)


def binary() -> Selector:
    """Select raw ``bytes`` columns (use :func:`wkbs` for WKB geometry)."""
    return by_dtype(BINARIES)


def maps() -> Selector:
    """Select ``dict[str, T]`` columns."""
    return by_dtype(MAPS)


def enums() -> Selector:
    """Select :class:`enum.Enum` subclass columns."""
    return by_dtype(ENUMS)


def ipv4s() -> Selector:
    """Select :class:`ipaddress.IPv4Address` columns."""
    return by_dtype(IPV4S)


def ipv6s() -> Selector:
    """Select :class:`ipaddress.IPv6Address` columns."""
    return by_dtype(IPV6S)


def wkbs() -> Selector:
    """Select :class:`~pydantable.types.WKB` (well-known binary) columns."""
    return by_dtype(WKBS)


def rename_map(
    selector: Selector, fn: Callable[[str], str]
) -> Callable[[Mapping[str, Any]], dict[str, str]]:
    """Build a rename mapping from a selector and renaming function (schema-driven)."""
    if not isinstance(selector, Selector):
        raise TypeError("rename_map(selector, fn) expects a Selector.")
    if not callable(fn):
        raise TypeError("rename_map(selector, fn) expects a callable.")

    def _mk(schema_field_types: Mapping[str, Any]) -> dict[str, str]:
        cols = selector.resolve(schema_field_types)
        if not cols:
            available = ", ".join(repr(c) for c in schema_field_types)
            raise ValueError(
                f"rename_map({selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        mapping = {c: str(fn(c)) for c in cols}
        if len(set(mapping.values())) != len(mapping):
            raise ValueError("rename_map(...) produced duplicate output column names.")
        return mapping

    return _mk
