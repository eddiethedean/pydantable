"""Registry for optional I/O integrations (readers and writers).

Third-party or experimental formats register callables with :func:`register_reader` /
:func:`register_writer`. Core I/O in :mod:`pydantable.io` uses the same mechanism.

``stable=True`` marks APIs intended as long-lived; unset or ``False`` indicates
experimental or internal surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class PluginFn:
    """Metadata for a registered reader or writer callable."""

    name: str
    fn: Callable[..., Any]
    kind: str  # "reader" | "writer"
    requires_extra: str | None = None
    stable: bool = False


_READERS: dict[str, PluginFn] = {}
_WRITERS: dict[str, PluginFn] = {}


def register_reader(
    name: str,
    fn: Callable[..., Any],
    *,
    requires_extra: str | None = None,
    stable: bool = False,
) -> None:
    """Register a named reader; ``requires_extra`` names a ``pip`` extra if needed."""
    _READERS[name] = PluginFn(
        name=name, fn=fn, kind="reader", requires_extra=requires_extra, stable=stable
    )


def register_writer(
    name: str,
    fn: Callable[..., Any],
    *,
    requires_extra: str | None = None,
    stable: bool = False,
) -> None:
    """Register a named writer; ``requires_extra`` names a ``pip`` extra if needed."""
    _WRITERS[name] = PluginFn(
        name=name, fn=fn, kind="writer", requires_extra=requires_extra, stable=stable
    )


def get_reader(name: str) -> Callable[..., Any]:
    """Return the registered reader callable or raise :exc:`ValueError` if unknown."""
    try:
        return _READERS[name].fn
    except KeyError:
        known = ", ".join(sorted(_READERS)) or "(none)"
        raise ValueError(
            f"unknown reader {name!r}; registered readers: {known}"
        ) from None


def get_writer(name: str) -> Callable[..., Any]:
    """Return the registered writer callable or raise :exc:`ValueError` if unknown."""
    try:
        return _WRITERS[name].fn
    except KeyError:
        known = ", ".join(sorted(_WRITERS)) or "(none)"
        raise ValueError(
            f"unknown writer {name!r}; registered writers: {known}"
        ) from None


def list_readers() -> list[PluginFn]:
    """Return all registered readers, sorted by name."""
    return sorted(_READERS.values(), key=lambda p: p.name)


def list_writers() -> list[PluginFn]:
    """Return all registered writers, sorted by name."""
    return sorted(_WRITERS.values(), key=lambda p: p.name)
