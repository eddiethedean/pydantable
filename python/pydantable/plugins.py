"""Simple plugin registry for pydantable I/O integrations (additive surface)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class PluginFn:
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
    _WRITERS[name] = PluginFn(
        name=name, fn=fn, kind="writer", requires_extra=requires_extra, stable=stable
    )


def get_reader(name: str) -> Callable[..., Any]:
    return _READERS[name].fn


def get_writer(name: str) -> Callable[..., Any]:
    return _WRITERS[name].fn


def list_readers() -> list[PluginFn]:
    return sorted(_READERS.values(), key=lambda p: p.name)


def list_writers() -> list[PluginFn]:
    return sorted(_WRITERS.values(), key=lambda p: p.name)
