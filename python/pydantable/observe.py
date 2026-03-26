"""Opt-in observability hooks for pydantable execution and I/O boundaries."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

Observer = Callable[[dict[str, Any]], None]

_OBSERVER: Observer | None = None
_TRACE_ENV = "PYDANTABLE_TRACE"


def set_observer(fn: Observer | None) -> None:
    """Set a global observer callback (or disable with `None`)."""
    global _OBSERVER
    _OBSERVER = fn


def get_observer() -> Observer | None:
    return _OBSERVER


def trace_enabled() -> bool:
    v = os.environ.get(_TRACE_ENV, "").strip().lower()
    return v in ("1", "true", "yes")


def _now() -> float:
    return time.perf_counter()


def emit(event: dict[str, Any]) -> None:
    """
    Emit an event to the observer if configured, otherwise no-op.

    If `PYDANTABLE_TRACE` is truthy and no observer is configured, events are
    still emitted to stderr via `print(...)` for a minimal default.
    """
    obs = _OBSERVER
    if obs is not None:
        obs(event)
        return
    if trace_enabled():
        # Keep this intentionally simple and stdlib-only.
        print(f"pydantable.trace {event}")


class span:
    """Context manager for timing an operation and emitting one event."""

    def __init__(self, op: str, **fields: Any) -> None:
        self._op = op
        self._fields = fields
        self._t0: float | None = None

    def __enter__(self) -> span:
        self._t0 = _now()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        t0 = self._t0 or _now()
        dt_ms = (_now() - t0) * 1000.0
        event = dict(self._fields)
        event.update(
            {
                "op": self._op,
                "duration_ms": dt_ms,
                "ok": exc_type is None,
            }
        )
        if exc_type is not None:
            event["error_type"] = getattr(exc_type, "__name__", str(exc_type))
        emit(event)
