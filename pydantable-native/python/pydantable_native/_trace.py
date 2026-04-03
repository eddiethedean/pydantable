"""Execution tracing: use ``pydantable.observe`` when installed, else a local copy."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

try:
    from pydantable.observe import span
except ImportError:  # pydantable not installed — standalone native wheel / tests
    Observer = Callable[[dict[str, Any]], None]

    _OBSERVER: Observer | None = None
    _TRACE_ENV = "PYDANTABLE_TRACE"

    def set_observer(fn: Observer | None) -> None:
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
        obs = _OBSERVER
        if obs is not None:
            obs(event)
            return
        if trace_enabled():
            print(f"pydantable.trace {event}")

    class _FallbackSpan:
        def __init__(self, op: str, **fields: Any) -> None:
            self._op = op
            self._fields = fields
            self._t0: float | None = None

        def __enter__(self) -> _FallbackSpan:
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

    span = _FallbackSpan
