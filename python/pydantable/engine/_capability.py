from __future__ import annotations

from typing import Any

from pydantable.errors import unsupported_engine_operation


def require(
    engine: Any,
    *,
    capability_flag: str,
    operation: str,
    hint: str | None = None,
) -> None:
    """Raise a consistent error if an engine lacks a capability flag."""

    caps = getattr(engine, "capabilities", None)
    backend = getattr(caps, "backend", None) if caps is not None else None
    if caps is None or not bool(getattr(caps, capability_flag, False)):
        raise unsupported_engine_operation(
            backend=backend,
            operation=operation,
            required_capability=capability_flag,
            hint=hint,
        )


def supports(engine: Any, capability_flag: str) -> bool:
    caps = getattr(engine, "capabilities", None)
    return bool(caps is not None and getattr(caps, capability_flag, False))


def prefer_native_async_collect_batches(engine: Any) -> bool:
    """Whether we should use engine-native async batch collection APIs."""

    return bool(getattr(engine, "has_async_collect_plan_batches", lambda: False)())
