"""Execution engine abstraction (default: native Rust + Polars)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pydantable.errors import UnsupportedEngineOperationError

from .native import NativePolarsEngine
from .protocols import (
    EngineCapabilities,
    ExecutionEngine,
    native_engine_capabilities,
    stub_engine_capabilities,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "EngineCapabilities",
    "ExecutionEngine",
    "NativePolarsEngine",
    "get_default_engine",
    "get_expression_runtime",
    "native_engine_capabilities",
    "set_default_engine",
    "set_expression_runtime",
    "stub_engine_capabilities",
]

_default_engine: ExecutionEngine | None = None

_expression_runtime_supplier: Callable[[], Any] | None = None


def get_default_engine() -> ExecutionEngine:
    """Return the process-wide default engine (lazily constructed)."""
    global _default_engine
    if _default_engine is None:
        _default_engine = cast("ExecutionEngine", NativePolarsEngine())
    return _default_engine


def set_default_engine(engine: ExecutionEngine | None) -> None:
    """Replace the default engine (primarily for tests)."""
    global _default_engine
    _default_engine = engine


def get_expression_runtime() -> Any:
    """Return the object used to build :class:`~pydantable.expressions.Expr` trees.

    Defaults to :attr:`NativePolarsEngine.rust_core` (``pydantable._core``) when the
    default engine is native. Non-native defaults must call
    :func:`set_expression_runtime` or operations that build expressions will raise
    :exc:`~pydantable.errors.UnsupportedEngineOperationError`.
    """
    if _expression_runtime_supplier is not None:
        return _expression_runtime_supplier()
    eng = get_default_engine()
    if isinstance(eng, NativePolarsEngine):
        return eng.rust_core
    raise UnsupportedEngineOperationError(
        "Expression building requires NativePolarsEngine or "
        "set_expression_runtime(...); "
        f"current default engine is {type(eng).__name__!r}."
    )


def set_expression_runtime(supplier: Callable[[], Any] | None) -> None:
    """Override expression construction (tests or alternate front-ends)."""
    global _expression_runtime_supplier
    _expression_runtime_supplier = supplier
