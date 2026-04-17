"""Execution engine abstraction (native engine lives in ``pydantable-native``)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pydantable._extension import MissingRustExtensionError
from pydantable.errors import UnsupportedEngineOperationError

from .protocols import (
    EngineCapabilities,
    ExecutionEngine,
    PlanExecutor,
    native_engine_capabilities,
    stub_engine_capabilities,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "EngineCapabilities",
    "ExecutionEngine",
    "NativePolarsEngine",
    "PlanExecutor",
    "get_default_engine",
    "get_expression_runtime",
    "native_engine_capabilities",
    "set_default_engine",
    "set_expression_runtime",
    "stub_engine_capabilities",
]

_default_engine: ExecutionEngine | None = None

_expression_runtime_supplier: Callable[[], Any] | None = None


NativePolarsEngine: Any = None
try:
    from pydantable_native.native import (  # type: ignore[import-not-found]
        NativePolarsEngine as _NativePolarsEngine,
    )
except (ImportError, OSError):  # pragma: no cover — missing wheel or failed .so load
    pass
else:
    NativePolarsEngine = _NativePolarsEngine


def get_default_engine() -> ExecutionEngine:
    """Return the process-wide default engine (lazily constructed)."""
    global _default_engine
    if _default_engine is None:
        if NativePolarsEngine is None:
            raise MissingRustExtensionError(
                "Native execution is not installed. Reinstall `pydantable` "
                "(it should pull `pydantable-native`) or call "
                "set_default_engine(...) with a custom backend."
            )
        _default_engine = cast("ExecutionEngine", NativePolarsEngine())
    return _default_engine


def set_default_engine(engine: ExecutionEngine | None) -> None:
    """Replace the default engine (primarily for tests)."""
    global _default_engine
    _default_engine = engine


def get_expression_runtime() -> Any:
    """Return the object used to build :class:`~pydantable.expressions.Expr` trees.

    Defaults to :attr:`NativePolarsEngine.rust_core` (native extension) when the
    default engine is native. Non-native defaults must call
    :func:`set_expression_runtime` or operations that build expressions will raise
    :exc:`~pydantable.errors.UnsupportedEngineOperationError`.
    """
    if _expression_runtime_supplier is not None:
        return _expression_runtime_supplier()
    eng = get_default_engine()
    if NativePolarsEngine is not None and isinstance(eng, NativePolarsEngine):
        return eng.rust_core  # type: ignore[attr-defined]
    raise UnsupportedEngineOperationError(
        "Expression building requires the native engine (pydantable-native) or "
        "set_expression_runtime(...); "
        f"current default engine is {type(eng).__name__!r}."
    )


def set_expression_runtime(supplier: Callable[[], Any] | None) -> None:
    """Override expression construction (tests or alternate front-ends)."""
    global _expression_runtime_supplier
    _expression_runtime_supplier = supplier
