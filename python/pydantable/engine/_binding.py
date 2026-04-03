"""Load and access ``pydantable._core`` (Rust extension).

Shared by :mod:`pydantable.rust_engine` compatibility shims and
:class:`~pydantable.engine.native.NativePolarsEngine`.
"""

from __future__ import annotations

from typing import Any

from pydantable._extension import MissingRustExtensionError

MISSING_SYMBOL_PREFIX = (
    "The pydantable native extension is present but does not implement "
)


def load_rust_core() -> Any | None:
    """Import the compiled Rust extension module (if available)."""
    try:
        from pydantable import _core as rust_core  # type: ignore

        return rust_core
    except ImportError:
        return None


_RUST_CORE = load_rust_core()


def require_rust_core() -> Any:
    """Return the loaded extension module or raise :exc:`MissingRustExtensionError`."""
    if _RUST_CORE is None:
        raise MissingRustExtensionError()
    return _RUST_CORE


def rust_core_loaded() -> Any | None:
    """Return the module or ``None`` if import failed (for capability probes)."""
    return _RUST_CORE


def rust_has_async_execute_plan() -> bool:
    """True if ``_core`` was built with ``async_execute_plan``."""
    return _RUST_CORE is not None and hasattr(_RUST_CORE, "async_execute_plan")


def rust_has_async_collect_plan_batches() -> bool:
    """True if ``_core`` exposes ``async_collect_plan_batches``."""
    return _RUST_CORE is not None and hasattr(_RUST_CORE, "async_collect_plan_batches")
