"""Native engine package for pydantable (Rust extension + ExecutionEngine impl)."""

from __future__ import annotations

from ._binding import require_rust_core, rust_core_loaded
from .capabilities import native_engine_capabilities
from .native import NativePolarsEngine

__all__ = [
    "NativePolarsEngine",
    "native_engine_capabilities",
    "require_rust_core",
    "rust_core_loaded",
]
