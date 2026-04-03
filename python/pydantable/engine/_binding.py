"""Native extension shim.

The real binding lives in the optional `pydantable-native` distribution.
This module exists for backward-compatible imports and tests.
"""

from __future__ import annotations

from pydantable_native._binding import (  # type: ignore[import-not-found]
    MISSING_SYMBOL_PREFIX,
    load_rust_core,
    require_rust_core,
    rust_core_loaded,
    rust_has_async_collect_plan_batches,
    rust_has_async_execute_plan,
)

__all__ = [
    "MISSING_SYMBOL_PREFIX",
    "load_rust_core",
    "require_rust_core",
    "rust_core_loaded",
    "rust_has_async_collect_plan_batches",
    "rust_has_async_execute_plan",
]
