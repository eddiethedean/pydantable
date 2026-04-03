"""Native execution engine shim.

The real implementation lives in the optional `pydantable-native` distribution.
This module exists for backward-compatible imports and tests.
"""

from __future__ import annotations

from pydantable_native.native import (  # type: ignore[import-not-found]
    NativePolarsEngine,
)

__all__ = ["NativePolarsEngine"]
