"""Execution policy knobs for multi-engine workflows (v2).

These types are intentionally lightweight (Literal-based) so they can appear in
public signatures without importing optional engine stacks.
"""

from __future__ import annotations

from typing import Literal

# Reader engine selection policy (already used by engine-backed readers).
EngineMode = Literal["auto", "default"]

# Terminal execution behavior when the current engine can't run a plan.
ExecutionPolicy = Literal[
    # Prefer staying on the current engine; error if a fallback would be required.
    "pushdown",
    # Allow fallback boundaries; the library may materialize + re-root (typically to native).
    "fallback_to_native",
    # Error if any fallback would occur (alias-like but explicit).
    "error_on_fallback",
]

# How to materialize when crossing an engine boundary.
HandoffMaterialize = Literal["columns", "rows"]

