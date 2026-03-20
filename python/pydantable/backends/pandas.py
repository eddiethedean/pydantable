from __future__ import annotations

from .polars import PolarsBackend


class PandasBackend(PolarsBackend):
    """
    Optional `pandas` interface backend.

    For now this is a fallback executor that uses the existing Rust/Polars
    engine, while keeping an explicit backend boundary for future replacement.
    """

    name = "pandas"

