from __future__ import annotations

from .polars import PolarsBackend


class PySparkBackend(PolarsBackend):
    """
    Optional `pyspark` interface backend.

    For now this is a fallback executor that uses the existing Rust/Polars
    engine, while exposing the backend boundary for later full implementations.
    """

    name = "pyspark"
