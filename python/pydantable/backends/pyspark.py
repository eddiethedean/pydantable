from __future__ import annotations

from .polars import PolarsBackend


class PySparkBackend(PolarsBackend):
    """
    Backend tag for the PySpark-named interface module.

    Execution uses the same Rust core (Polars engine) as the default backend;
    the `pyspark` name only selects dispatch/identity in the Python layer.
    """

    name = "pyspark"
