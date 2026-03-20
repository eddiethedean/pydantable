from __future__ import annotations

from .polars import PolarsBackend


class PandasBackend(PolarsBackend):
    """
    Backend tag for the pandas-named interface module.

    Execution uses the same Rust core (Polars engine) as the default backend;
    the `pandas` name only selects dispatch/identity in the Python layer.
    """

    name = "pandas"
