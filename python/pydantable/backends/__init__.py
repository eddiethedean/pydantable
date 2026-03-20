from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Backend

from .pandas import PandasBackend
from .polars import PolarsBackend
from .pyspark import PySparkBackend

_BACKENDS: dict[str, Backend] = {
    "polars": PolarsBackend(),
    "pandas": PandasBackend(),
    "pyspark": PySparkBackend(),
}


def get_backend(name: str | None) -> Backend:
    resolved = (name or "polars").lower()
    try:
        return _BACKENDS[resolved]
    except KeyError as e:
        known = ", ".join(sorted(_BACKENDS.keys()))
        raise ValueError(f"Unknown backend {name!r}. Known backends: {known}.") from e
