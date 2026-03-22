"""Spark-style window specifications for typed window expressions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WindowSpec:
    """Partition and ordering keys for :meth:`row_number().over` and similar."""

    partition_by: tuple[str, ...]
    order_by: tuple[tuple[str, bool], ...]


class Window:
    """Minimal PySpark-shaped ``Window.partitionBy(...).orderBy(...)`` surface."""

    @staticmethod
    def partitionBy(*cols: str) -> _WindowPartitionBuilder:
        if not cols:
            raise ValueError("partitionBy() requires at least one column name.")
        return _WindowPartitionBuilder(cols)


class _WindowPartitionBuilder:
    def __init__(self, partition_by: tuple[str, ...]):
        self._partition_by = partition_by

    def orderBy(
        self,
        *cols: str,
        ascending: bool | list[bool] = True,
    ) -> WindowSpec:
        if not cols:
            raise ValueError("orderBy() requires at least one column.")
        if isinstance(ascending, bool):
            flags = [ascending] * len(cols)
        else:
            if len(ascending) != len(cols):
                raise ValueError("ascending length must match columns length.")
            flags = list(ascending)
        order_by = tuple((c, asc) for c, asc in zip(cols, flags, strict=True))
        return WindowSpec(partition_by=self._partition_by, order_by=order_by)

    def spec(self) -> WindowSpec:
        """Partition keys only (no ``orderBy``)."""
        return WindowSpec(partition_by=self._partition_by, order_by=())
