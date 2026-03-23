"""Spark-style :class:`Window` / :class:`WindowSpec` for partition and order keys.

Used with window functions in :mod:`pydantable.expressions` (e.g. ``row_number``,
``lag``) via ``.over(...)``.

Window ``orderBy`` supports per-key **ascending** / **descending** and optional
**``nulls_last``** (``True`` ≈ **NULLS LAST**, ``False`` ≈ **NULLS FIRST**). The
default is ``nulls_last=False`` (nulls sort before non-nulls for ascending keys),
matching previous pydantable framed-window behavior. For Polars ``.over(...)``
lowering, **only the first** ``orderBy`` column's ``nulls_last`` is passed to
Polars ``SortOptions``; multi-key **framed** execution (``rowsBetween`` /
``rangeBetween``) uses the full per-key null placement. See
``docs/WINDOW_SQL_SEMANTICS.md``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WindowSpec:
    """Partition and ordering keys for :meth:`row_number().over` and similar."""

    partition_by: tuple[str, ...]
    order_by: tuple[tuple[str, bool, bool], ...]
    frame_kind: str | None = None
    frame_start: int | None = None
    frame_end: int | None = None

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        """Attach an inclusive row frame with Spark-style offsets."""
        return WindowSpec(
            partition_by=self.partition_by,
            order_by=self.order_by,
            frame_kind="rows",
            frame_start=int(start),
            frame_end=int(end),
        )

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        """Attach an inclusive range frame.

        Bounds apply to the **first** ``orderBy`` column; further keys sort only.
        See ``docs/WINDOW_SQL_SEMANTICS.md``.
        """
        return WindowSpec(
            partition_by=self.partition_by,
            order_by=self.order_by,
            frame_kind="range",
            frame_start=int(start),
            frame_end=int(end),
        )


class Window:
    """Entry point matching PySpark: ``Window.partitionBy(...).orderBy(...)``."""

    @staticmethod
    def partitionBy(*cols: str) -> _WindowPartitionBuilder:
        """Start a spec with one or more partition key column names."""
        if not cols:
            raise ValueError("partitionBy() requires at least one column name.")
        return _WindowPartitionBuilder(cols)


class _WindowPartitionBuilder:
    """Fluent builder after :meth:`Window.partitionBy`."""

    def __init__(self, partition_by: tuple[str, ...]):
        self._partition_by = partition_by

    def orderBy(
        self,
        *cols: str,
        ascending: bool | list[bool] = True,
        nulls_last: bool | list[bool] | None = None,
    ) -> WindowSpec:
        """Add sort keys.

        Use one bool or a list matching ``cols`` for ``ascending``. Optional
        ``nulls_last`` (bool or per-column list): ``True`` places nulls after
        non-nulls for that sort key (**NULLS LAST**); ``False`` places nulls
        first (**NULLS FIRST**). Default ``None`` uses ``False`` for every key.
        """
        if not cols:
            raise ValueError("orderBy() requires at least one column.")
        if isinstance(ascending, bool):
            flags = [ascending] * len(cols)
        else:
            if len(ascending) != len(cols):
                raise ValueError("ascending length must match columns length.")
            flags = list(ascending)
        if nulls_last is None:
            nl_flags = [False] * len(cols)
        elif isinstance(nulls_last, bool):
            nl_flags = [nulls_last] * len(cols)
        else:
            if len(nulls_last) != len(cols):
                raise ValueError("nulls_last length must match columns length.")
            nl_flags = list(nulls_last)
        order_by = tuple(
            (c, asc, nl) for c, asc, nl in zip(cols, flags, nl_flags, strict=True)
        )
        return WindowSpec(partition_by=self._partition_by, order_by=order_by)

    def spec(self) -> WindowSpec:
        """Partition keys only (no ``orderBy``)."""
        return WindowSpec(partition_by=self._partition_by, order_by=())

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        """Build a partition-only row frame window spec."""
        return self.spec().rowsBetween(start, end)

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        """Build a partition-only range frame window spec."""
        return self.spec().rangeBetween(start, end)
