from __future__ import annotations

from dataclasses import dataclass

__all__ = ['Window', 'WindowSpec']

@dataclass(frozen=True)
class WindowSpec:
    partition_by: tuple[str, ...]
    order_by: tuple[tuple[str, bool, bool], ...]
    frame_kind: str | None = None
    frame_start: int | None = None
    frame_end: int | None = None

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        ...

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        ...

class Window:

    @staticmethod
    def partitionBy(*cols: str) -> _WindowPartitionBuilder:
        ...

class _WindowPartitionBuilder:

    def __init__(self, partition_by: tuple[str, ...]):
        ...

    def orderBy(self, *cols: str, ascending: bool | list[bool]=True, nulls_last: bool | list[bool] | None=None) -> WindowSpec:
        ...

    def spec(self) -> WindowSpec:
        ...

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        ...

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        ...

__all__ = ['Window', 'WindowSpec']
