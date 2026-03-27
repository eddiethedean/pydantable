from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from concurrent.futures import Executor

RowT = TypeVar("RowT", bound=BaseModel)


class DataFrameModelWithRow(Protocol[RowT]):
    """
    Structural helper for "any DataFrameModel whose RowModel is RowT".

    This is intended for cross-model helper functions without claiming nominal
    identity like `DataFrameModel[Row] == Subclass`.
    """

    RowModel: type[RowT]

    def rows(self) -> list[RowT]: ...

    def arows(self, *, executor: Executor | None = None) -> Awaitable[list[RowT]]: ...


__all__ = ["DataFrameModelWithRow", "RowT"]
