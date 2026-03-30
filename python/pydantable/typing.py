from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Coroutine
    from concurrent.futures import Executor

RowT = TypeVar("RowT", bound=BaseModel)


@runtime_checkable
class SupportsLazyAsyncMaterialize(Protocol[RowT]):
    """Structural type for async terminal materialization via :meth:`acollect`.

    Use this when a helper should accept **either**:

    - a concrete :class:`~pydantable.dataframe_model.DataFrameModel`, or
    - a lazy :class:`~pydantable.awaitable_dataframe_model.AwaitableDataFrameModel`
      (e.g. after :meth:`~pydantable.dataframe_model.DataFrameModel.aread_parquet`
      or chained transforms such as ``select`` / ``filter``),

    and only needs to **await** the same async materialization path as
    :meth:`~pydantable.dataframe_model.DataFrameModel.acollect`.

    This protocol intentionally models **``acollect``**, not sync
    :meth:`~pydantable.dataframe_model.DataFrameModel.collect`. For row-typed
    helpers that need :meth:`~pydantable.dataframe_model.DataFrameModel.rows` /
    :meth:`~pydantable.dataframe_model.DataFrameModel.arows`, prefer
    :class:`DataFrameModelWithRow` instead.

    The underlying :class:`~pydantable.dataframe.DataFrame` also implements a
    compatible ``acollect``; the protocol is satisfied structurally by any object
    with a matching ``acollect`` method.

    .. note::

        With :func:`@runtime_checkable <typing.runtime_checkable>`,
        :func:`isinstance(obj, SupportsLazyAsyncMaterialize)` only checks that
        ``obj`` has a callable ``acollect`` attribute (duck typing at runtime). It
        does **not** validate return types or full signatures. Static type checkers
        enforce the protocol more strictly.
    """

    def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]: ...


class DataFrameModelWithRow(Protocol[RowT]):
    """
    Structural helper for "any DataFrameModel whose RowModel is RowT".

    This is intended for cross-model helper functions without claiming nominal
    identity like `DataFrameModel[Row] == Subclass`.
    """

    RowModel: type[RowT]

    def rows(self) -> list[RowT]: ...

    def arows(self, *, executor: Executor | None = None) -> Awaitable[list[RowT]]: ...


__all__ = ["DataFrameModelWithRow", "RowT", "SupportsLazyAsyncMaterialize"]
