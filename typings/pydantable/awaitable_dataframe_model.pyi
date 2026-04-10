from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from concurrent.futures import Executor
from typing import Any, Generic, TypeVar

from planframe.expr import api as pf

GroupedT = TypeVar("GroupedT")
RowT = TypeVar("RowT")

class AwaitableDataFrameModel(Generic[RowT]):
    def __init__(
        self,
        get_df: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = ...,
    ) -> None: ...
    def __await__(self) -> Any: ...
    def __repr__(self) -> str: ...
    @classmethod
    def concat(
        cls,
        *frames: Any,
        how: str = ...,
    ) -> AwaitableDataFrameModel[Any]: ...
    def then(self, fn: Callable[[Any], Any]) -> AwaitableDataFrameModel[Any]: ...
    def join(
        self,
        other: Any,
        *,
        on: str | pf.Expr[Any] | Sequence[str | pf.Expr[Any]] | None = ...,
        left_on: Any = ...,
        right_on: Any = ...,
        how: str = ...,
        suffix: str = ...,
    ) -> AwaitableDataFrameModel[Any]: ...
    def group_by(self, *keys: str | pf.Col) -> AwaitableGroupedDataFrameModel[Any]: ...
    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = ...,
        by: Sequence[str] | None = ...,
    ) -> AwaitableDynamicGroupedDataFrameModel[Any]: ...
    def acollect(
        self,
        *,
        as_lists: bool = ...,
        as_numpy: bool = ...,
        as_polars: bool | None = ...,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def ato_dict(
        self,
        *,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def ato_polars(
        self,
        *,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def ato_arrow(
        self,
        *,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def arows(
        self,
        *,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def ato_dicts(
        self,
        *,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
        **model_dump_kwargs: Any,
    ) -> Coroutine[Any, Any, Any]: ...
    def collect(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def to_dict(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def to_polars(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def to_arrow(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def rows(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def to_dicts(self, **kwargs: Any) -> Coroutine[Any, Any, Any]: ...
    def stream(self, **kwargs: Any) -> Any: ...
    def submit(
        self,
        *,
        as_lists: bool = ...,
        as_numpy: bool = ...,
        as_polars: bool | None = ...,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Coroutine[Any, Any, Any]: ...
    def astream(
        self,
        *,
        batch_size: int = ...,
        streaming: bool | None = ...,
        engine_streaming: bool | None = ...,
        executor: Executor | None = ...,
    ) -> Any: ...
    def __getattr__(self, name: str) -> Any: ...

class AwaitableGroupedDataFrameModel(Generic[GroupedT]):
    def __init__(
        self,
        get_g: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def agg(self, **aggregations: Any) -> AwaitableDataFrameModel[Any]: ...

class AwaitableDynamicGroupedDataFrameModel(Generic[GroupedT]):
    def __init__(
        self,
        get_g: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = ...,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def agg(self, **aggregations: Any) -> AwaitableDataFrameModel[Any]: ...
