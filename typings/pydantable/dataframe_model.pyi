from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import Executor
from typing import Any, Generic, Literal, TypeVar

from pydantable.dataframe import ExecutionHandle
from pydantic import BaseModel
from typing_extensions import Self

RowT = TypeVar("RowT", bound=BaseModel)
AfterModelT = TypeVar("AfterModelT", bound="DataFrameModel[Any]")
GroupedModelT = TypeVar("GroupedModelT", bound="DataFrameModel[Any]")

class DataFrameModel(Generic[RowT]):
    _df: Any
    RowModel: type[RowT]

    @classmethod
    def _from_dataframe(cls, df: Any) -> Self: ...
    def __init__(
        self,
        data: Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Any | None = None,
    ) -> None: ...
    def schema_fields(self) -> dict[str, Any]: ...
    def as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT: ...
    def try_as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT | None: ...
    def assert_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT: ...
    def select(self, *cols: Any) -> DataFrameModel[Any]: ...
    def with_columns(self, **new_columns: Any) -> DataFrameModel[Any]: ...
    def drop(self, *columns: Any) -> DataFrameModel[Any]: ...
    def rename(self, columns: Mapping[str, str]) -> DataFrameModel[Any]: ...
    def join(
        self,
        other: DataFrameModel[Any],
        *,
        on: str | Sequence[str] | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> DataFrameModel[Any]: ...
    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: Sequence[str] | None = None,
    ) -> DataFrameModel[Any]: ...
    def drop_nulls(
        self, subset: Sequence[str] | None = None
    ) -> DataFrameModel[Any]: ...
    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel[Any]: ...
    def unpivot(
        self,
        *,
        index: Sequence[str] | None = None,
        on: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel[Any]: ...
    def rolling_agg(
        self,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
    ) -> DataFrameModel[Any]: ...
    def explode(self, columns: str | Sequence[str]) -> DataFrameModel[Any]: ...
    def unnest(self, columns: str | Sequence[str]) -> DataFrameModel[Any]: ...
    def to_dict(self, *, streaming: bool | None = None) -> dict[str, list[Any]]: ...
    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any: ...
    def rows(self) -> list[RowT]: ...
    def to_dicts(self) -> list[dict[str, Any]]: ...
    async def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any: ...
    async def ato_dict(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> dict[str, list[Any]]: ...
    async def ato_polars(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any: ...
    async def ato_arrow(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any: ...
    def submit(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> ExecutionHandle: ...
    def stream(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Iterator[dict[str, list[Any]]]: ...
    def astream(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any: ...
    async def arows(self, *, executor: Executor | None = None) -> list[RowT]: ...
    async def ato_dicts(
        self, *, executor: Executor | None = None
    ) -> list[dict[str, Any]]: ...
    def filter(self, condition: Any) -> Self: ...
    def sort(self, *by: Any, descending: bool | Sequence[bool] = False) -> Self: ...
    def slice(self, offset: int, length: int) -> Self: ...
    def head(self, n: int = 5) -> Self: ...
    def tail(self, n: int = 5) -> Self: ...
    def group_by(self, *keys: Any) -> GroupedDataFrameModel[Self]: ...
    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrameModel[Self]: ...

class GroupedDataFrameModel(Generic[GroupedModelT]):
    _grouped_df: Any
    _model_type: type[GroupedModelT]

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None: ...
    def agg(self, **aggregations: Any) -> DataFrameModel[Any]: ...

class DynamicGroupedDataFrameModel(Generic[GroupedModelT]):
    _grouped_df: Any
    _model_type: type[GroupedModelT]

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None: ...
    def agg(self, **aggregations: Any) -> DataFrameModel[Any]: ...
