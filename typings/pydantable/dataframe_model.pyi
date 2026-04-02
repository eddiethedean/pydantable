from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Executor
from typing import Any, Generic, Literal, TypeVar

from pydantable.awaitable_dataframe_model import AwaitableDataFrameModel
from pydantable.dataframe import ExecutionHandle
from pydantable.schema import Schema
from pydantic import BaseModel
from typing_extensions import Self

RowT = TypeVar("RowT", bound=BaseModel)
AfterModelT = TypeVar("AfterModelT", bound="DataFrameModel[Any]")
GroupedModelT = TypeVar("GroupedModelT", bound="DataFrameModel[Any]")

class DataFrameModelAsyncIO(Generic[RowT]):
    def read_parquet(
        self, *args: Any, **kwargs: Any
    ) -> AwaitableDataFrameModel[RowT]: ...
    def read_ipc(self, *args: Any, **kwargs: Any) -> AwaitableDataFrameModel[RowT]: ...
    def read_csv(self, *args: Any, **kwargs: Any) -> AwaitableDataFrameModel[RowT]: ...
    def read_ndjson(
        self, *args: Any, **kwargs: Any
    ) -> AwaitableDataFrameModel[RowT]: ...
    def read_json(self, *args: Any, **kwargs: Any) -> AwaitableDataFrameModel[RowT]: ...
    def read_parquet_url_ctx(self, *args: Any, **kwargs: Any) -> Any: ...
    def write_sql(self, *args: Any, **kwargs: Any) -> Any: ...
    def export_parquet(self, *args: Any, **kwargs: Any) -> Any: ...
    def export_csv(self, *args: Any, **kwargs: Any) -> Any: ...
    def export_ndjson(self, *args: Any, **kwargs: Any) -> Any: ...
    def export_ipc(self, *args: Any, **kwargs: Any) -> Any: ...
    def export_json(self, *args: Any, **kwargs: Any) -> Any: ...

class DataFrameModel(Generic[RowT]):
    _df: Any
    RowModel: type[RowT]
    Async: DataFrameModelAsyncIO[RowT]

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

    # --- I/O classmethods (lazy reads, eager exports) ---
    @classmethod
    def read_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self: ...
    @classmethod
    def iter_parquet(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Iterator[Self]: ...
    @classmethod
    def read_parquet_url(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **kwargs: Any,
    ) -> Self: ...
    @classmethod
    def read_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self: ...
    @classmethod
    def iter_ipc(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Iterator[Self]: ...
    @classmethod
    def read_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self: ...
    @classmethod
    def iter_csv(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Iterator[Self]: ...
    @classmethod
    def read_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self: ...
    @classmethod
    def iter_ndjson(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Iterator[Self]: ...
    @classmethod
    def read_json(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self: ...
    @classmethod
    def export_parquet(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None: ...
    @classmethod
    def export_csv(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None: ...
    @classmethod
    def export_ndjson(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None: ...
    @classmethod
    def export_ipc(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None: ...
    @classmethod
    def export_json(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        indent: int | None = None,
    ) -> None: ...
    @classmethod
    async def aexport_parquet(
        cls,
        path: Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    async def aexport_csv(
        cls,
        path: Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    async def aexport_ndjson(
        cls,
        path: Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    async def aexport_ipc(
        cls,
        path: Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    async def aexport_json(
        cls,
        path: Any,
        data: dict[str, list[Any]],
        *,
        indent: int | None = None,
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    def write_sql(
        cls,
        data: dict[str, list[Any]],
        table_name: str,
        bind: Any,
        *,
        schema: str | None = None,
        if_exists: str = "append",
    ) -> None: ...
    @classmethod
    async def awrite_sql(
        cls,
        data: dict[str, list[Any]],
        table_name: str,
        bind: Any,
        *,
        schema: str | None = None,
        if_exists: str = "append",
        executor: Executor | None = None,
    ) -> None: ...
    @classmethod
    def read_parquet_url_ctx(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        **kwargs: Any,
    ) -> Any: ...
    @classmethod
    async def aread_parquet_url_ctx(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        **kwargs: Any,
    ) -> Any: ...
    @classmethod
    def aread_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> AwaitableDataFrameModel[RowT]: ...
    @classmethod
    def aread_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> AwaitableDataFrameModel[RowT]: ...
    @classmethod
    def aread_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> AwaitableDataFrameModel[RowT]: ...
    @classmethod
    def aread_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> AwaitableDataFrameModel[RowT]: ...
    @classmethod
    def aread_json(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> AwaitableDataFrameModel[RowT]: ...

    # --- Lazy write + batch helpers (instance / class) ---
    def write_parquet(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
        partition_by: tuple[str, ...] | list[str] | None = None,
        mkdir: bool = True,
    ) -> None: ...
    def write_csv(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        separator: str = ",",
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...
    def write_ipc(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...
    def write_ndjson(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...
    @classmethod
    def write_parquet_batches(
        cls,
        path: str | Any,
        batches: Any,
        *,
        compression: str | None = None,
    ) -> None: ...
    @classmethod
    def write_ipc_batches(
        cls,
        path: str | Any,
        batches: Any,
        *,
        as_stream: bool = True,
    ) -> None: ...
    @classmethod
    def write_csv_batches(
        cls,
        path: str | Any,
        batches: Any,
        *,
        mode: str = "w",
        encoding: str = "utf-8",
        write_header: bool = True,
    ) -> None: ...
    @classmethod
    def write_ndjson_batches(
        cls,
        path: str | Any,
        batches: Any,
        *,
        mode: str = "w",
        encoding: str = "utf-8",
    ) -> None: ...
    def collect_batches(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> list[Any]: ...
    @property
    def columns(self) -> list[str]: ...
    @property
    def shape(self) -> tuple[int, int]: ...
    @property
    def empty(self) -> bool: ...
    @property
    def dtypes(self) -> dict[str, Any]: ...
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
    def info(self) -> str: ...
    def describe(self) -> str: ...
    def explain(
        self,
        *,
        format: Literal["text", "json"] = "text",
        streaming: bool | None = None,
    ) -> str | dict[str, Any]: ...
    def value_counts(
        self,
        column: str,
        *,
        normalize: bool = False,
        dropna: bool = True,
    ) -> dict[Any, int | float]: ...
    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any: ...
    def to_dict(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> dict[str, list[Any]]: ...
    def to_polars(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any: ...
    def to_arrow(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any: ...
    def __dataframe__(
        self, *, nan_as_null: bool = False, allow_copy: bool = True
    ) -> Any: ...
    def __dataframe_consortium_standard__(
        self, api_version: str | None = None
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
    def select(self, *cols: Any) -> DataFrameModel[Any]: ...
    def select_schema(self, selector: Any) -> DataFrameModel[Any]: ...
    def with_columns(self, **new_columns: Any) -> DataFrameModel[Any]: ...
    def with_columns_cast(
        self, selector: Any, dtype: Any, *, strict: bool = True
    ) -> DataFrameModel[Any]: ...
    def with_columns_fill_null(
        self,
        selector: Any,
        *,
        value: Any = None,
        strategy: str | None = None,
        strict: bool = True,
    ) -> DataFrameModel[Any]: ...
    def filter(self, condition: Any) -> Self: ...
    def sort(self, *by: Any, descending: bool | Sequence[bool] = False) -> Self: ...
    def unique(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> Self: ...
    def distinct(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> Self: ...
    def drop(self, *columns: Any, strict: bool = True) -> DataFrameModel[Any]: ...
    def rename(
        self, columns: Mapping[str, str], *, strict: bool = True
    ) -> DataFrameModel[Any]: ...
    def rename_upper(
        self, selector: Any = None, *, strict: bool = True
    ) -> DataFrameModel[Any]: ...
    def rename_lower(
        self, selector: Any = None, *, strict: bool = True
    ) -> DataFrameModel[Any]: ...
    def rename_title(
        self, selector: Any = None, *, strict: bool = True
    ) -> DataFrameModel[Any]: ...
    def rename_strip(
        self,
        selector: Any = None,
        *,
        chars: str | None = None,
        strict: bool = True,
    ) -> DataFrameModel[Any]: ...
    def slice(self, offset: int, length: int) -> Self: ...
    def with_row_count(
        self, name: str = "row_nr", *, offset: int = 0
    ) -> DataFrameModel[Any]: ...
    def head(self, n: int = 5) -> Self: ...
    def tail(self, n: int = 5) -> Self: ...
    def pipe(self, fn: Any, *args: Any, **kwargs: Any) -> Any: ...
    def clip(
        self,
        *,
        lower: Any | None = None,
        upper: Any | None = None,
        subset: str | Sequence[str] | Any | None = None,
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
    def pivot_longer(
        self,
        *,
        id_vars: str | Sequence[str] | Any | None = None,
        value_vars: str | Sequence[str] | Any | None = None,
        names_to: str = "variable",
        values_to: str = "value",
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def pivot_wider(
        self,
        *,
        index: str | Sequence[str] | Any,
        names_from: str | Any,
        values_from: str | Sequence[str] | Any,
        aggregate_function: str = "first",
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def pivot(
        self,
        *,
        index: str | Sequence[str] | Any,
        columns: Any,
        values: str | Sequence[str] | Any,
        aggregate_function: str = "first",
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def explode(
        self,
        columns: str | Sequence[str] | Any,
        *,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def explode_outer(
        self, columns: str | Sequence[str] | Any, *, streaming: bool | None = None
    ) -> DataFrameModel[Any]: ...
    def posexplode(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def posexplode_outer(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def unnest(
        self, columns: str | Sequence[str] | Any, *, streaming: bool | None = None
    ) -> DataFrameModel[Any]: ...
    def explode_all(self, *, streaming: bool | None = None) -> DataFrameModel[Any]: ...
    def unnest_all(self, *, streaming: bool | None = None) -> DataFrameModel[Any]: ...
    def join(
        self,
        other: DataFrameModel[Any],
        *,
        on: str | Sequence[str] | Any | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
    ) -> DataFrameModel[Any]: ...
    def group_by(self, *keys: Any) -> GroupedDataFrameModel[Self]: ...
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
    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrameModel[Self]: ...
    def __getattr__(self, item: str) -> Any: ...
    @classmethod
    def row_model(cls) -> type[RowT]: ...
    @classmethod
    def schema_model(cls) -> type[Schema]: ...
    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrameModel[Any]],
        *,
        how: str = "vertical",
    ) -> DataFrameModel[Any]: ...

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
