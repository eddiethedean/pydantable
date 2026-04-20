"""Typed :class:`DataFrame`, plan chaining, and grouped aggregation handles.

Logical plans and expression typing live in Rust; Python holds schema state and
forwards transforms. Materialization is via :meth:`DataFrame.collect`,
:meth:`DataFrame.to_dict`, :meth:`DataFrame.to_polars`, or :meth:`DataFrame.to_arrow`.
Non-blocking variants :meth:`DataFrame.acollect`, :meth:`DataFrame.ato_dict`,
:meth:`DataFrame.ato_polars`, and :meth:`DataFrame.ato_arrow` prefer a Rust awaitable
(Tokio + ``pyo3-async-runtimes``) when available, else :func:`asyncio.to_thread` /
``executor=``. :meth:`DataFrame.submit` runs :meth:`collect` in the background.
:meth:`DataFrame.stream` (sync) and :meth:`DataFrame.astream` (async) yield column
``dict`` chunks after one engine collect for streaming HTTP responses (e.g. FastAPI).
"""

from __future__ import annotations

import concurrent.futures
import functools
import html
import importlib
import logging
import random
import statistics
import threading
from datetime import date, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

from pydantic import BaseModel

from pydantable.display import get_repr_html_limits
from pydantable.engine import get_default_engine
from pydantable.expressions import AliasedExpr, ColumnRef, Expr
from pydantable.schema import (
    _is_polars_dataframe,
    field_types_for_rust,
    make_derived_schema_type,
    merge_field_types_preserving_identity,
    previous_field_types_for_join,
    schema_field_types,
    schema_from_descriptors,
    validate_columns_strict,
)
from pydantable.selectors import Selector

from ._column_rows import _coerce_enum_columns, _rows_from_column_dict
from ._describe_dtype import (
    _dtype_repr,
    _is_describe_bool,
    _is_describe_numeric,
    _is_describe_str,
    _is_describe_temporal,
)
from ._execution_handle import ExecutionHandle, _materialize_in_thread
from ._materialize_scan_fallback import (
    materialize_with_optional_scan_fallback_async,
    materialize_with_optional_scan_fallback_sync,
)
from ._repr_display import _REPR_MAX_COLUMNS, _dataframe_to_html_fragment
from ._scan import _is_scan_file_root
from ._streaming import _resolve_engine_streaming
from .grouped import DynamicGroupedDataFrame, GroupedDataFrame, _DataFrameForGroupBy

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from concurrent.futures import Executor


SchemaT = TypeVar("SchemaT", bound=BaseModel)
AfterSchemaT = TypeVar("AfterSchemaT", bound=BaseModel)


class DataFrame(_DataFrameForGroupBy, Generic[SchemaT]):
    """Strongly typed lazy table: schema at construction, transforms, then ``collect``.

    Construct with ``DataFrame[SchemaSubclass](data)``. Pass ``engine=`` to use a
    custom :class:`~pydantable.engine.protocols.ExecutionEngine` instance; the
    default is :func:`~pydantable.engine.get_default_engine` (native Polars/Rust).

    Column types come from the schema model; expressions are built with
    :class:`~pydantable.expressions.Expr` or attribute access (``df.colname``). The
    native engine validates operators and lowers plans to Polars for execution.
    """

    _schema_type: type[BaseModel] | None = None

    def __class_getitem__(cls, schema_type: Any) -> type[DataFrame[Any]]:
        if not isinstance(schema_type, type) or not issubclass(schema_type, BaseModel):
            raise TypeError("DataFrame[Schema] expects a Pydantic BaseModel type.")

        name = f"{cls.__name__}[{schema_type.__name__}]"
        # Important: avoid referencing `DataFrame[Any]` in a runtime `cast(...)`
        # because it triggers `__class_getitem__` again.
        return type(name, (cls,), {"_schema_type": schema_type})

    def __init__(
        self,
        data: Mapping[str, Sequence[Any]] | Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        column_strictness_default: Literal[
            "inherit", "coerce", "strict", "off"
        ] = "coerce",
        nested_strictness_default: Literal[
            "inherit", "coerce", "strict", "off"
        ] = "inherit",
        engine: Any | None = None,
    ) -> None:
        if self._schema_type is None:
            raise TypeError(
                "Use DataFrame[SchemaType](data) to construct a typed DataFrame."
            )

        root_data = validate_columns_strict(
            data,
            self._schema_type,
            validate_elements=None,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            column_strictness_default=column_strictness_default,
            nested_strictness_default=nested_strictness_default,
        )
        self._root_data: Any = root_data
        self._root_schema_type: type[BaseModel] = self._schema_type
        self._current_schema_type: type[BaseModel] = self._schema_type
        self._current_field_types = schema_field_types(self._current_schema_type)
        # Execution engine owns logical planning and materialization
        # (native Polars by default). Pass ``engine=`` to use a custom backend.
        self._engine = engine if engine is not None else get_default_engine()
        self._rust_plan = self._engine.make_plan(
            field_types_for_rust(self.schema_fields())
        )
        # Optional validation options attached to lazy scan roots. These are applied
        # at materialization time (after the engine produces columns), because
        # scan roots are lazy and cannot validate rows up front.
        self._io_validation_enabled: bool = False
        self._io_validation_trusted_mode: (
            Literal["off", "shape_only", "strict"] | None
        ) = None
        self._io_validation_fill_missing_optional: bool = True
        self._io_validation_ignore_errors: bool = False
        self._io_validation_on_validation_errors: (
            Callable[[list[dict[str, Any]]], None] | None
        ) = None
        # Optional default for Polars streaming collect on this object.
        self._engine_streaming_default: bool | None = None

    @classmethod
    def _from_scan_root(
        cls,
        root: Any,
        *,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> DataFrame[Any]:
        """Build a lazy frame from :class:`pydantable_native._core.ScanFileRoot`.

        Column validation is not applied yet.
        """
        from ._impl_lazy_sources import _from_scan_root as _fsr

        return _fsr(
            cls,
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

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
    ) -> DataFrame[Any]:
        """Lazy Parquet read (local file path)."""
        from ._impl_lazy_sources import read_parquet as _read_parquet

        return _read_parquet(
            cls,
            path,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

    @classmethod
    async def aread_parquet(
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
    ) -> DataFrame[Any]:
        """Async lazy Parquet read (local path).

        Returns a typed lazy frame backed by a native `ScanFileRoot`. Ingest
        validation options are applied when you materialize (`to_dict()` /
        `collect()` / `to_arrow()` / `to_polars()`), not at scan time.
        """
        from ._impl_lazy_sources import aread_parquet as _aread_parquet

        return await _aread_parquet(
            cls,
            path,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

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
    ) -> DataFrame[Any]:
        """Lazy Parquet read after HTTP(S) download to a temp file.

        See the DATA_IO_SOURCES guide (project docs).
        """
        from ._impl_lazy_sources import read_parquet_url as _read_parquet_url

        return _read_parquet_url(
            cls,
            url,
            experimental=experimental,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **kwargs,
        )

    @classmethod
    async def aread_parquet_url(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **kwargs: Any,
    ) -> DataFrame[Any]:
        """Async lazy Parquet over HTTP(S) (downloads to a temp file).

        Prefer `DataFrameModel.aread_parquet_url_ctx` /
        `pydantable.io.aread_parquet_url_ctx` for automatic temp-file cleanup.
        Validation options apply on materialization.
        """
        from ._impl_lazy_sources import aread_parquet_url as _aread_parquet_url

        return await _aread_parquet_url(
            cls,
            url,
            experimental=experimental,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **kwargs,
        )

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
    ) -> DataFrame[Any]:
        from ._impl_lazy_sources import read_csv as _read_csv

        return _read_csv(
            cls,
            path,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

    @classmethod
    async def aread_csv(
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
    ) -> DataFrame[Any]:
        """Async lazy CSV read (local path).

        Validation options apply on materialization (see `aread_parquet`).
        """
        from ._impl_lazy_sources import aread_csv as _aread_csv

        return await _aread_csv(
            cls,
            path,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

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
    ) -> DataFrame[Any]:
        from ._impl_lazy_sources import read_ndjson as _read_ndjson

        return _read_ndjson(
            cls,
            path,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

    @classmethod
    async def aread_ndjson(
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
    ) -> DataFrame[Any]:
        """Async lazy NDJSON read (local path).

        Validation options apply on materialization (see `aread_parquet`).
        """
        from ._impl_lazy_sources import aread_ndjson as _aread_ndjson

        return await _aread_ndjson(
            cls,
            path,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

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
    ) -> DataFrame[Any]:
        """Lazy JSON Lines (same as :meth:`read_ndjson`)."""
        from ._impl_lazy_sources import read_json as _read_json

        return _read_json(
            cls,
            path,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

    @classmethod
    async def aread_json(
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
    ) -> DataFrame[Any]:
        """Async lazy JSON Lines read (local path; alias of NDJSON engine).

        Validation options apply on materialization (see `aread_parquet`).
        """
        from ._impl_lazy_sources import aread_json as _aread_json

        return await _aread_json(
            cls,
            path,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

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
    ) -> DataFrame[Any]:
        from ._impl_lazy_sources import read_ipc as _read_ipc

        return _read_ipc(
            cls,
            path,
            columns=columns,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

    @classmethod
    async def aread_ipc(
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
    ) -> DataFrame[Any]:
        """Async lazy Arrow IPC file read (local path).

        Validation options apply on materialization (see `aread_parquet`).
        """
        from ._impl_lazy_sources import aread_ipc as _aread_ipc

        return await _aread_ipc(
            cls,
            path,
            columns=columns,
            executor=executor,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )

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
    ):
        """Stream Parquet as batches of in-memory typed frames (PyArrow-backed)."""
        from ._impl_lazy_sources import iter_parquet as _iter_parquet

        yield from _iter_parquet(
            cls,
            path,
            batch_size=batch_size,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def iter_ipc(
        cls,
        source: str | Any,
        *,
        batch_size: int = 65_536,
        as_stream: bool = False,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        """Stream IPC as batches of in-memory typed frames (PyArrow-backed)."""
        from ._impl_lazy_sources import iter_ipc as _iter_ipc

        yield from _iter_ipc(
            cls,
            source,
            batch_size=batch_size,
            as_stream=as_stream,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def iter_csv(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        encoding: str = "utf-8",
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        """Stream CSV as batches of in-memory typed frames (stdlib CSV)."""
        from ._impl_lazy_sources import iter_csv as _iter_csv

        yield from _iter_csv(
            cls,
            path,
            batch_size=batch_size,
            encoding=encoding,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def iter_ndjson(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        encoding: str = "utf-8",
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        """Stream NDJSON/JSONL as batches of in-memory typed frames (stdlib json)."""
        from ._impl_lazy_sources import iter_ndjson as _iter_ndjson

        yield from _iter_ndjson(
            cls,
            path,
            batch_size=batch_size,
            encoding=encoding,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def iter_json_lines(
        cls,
        path: str | Any,
        *,
        batch_size: int = 65_536,
        encoding: str = "utf-8",
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        """Stream JSON Lines as batches of in-memory typed frames."""
        from ._impl_lazy_sources import iter_json_lines as _iter_json_lines

        yield from _iter_json_lines(
            cls,
            path,
            batch_size=batch_size,
            encoding=encoding,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def _from_plan(
        cls,
        *,
        root_data: Any,
        root_schema_type: type[BaseModel],
        current_schema_type: type[BaseModel],
        rust_plan: Any,
        engine: Any | None = None,
    ) -> DataFrame[Any]:
        obj = cls.__new__(cls)
        obj._engine = engine if engine is not None else get_default_engine()
        obj._root_data = root_data
        obj._root_schema_type = root_schema_type
        obj._current_schema_type = current_schema_type
        obj._current_field_types = schema_field_types(current_schema_type)
        obj._rust_plan = rust_plan
        obj._schema_type = None
        obj._io_validation_enabled = False
        obj._io_validation_trusted_mode = None
        obj._io_validation_fill_missing_optional = True
        obj._io_validation_ignore_errors = False
        obj._io_validation_on_validation_errors = None
        # Optional default for Polars streaming collect on this object.
        obj._engine_streaming_default = None
        return cast("DataFrame[Any]", obj)

    def _apply_io_validation_if_configured(
        self, column_dict: dict[str, list[Any]]
    ) -> dict[str, list[Any]]:
        """
        Apply optional validation/row-skipping configured on lazy scan reads.

        This is intentionally post-execution: scan roots are lazy, so we can only
        validate once we have materialized Python columns.
        """
        if not self._io_validation_enabled:
            return column_dict
        mode = self._io_validation_trusted_mode or "off"
        return validate_columns_strict(
            column_dict,
            self._current_schema_type,
            validate_elements=None,
            trusted_mode=mode,
            fill_missing_optional=self._io_validation_fill_missing_optional,
            ignore_errors=self._io_validation_ignore_errors,
            on_validation_errors=self._io_validation_on_validation_errors,
        )

    def _column_dict_in_schema_order(
        self, column_dict: dict[str, list[Any]]
    ) -> dict[str, list[Any]]:
        """Materialized dicts may follow engine hash order; align to schema key order.

        Columns present in the materialized output but not in the current schema
        (e.g. ``_merge`` from ``merge(indicator=True)``) are appended after, stable
        by insertion order from the engine dict.
        """
        order = list(self._current_field_types.keys())
        out: dict[str, list[Any]] = {}
        for k in order:
            if k in column_dict:
                out[k] = column_dict[k]
        for k, v in column_dict.items():
            if k not in out:
                out[k] = v
        return out

    def _materialize_columns_with_missing_optional_fallback(
        self, *, streaming: bool
    ) -> dict[str, list[Any]]:
        """
        Materialize columns via the Rust engine, with a fallback for scan roots
        missing optional schema fields.

        Polars scans will error if a selected column is absent. For optional
        schema fields, we treat missing columns as all-null and retry execution
        with those columns omitted, then fill them with `None` during validation.
        """
        return materialize_with_optional_scan_fallback_sync(
            self._engine,
            plan=self._rust_plan,
            root_data=self._root_data,
            field_types=dict(self._current_field_types),
            current_schema_type=self._current_schema_type,
            io_validation_fill_missing_optional=self._io_validation_fill_missing_optional,
            streaming=streaming,
            error_context=self._materialize_error_context(),
        )

    async def _materialize_columns_with_missing_optional_fallback_async(
        self, *, streaming: bool, executor: Executor | None
    ) -> dict[str, list[Any]]:
        return await materialize_with_optional_scan_fallback_async(
            self._engine,
            plan=self._rust_plan,
            root_data=self._root_data,
            field_types=dict(self._current_field_types),
            current_schema_type=self._current_schema_type,
            io_validation_fill_missing_optional=self._io_validation_fill_missing_optional,
            streaming=streaming,
            error_context=self._materialize_error_context(),
            executor=executor,
        )

    async def _materialize_columns_async(
        self, *, streaming: bool, executor: Executor | None
    ) -> dict[str, list[Any]]:
        raw = await self._materialize_columns_with_missing_optional_fallback_async(
            streaming=streaming, executor=executor
        )
        raw = _coerce_enum_columns(raw, self._current_field_types)
        return self._apply_io_validation_if_configured(raw)

    @property
    def schema_type(self) -> type[BaseModel]:
        return self._current_schema_type

    def schema_fields(self) -> dict[str, Any]:
        return dict(self._current_field_types)

    @staticmethod
    def _expected_schema_fields(schema: type[BaseModel]) -> dict[str, Any]:
        # Prefer the direct Pydantic model_fields mapping to avoid evaluating
        # inherited annotations via typing.get_type_hints at runtime.
        return {
            name: field.annotation
            for name, field in schema.model_fields.items()
            if not name.startswith("_")
        }

    def as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(schema, type) or not issubclass(schema, BaseModel):
            raise TypeError("as_schema(schema=...) expects a Pydantic BaseModel type.")
        if validate_schema:
            expected = self._expected_schema_fields(schema)
            actual = self.schema_fields()
            if set(expected) != set(actual) or any(
                expected[k] != actual[k] for k in expected if k in actual
            ):
                raise TypeError(
                    "as_schema(schema mismatch): expected "
                    f"{sorted(expected)} got {sorted(actual)}"
                )
        # Avoid `DataFrame[schema]` here: some type checkers treat `[...]` as a
        # type expression and reject runtime variables.
        df_cls = DataFrame.__class_getitem__(schema)
        return cast(
            "DataFrame[AfterSchemaT]",
            df_cls._from_plan(
                root_data=self._root_data,
                root_schema_type=self._root_schema_type,
                current_schema_type=schema,
                rust_plan=self._rust_plan,
                engine=self._engine,
            ),
        )

    def try_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        if not isinstance(schema, type) or not issubclass(schema, BaseModel):
            raise TypeError(
                "try_as_schema(schema=...) expects a Pydantic BaseModel type."
            )
        if not validate_schema:
            return self.as_schema(schema, validate_schema=False)
        expected = self._expected_schema_fields(schema)
        actual = self.schema_fields()
        if set(expected) != set(actual) or any(
            expected[k] != actual[k] for k in expected if k in actual
        ):
            return None
        return self.as_schema(schema, validate_schema=False)

    def assert_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(schema, type) or not issubclass(schema, BaseModel):
            raise TypeError(
                "assert_schema(schema=...) expects a Pydantic BaseModel type."
            )
        if not validate_schema:
            return self.as_schema(schema, validate_schema=False)
        expected = self._expected_schema_fields(schema)
        actual = self.schema_fields()
        if set(expected) != set(actual) or any(
            expected[k] != actual[k] for k in expected if k in actual
        ):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            mismatched = sorted(
                k for k in set(expected) & set(actual) if expected[k] != actual[k]
            )
            raise TypeError(
                "assert_schema(schema mismatch): "
                f"missing={missing} extra={extra} mismatched={mismatched}"
            )
        return self.as_schema(schema, validate_schema=False)

    # Aliases for parity with DataFrameModel's after-model helpers.
    def as_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.as_schema(schema, validate_schema=validate_schema)

    def try_as_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.try_as_schema(schema, validate_schema=validate_schema)

    def assert_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.assert_schema(schema, validate_schema=validate_schema)

    @property
    def columns(self) -> list[str]:
        """Current logical column names (schema order)."""
        return list(self._current_field_types.keys())

    @property
    def shape(self) -> tuple[int, int]:
        """``(n_rows, n_cols)`` from the **root** column buffers when present.

        After lazy transforms (e.g. :meth:`filter`) that do not replace
        ``_root_data``, the row count may **not** match materialized output; use
        :meth:`to_dict` or :meth:`collect` for the true row count.
        """
        if not self._root_data:
            return (0, len(self._current_field_types))
        rd = self._root_data
        if _is_scan_file_root(rd):
            return (0, len(self._current_field_types))
        if _is_polars_dataframe(rd):
            return (len(rd), len(self._current_field_types))
        first = next(iter(rd.values()))
        return (len(first), len(self._current_field_types))

    @property
    def empty(self) -> bool:
        """True when the root buffer has zero rows (see :attr:`shape`)."""
        return self.shape[0] == 0

    @property
    def dtypes(self) -> dict[str, Any]:
        """Map column name → Pydantic field annotation (not pandas dtype objects)."""
        return dict(self._current_field_types)

    def info(self) -> str:
        """Multi-line summary string (schema, dtypes, :attr:`shape` caveat).

        **Cost:** does not materialize row data; uses schema and root-buffer
        :attr:`shape` only. See the EXECUTION guide (project docs)
        **Materialization costs**.
        """
        schema_qn = getattr(
            self._current_schema_type,
            "__qualname__",
            self._current_schema_type.__name__,
        )
        lines = [
            f"{self.__class__.__name__}",
            f"  schema: {schema_qn}",
            f"  columns: {len(self._current_field_types)}",
            f"  shape (root buffer): {self.shape[0]} x {self.shape[1]}",
            "  Note: after lazy transforms (e.g. filter), root row count may not "
            "match materialized rows; use to_dict() or collect() for true count.",
            "",
            "dtypes:",
        ]
        for name, ann in self._current_field_types.items():
            lines.append(f"  {name}: {_dtype_repr(ann)}")
        return "\n".join(lines)

    def describe(self) -> str:
        """Summary statistics for int, float, bool, str, and date/datetime columns.

        **Cost:** one full :meth:`to_dict()` materialization. String columns
        ``n_unique`` scans all non-null strings. See the EXECUTION guide (project docs).
        """
        numeric = [
            n for n, a in self._current_field_types.items() if _is_describe_numeric(a)
        ]
        bool_cols = [
            n for n, a in self._current_field_types.items() if _is_describe_bool(a)
        ]
        str_cols = [
            n for n, a in self._current_field_types.items() if _is_describe_str(a)
        ]
        temporal_cols = [
            n for n, a in self._current_field_types.items() if _is_describe_temporal(a)
        ]
        if not numeric and not bool_cols and not str_cols and not temporal_cols:
            return "describe(): no int/float/bool/str/date/datetime columns in schema."
        data = self.to_dict()
        lines = [
            "describe() — one to_dict(); int/float/bool/str/date/datetime columns.",
            "",
        ]
        for name in numeric:
            col = data[name]
            vals = [x for x in col if x is not None]
            c = len(vals)
            if c == 0:
                lines.append(f"{name}: count=0 (all null)")
                continue
            mean_v = statistics.mean(vals)
            mn, mx = min(vals), max(vals)
            if c >= 2:
                std_v = statistics.stdev(vals)
                extra = ""
                if c >= 4:
                    try:
                        import numpy as np  # type: ignore[import-not-found]

                        arr = np.asarray(vals, dtype=float)
                        m = float(arr.mean())
                        s = float(arr.std(ddof=1))
                        if s > 0:
                            skew_v = float(np.mean(((arr - m) / s) ** 3))
                            kurt_v = float(np.mean(((arr - m) / s) ** 4) - 3.0)
                            sem_v = s / (c**0.5)
                            extra = (
                                f" skew={skew_v:.6g} kurtosis={kurt_v:.6g} "
                                f"sem={sem_v:.6g}"
                            )
                    except ImportError:
                        pass
                lines.append(
                    f"{name}: count={c} mean={mean_v:.6g} std={std_v:.6g} "
                    f"min={mn} max={mx}{extra}"
                )
            else:
                lines.append(f"{name}: count={c} mean={mean_v:.6g} min={mn} max={mx}")
        for name in bool_cols:
            col = data[name]
            non_null = [x for x in col if x is not None]
            n_null = len(col) - len(non_null)
            nt = sum(1 for x in non_null if x is True)
            nf = sum(1 for x in non_null if x is False)
            lines.append(
                f"{name}: count={len(non_null)} true={nt} false={nf} null={n_null}"
            )
        for name in str_cols:
            col = data[name]
            raw = [x for x in col if x is not None]
            str_vals = [x for x in raw if isinstance(x, str)]
            n_null = len(col) - len(raw)
            if not str_vals:
                lines.append(f"{name}: count=0 (all null)")
                continue
            n_unique = len(set(str_vals))
            lens = [len(s) for s in str_vals]
            lines.append(
                f"{name}: count={len(str_vals)} n_unique={n_unique} "
                f"min_len={min(lens)} max_len={max(lens)} null={n_null}"
            )
        for name in temporal_cols:
            col = data[name]
            raw = [x for x in col if x is not None]
            n_null = len(col) - len(raw)
            if not raw:
                lines.append(f"{name}: count=0 (all null)")
                continue
            vals = [x for x in raw if isinstance(x, (date, datetime))]
            if not vals:
                lines.append(f"{name}: count=0 (all null)")
                continue
            mn, mx = min(vals), max(vals)
            lines.append(
                f"{name}: count={len(vals)} min={mn!s} max={mx!s} null={n_null}"
            )
        return "\n".join(lines)

    def value_counts(
        self,
        column: str,
        *,
        normalize: bool = False,
        dropna: bool = True,
    ) -> dict[Any, int | float]:
        """Count rows per distinct value in ``column`` (group-by aggregation).

        **Cost:** engine aggregation (same path as :meth:`group_by` / :meth:`agg`).
        See the EXECUTION guide (project docs).

        Returns a dict sorted by count descending, then ``repr(key)``. When
        ``normalize=True``, values are fractions in ``[0, 1]`` (``float``).
        """
        if column not in self._current_field_types:
            raise KeyError(f"Unknown column {column!r} for current schema.")
        out_name = "__pydantable_vc_n"
        g = self.group_by(column, drop_nulls=bool(dropna)).agg(
            streaming=None,
            **{out_name: ("count", column)},
        )
        d = g.to_dict()
        keys = d[column]
        counts = d[out_name]
        result: dict[Any, int] = {}
        for k, c in zip(keys, counts, strict=True):
            if dropna and k is None:
                continue
            result[k] = int(c) if c is not None else 0
        total = sum(result.values())
        if normalize:
            if total == 0:
                return {k: 0.0 for k in result}
            return {k: v / total for k, v in result.items()}
        return dict(sorted(result.items(), key=lambda kv: (-kv[1], repr(kv[0]))))

    def _materialize_error_context(self) -> str:
        st = self._current_schema_type
        qn = getattr(st, "__qualname__", st.__name__)
        return f"schema={qn}"

    def _field_types_from_descriptors(
        self,
        descriptors: Mapping[str, Mapping[str, Any]],
        *,
        previous: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        derived = schema_from_descriptors(descriptors)
        prev = self._current_field_types if previous is None else previous
        return merge_field_types_preserving_identity(prev, descriptors, derived)

    def col(self, name: str) -> ColumnRef:
        if name not in self._current_field_types:
            raise KeyError(f"Unknown column {name!r} for current schema.")
        return ColumnRef(name=name, dtype=self._current_field_types[name])

    def __getattr__(self, item: str) -> Any:
        # Called only when attribute resolution fails; treat schema fields as columns.
        if item in self._current_field_types:
            return self.col(item)
        raise AttributeError(item)

    def __repr__(self) -> str:
        fields = self._current_field_types
        schema = self._current_schema_type
        schema_qn = getattr(schema, "__qualname__", schema.__name__)
        cls_name = self.__class__.__name__
        n = len(fields)
        lines = [
            f"{cls_name}",
            f"  schema: {schema_qn}",
            f"  columns ({n}):",
        ]
        if not fields:
            lines.append("    (none)")
            return "\n".join(lines)
        items = list(fields.items())
        shown = items[:_REPR_MAX_COLUMNS]
        name_w = max(len(name) for name, _ in shown)
        for name, ann in shown:
            dtype_s = _dtype_repr(ann)
            lines.append(f"    {name:<{name_w}}  {dtype_s}")
        rest = len(items) - len(shown)
        if rest > 0:
            lines.append(f"    … and {rest} more")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """Rich HTML table for Jupyter / IPython (preview only; materializes data).

        Row/column/cell limits default to module constants but can be set via
        :mod:`pydantable.display` or ``PYDANTABLE_REPR_HTML_*`` env vars.
        **Cost:** same as :meth:`head` for the bounded slice plus :meth:`to_dict`
        on that slice. See the EXECUTION guide (project docs) **Jupyter / HTML**.
        """
        try:
            return self._repr_html_impl()
        except Exception as e:  # pragma: no cover - defensive for notebook UX
            logging.getLogger(__name__).debug(
                "HTML repr failed; rendering error fallback.", exc_info=True
            )
            err = html.escape(str(e))
            return (
                '<div class="pydantable-render pydantable-render--error" '
                'style="font-family:ui-sans-serif,system-ui,sans-serif;'
                "font-size:13px;margin:0 0 1rem 0;padding:14px 16px;"
                "border-radius:12px;border:1px solid #fecaca;background:#fef2f2;"
                'color:#991b1b;box-shadow:0 1px 2px rgba(127,29,29,0.06);">'
                '<p style="margin:0 0 8px 0;font-weight:600;">HTML preview failed</p>'
                f'<pre style="margin:0;white-space:pre-wrap;word-break:break-word;'
                f'font-size:12px;color:#7f1d1d;">{err}</pre></div>'
            )

    def _repr_mimebundle_(
        self,
        include: Any = None,
        exclude: Any = None,
    ) -> dict[str, Any]:
        """Jupyter / IPython: prefer HTML table plus plain text fallback."""
        return {
            "text/plain": repr(self),
            "text/html": self._repr_html_(),
        }

    def _repr_html_impl(self) -> str:
        cols_all = list(self._current_field_types.keys())
        if not cols_all:
            return (
                '<div class="pydantable-render" style="margin:0 0 1rem 0;">'
                '<div class="pydantable-surface" style="border-radius:12px;'
                "border:1px dashed #cbd5e1;background:#f8fafc;"
                "padding:1.5rem 1.25rem;text-align:center;color:#64748b;"
                "font-family:ui-sans-serif,system-ui,sans-serif;font-size:13px;"
                'box-shadow:0 1px 2px rgba(15,23,42,0.04);">'
                '<p style="margin:0;"><em>No columns</em></p></div></div>'
            )

        lim = get_repr_html_limits()
        preview = self.head(lim.max_rows)
        data = preview.to_dict()
        n_rows = len(next(iter(data.values()))) if data else 0

        col_order = [c for c in cols_all if c in data]
        for c in data:
            if c not in col_order:
                col_order.append(c)

        n_cols_total = len(col_order)
        col_trunc = n_cols_total > lim.max_cols
        shown_cols = col_order[: lim.max_cols]
        sub: dict[str, list[Any]] = {c: data[c] for c in shown_cols}

        st = self._current_schema_type
        schema_qn = getattr(st, "__qualname__", st.__name__)
        caption = f"{self.__class__.__name__} · schema={schema_qn}"

        note_parts: list[str] = [
            f"Preview: {n_rows} row{'s' if n_rows != 1 else ''} x "
            f"{len(shown_cols)} column{'s' if len(shown_cols) != 1 else ''} shown"
        ]
        if n_rows >= lim.max_rows:
            note_parts.append(f"(up to {lim.max_rows} rows)")
        if col_trunc:
            rest = n_cols_total - len(shown_cols)
            col_s = "column" if rest == 1 else "columns"
            note_parts.append(f"(… and {rest} more {col_s} omitted)")
        note = " ".join(note_parts)

        return _dataframe_to_html_fragment(
            column_dict=sub,
            column_order=shown_cols,
            caption=caption,
            note=note,
            max_cell_len=lim.max_cell_len,
        )

    def with_columns(self, *exprs: Any, **new_columns: Expr | Any) -> DataFrame[Any]:
        """Add or replace columns.

        Values are :class:`~pydantable.expressions.Expr` or plain literals.
        """
        from ._ops import plan_with_columns

        return plan_with_columns(self, *exprs, **new_columns)

    def with_columns_cast(
        self, selector: Selector, dtype: Any, *, strict: bool = True
    ) -> DataFrame[Any]:
        """Cast columns selected by a schema-driven Selector."""
        if not isinstance(selector, Selector):
            raise TypeError("with_columns_cast(selector=...) expects a Selector.")
        selected = selector.resolve(self._current_field_types)
        if not selected:
            if strict:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"with_columns_cast({selector!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            return self
        updates: dict[str, Expr] = {c: self.col(c).cast(dtype) for c in selected}
        return self.with_columns(**updates)

    def with_columns_fill_null(
        self,
        selector: Selector,
        *,
        value: Any = None,
        strategy: str | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        """Fill nulls for columns selected by a schema-driven Selector."""
        if not isinstance(selector, Selector):
            raise TypeError("with_columns_fill_null(selector=...) expects a Selector.")
        selected = selector.resolve(self._current_field_types)
        if not selected:
            if strict:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"with_columns_fill_null({selector!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            return self
        return self.fill_null(value, strategy=strategy, subset=selected)

    def select_schema(self, selector: Selector) -> DataFrame[Any]:
        """Project columns using a schema-first Selector (explicit helper)."""
        if not isinstance(selector, Selector):
            raise TypeError("select_schema(selector) expects a Selector.")
        return self.select(selector)

    def select(
        self,
        *cols: str | ColumnRef | Expr | AliasedExpr | Selector,
        exclude: Selector | Sequence[str] | None = None,
        **named: Any,
    ) -> DataFrame[Any]:
        """Project columns and/or compute a **single-row** frame of global aggregates.

        Positional arguments: base column names, single-column refs, or globals such
        as :func:`~pydantable.expressions.global_sum`. Keyword arguments are only for
        named global aggregates. Plain projections and globals cannot be mixed.
        """
        named_items: list[tuple[str, Any]] = []
        for name, e in named.items():
            if not isinstance(e, Expr):
                raise TypeError(
                    "select() keyword arguments must be Expr instances "
                    "(global aggregates)."
                )
            named_items.append((name, e._rust_expr))

        exclude_set: set[str] = set()
        if exclude is not None:
            if isinstance(exclude, Selector):
                exclude_set = set(exclude.resolve(self._current_field_types))
            else:
                exclude_set = {str(c) for c in exclude}

        aggs: list[tuple[str, Any]] = []
        projects: list[str] = []
        computed: dict[str, Any] = {}
        for col in cols:
            if isinstance(col, str):
                projects.append(col)
            elif isinstance(col, Selector):
                resolved = col.resolve(self._current_field_types)
                if not resolved:
                    available = ", ".join(repr(c) for c in self._current_field_types)
                    raise ValueError(
                        f"select({col!r}) matched no columns. "
                        f"Available columns: [{available}]"
                    )
                projects.extend(resolved)
            elif isinstance(col, AliasedExpr):
                if not isinstance(col.expr, Expr):
                    raise TypeError("select(AliasedExpr) expects an Expr.")
                computed[col.name] = col.expr._rust_expr
                projects.append(col.name)
            elif isinstance(col, Expr):
                if self._engine.expr_is_global_agg(col._rust_expr):
                    alias = self._engine.expr_global_default_alias(col._rust_expr)
                    if alias is None:
                        raise TypeError(
                            "global aggregate in select() is missing a default "
                            "output name."
                        )
                    aggs.append((alias, col._rust_expr))
                else:
                    if isinstance(col, ColumnRef):
                        projects.append(col._column_name)  # type: ignore[attr-defined]
                    else:
                        raise TypeError(
                            "select() accepts column names, ColumnRef expressions, "
                            "global aggregates, or Expr.alias('name') for computed "
                            "expressions."
                        )
            else:
                raise TypeError(
                    "select() accepts column names, Selector objects, "
                    "ColumnRef expressions, global aggregates, or "
                    "Expr.alias('name') (AliasedExpr)."
                )

        if named_items and (projects or aggs):
            raise TypeError(
                "select() cannot mix keyword aggregates with positional column "
                "names or aggregates."
            )
        if aggs and projects:
            raise TypeError(
                "select() cannot mix global aggregates with plain column projections."
            )
        if exclude_set and (named_items or aggs):
            raise TypeError(
                "select(exclude=...) cannot be used with global aggregates."
            )
        if named_items:
            rust_plan = self._engine.plan_global_select(self._rust_plan, named_items)
        elif aggs:
            rust_plan = self._engine.plan_global_select(self._rust_plan, aggs)
        else:
            if not projects:
                if not exclude_set:
                    raise ValueError("select() requires at least one column.")
                projects = [
                    c for c in self._current_field_types if c not in exclude_set
                ]
            else:
                projects = [c for c in projects if c not in exclude_set]
            if not projects:
                raise ValueError("select(...) produced an empty projection.")
            rust_plan = self._rust_plan
            if computed:
                rust_plan = self._engine.plan_with_columns(rust_plan, computed)
            rust_plan = self._engine.plan_select(rust_plan, projects)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        # Preserve order: for plain projections, match call order.
        if projects and not aggs and not named_items:
            desired_order = [c for c in projects if c in derived_fields]
            ordered: dict[str, Any] = {k: derived_fields[k] for k in desired_order}
            for k, v in derived_fields.items():
                if k not in ordered:
                    ordered[k] = v
            derived_fields = ordered
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def select_all(self) -> DataFrame[Any]:
        """Select all columns in schema order (schema-driven helper)."""
        return self.select(*list(self._current_field_types.keys()))

    def select_prefix(self, prefix: str) -> DataFrame[Any]:
        """Select columns whose names start with `prefix` (schema-driven helper)."""
        if not isinstance(prefix, str):
            raise TypeError("select_prefix(prefix) expects a string.")
        cols = [c for c in self._current_field_types if c.startswith(prefix)]
        if not cols:
            raise ValueError(f"select_prefix({prefix!r}) matched no columns.")
        return self.select(*cols)

    def select_suffix(self, suffix: str) -> DataFrame[Any]:
        """Select columns whose names end with `suffix` (schema-driven helper)."""
        if not isinstance(suffix, str):
            raise TypeError("select_suffix(suffix) expects a string.")
        cols = [c for c in self._current_field_types if c.endswith(suffix)]
        if not cols:
            raise ValueError(f"select_suffix({suffix!r}) matched no columns.")
        return self.select(*cols)

    def _resolve_column_names_or_selector(
        self, item: str | Selector, *, arg_name: str
    ) -> list[str]:
        if isinstance(item, str):
            return [item]
        if isinstance(item, Selector):
            resolved = item.resolve(self._current_field_types)
            if not resolved:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"{arg_name}={item!r} matched no columns. "
                    f"Available columns: [{available}]"
                )
            return resolved
        raise TypeError(f"{arg_name} expects a column name or Selector.")

    def reorder_columns(self, order: Sequence[str | Selector]) -> DataFrame[Any]:
        """Reorder columns by explicit names/selectors; append remaining columns."""
        wanted: list[str] = []
        for it in order:
            wanted.extend(self._resolve_column_names_or_selector(it, arg_name="order"))
        if len(set(wanted)) != len(wanted):
            raise ValueError("reorder_columns(order=...) contains duplicate columns.")
        remainder = [c for c in self._current_field_types if c not in wanted]
        return self.select(*wanted, *remainder)

    def select_first(self, *cols_or_selectors: str | Selector) -> DataFrame[Any]:
        """Move selected columns to the front; keep remaining order."""
        wanted: list[str] = []
        for it in cols_or_selectors:
            wanted.extend(self._resolve_column_names_or_selector(it, arg_name="cols"))
        if len(set(wanted)) != len(wanted):
            raise ValueError("select_first(...) contains duplicate columns.")
        remainder = [c for c in self._current_field_types if c not in wanted]
        return self.select(*wanted, *remainder)

    def select_last(self, *cols_or_selectors: str | Selector) -> DataFrame[Any]:
        """Move selected columns to the end; keep remaining order."""
        wanted: list[str] = []
        for it in cols_or_selectors:
            wanted.extend(self._resolve_column_names_or_selector(it, arg_name="cols"))
        if len(set(wanted)) != len(wanted):
            raise ValueError("select_last(...) contains duplicate columns.")
        remainder = [c for c in self._current_field_types if c not in wanted]
        return self.select(*remainder, *wanted)

    def move(
        self,
        cols_or_selector: str | Selector,
        *,
        before: str | None = None,
        after: str | None = None,
    ) -> DataFrame[Any]:
        """Move column(s) before/after an anchor column (schema-first helper)."""
        if (before is None) == (after is None):
            raise TypeError(
                "move(..., before=...) or move(..., after=...) is required."
            )
        moving = self._resolve_column_names_or_selector(
            cols_or_selector, arg_name="cols"
        )
        anchor = before if before is not None else after
        if not isinstance(anchor, str):
            raise TypeError("move(..., before/after=...) expects a column name.")
        if anchor not in self._current_field_types:
            raise KeyError(f"move() unknown anchor column {anchor!r}.")
        if anchor in moving:
            raise ValueError("move() cannot move a column relative to itself.")
        # Remove moving cols, then re-insert as a block.
        base = [c for c in self._current_field_types if c not in moving]
        idx = base.index(anchor)
        insert_at = idx if before is not None else idx + 1
        new_order = [*base[:insert_at], *moving, *base[insert_at:]]
        return self.select(*new_order)

    def filter(self, condition: Expr) -> DataFrame[Any]:
        """Keep rows where the boolean ``condition`` is true."""
        from ._ops import plan_filter

        return plan_filter(self, condition)

    def sort(
        self,
        *by: str | ColumnRef,
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] | None = None,
        maintain_order: bool = False,
    ) -> DataFrame[Any]:
        """Sort by one or more columns (names or single-column expressions)."""
        keys: list[str] = []
        for key in by:
            if isinstance(key, str):
                keys.append(key)
            elif isinstance(key, Expr):
                referenced = key.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "sort() accepts column names or a ColumnRef expression."
                    )
                keys.append(next(iter(referenced)))
            else:
                raise TypeError("sort() accepts column names or ColumnRef objects.")

        desc = (
            [descending] * len(keys)
            if isinstance(descending, bool)
            else list(descending)
        )
        if len(desc) != len(keys):
            raise ValueError("sort(descending=...) length must match sort keys.")
        if nulls_last is None:
            nl = []
        elif isinstance(nulls_last, bool):
            nl = [nulls_last] * len(keys)
        else:
            nl = list(nulls_last)
        if nl and len(nl) != len(keys):
            raise ValueError("sort(nulls_last=...) length must match sort keys.")
        rust_plan = self._engine.plan_sort(
            self._rust_plan, keys, desc, nl, bool(maintain_order)
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def unique(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> DataFrame[Any]:
        rust_plan = self._engine.plan_unique(
            self._rust_plan,
            None if subset is None else list(subset),
            keep,
            bool(maintain_order),
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def duplicated(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str | bool = "first",
    ) -> DataFrame[Any]:
        """Single-column boolean frame ``duplicated`` (row-wise duplicate mask)."""
        if keep is True:
            raise ValueError("duplicated(keep=True) is invalid; use 'first' or 'last'.")
        keep_s = "none" if keep is False else str(keep)
        if keep_s not in ("first", "last", "none"):
            raise ValueError(
                "duplicated(keep=...) must be 'first', 'last', or False "
                "(pandas parity)."
            )
        rust_plan = self._engine.plan_duplicate_mask(
            self._rust_plan,
            None if subset is None else list(subset),
            keep_s,
        )
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def drop_duplicate_groups(
        self,
        subset: Sequence[str] | None = None,
    ) -> DataFrame[Any]:
        """Drop rows whose key appears in a duplicate group.

        ``subset`` selects key columns; if omitted, all columns participate.
        Same filter as pandas ``drop_duplicates(keep=False)``.
        """
        rust_plan = self._engine.plan_drop_duplicate_groups(
            self._rust_plan,
            None if subset is None else list(subset),
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def distinct(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrame[Any]:
        return self.unique(subset=subset, keep=keep)

    def drop(
        self, *columns: str | ColumnRef | Selector, strict: bool = True
    ) -> DataFrame[Any]:
        selected: list[str] = []
        for col in columns:
            if isinstance(col, str):
                selected.append(col)
            elif isinstance(col, Selector):
                selected.extend(col.resolve(self._current_field_types))
            elif isinstance(col, Expr):
                referenced = col.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "drop() accepts column names or a ColumnRef expression."
                    )
                selected.append(next(iter(referenced)))
            else:
                raise TypeError("drop() accepts column names or ColumnRef objects.")
        if not strict:
            selected = [c for c in selected if c in self._current_field_types]
        if not selected:
            return self
        rust_plan = self._engine.plan_drop(self._rust_plan, selected)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        # Preserve order: keep existing column order minus dropped columns.
        desired_order = [k for k in self._current_field_types if k in derived_fields]
        ordered: dict[str, Any] = {k: derived_fields[k] for k in desired_order}
        for k, v in derived_fields.items():
            if k not in ordered:
                ordered[k] = v
        derived_fields = ordered
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def rename(
        self, columns: Mapping[str, str], *, strict: bool = True
    ) -> DataFrame[Any]:
        rename_map = dict(columns)
        if not strict:
            rename_map = {
                k: v for k, v in rename_map.items() if k in self._current_field_types
            }
        rust_plan = self._engine.plan_rename(self._rust_plan, rename_map)
        desc = rust_plan.schema_descriptors()
        rename_prev: dict[str, Any] = dict(self._current_field_types)
        for old_name, new_name in rename_map.items():
            if old_name in self._current_field_types:
                rename_prev[new_name] = self._current_field_types[old_name]
        derived_fields = self._field_types_from_descriptors(desc, previous=rename_prev)
        # Preserve column order: renamed columns stay in the original position.
        desired_order: list[str] = []
        for name in self._current_field_types:
            desired_order.append(rename_map.get(name, name))
        ordered: dict[str, Any] = {
            k: derived_fields[k] for k in desired_order if k in derived_fields
        }
        for k, v in derived_fields.items():
            if k not in ordered:
                ordered[k] = v
        derived_fields = ordered
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def rename_with_selector(
        self,
        selector: Selector,
        fn: Callable[[str], str],
        *,
        strict: bool = True,
    ) -> DataFrame[Any]:
        """Rename columns selected by a schema-driven Selector."""
        if not isinstance(selector, Selector):
            raise TypeError("rename_with_selector(selector, ...) expects a Selector.")
        if not callable(fn):
            raise TypeError("rename_with_selector(..., fn=...) expects a callable.")
        selected = selector.resolve(self._current_field_types)
        if not selected:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_with_selector({selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map: dict[str, str] = {old: str(fn(old)) for old in selected}
        new_names = list(rename_map.values())
        if len(set(new_names)) != len(new_names):
            raise ValueError(
                "rename_with_selector(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_prefix(
        self,
        prefix: str,
        *,
        selector: Selector | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        if not isinstance(prefix, str):
            raise TypeError("rename_prefix(prefix) expects a string.")
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_prefix(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: f"{prefix}{c}" for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_prefix(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_suffix(
        self,
        suffix: str,
        *,
        selector: Selector | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        if not isinstance(suffix, str):
            raise TypeError("rename_suffix(suffix) expects a string.")
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_suffix(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: f"{c}{suffix}" for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_suffix(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_replace(
        self,
        old: str,
        new: str,
        *,
        selector: Selector | None = None,
        strict: bool = True,
        literal: bool = True,
    ) -> DataFrame[Any]:
        if not isinstance(old, str) or not isinstance(new, str):
            raise TypeError("rename_replace(old, new) expects strings.")
        if literal is not True:
            raise NotImplementedError(
                "rename_replace(literal=False) is not supported "
                "(schema-first rename only)."
            )
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_replace(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: c.replace(old, new) for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_replace(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_upper(
        self, selector: Selector | None = None, *, strict: bool = True
    ) -> DataFrame[Any]:
        """Uppercase column names for a subset selected by a schema-driven Selector."""
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_upper(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: c.upper() for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_upper(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_lower(
        self, selector: Selector | None = None, *, strict: bool = True
    ) -> DataFrame[Any]:
        """Lowercase column names for a subset selected by a schema-driven Selector."""
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_lower(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: c.lower() for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_lower(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_title(
        self, selector: Selector | None = None, *, strict: bool = True
    ) -> DataFrame[Any]:
        """Title-case column names for a subset selected by a schema-driven Selector."""
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_title(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: c.title() for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_title(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def rename_strip(
        self,
        selector: Selector | None = None,
        *,
        chars: str | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        """Strip leading/trailing characters from column names (schema-first)."""
        if chars is not None and not isinstance(chars, str):
            raise TypeError("rename_strip(chars=...) expects a string or None.")
        target = (
            list(self._current_field_types.keys())
            if selector is None
            else selector.resolve(self._current_field_types)
        )
        if selector is not None and not target:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"rename_strip(selector={selector!r}) matched no columns. "
                f"Available columns: [{available}]"
            )
        rename_map = {c: c.strip(chars) for c in target}
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError(
                "rename_strip(...) produced duplicate output column names."
            )
        return self.rename(rename_map, strict=strict)

    def slice(self, offset: int, length: int) -> DataFrame[Any]:
        rust_plan = self._engine.plan_slice(self._rust_plan, int(offset), int(length))
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def with_row_count(
        self, name: str = "row_nr", *, offset: int = 0
    ) -> DataFrame[Any]:
        """Add a deterministic row number column (Polars-style `with_row_count`)."""
        if not isinstance(name, str) or not name:
            raise TypeError("with_row_count(name=...) expects a non-empty string.")
        rust_plan = self._engine.plan_with_row_count(
            self._rust_plan, str(name), int(offset)
        )
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def head(self, n: int = 5) -> DataFrame[Any]:
        """First ``n`` rows (lazy slice).

        Materialize the result with :meth:`to_dict` or :meth:`collect`.
        **Cost:** extends the logical plan only until you materialize. See
        the EXECUTION guide (project docs) **Materialization costs**.
        """
        return self.slice(0, n)

    def limit(self, n: int = 5) -> DataFrame[Any]:
        """First ``n`` rows (Polars-style alias of :meth:`head`)."""
        return self.head(n)

    def pipe(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Call ``fn(self, *args, **kwargs)`` (pandas/Polars-style helper)."""
        if not callable(fn):
            raise TypeError("pipe(fn, ...) expects a callable.")
        return fn(self, *args, **kwargs)

    def clip(
        self,
        *,
        lower: Any | None = None,
        upper: Any | None = None,
        subset: str | Sequence[str] | Selector | None = None,
    ) -> DataFrame[Any]:
        """Clamp numeric columns to the given bounds (schema-first)."""
        from pydantable import selectors as _selectors
        from pydantable.expressions import Literal, when

        if lower is None and upper is None:
            raise ValueError("clip() requires at least one of lower=... or upper=....")

        if isinstance(subset, str):
            subset = [subset]
        if isinstance(subset, Selector):
            cols = subset.resolve(self._current_field_types)
            if not cols:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"clip(subset={subset!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            subset = cols

        targets = (
            list(subset)
            if subset is not None
            else _selectors.numeric().resolve(self._current_field_types)
        )
        if not targets:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                f"clip() matched no numeric columns. Available columns: [{available}]"
            )

        updates: dict[str, Expr] = {}
        for c in targets:
            dt = self._current_field_types.get(c)
            if dt is None:
                raise KeyError(f"clip() unknown subset column {c!r}.")
            if not _is_describe_numeric(dt):
                raise TypeError(
                    f"clip(subset=...) expects numeric columns, got {c!r} dtype={dt!r}."
                )
            expr: Expr = self.col(c)
            if lower is not None:
                lo = Literal(value=lower).cast(expr.dtype)
                expr = when(expr < lo, lo).otherwise(expr)
            if upper is not None:
                hi = Literal(value=upper).cast(expr.dtype)
                expr = when(expr > hi, hi).otherwise(expr)
            updates[c] = expr

        return self.with_columns(**updates)

    def first(self) -> DataFrame[Any]:
        """First row as a single-row DataFrame (lazy slice)."""
        return self.head(1)

    def tail(self, n: int = 5) -> DataFrame[Any]:
        """Last ``n`` rows (lazy slice). **Cost:** same idea as :meth:`head`."""
        return self.slice(-n, n)

    def last(self) -> DataFrame[Any]:
        """Last row as a single-row DataFrame (lazy slice)."""
        return self.tail(1)

    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: str | Sequence[str] | Selector | None = None,
    ) -> DataFrame[Any]:
        if isinstance(subset, str):
            subset = [subset]
        if isinstance(subset, Selector):
            subset_cols = subset.resolve(self._current_field_types)
            if not subset_cols:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"fill_null(subset={subset!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            subset = subset_cols
        if value is None and strategy is None:
            raise ValueError("fill_null() requires either value or strategy.")
        if value is not None and strategy is not None:
            raise ValueError("fill_null() accepts value or strategy, not both.")
        rust_plan = self._engine.plan_fill_null(
            self._rust_plan,
            None if subset is None else list(subset),
            value,
            strategy,
        )
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def drop_nulls(
        self,
        subset: str | Sequence[str] | Selector | None = None,
        *,
        how: str = "any",
        threshold: int | None = None,
    ) -> DataFrame[Any]:
        if isinstance(subset, str):
            subset = [subset]
        if isinstance(subset, Selector):
            subset_cols = subset.resolve(self._current_field_types)
            if not subset_cols:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"drop_nulls(subset={subset!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            subset = subset_cols
        rust_plan = self._engine.plan_drop_nulls(
            self._rust_plan,
            None if subset is None else list(subset),
            str(how),
            threshold,
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def melt(
        self,
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        if isinstance(id_vars, str):
            id_vars = [id_vars]
        if isinstance(value_vars, str):
            value_vars = [value_vars]
        if isinstance(id_vars, Selector):
            resolved = id_vars.resolve(self._current_field_types)
            if not resolved:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"melt(id_vars={id_vars!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            id_vars = resolved
        if isinstance(value_vars, Selector):
            resolved = value_vars.resolve(self._current_field_types)
            if not resolved:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"melt(value_vars={value_vars!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            value_vars = resolved
        out_data, schema_descriptors = self._engine.execute_melt(
            self._rust_plan,
            self._root_data,
            [] if id_vars is None else list(id_vars),
            None if value_vars is None else list(value_vars),
            variable_name,
            value_name,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def melt_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).as_schema(schema, validate_schema=validate_schema)

    def melt_try_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).try_as_schema(schema, validate_schema=validate_schema)

    def melt_assert_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).assert_schema(schema, validate_schema=validate_schema)

    # Aliases for parity with DataFrameModel's after-model helpers.
    def melt_as_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT]:
        return self.melt_as_schema(schema, validate_schema=validate_schema, **kwargs)

    def melt_try_as_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT] | None:
        return self.melt_try_as_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def melt_assert_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT]:
        return self.melt_assert_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def unpivot(
        self,
        *,
        index: str | Sequence[str] | Selector | None = None,
        on: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        return self.melt(
            id_vars=index,
            value_vars=on,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        )

    def unpivot_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        index: str | Sequence[str] | Selector | None = None,
        on: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.unpivot(
            index=index,
            on=on,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).as_schema(schema, validate_schema=validate_schema)

    def unpivot_try_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        index: str | Sequence[str] | Selector | None = None,
        on: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.unpivot(
            index=index,
            on=on,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).try_as_schema(schema, validate_schema=validate_schema)

    def unpivot_assert_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        index: str | Sequence[str] | Selector | None = None,
        on: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.unpivot(
            index=index,
            on=on,
            variable_name=variable_name,
            value_name=value_name,
            streaming=streaming,
        ).assert_schema(schema, validate_schema=validate_schema)

    def unpivot_as_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT]:
        return self.unpivot_as_schema(schema, validate_schema=validate_schema, **kwargs)

    def unpivot_try_as_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT] | None:
        return self.unpivot_try_as_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def unpivot_assert_model(
        self, schema: type[AfterSchemaT], *, validate_schema: bool = True, **kwargs: Any
    ) -> DataFrame[AfterSchemaT]:
        return self.unpivot_assert_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def pivot_longer(
        self,
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        names_to: str = "variable",
        values_to: str = "value",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Polars-friendly alias of :meth:`melt` (aka pivot_longer)."""
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=names_to,
            value_name=values_to,
            streaming=streaming,
        )

    def pivot_wider(
        self,
        *,
        index: str | Sequence[str] | Selector,
        names_from: str | Selector | ColumnRef,
        values_from: str | Sequence[str] | Selector,
        aggregate_function: str = "first",
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Polars-friendly alias of :meth:`pivot` (aka pivot_wider)."""
        return self.pivot(
            index=index,
            columns=names_from,
            values=values_from,
            aggregate_function=aggregate_function,
            sort_columns=sort_columns,
            separator=separator,
            streaming=streaming,
        )

    def top_k(
        self,
        n: int,
        *,
        by: str | Sequence[str],
        descending: bool = True,
        nulls_last: bool | None = None,
    ) -> DataFrame[Any]:
        """Top-k rows by sort key(s) (schema-first helper: sort then limit)."""
        keys = [by] if isinstance(by, str) else list(by)
        return self.sort(
            *keys,
            descending=[bool(descending)] * len(keys),
            nulls_last=None if nulls_last is None else [bool(nulls_last)] * len(keys),
            maintain_order=True,
        ).limit(n)

    def bottom_k(
        self,
        n: int,
        *,
        by: str | Sequence[str],
        nulls_last: bool | None = None,
    ) -> DataFrame[Any]:
        """Bottom-k rows by sort key(s) (schema-first helper: sort then limit)."""
        keys = [by] if isinstance(by, str) else list(by)
        return self.sort(
            *keys,
            descending=[False] * len(keys),
            nulls_last=None if nulls_last is None else [bool(nulls_last)] * len(keys),
            maintain_order=True,
        ).limit(n)

    def pivot(
        self,
        *,
        index: str | Sequence[str] | Selector,
        columns: str | Selector | ColumnRef,
        values: str | Sequence[str] | Selector,
        aggregate_function: str = "first",
        pivot_values: Sequence[Any] | None = None,
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        if isinstance(index, Selector):
            resolved = index.resolve(self._current_field_types)
            if not resolved:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"pivot(index={index!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            index_cols = resolved
        else:
            index_cols = [index] if isinstance(index, str) else list(index)

        if isinstance(values, Selector):
            resolved = values.resolve(self._current_field_types)
            if not resolved:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"pivot(values={values!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
            value_cols = resolved
        else:
            value_cols = [values] if isinstance(values, str) else list(values)

        if isinstance(columns, Selector):
            resolved = columns.resolve(self._current_field_types)
            if len(resolved) != 1:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    "pivot(columns=...) selector must match exactly one column; "
                    f"matched={resolved}. Available columns: [{available}]"
                )
            columns_col = resolved[0]
        elif isinstance(columns, str):
            columns_col = columns
        elif isinstance(columns, Expr):
            referenced = columns.referenced_columns()
            if len(referenced) != 1:
                raise TypeError(
                    "pivot(columns=...) expects a column name or "
                    "single-column ColumnRef; "
                    f"referenced_columns={sorted(referenced)!r}"
                )
            columns_col = next(iter(referenced))
        else:
            raise TypeError(
                "pivot(columns=...) expects a column name, Selector, or "
                "single-column ColumnRef."
            )
        if not isinstance(separator, str) or not separator:
            raise TypeError("pivot(separator=...) expects a non-empty string.")
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_pivot(
            self._rust_plan,
            self._root_data,
            index_cols,
            columns_col,
            value_cols,
            aggregate_function,
            pivot_values=pivot_values,
            sort_columns=bool(sort_columns),
            separator=str(separator),
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def explode(
        self,
        columns: str | Sequence[str] | Selector,
        *,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        if isinstance(columns, Selector):
            cols = columns.resolve(self._current_field_types)
            if not cols:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"explode(columns={columns!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
        else:
            cols = [columns] if isinstance(columns, str) else list(columns)
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_explode(
            self._rust_plan,
            self._root_data,
            cols,
            streaming=use_streaming,
            outer=outer,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def explode_outer(
        self,
        columns: str | Sequence[str] | Selector,
        *,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Explode lists; Spark-ish *outer* null/empty handling (see docs)."""
        return self.explode(columns, outer=True, streaming=streaming)

    def posexplode(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Explode one list column with a 0-based index (Spark ``posexplode``)."""
        if not isinstance(column, str) or not column:
            raise TypeError("posexplode() expects a non-empty str column name.")
        if not isinstance(pos, str) or not pos:
            raise TypeError("posexplode(pos=...) must be a non-empty str.")
        value_name = column if value is None else value
        if not isinstance(value_name, str) or not value_name:
            raise TypeError("posexplode(value=...) must be a non-empty str when set.")
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_posexplode(
            self._rust_plan,
            self._root_data,
            column,
            pos,
            value_name,
            streaming=use_streaming,
            outer=outer,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def posexplode_outer(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """``posexplode(..., outer=True)`` alias."""
        return self.posexplode(
            column, pos=pos, value=value, outer=True, streaming=streaming
        )

    def unnest(
        self, columns: str | Sequence[str] | Selector, *, streaming: bool | None = None
    ) -> DataFrame[Any]:
        if isinstance(columns, Selector):
            cols = columns.resolve(self._current_field_types)
            if not cols:
                available = ", ".join(repr(c) for c in self._current_field_types)
                raise ValueError(
                    f"unnest(columns={columns!r}) matched no columns. "
                    f"Available columns: [{available}]"
                )
        else:
            cols = [columns] if isinstance(columns, str) else list(columns)
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_unnest(
            self._rust_plan, self._root_data, cols, streaming=use_streaming
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def explode_all(self, *, streaming: bool | None = None) -> DataFrame[Any]:
        """Explode all list-typed columns (schema-driven)."""
        from pydantable import selectors as _selectors

        sel = _selectors.by_dtype(_selectors.LIST)
        matched = sel.resolve(self._current_field_types)
        if not matched:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                "explode_all() matched no list-typed columns. "
                f"Available columns: [{available}]"
            )
        return self.explode(sel, streaming=streaming)

    def unnest_all(self, *, streaming: bool | None = None) -> DataFrame[Any]:
        """Unnest all struct-typed columns (schema-driven)."""
        from pydantable import selectors as _selectors

        sel = _selectors.by_dtype(_selectors.STRUCT)
        matched = sel.resolve(self._current_field_types)
        if not matched:
            available = ", ".join(repr(c) for c in self._current_field_types)
            raise ValueError(
                "unnest_all() matched no struct-typed columns. "
                f"Available columns: [{available}]"
            )
        return self.unnest(sel, streaming=streaming)

    def join(
        self,
        other: DataFrame[Any],
        *,
        on: str | Sequence[str] | Selector | None = None,
        left_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        right_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Join two frames on key column(s); ``how`` is e.g. ``inner``, ``left``.

        ``allow_parallel`` and ``force_parallel`` match Polars' keyword names but are
        not implemented in the native engine: passing either raises
        :exc:`NotImplementedError` (see the parity scorecard in the docs).
        """
        if not isinstance(other, DataFrame):
            raise TypeError("join(other=...) expects another DataFrame.")
        if coalesce is not None and not isinstance(coalesce, bool):
            raise TypeError("join(coalesce=...) expects a bool or None.")
        if on is not None and (left_on is not None or right_on is not None):
            raise ValueError(
                "join() use either on=... or left_on=/right_on=..., not both."
            )

        def _available_cols_text(field_types: Mapping[str, Any]) -> str:
            return ", ".join(repr(c) for c in field_types)

        def _resolve_selector(
            *, sel: Selector, field_types: Mapping[str, Any], arg_name: str
        ) -> list[str]:
            matched = sel.resolve(field_types)
            if not matched:
                available = _available_cols_text(field_types)
                raise ValueError(
                    f"join({arg_name}=...) selector matched no columns. "
                    f"Available columns: [{available}]"
                )
            return matched

        def _resolve_keys(keys: str | Expr | Sequence[str | Expr] | None) -> list[str]:
            if keys is None:
                return []
            raw: list[str | Expr] = (
                [keys] if isinstance(keys, (str, Expr)) else list(keys)
            )
            out: list[str] = []
            for key in raw:
                if isinstance(key, str):
                    out.append(key)
                elif isinstance(key, Expr):
                    referenced = key.referenced_columns()
                    if len(referenced) != 1:
                        raise TypeError(
                            "join expression keys must reference exactly one column."
                        )
                    out.append(next(iter(referenced)))
                else:
                    raise TypeError(
                        "join keys must be str, Expr, or sequences thereof."
                    )
            return out

        def _base_type(tp: Any) -> Any:
            origin = get_origin(tp)
            if origin is None:
                return tp
            if origin is getattr(__import__("typing"), "Union", object()) or str(
                origin
            ).endswith("types.UnionType"):
                args = [a for a in get_args(tp) if a is not type(None)]
                if len(args) == 1:
                    return args[0]
            return tp

        if on is not None:
            if isinstance(on, Selector):
                left_keys = _resolve_selector(
                    sel=on, field_types=self._current_field_types, arg_name="on"
                )
            else:
                left_keys = [on] if isinstance(on, str) else list(on)
            missing_in_right = [
                k for k in left_keys if k not in other._current_field_types
            ]
            if missing_in_right:
                available = _available_cols_text(other._current_field_types)
                missing = ", ".join(repr(c) for c in missing_in_right)
                raise KeyError(
                    "join() unknown right join key(s): "
                    f"[{missing}]. Right available columns: [{available}]. "
                    "Hint: use left_on=.../right_on=... for differently named "
                    "key columns."
                )
            right_keys = list(left_keys)
            used_non_columnref_expr_keys = False
        else:
            used_non_columnref_expr_keys = False
            if isinstance(left_on, Selector):
                left_keys = _resolve_selector(
                    sel=left_on,
                    field_types=self._current_field_types,
                    arg_name="left_on",
                )
            else:
                left_keys = _resolve_keys(left_on)

            if isinstance(right_on, Selector):
                right_keys = _resolve_selector(
                    sel=right_on,
                    field_types=other._current_field_types,
                    arg_name="right_on",
                )
            else:
                right_keys = _resolve_keys(right_on)
            raw_left = (
                []
                if left_on is None
                else (
                    [left_on]
                    if isinstance(left_on, (str, Expr, Selector))
                    else list(left_on)
                )
            )
            raw_right = (
                []
                if right_on is None
                else (
                    [right_on]
                    if isinstance(right_on, (str, Expr, Selector))
                    else list(right_on)
                )
            )
            used_non_columnref_expr_keys = any(
                isinstance(x, Expr) and not isinstance(x, ColumnRef)
                for x in [*raw_left, *raw_right]
            )

        if validate is not None:
            v = str(validate)
            mapping = {
                "1:1": "one_to_one",
                "1:m": "one_to_many",
                "m:1": "many_to_one",
                "m:m": "many_to_many",
            }
            v = mapping.get(v, v)
            if v not in ("one_to_one", "one_to_many", "many_to_one", "many_to_many"):
                raise ValueError(
                    "join(validate=...) must be one of one_to_one, one_to_many, "
                    "many_to_one, many_to_many (or 1:1/1:m/m:1/m:m)."
                )
            if how == "cross":
                raise ValueError(
                    "cross join does not support validate=...; remove validate or "
                    "use a keyed join."
                )
            validate = v

        if join_nulls is not None and not isinstance(join_nulls, bool):
            raise TypeError("join(join_nulls=...) expects a bool or None.")
        if allow_parallel is not None or force_parallel is not None:
            # Polars join parallelism knobs are not currently exposed in the Polars
            # Rust API version pinned by pydantable-core; keep the argument surface
            # for future parity but fail explicitly for now.
            raise NotImplementedError(
                "join(allow_parallel=..., force_parallel=...) is not supported in "
                "this build."
            )

        maintain_order_norm: str | None
        if maintain_order is None:
            maintain_order_norm = None
        elif isinstance(maintain_order, bool):
            maintain_order_norm = "left" if maintain_order else "none"
        else:
            m = str(maintain_order).strip().lower()
            if m not in ("none", "left", "right"):
                raise ValueError(
                    "join(maintain_order=...) must be one of 'none', 'left', 'right', "
                    "a bool (True->'left', False->'none'), or None."
                )
            maintain_order_norm = m

        if coalesce is True:
            if how == "cross":
                raise ValueError(
                    "cross join does not support coalesce=...; remove coalesce or "
                    "use a keyed join."
                )
            if used_non_columnref_expr_keys and on is None:
                raise NotImplementedError(
                    "join(coalesce=True) is only supported for column-name keys or "
                    "simple ColumnRef expression keys."
                )
            if how in ("semi", "anti"):
                # Left-only output: coalesce has no observable effect but is accepted
                # for parity.
                pass
            elif how in ("full", "outer") and on is None and left_keys != right_keys:
                # Typed-safe subset: require exact base dtype match per key pair
                # (no casts).
                for lk, rk in zip(left_keys, right_keys, strict=True):
                    lt = self._current_field_types.get(lk)
                    rt = other._current_field_types.get(rk)
                    if lt is None or rt is None:
                        continue
                    if _base_type(lt) is not _base_type(rt):
                        raise NotImplementedError(
                            "join(coalesce=True) for full joins requires matching key "
                            "base dtypes (no casts)."
                        )

        if coalesce is False and on is None and left_keys != right_keys:
            # Typed-safe subset: only keep both key columns when it won't collide
            # with an existing left-side column name.
            for _lk, rk in zip(left_keys, right_keys, strict=True):
                if rk in self._current_field_types:
                    raise NotImplementedError(
                        "join(coalesce=False) cannot retain both key columns when the "
                        "right key name collides with an existing left column."
                    )

        if how == "cross":
            if left_keys or right_keys:
                raise ValueError("cross join does not accept on/left_on/right_on keys.")
            for name, val in (
                ("join_nulls", join_nulls),
                ("maintain_order", maintain_order),
                ("allow_parallel", allow_parallel),
                ("force_parallel", force_parallel),
            ):
                if val is not None:
                    raise ValueError(f"cross join does not support {name}=...")
        else:
            if not left_keys or not right_keys:
                raise ValueError(
                    "join() requires on=... or both left_on=... "
                    "and right_on=... for non-cross joins."
                )
            if len(left_keys) != len(right_keys):
                raise ValueError(
                    "join() left_on and right_on must have the same length."
                )

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        joined_data, schema_descriptors = self._engine.execute_join(
            self._rust_plan,
            self._root_data,
            other._rust_plan,
            other._root_data,
            left_keys,
            right_keys,
            how,
            suffix,
            validate=validate,
            coalesce=coalesce,
            join_nulls=join_nulls,
            maintain_order=maintain_order_norm,
            allow_parallel=allow_parallel,
            force_parallel=force_parallel,
            as_python_lists=True,
            streaming=use_streaming,
        )
        join_prev = previous_field_types_for_join(
            self._current_field_types,
            other._current_field_types,
            suffix=suffix,
            output_columns=list(schema_descriptors.keys()),
        )
        derived_fields = self._field_types_from_descriptors(
            schema_descriptors, previous=join_prev
        )
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return self._from_plan(
            root_data=joined_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def join_as_schema(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        on: str | Sequence[str] | Selector | None = None,
        left_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        right_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.join(
            other,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix,
            coalesce=coalesce,
            validate=validate,
            join_nulls=join_nulls,
            maintain_order=maintain_order,
            allow_parallel=allow_parallel,
            force_parallel=force_parallel,
            streaming=streaming,
        ).as_schema(schema, validate_schema=validate_schema)

    def join_try_as_schema(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        on: str | Sequence[str] | Selector | None = None,
        left_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        right_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.join(
            other,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix,
            coalesce=coalesce,
            validate=validate,
            join_nulls=join_nulls,
            maintain_order=maintain_order,
            allow_parallel=allow_parallel,
            force_parallel=force_parallel,
            streaming=streaming,
        ).try_as_schema(schema, validate_schema=validate_schema)

    def join_assert_schema(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        on: str | Sequence[str] | Selector | None = None,
        left_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        right_on: str | Expr | Sequence[str | Expr] | Selector | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.join(
            other,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix,
            coalesce=coalesce,
            validate=validate,
            join_nulls=join_nulls,
            maintain_order=maintain_order,
            allow_parallel=allow_parallel,
            force_parallel=force_parallel,
            streaming=streaming,
        ).assert_schema(schema, validate_schema=validate_schema)

    def join_as_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        return self.join_as_schema(
            other, schema, validate_schema=validate_schema, **kwargs
        )

    def join_try_as_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.join_try_as_schema(
            other, schema, validate_schema=validate_schema, **kwargs
        )

    def join_assert_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        return self.join_assert_schema(
            other, schema, validate_schema=validate_schema, **kwargs
        )

    def group_by(
        self,
        *keys: str | ColumnRef,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> GroupedDataFrame:
        """Group by key column(s); finish with :meth:`GroupedDataFrame.agg`.

        `maintain_order` / `drop_nulls` are accepted for Polars parity, but only the
        default behavior is implemented today.
        """
        selected: list[str] = []
        for key in keys:
            if isinstance(key, str):
                selected.append(key)
            elif isinstance(key, Expr):
                referenced = key.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "group_by() accepts column names or ColumnRef expressions."
                    )
                selected.append(next(iter(referenced)))
            else:
                raise TypeError(
                    "group_by() accepts column names or ColumnRef expressions."
                )
        return GroupedDataFrame(
            self,
            selected,
            maintain_order=bool(maintain_order),
            drop_nulls=bool(drop_nulls),
        )

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
    ) -> DataFrame[Any]:
        data = self.collect(as_lists=True)
        if on not in data or column not in data:
            raise KeyError("rolling_agg() requires existing on/column names.")
        by_cols = [] if by is None else list(by)
        for c in by_cols:
            if c not in data:
                raise KeyError(f"rolling_agg() unknown grouping column '{c}'.")
        n = len(data[on])
        idxs = list(range(n))
        idxs.sort(
            key=lambda i: tuple(data[c][i] for c in [*by_cols, on])  # type: ignore[misc]
        )
        out: list[Any] = [None] * n

        def _duration_seconds(v: int | str) -> float:
            if isinstance(v, int):
                return float(v)
            unit = v[-1]
            num = float(v[:-1])
            factors = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
            if unit not in factors:
                raise ValueError(
                    "rolling_agg(window_size=...) supports s/m/h/d suffix."
                )
            return num * factors[unit]

        def _to_seconds(x: Any) -> float:
            if isinstance(x, datetime):
                return x.timestamp()
            if isinstance(x, date):
                return float(datetime.combine(x, datetime.min.time()).timestamp())
            if isinstance(x, timedelta):
                return x.total_seconds()
            if isinstance(x, (int, float)):
                return float(x)
            raise TypeError(
                "rolling_agg(on=...) requires numeric/date/datetime/timedelta."
            )

        win_seconds = _duration_seconds(window_size)
        supported = {"sum", "mean", "min", "max", "count"}
        if op not in supported:
            raise ValueError(
                f"Unsupported rolling op '{op}'. Use one of {sorted(supported)}."
            )
        for pos, i in enumerate(idxs):
            current_group = tuple(data[c][i] for c in by_cols)
            current_t = _to_seconds(data[on][i])
            window_idxs: list[int] = []
            j = pos
            while j >= 0:
                k = idxs[j]
                if tuple(data[c][k] for c in by_cols) != current_group:
                    break
                if current_t - _to_seconds(data[on][k]) <= win_seconds:
                    window_idxs.append(k)
                    j -= 1
                else:
                    break
            vals = [
                data[column][k]
                for k in reversed(window_idxs)
                if data[column][k] is not None
            ]
            if len(vals) < min_periods:
                out[i] = None
                continue
            if op == "count":
                out[i] = len(vals)
            elif op == "sum":
                out[i] = sum(vals)
            elif op == "mean":
                out[i] = sum(vals) / len(vals) if vals else None
            elif op == "min":
                out[i] = min(vals) if vals else None
            else:
                out[i] = max(vals) if vals else None

        out_data = dict(data)
        out_data[out_name] = out
        fields = dict(self._current_field_types)
        in_dtype = self._current_field_types[column]
        if op == "count":
            fields[out_name] = int
        elif op == "mean":
            fields[out_name] = float | None
        elif op in {"sum", "min", "max"}:
            fields[out_name] = in_dtype
        else:
            fields[out_name] = in_dtype
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, fields
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(fields))
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def rolling_agg_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.rolling_agg(
            on=on,
            column=column,
            window_size=window_size,
            op=op,
            out_name=out_name,
            by=by,
            min_periods=min_periods,
        ).as_schema(schema, validate_schema=validate_schema)

    def rolling_agg_try_as_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.rolling_agg(
            on=on,
            column=column,
            window_size=window_size,
            op=op,
            out_name=out_name,
            by=by,
            min_periods=min_periods,
        ).try_as_schema(schema, validate_schema=validate_schema)

    def rolling_agg_assert_schema(
        self,
        schema: type[AfterSchemaT],
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
        validate_schema: bool = True,
    ) -> DataFrame[AfterSchemaT]:
        return self.rolling_agg(
            on=on,
            column=column,
            window_size=window_size,
            op=op,
            out_name=out_name,
            by=by,
            min_periods=min_periods,
        ).assert_schema(schema, validate_schema=validate_schema)

    def rolling_agg_as_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        return self.rolling_agg_as_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def rolling_agg_try_as_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT] | None:
        return self.rolling_agg_try_as_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def rolling_agg_assert_model(
        self,
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        return self.rolling_agg_assert_schema(
            schema, validate_schema=validate_schema, **kwargs
        )

    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrame:
        return DynamicGroupedDataFrame(
            self,
            index_column=index_column,
            every=every,
            period=period,
            by=[] if by is None else list(by),
        )

    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any:
        """
        Materialize this typed logical DataFrame.

        **Cost:** full engine execution on the current thread (same as :meth:`to_dict`
        when returning rows or lists). See the EXECUTION guide (project docs)
        **Materialization costs**.

        By default returns a list of Pydantic models, one per row, validated
        against :attr:`schema_type` (the current projected schema).

        Use ``as_lists=True`` for a columnar ``dict[str, list]``. Use
        :meth:`to_dict` as a readable alias for that shape.

        With ``as_numpy=True``, returns ``dict[str, numpy.ndarray]`` (requires
        ``numpy``).

        With ``streaming=True``, or when the environment variable
        ``PYDANTABLE_ENGINE_STREAMING`` is set to a truthy value (and
        ``streaming`` is omitted), Polars uses its streaming engine for
        ``collect`` where supported (best-effort; some plans fall back or error).

        """
        if as_numpy and as_lists:
            raise ValueError(
                "collect() cannot specify both as_numpy=True and as_lists=True."
            )
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        column_dict = self._materialize_columns_with_missing_optional_fallback(
            streaming=use_streaming
        )
        column_dict = _coerce_enum_columns(column_dict, self._current_field_types)
        column_dict = self._apply_io_validation_if_configured(column_dict)
        if as_lists:
            return self._column_dict_in_schema_order(column_dict)
        if as_numpy:
            import numpy as np  # type: ignore[import-not-found]

            ordered = self._column_dict_in_schema_order(column_dict)
            return {k: np.asarray(v) for k, v in ordered.items()}
        ordered = self._column_dict_in_schema_order(column_dict)
        return _rows_from_column_dict(ordered, self._current_schema_type)

    def to_dict(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> dict[str, list[Any]]:
        """Columnar materialization (alias for ``collect(as_lists=True)`` shape).

        **Cost:** full Rust execution for the current plan. See the EXECUTION guide
        (project docs).
        Pass ``streaming=`` or set ``PYDANTABLE_ENGINE_STREAMING`` like :meth:`collect`.
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        raw = self._materialize_columns_with_missing_optional_fallback(
            streaming=use_streaming
        )
        raw = _coerce_enum_columns(raw, self._current_field_types)
        raw = self._apply_io_validation_if_configured(raw)
        return self._column_dict_in_schema_order(raw)

    def _materialize_for_engine_handoff(
        self,
        *,
        materialize: Literal["columns", "rows"],
        streaming: bool | None,
        engine_streaming: bool | None,
    ) -> dict[str, list[Any]]:
        if materialize == "columns":
            return self.to_dict(streaming=streaming, engine_streaming=engine_streaming)
        if materialize == "rows":
            rows = cast("list[Any]", self.collect(streaming=streaming))
            cols: dict[str, list[Any]] = {
                name: [] for name in self._current_field_types
            }
            for r in rows:
                if hasattr(r, "model_dump"):
                    d = r.model_dump()
                elif isinstance(r, dict):
                    d = r
                else:
                    raise TypeError(
                        "materialize='rows' requires row objects that are "
                        "Pydantic models or dicts."
                    )
                for name in cols:
                    cols[name].append(d.get(name))
            return cols
        raise ValueError("materialize must be 'columns' or 'rows'.")

    def to_engine(
        self,
        target_engine: Any,
        *,
        materialize: Literal["columns", "rows"] = "columns",
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> DataFrame[Any]:
        """Materialize this frame and re-root it under *target_engine*.

        This is the supported way to flow between backends (e.g. SQL → native):
        plans are engine-defined, so switching engines is an explicit boundary.
        """
        if target_engine is None:
            raise TypeError("to_engine(target_engine=...) requires a target engine.")

        cols = self._materialize_for_engine_handoff(
            materialize=materialize,
            streaming=streaming,
            engine_streaming=engine_streaming,
        )

        schema_t = self._current_schema_type
        df_cls: Any = DataFrame[schema_t]  # type: ignore[valid-type, index]
        return df_cls(
            cols,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            engine=target_engine,
        )

    def to_native(
        self,
        *,
        materialize: Literal["columns", "rows"] = "columns",
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> DataFrame[Any]:
        """Materialize and re-root under the native Rust/Polars engine."""
        from pydantable._extension import MissingRustExtensionError
        from pydantable.engine import NativePolarsEngine, get_default_engine

        if NativePolarsEngine is None:
            raise MissingRustExtensionError(
                "Native execution is not installed. Reinstall `pydantable` "
                "(it should pull `pydantable-native`)."
            )
        # Reuse the process-wide default native engine when it's already native
        # (reduces extra engine allocations and keeps caches consistent).
        default_eng = get_default_engine()
        target = (
            default_eng
            if isinstance(default_eng, NativePolarsEngine)
            else NativePolarsEngine()
        )
        return self.to_engine(
            target,
            materialize=materialize,
            streaming=streaming,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    def to_sql_engine(
        self,
        *,
        sql_config: Any | None = None,
        sql_engine: Any | None = None,
        engine: Any | None = None,
        engine_mode: Literal["auto", "default"] = "auto",
        materialize: Literal["columns", "rows"] = "columns",
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Any:
        """Materialize and re-root under the lazy-SQL execution engine.

        Convenience helper for multi-engine workflows. This is equivalent to
        materializing to columns and constructing a `SqlDataFrame[Schema](...)`
        with the resolved SQL engine (or `engine_mode="default"`).
        """
        # Lazy import: keep optional `[sql]` stack out of import-time costs.
        from pydantable.sql_dataframe import SqlDataFrame

        cols = self._materialize_for_engine_handoff(
            materialize=materialize,
            streaming=streaming,
            engine_streaming=engine_streaming,
        )

        # Construct via SqlDataFrame[Schema] so the new frame is typed.
        df_cls: Any = SqlDataFrame[self._current_schema_type]  # type: ignore[valid-type, index]
        return df_cls(
            cols,
            sql_config=sql_config,
            sql_engine=sql_engine,
            engine=engine,
            engine_mode=engine_mode,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    def to_mongo_engine(
        self,
        *,
        engine: Any | None = None,
        engine_mode: Literal["auto", "default"] = "auto",
        materialize: Literal["columns", "rows"] = "columns",
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Any:
        """Materialize and re-root under the Mongo execution engine.

        This is a convenience wrapper for multi-engine workflows. It materializes
        the current frame (columnar by default) and constructs a `MongoDataFrame`
        with the resolved engine.
        """
        from pydantable.engine import get_default_engine
        from pydantable.mongo_dataframe import (
            MongoDataFrame,
            _import_mongo_engine_types,
        )

        MongoPydantableEngine, _MongoRoot = _import_mongo_engine_types()
        if engine is not None:
            resolved = engine
        elif engine_mode == "default":
            resolved = get_default_engine()
        else:
            resolved = MongoPydantableEngine()

        schema_t = self._current_schema_type
        df_cls: Any = MongoDataFrame[schema_t]  # type: ignore[valid-type, index]
        cols = self._materialize_for_engine_handoff(
            materialize=materialize,
            streaming=streaming,
            engine_streaming=engine_streaming,
        )
        return df_cls(
            cols,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            engine=resolved,
        )

    def to_spark_engine(
        self,
        *,
        engine: Any | None = None,
        engine_mode: Literal["auto", "default"] = "auto",
        materialize: Literal["columns", "rows"] = "columns",
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Any:
        """Materialize and re-root under the Spark execution engine (raikou-core)."""
        from pydantable.engine import get_default_engine
        from pydantable.spark_dataframe import (
            SparkDataFrame,
            _import_spark_engine_types,
        )

        SparkExecutionEngine, _SparkRoot = _import_spark_engine_types()
        if engine is not None:
            resolved = engine
        elif engine_mode == "default":
            resolved = get_default_engine()
        else:
            resolved = SparkExecutionEngine()

        schema_t = self._current_schema_type
        df_cls: Any = SparkDataFrame[schema_t]  # type: ignore[valid-type, index]
        cols = self._materialize_for_engine_handoff(
            materialize=materialize,
            streaming=streaming,
            engine_streaming=engine_streaming,
        )
        return df_cls(
            cols,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            engine=resolved,
        )

    def null_count(self) -> dict[str, int]:
        """Count nulls per column (materializes via :meth:`to_dict`)."""
        d = self.to_dict()
        return {k: sum(v is None for v in vs) for k, vs in d.items()}

    def is_empty(self) -> bool:
        """True if the materialized result has zero rows.

        This is eager (materializes via :meth:`to_dict`).
        """
        d = self.to_dict()
        if not d:
            return True
        return len(next(iter(d.values()))) == 0

    def shift(self, periods: int = 1) -> DataFrame[Any]:
        """Shift every column by `periods` rows.

        This is eager (materializes via :meth:`to_dict`).
        """
        p = int(periods)
        d = self.to_dict()
        if not d:
            return self
        n = len(next(iter(d.values())))
        out: dict[str, list[Any]] = {}
        for k, vs in d.items():
            if p == 0:
                out[k] = list(vs)
            elif p > 0:
                out[k] = [None] * min(p, n) + list(vs[: max(0, n - p)])
            else:
                q = -p
                out[k] = list(vs[q:]) + [None] * min(q, n)
        fields = dict(self._current_field_types)
        schema_t = make_derived_schema_type(self._current_schema_type, fields)
        plan = self._engine.make_plan(field_types_for_rust(fields))
        return self._from_plan(
            root_data=out,
            root_schema_type=schema_t,
            current_schema_type=schema_t,
            rust_plan=plan,
            engine=self._engine,
        )

    def sample(
        self,
        *,
        n: int | None = None,
        fraction: float | None = None,
        seed: int | None = None,
        with_replacement: bool = False,
    ) -> DataFrame[Any]:
        """Sample rows (eager; materializes via :meth:`to_dict`)."""
        if n is not None and fraction is not None:
            raise ValueError("sample() accepts n or fraction, not both.")
        if n is None and fraction is None:
            raise ValueError("sample() requires n or fraction.")
        d = self.to_dict()
        if not d:
            return self
        row_count = len(next(iter(d.values())))
        if fraction is not None:
            f = float(fraction)
            if f < 0 or f > 1:
                raise ValueError("sample(fraction=...) must be between 0 and 1.")
            n = int(row_count * f)
        assert n is not None
        if n < 0:
            raise ValueError("sample(n=...) must be >= 0.")
        rng = random.Random(seed)
        if with_replacement:
            idxs = [rng.randrange(row_count) for _ in range(n)]
        else:
            idxs = list(range(row_count))
            rng.shuffle(idxs)
            idxs = idxs[: min(n, row_count)]
        out: dict[str, list[Any]] = {k: [vs[i] for i in idxs] for k, vs in d.items()}
        fields = dict(self._current_field_types)
        schema_t = make_derived_schema_type(self._current_schema_type, fields)
        plan = self._engine.make_plan(field_types_for_rust(fields))
        return self._from_plan(
            root_data=out,
            root_schema_type=schema_t,
            current_schema_type=schema_t,
            rust_plan=plan,
            engine=self._engine,
        )

    def write_parquet(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
        partition_by: tuple[str, ...] | list[str] | None = None,
        mkdir: bool = True,
    ) -> None:
        """Write this lazy plan to Parquet without a Python ``dict[str, list]``.

        Optional ``streaming=`` (or ``PYDANTABLE_ENGINE_STREAMING``) uses Polars
        streaming collect before writing, when supported.

        ``write_kwargs`` may include Polars writer options such as ``compression``,
        ``row_group_size``, ``data_page_size``, ``statistics``, ``parallel``.

        ``partition_by`` is an optional list of column names; when set, ``path`` is a
        **dataset root directory** and rows are written as hive-style
        ``col=value/.../00000000.parquet`` shards (partition columns are omitted from
        each file). Use ``mkdir=False`` only if the root directory already exists.
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        self._engine.write_parquet(
            self._rust_plan,
            self._root_data,
            str(path),
            streaming=use_streaming,
            write_kwargs=write_kwargs,
            partition_by=partition_by,
            mkdir=mkdir,
        )

    def write_csv(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        separator: str = ",",
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Write this lazy plan to CSV from Rust (no Python column dict)."""
        if len(separator) != 1:
            raise ValueError("write_csv separator must be a single character.")
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        self._engine.write_csv(
            self._rust_plan,
            self._root_data,
            str(path),
            streaming=use_streaming,
            separator=ord(separator),
            write_kwargs=write_kwargs,
        )

    def write_ipc(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Write Arrow IPC file from Rust.

        ``compression`` is ``None``, ``'lz4'``, or ``'zstd'``.
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        self._engine.write_ipc(
            self._rust_plan,
            self._root_data,
            str(path),
            streaming=use_streaming,
            compression=compression,
            write_kwargs=write_kwargs,
        )

    def write_ndjson(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Write newline-delimited JSON from Rust.

        ``write_kwargs`` may include ``json_format``.
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        self._engine.write_ndjson(
            self._rust_plan,
            self._root_data,
            str(path),
            streaming=use_streaming,
            write_kwargs=write_kwargs,
        )

    def collect_batches(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> list[Any]:
        """Return Polars ``DataFrame`` chunks after one engine collect.

        See the EXECUTION guide (project docs).
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        return self._engine.collect_batches(
            self._rust_plan,
            self._root_data,
            batch_size=batch_size,
            streaming=use_streaming,
        )

    def stream(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Iterator[dict[str, list[Any]]]:
        """Yield ``dict[str, list]`` row chunks after one full engine collect.

        Synchronous counterpart of :meth:`astream` for **sync** route handlers and
        ``StreamingResponse`` iterators that must not ``await``. Same semantics as
        :meth:`collect_batches` (full collect, then slice — not out-of-core
        streaming). Requires ``pip install 'pydantable[polars]'`` for chunk
        conversion.

        **FastAPI:** use ``stream()`` from ``def`` routes; use ``async for`` over
        ``astream()`` from ``async def`` routes when you want the event loop free
        between chunks.
        """
        try:
            importlib.import_module("polars")
        except ImportError as e:
            raise ImportError(
                "polars is required for stream(). Install with: "
                "pip install 'pydantable[polars]'"
            ) from e
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        pl_batches = self._engine.collect_batches(
            self._rust_plan,
            self._root_data,
            batch_size=batch_size,
            streaming=use_streaming,
        )
        for pl_df in pl_batches:
            yield pl_df.to_dict(as_series=False)

    def to_polars(
        self, *, streaming: bool | None = None, engine_streaming: bool | None = None
    ) -> Any:
        """
        Materialize as a Polars ``DataFrame`` (requires the optional ``polars``
        Python package: ``pip install 'pydantable[polars]'``).

        **Cost:** builds a columnar dict via the Rust engine, then a Polars frame.
        See the EXECUTION guide (project docs). Optional ``streaming=`` matches
        :meth:`collect`.
        """
        try:
            pl = importlib.import_module("polars")
        except ImportError as e:
            raise ImportError(
                "polars is required for to_polars(). Install with: "
                "pip install 'pydantable[polars]'"
            ) from e
        return pl.DataFrame(
            self.to_dict(streaming=streaming, engine_streaming=engine_streaming)
        )

    def to_arrow(
        self, *, streaming: bool | None = None, engine_streaming: bool | None = None
    ) -> Any:
        """
        Materialize as a PyArrow ``Table`` (requires the optional ``pyarrow``
        package: ``pip install 'pydantable[arrow]'``).

        This runs the same Rust execution path as :meth:`to_dict`, then builds
        Arrow arrays from Python lists—it is not a zero-copy export of internal
        buffers. **Cost:** same materialization class as :meth:`to_dict`.
        Optional ``streaming=`` matches :meth:`collect`.
        """
        try:
            pa = importlib.import_module("pyarrow")
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for to_arrow(). Install with: "
                "pip install 'pydantable[arrow]'"
            ) from e
        return pa.Table.from_pydict(
            self.to_dict(streaming=streaming, engine_streaming=engine_streaming)
        )

    def __dataframe__(
        self, *, nan_as_null: bool = False, allow_copy: bool = True
    ) -> Any:
        """
        Python DataFrame Interchange Protocol export (for Streamlit and friends).

        This materializes the current plan to a PyArrow ``Table`` (same cost class as
        :meth:`to_arrow`) and then delegates to Arrow's protocol implementation.

        Requires the optional ``pyarrow`` dependency:
        ``pip install 'pydantable[arrow]'``.
        """
        table = self.to_arrow()
        # PyArrow implements the interchange protocol on Table; delegate to it.
        return table.__dataframe__(nan_as_null=nan_as_null, allow_copy=allow_copy)

    def __dataframe_consortium_standard__(self, api_version: str | None = None) -> Any:
        """
        Entry point to the Dataframe API Consortium Standard (optional).

        This is distinct from the interchange protocol (`__dataframe__`). When the
        optional `dataframe-api-compat` package is installed, this returns a
        standards-compliant wrapper object that exposes the Consortium's draft
        DataFrame API.
        """
        try:
            import dataframe_api_compat  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "dataframe-api-compat is required for "
                "__dataframe_consortium_standard__(). "
                "Install with: pip install 'dataframe-api-compat'"
            ) from e
        try:
            import pandas as pd  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "pandas is required for pydantable's "
                "__dataframe_consortium_standard__() implementation. "
                "Install with: pip install 'pydantable[pandas]'"
            ) from e

        pdf = pd.DataFrame(self.to_dict())
        converter = (
            dataframe_api_compat.pandas_standard.convert_to_standard_compliant_dataframe
        )
        return converter(pdf, api_version=api_version)

    async def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`collect`: same semantics.

        When :meth:`~pydantable.engine.protocols.ExecutionEngine.has_async_execute_plan`
        is true on this frame's engine, work is awaited via a native async path
        (Rust/Tokio when using :class:`~pydantable.engine.native.NativePolarsEngine`);
        otherwise the same logic runs in :func:`asyncio.to_thread` or in ``executor``.

        Cancelling the awaiting task does **not** cancel in-flight Rust/Polars
        execution.
        """
        if as_numpy and as_lists:
            raise ValueError(
                "collect() cannot specify both as_numpy=True and as_lists=True."
            )
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        column_dict = await self._materialize_columns_async(
            streaming=use_streaming, executor=executor
        )
        column_dict = self._column_dict_in_schema_order(column_dict)
        if as_lists:
            return column_dict
        if as_numpy:
            import numpy as np  # type: ignore[import-not-found]

            return {k: np.asarray(v) for k, v in column_dict.items()}
        return _rows_from_column_dict(column_dict, self._current_schema_type)

    async def ato_dict(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> dict[str, list[Any]]:
        """Async version of :meth:`to_dict` (see :meth:`acollect`)."""
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        raw = await self._materialize_columns_async(
            streaming=use_streaming, executor=executor
        )
        return self._column_dict_in_schema_order(raw)

    async def ato_polars(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`to_polars` (see :meth:`acollect`).

        This still materializes a columnar Python dict first, then builds the
        Polars frame—same copies as the synchronous path.
        """
        try:
            pl = importlib.import_module("polars")
        except ImportError as e:
            raise ImportError(
                "polars is required for to_polars(). Install with: "
                "pip install 'pydantable[polars]'"
            ) from e
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        d = await self._materialize_columns_async(
            streaming=use_streaming, executor=executor
        )
        return pl.DataFrame(d)

    async def ato_arrow(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`to_arrow` (see :meth:`acollect`).

        Same materialization and copies as the synchronous path.
        """
        try:
            pa = importlib.import_module("pyarrow")
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for to_arrow(). Install with: "
                "pip install 'pydantable[arrow]'"
            ) from e
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        d = await self._materialize_columns_async(
            streaming=use_streaming, executor=executor
        )
        return pa.Table.from_pydict(d)

    def submit(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> ExecutionHandle:
        """Background :meth:`collect`; await :meth:`ExecutionHandle.result`."""

        def _run() -> Any:
            return self.collect(
                as_lists=as_lists,
                as_numpy=as_numpy,
                streaming=streaming,
                engine_streaming=engine_streaming,
            )

        if executor is not None:
            fut: concurrent.futures.Future[Any] = executor.submit(_run)
        else:
            fut = concurrent.futures.Future[Any]()

            def _bg() -> None:
                # Mirror ThreadPoolExecutor semantics: if the user cancels the
                # handle before work starts, do not execute or attempt to set
                # results on a cancelled future.
                if not fut.set_running_or_notify_cancel():
                    return
                try:
                    fut.set_result(_run())
                except Exception as e:
                    # If the future was cancelled mid-flight, avoid raising
                    # InvalidStateError from set_exception on a cancelled future.
                    if fut.cancelled():
                        return
                    fut.set_exception(e)

            threading.Thread(target=_bg, daemon=True).start()
        return ExecutionHandle(fut)

    async def astream(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """Yield ``dict[str, list]`` chunks after one full engine collect.

        Async counterpart of :meth:`stream` for ``async def`` handlers. See
        :meth:`stream` for semantics and FastAPI usage.
        """
        use_streaming = _resolve_engine_streaming(
            streaming=streaming,
            engine_streaming=engine_streaming,
            default=self._engine_streaming_default,
        )
        from pydantable.engine._capability import prefer_native_async_collect_batches

        if prefer_native_async_collect_batches(self._engine):
            pl_batches = await self._engine.async_collect_plan_batches(
                self._rust_plan,
                self._root_data,
                batch_size=batch_size,
                streaming=use_streaming,
            )
        else:
            pl_batches = await _materialize_in_thread(
                functools.partial(
                    self.collect_batches,
                    batch_size=batch_size,
                    streaming=streaming,
                    engine_streaming=engine_streaming,
                ),
                executor=executor,
            )
        try:
            importlib.import_module("polars")
        except ImportError as e:
            raise ImportError(
                "polars is required for astream() (Polars chunk type). Install with: "
                "pip install 'pydantable[polars]'"
            ) from e
        for pl_df in pl_batches:
            col = await _materialize_in_thread(
                functools.partial(pl_df.to_dict, as_series=False),
                executor=executor,
            )
            yield col

    def explain(
        self,
        *,
        format: Literal["text", "json"] = "text",
        streaming: bool | None = None,
    ) -> str | dict[str, Any]:
        """
        Introspect the current logical plan.

        - `format="text"` returns a compact, stable string summary.
        - `format="json"` returns a JSON-serializable `dict`.

        This is a *plan* view, not an execution trace; it does not materialize data.
        """
        from pydantable.plan import explain as _explain

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        root_kind = (
            "scan_file_root" if _is_scan_file_root(self._root_data) else "in_memory"
        )
        return _explain(
            self._rust_plan,
            format=format,
            engine_streaming=use_streaming,
            root_data_kind=root_kind,
        )

    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrame[Any]],
        *,
        how: str = "vertical",
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        """Stack or otherwise combine two or more frames (see Rust ``how`` values)."""
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrame inputs.")
        base = dfs[0]
        out_data = base._root_data
        out_schema_type = base._current_schema_type
        merged_ft = dict(base._current_field_types)
        out_plan = base._rust_plan
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=base._engine_streaming_default
        )
        for df in dfs[1:]:
            out_data, schema_descriptors = base._engine.execute_concat(
                out_plan,
                out_data,
                df._rust_plan,
                df._root_data,
                how,
                as_python_lists=True,
                streaming=use_streaming,
            )
            derived_fields = schema_from_descriptors(schema_descriptors)
            merged_ft = merge_field_types_preserving_identity(
                merged_ft, schema_descriptors, derived_fields
            )
            out_schema_type = make_derived_schema_type(out_schema_type, merged_ft)
            out_plan = base._engine.make_plan(field_types_for_rust(merged_ft))
        return cls._from_plan(
            root_data=out_data,
            root_schema_type=out_schema_type,
            current_schema_type=out_schema_type,
            rust_plan=out_plan,
            engine=base._engine,
        )
