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
import warnings
from collections.abc import Mapping
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
    from collections.abc import Callable, Iterator, Sequence
    from concurrent.futures import Executor


SchemaT = TypeVar("SchemaT", bound=BaseModel)
AfterSchemaT = TypeVar("AfterSchemaT", bound=BaseModel)


class _ColumnNamespace(Generic[SchemaT]):
    __slots__ = ("_df",)

    def __init__(self, df: DataFrame[SchemaT]) -> None:
        self._df = df

    def __getattr__(self, name: str) -> ColumnRef:
        if name in self._df._current_field_types:
            return self._df._col_by_name(name)
        raise AttributeError(name)

    def __dir__(self) -> list[str]:  # pragma: no cover
        return sorted(set(super().__dir__()) | set(self._df._current_field_types))


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
        if cls._schema_type is None:
            raise TypeError(
                "Use DataFrame[SchemaType].read_* to construct from a lazy file read."
            )
        eng = get_default_engine()
        plan = eng.make_plan(field_types_for_rust(schema_field_types(cls._schema_type)))
        df = cls._from_plan(
            root_data=root,
            root_schema_type=cls._schema_type,
            current_schema_type=cls._schema_type,
            rust_plan=plan,
            engine=eng,
        )
        df._io_validation_enabled = True
        df._io_validation_trusted_mode = trusted_mode
        df._io_validation_fill_missing_optional = fill_missing_optional
        df._io_validation_ignore_errors = bool(ignore_errors)
        df._io_validation_on_validation_errors = on_validation_errors
        df._engine_streaming_default = engine_streaming
        return df

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
        from pydantable.io import read_parquet as _read_parquet

        return cls._from_scan_root(
            _read_parquet(path, columns=columns, **scan_kwargs),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_parquet as _aread_parquet

        root = await _aread_parquet(
            path, columns=columns, executor=executor, **scan_kwargs
        )
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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

        See {doc}`DATA_IO_SOURCES`.
        """
        from pydantable.io import read_parquet_url as _read_parquet_url

        return cls._from_scan_root(
            _read_parquet_url(
                url, experimental=experimental, columns=columns, **kwargs
            ),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_parquet_url as _aread_parquet_url

        root = await _aread_parquet_url(
            url,
            experimental=experimental,
            columns=columns,
            executor=executor,
            **kwargs,
        )
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import read_csv as _read_csv

        return cls._from_scan_root(
            _read_csv(path, columns=columns, **scan_kwargs),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_csv as _aread_csv

        root = await _aread_csv(path, columns=columns, executor=executor, **scan_kwargs)
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import read_ndjson as _read_ndjson

        return cls._from_scan_root(
            _read_ndjson(path, columns=columns, **scan_kwargs),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_ndjson as _aread_ndjson

        root = await _aread_ndjson(
            path, columns=columns, executor=executor, **scan_kwargs
        )
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import read_json as _read_json

        return cls._from_scan_root(
            _read_json(path, columns=columns, **scan_kwargs),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_json as _aread_json

        root = await _aread_json(
            path, columns=columns, executor=executor, **scan_kwargs
        )
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import read_ipc as _read_ipc

        return cls._from_scan_root(
            _read_ipc(path, columns=columns, **scan_kwargs),
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io import aread_ipc as _aread_ipc

        root = await _aread_ipc(path, columns=columns, executor=executor, **scan_kwargs)
        return cls._from_scan_root(
            root,
            engine_streaming=engine_streaming,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
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
        from pydantable.io.iter_file import iter_parquet as _iter

        for cols_dict in _iter(path, batch_size=batch_size, columns=columns):
            yield cls(
                cols_dict,
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
        from pydantable.io.iter_file import iter_ipc as _iter

        for cols_dict in _iter(source, batch_size=batch_size, as_stream=as_stream):
            yield cls(
                cols_dict,
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
        from pydantable.io.iter_file import iter_csv as _iter

        for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
            yield cls(
                cols_dict,
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
        from pydantable.io.iter_file import iter_ndjson as _iter

        for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
            yield cls(
                cols_dict,
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
        from pydantable.io.iter_file import iter_json_lines as _iter

        for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
            yield cls(
                cols_dict,
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
        :attr:`shape` only. See {doc}`EXECUTION` **Materialization costs**.
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
        ``n_unique`` scans all non-null strings. See {doc}`EXECUTION`.
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
        See {doc}`EXECUTION`.

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

    def _col_by_name(self, name: str) -> ColumnRef:
        if name not in self._current_field_types:
            raise KeyError(f"Unknown column {name!r} for current schema.")
        return ColumnRef(name=name, dtype=self._current_field_types[name])

    @property
    def col(self) -> _ColumnNamespace[SchemaT]:
        """Typed column namespace: use ``df.col.some_field`` (no ``df.some_field``)."""
        return _ColumnNamespace(self)

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
        on that slice. See {doc}`EXECUTION` **Jupyter / HTML**.
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

    @staticmethod
    def _schema_field_types(schema_type: type[BaseModel]) -> dict[str, Any]:
        return schema_field_types(schema_type)

    def _assert_after_schema(
        self,
        after_schema_type: type[BaseModel],
        *,
        derived_fields: Mapping[str, Any],
        op_name: str,
    ) -> None:
        expected = self._schema_field_types(after_schema_type)
        got = dict(derived_fields)
        if set(expected) != set(got) or any(expected[k] != got[k] for k in expected):
            missing = sorted(set(expected) - set(got))
            extra = sorted(set(got) - set(expected))
            mismatched = sorted(
                k for k in set(expected) & set(got) if expected[k] != got[k]
            )
            parts: list[str] = []
            if missing:
                parts.append(f"missing={missing}")
            if extra:
                parts.append(f"extra={extra}")
            if mismatched:
                parts.append(f"mismatched_types={mismatched}")
            details = "; ".join(parts) if parts else "schema mismatch"
            raise TypeError(
                f"{op_name}(AfterSchema, ...) schema mismatch vs AfterSchema: {details}"
            )

    def with_columns(self, *exprs: Any, **new_columns: Expr | Any) -> DataFrame[Any]:
        raise TypeError(
            "with_columns() is removed in pydantable 2.0 strict mode. "
            "Use with_columns_as(AfterSchema, ...) so the output schema is explicit."
        )

    def with_columns_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *exprs: Any,
        **new_columns: Expr | Any,
    ) -> DataFrame[AfterSchemaT]:
        """Add or replace columns with an explicit output schema."""
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("with_columns_as(after_schema_type, ...) expects a schema.")

        rust_columns: dict[str, Any] = {}

        for item in exprs:
            if not isinstance(item, AliasedExpr):
                raise TypeError(
                    "with_columns_as() positional args must be Expr.alias('name') "
                    "(AliasedExpr)."
                )
            if item.name in rust_columns or item.name in new_columns:
                raise ValueError(
                    f"with_columns_as() duplicate output column {item.name!r}."
                )
            rust_columns[item.name] = item.expr._rust_expr

        for name, value in new_columns.items():
            if isinstance(value, Expr):
                rust_columns[name] = value._rust_expr
            else:
                rust_columns[name] = self._engine.make_literal(value=value)

        rust_plan = self._engine.plan_with_columns(self._rust_plan, rust_columns)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="with_columns_as"
        )

        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=self._root_data,
                root_schema_type=self._root_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

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
        updates: dict[str, Expr] = {
            c: self._col_by_name(c).cast(dtype) for c in selected
        }
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
        raise TypeError(
            "select_schema(...) is removed in pydantable 2.0 strict mode "
            "(dynamic column sets are forbidden)."
        )

    def select(self, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        raise TypeError(
            "select() is removed in pydantable 2.0 strict mode. "
            "Use select_as(AfterSchema, ...) so the output schema is explicit."
        )

    def select_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *cols: ColumnRef | Expr | AliasedExpr,
        **named: Any,
    ) -> DataFrame[AfterSchemaT]:
        """Project columns / compute globals with an explicit output schema."""
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("select_as(after_schema_type, ...) expects a schema.")

        named_items: list[tuple[str, Any]] = []
        for name, e in named.items():
            if not isinstance(e, Expr):
                raise TypeError(
                    "select_as() keyword arguments must be Expr instances "
                    "(global aggregates)."
                )
            named_items.append((name, e._rust_expr))

        aggs: list[tuple[str, Any]] = []
        projects: list[str] = []
        computed: dict[str, Any] = {}
        for col in cols:
            if isinstance(col, AliasedExpr):
                computed[col.name] = col.expr._rust_expr
                projects.append(col.name)
                continue
            if isinstance(col, ColumnRef):
                projects.append(col._column_name)  # type: ignore[attr-defined]
                continue
            if isinstance(col, Expr) and self._engine.expr_is_global_agg(
                col._rust_expr
            ):
                alias = self._engine.expr_global_default_alias(col._rust_expr)
                if alias is None:
                    raise TypeError(
                        "global aggregate in select_as() is missing a default "
                        "output name."
                    )
                aggs.append((alias, col._rust_expr))
                continue
            raise TypeError(
                "select_as() accepts ColumnRef, Expr.alias('name') (AliasedExpr), "
                "or global aggregate Expr values."
            )

        if named_items and (projects or aggs):
            raise TypeError(
                "select_as() cannot mix keyword aggregates with positional projections."
            )
        if aggs and projects:
            raise TypeError(
                "select_as() cannot mix global aggregates with plain projections."
            )

        if named_items:
            rust_plan = self._engine.plan_global_select(self._rust_plan, named_items)
        elif aggs:
            rust_plan = self._engine.plan_global_select(self._rust_plan, aggs)
        else:
            if not projects:
                raise ValueError("select_as() requires at least one column.")
            rust_plan = self._rust_plan
            if computed:
                rust_plan = self._engine.plan_with_columns(rust_plan, computed)
            rust_plan = self._engine.plan_select(rust_plan, projects)

        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="select_as"
        )

        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=self._root_data,
                root_schema_type=self._root_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

    def select_all(self) -> DataFrame[Any]:
        raise TypeError("select_all() is removed in pydantable 2.0 strict mode.")

    def select_prefix(self, prefix: str) -> DataFrame[Any]:
        raise TypeError(
            "select_prefix(...) is removed in pydantable 2.0 strict mode "
            "(dynamic column sets are forbidden)."
        )

    def select_suffix(self, suffix: str) -> DataFrame[Any]:
        raise TypeError(
            "select_suffix(...) is removed in pydantable 2.0 strict mode "
            "(dynamic column sets are forbidden)."
        )

    def _resolve_column_names_or_selector(
        self, item: str | Selector, *, arg_name: str
    ) -> list[str]:
        raise TypeError(
            "Dynamic name/Selector resolution is removed in pydantable 2.0 strict mode."
        )

    def reorder_columns(self, order: Sequence[str | Selector]) -> DataFrame[Any]:
        raise TypeError(
            "reorder_columns(...) is removed in pydantable 2.0 strict mode."
        )

    def select_first(self, *cols_or_selectors: str | Selector) -> DataFrame[Any]:
        raise TypeError("select_first(...) is removed in pydantable 2.0 strict mode.")

    def select_last(self, *cols_or_selectors: str | Selector) -> DataFrame[Any]:
        raise TypeError("select_last(...) is removed in pydantable 2.0 strict mode.")

    def move(
        self,
        cols_or_selector: str | Selector,
        *,
        before: str | None = None,
        after: str | None = None,
    ) -> DataFrame[Any]:
        raise TypeError("move(...) is removed in pydantable 2.0 strict mode.")

    def filter(self, condition: Expr) -> DataFrame[Any]:
        """Keep rows where the boolean ``condition`` is true."""
        if not isinstance(condition, Expr):
            raise TypeError("filter(condition) expects an Expr.")

        rust_plan = self._engine.plan_filter(self._rust_plan, condition._rust_expr)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

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

    def drop(self, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        raise TypeError(
            "drop() is removed in pydantable 2.0 strict mode. "
            "Use drop_as(AfterSchema, ...) so the output schema is explicit."
        )

    def drop_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *columns: ColumnRef,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("drop_as(after_schema_type, ...) expects a schema.")
        selected: list[str] = []
        for col in columns:
            if not isinstance(col, ColumnRef):
                raise TypeError("drop_as(..., *columns) expects ColumnRef values.")
            referenced = col.referenced_columns()
            if len(referenced) != 1:
                raise TypeError("drop_as() expects single-column references.")
            selected.append(next(iter(referenced)))
        if not selected:
            raise ValueError("drop_as() requires at least one column.")
        rust_plan = self._engine.plan_drop(self._rust_plan, selected)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="drop_as"
        )
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=self._root_data,
                root_schema_type=self._root_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

    def rename(self, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        raise TypeError(
            "rename() is removed in pydantable 2.0 strict mode. "
            "Use rename_as(AfterSchema, ...) with ColumnRef keys."
        )

    def rename_as(
        self,
        after_schema_type: type[AfterSchemaT],
        columns: Mapping[ColumnRef, str],
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("rename_as(after_schema_type, ...) expects a schema.")
        if not isinstance(columns, Mapping):
            raise TypeError("rename_as(..., columns=...) expects a mapping.")
        rename_map: dict[str, str] = {}
        for old_ref, new_name in columns.items():
            if not isinstance(old_ref, ColumnRef):
                raise TypeError("rename_as mapping keys must be ColumnRef values.")
            referenced = old_ref.referenced_columns()
            if len(referenced) != 1:
                raise TypeError("rename_as mapping keys must be single-column refs.")
            old_name = next(iter(referenced))
            if not isinstance(new_name, str) or not new_name:
                raise TypeError("rename_as mapping values must be non-empty strings.")
            rename_map[old_name] = new_name
        if len(set(rename_map.values())) != len(rename_map):
            raise ValueError("rename_as(...) produced duplicate output column names.")

        rust_plan = self._engine.plan_rename(self._rust_plan, rename_map)
        desc = rust_plan.schema_descriptors()
        rename_prev: dict[str, Any] = dict(self._current_field_types)
        for old_name, new_name in rename_map.items():
            if old_name in self._current_field_types:
                rename_prev[new_name] = self._current_field_types[old_name]
        derived_fields = self._field_types_from_descriptors(desc, previous=rename_prev)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="rename_as"
        )
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=self._root_data,
                root_schema_type=self._root_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

    def rename_with_selector(
        self,
        selector: Selector,
        fn: Callable[[str], str],
        *,
        strict: bool = True,
    ) -> DataFrame[Any]:
        raise TypeError(
            "rename_with_selector(...) is removed in pydantable 2.0 strict mode "
            "(dynamic schema changes and Python callables are forbidden). "
            "Use rename_as(AfterSchema, ...) with explicit ColumnRef mappings."
        )

    def rename_prefix(
        self,
        prefix: str,
        *,
        selector: Selector | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        raise TypeError(
            "rename_prefix(...) is removed in pydantable 2.0 strict mode. "
            "Use rename_as(AfterSchema, ...) with explicit mappings."
        )

    def rename_suffix(
        self,
        suffix: str,
        *,
        selector: Selector | None = None,
        strict: bool = True,
    ) -> DataFrame[Any]:
        raise TypeError(
            "rename_suffix(...) is removed in pydantable 2.0 strict mode. "
            "Use rename_as(AfterSchema, ...) with explicit mappings."
        )

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
        {doc}`EXECUTION` **Materialization costs**.
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
            expr: Expr = self._col_by_name(c)
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
        if isinstance(subset, Selector):
            raise TypeError(
                "drop_nulls(subset=Selector) is removed in pydantable 2.0 strict mode."
            )
        if isinstance(subset, str):
            subset = [subset]
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
        raise TypeError(
            "melt()/unpivot()/pivot_*() are removed in pydantable 2.0 strict mode "
            "(reshape output schemas depend on runtime values)."
        )

    def _colref_names(self, cols: Sequence[ColumnRef], *, arg_name: str) -> list[str]:
        out: list[str] = []
        for c in cols:
            if not isinstance(c, ColumnRef):
                raise TypeError(f"{arg_name} expects ColumnRef values.")
            referenced = c.referenced_columns()
            if len(referenced) != 1:
                raise TypeError(f"{arg_name} expects single-column ColumnRef values.")
            out.append(next(iter(referenced)))
        return out

    def melt_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *,
        id_vars: Sequence[ColumnRef],
        value_vars: Sequence[ColumnRef] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
    ) -> DataFrame[AfterSchemaT]:
        """Strict melt with explicit schema and ColumnRef inputs."""
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("melt_as(after_schema_type, ...) expects a schema.")
        if not id_vars:
            raise ValueError("melt_as(id_vars=...) requires at least one id column.")
        if not isinstance(variable_name, str) or not variable_name:
            raise TypeError("melt_as(variable_name=...) expects a non-empty string.")
        if not isinstance(value_name, str) or not value_name:
            raise TypeError("melt_as(value_name=...) expects a non-empty string.")

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        id_names = self._colref_names(id_vars, arg_name="melt_as(id_vars=...)")
        value_names = (
            None
            if value_vars is None
            else self._colref_names(value_vars, arg_name="melt_as(value_vars=...)")
        )
        out_data, schema_descriptors = self._engine.execute_melt(
            self._rust_plan,
            self._root_data,
            id_names,
            value_names,
            variable_name,
            value_name,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="melt_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=out_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
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
        raise TypeError(
            "unpivot() is removed in pydantable 2.0 strict mode "
            "(reshape output schemas depend on runtime values)."
        )

    # 2.0 strict mode: use melt_as(...) and pivot_as(...) only.

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
        raise TypeError("pivot() is removed in pydantable 2.0 strict mode.")

    def pivot_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *,
        index: Sequence[ColumnRef],
        columns: ColumnRef,
        values: Sequence[ColumnRef],
        aggregate_function: str = "first",
        pivot_values: Sequence[Any],
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> DataFrame[AfterSchemaT]:
        """Strict pivot with explicit schema and explicit pivot_values."""
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("pivot_as(after_schema_type, ...) expects a schema.")
        if not index:
            raise ValueError("pivot_as(index=...) requires at least one index column.")
        if not values:
            raise ValueError("pivot_as(values=...) requires at least one value column.")
        if not pivot_values:
            raise TypeError(
                "pivot_as(pivot_values=...) is required in strict mode so the output "
                "columns are statically enumerable."
            )
        if not isinstance(separator, str) or not separator:
            raise TypeError("pivot_as(separator=...) expects a non-empty string.")

        index_cols = self._colref_names(index, arg_name="pivot_as(index=...)")
        referenced = columns.referenced_columns()
        if len(referenced) != 1:
            raise TypeError("pivot_as(columns=...) expects a single-column ColumnRef.")
        columns_col = next(iter(referenced))
        value_cols = self._colref_names(values, arg_name="pivot_as(values=...)")

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
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="pivot_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=out_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

    def explode(
        self,
        columns: str | Sequence[str] | Selector,
        *,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame[Any]:
        raise TypeError(
            "explode() is removed in pydantable 2.0 strict mode. "
            "Use an explicit-schema explode_as(AfterSchema, cols=[...]) API "
            "(not yet implemented)."
        )

    def explode_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *,
        columns: Sequence[ColumnRef],
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("explode_as(after_schema_type, ...) expects a schema.")
        if not columns:
            raise ValueError("explode_as(columns=...) requires at least one column.")
        cols = self._colref_names(columns, arg_name="explode_as(columns=...)")
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_explode(
            self._rust_plan,
            self._root_data,
            cols,
            streaming=use_streaming,
            outer=bool(outer),
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="explode_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=out_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
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
        raise TypeError(
            "unnest() is removed in pydantable 2.0 strict mode. "
            "Use an explicit-schema unnest_as(AfterSchema, cols=[...]) API "
            "(not yet implemented)."
        )

    def unnest_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *,
        columns: Sequence[ColumnRef],
        streaming: bool | None = None,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("unnest_as(after_schema_type, ...) expects a schema.")
        if not columns:
            raise ValueError("unnest_as(columns=...) requires at least one column.")
        cols = self._colref_names(columns, arg_name="unnest_as(columns=...)")
        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        out_data, schema_descriptors = self._engine.execute_unnest(
            self._rust_plan, self._root_data, cols, streaming=use_streaming
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="unnest_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=out_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
        )

    # 2.0 strict mode: no explode_all / unnest_all (dynamic column sets forbidden).

    def _join_impl_legacy(
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

    def join(self, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        raise TypeError(
            "join() is removed in pydantable 2.0 strict mode. "
            "Use join_as(AfterSchema, other, ...) with ColumnRef keys."
        )

    def join_as(
        self,
        after_schema_type: type[AfterSchemaT],
        other: DataFrame[Any],
        *,
        on: Sequence[ColumnRef] | None = None,
        left_on: Sequence[ColumnRef] | None = None,
        right_on: Sequence[ColumnRef] | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        streaming: bool | None = None,
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("join_as(after_schema_type, ...) expects a schema.")
        if not isinstance(other, DataFrame):
            raise TypeError("join_as(other=...) expects another DataFrame.")
        if on is not None and (left_on is not None or right_on is not None):
            raise ValueError(
                "join_as() use either on=... or left_on=/right_on=..., not both."
            )

        def _names(cols: Sequence[ColumnRef] | None) -> list[str]:
            if cols is None:
                return []
            out: list[str] = []
            for c in cols:
                if not isinstance(c, ColumnRef):
                    raise TypeError("join_as() keys must be ColumnRef values.")
                referenced = c.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError("join_as() keys must reference exactly one column.")
                out.append(next(iter(referenced)))
            return out

        if on is not None:
            left_keys = _names(on)
            right_keys = list(left_keys)
        else:
            left_keys = _names(left_on)
            right_keys = _names(right_on)

        if how != "cross":
            if not left_keys or not right_keys:
                raise ValueError(
                    "join_as() requires join keys for non-cross joins "
                    "(use on=... or left_on/right_on)."
                )
            if len(left_keys) != len(right_keys):
                raise ValueError("join_as() left_on and right_on lengths must match.")

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
            maintain_order=maintain_order,
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
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="join_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=joined_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
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
        raise TypeError(
            "join_as_schema(...) is removed in pydantable 2.0 strict mode. "
            "Use join_as(AfterSchema, ...) with ColumnRef keys."
        )

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
        raise TypeError(
            "join_try_as_schema(...) is removed in pydantable 2.0 strict mode. "
            "Use join_as(AfterSchema, ...) and handle errors explicitly."
        )

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
        raise TypeError(
            "join_assert_schema(...) is removed in pydantable 2.0 strict mode. "
            "Use join_as(AfterSchema, ...) which validates schema eagerly."
        )

    def join_as_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        raise TypeError(
            "join_as_model(...) is removed in pydantable 2.0 strict mode. "
            "Use join_as(AfterSchema, ...) on DataFrame, or join_as(AfterModel, ...) "
            "on DataFrameModel."
        )

    def join_try_as_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT] | None:
        raise TypeError(
            "join_try_as_model(...) is removed in pydantable 2.0 strict mode."
        )

    def join_assert_model(
        self,
        other: DataFrame[Any],
        schema: type[AfterSchemaT],
        *,
        validate_schema: bool = True,
        **kwargs: Any,
    ) -> DataFrame[AfterSchemaT]:
        raise TypeError(
            "join_assert_model(...) is removed in pydantable 2.0 strict mode."
        )

    def group_by(
        self,
        *keys: str | ColumnRef,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> GroupedDataFrame:
        raise TypeError(
            "group_by() is removed in pydantable 2.0 strict mode. "
            "Use group_by_agg_as(AfterSchema, keys=[...], ...) with explicit output "
            "schema."
        )

    def group_by_agg_as(
        self,
        after_schema_type: type[AfterSchemaT],
        *,
        keys: Sequence[ColumnRef],
        maintain_order: bool = False,
        drop_nulls: bool = True,
        streaming: bool | None = None,
        **aggregations: tuple[str, ColumnRef | Expr],
    ) -> DataFrame[AfterSchemaT]:
        if not isinstance(after_schema_type, type) or not issubclass(
            after_schema_type, BaseModel
        ):
            raise TypeError("group_by_agg_as(after_schema_type, ...) expects a schema.")
        if not keys:
            raise ValueError("group_by_agg_as(keys=...) requires at least one key.")

        key_names: list[str] = []
        for k in keys:
            if not isinstance(k, ColumnRef):
                raise TypeError("group_by_agg_as(keys=...) expects ColumnRef values.")
            referenced = k.referenced_columns()
            if len(referenced) != 1:
                raise TypeError("group_by_agg_as() keys must reference one column.")
            key_names.append(next(iter(referenced)))

        agg_specs: dict[str, tuple[str, str]] = {}
        for out_name, spec in aggregations.items():
            if not isinstance(spec, tuple) or len(spec) != 2:
                raise TypeError("Aggregations must be out_name=(op, column_ref).")
            op, col_spec = spec
            if not isinstance(op, str):
                raise TypeError("Aggregation operator must be a string.")
            if isinstance(col_spec, Expr):
                referenced = col_spec.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError("Aggregation Expr must reference one column.")
                in_col = next(iter(referenced))
            else:
                raise TypeError("Aggregation column must be an Expr / ColumnRef.")
            agg_specs[out_name] = (op, in_col)

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._engine_streaming_default
        )
        grouped_data, schema_descriptors = self._engine.execute_groupby_agg(
            self._rust_plan,
            self._root_data,
            key_names,
            agg_specs,
            maintain_order=bool(maintain_order),
            drop_nulls=bool(drop_nulls),
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        self._assert_after_schema(
            after_schema_type, derived_fields=derived_fields, op_name="group_by_agg_as"
        )
        rust_plan = self._engine.make_plan(field_types_for_rust(derived_fields))
        return cast(
            "DataFrame[AfterSchemaT]",
            self._from_plan(
                root_data=grouped_data,
                root_schema_type=after_schema_type,
                current_schema_type=after_schema_type,
                rust_plan=rust_plan,
                engine=self._engine,
            ),
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
        raise TypeError(
            "rolling_agg() is removed in pydantable 2.0 strict mode "
            "(schema-changing and stringly-typed parameters)."
        )
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
        raise TypeError(
            "group_by_dynamic() is removed in pydantable 2.0 strict mode "
            "(time bucket outputs are schema-changing and frequently "
            "runtime-dependent)."
        )

    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
    ) -> Any:
        """
        Materialize this typed logical DataFrame.

        **Cost:** full engine execution on the current thread (same as :meth:`to_dict`
        when returning rows or lists). See {doc}`EXECUTION` **Materialization costs**.

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

        The ``as_polars`` argument is deprecated: use :meth:`to_polars` for a
        Polars ``DataFrame`` when the optional ``polars`` package is installed.
        """
        if as_polars is not None:
            warnings.warn(
                "as_polars is deprecated and will be removed in pydantable 2.0.0; "
                "use to_polars() for a Polars DataFrame, or collect(as_lists=True) "
                "/ to_dict() for columnar dicts.",
                DeprecationWarning,
                stacklevel=2,
            )
            if as_polars:
                return self.to_polars(streaming=streaming)
            return self.to_dict(streaming=streaming)
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

        **Cost:** full Rust execution for the current plan. See {doc}`EXECUTION`.
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

        See {doc}`EXECUTION`.
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
        See {doc}`EXECUTION`. Optional ``streaming=`` matches :meth:`collect`.
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
        as_polars: bool | None = None,
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
        if as_polars is not None:
            warnings.warn(
                "as_polars is deprecated and will be removed in pydantable 2.0.0; "
                "use to_polars() for a Polars DataFrame, or collect(as_lists=True) "
                "/ to_dict() for columnar dicts.",
                DeprecationWarning,
                stacklevel=2,
            )
            if as_polars:
                return await self.ato_polars(
                    streaming=streaming,
                    engine_streaming=engine_streaming,
                    executor=executor,
                )
            return await self.ato_dict(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )
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
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> ExecutionHandle:
        """Background :meth:`collect`; await :meth:`ExecutionHandle.result`."""

        def _run() -> Any:
            return self.collect(
                as_lists=as_lists,
                as_numpy=as_numpy,
                as_polars=as_polars,
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
        if self._engine.has_async_collect_plan_batches():
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
