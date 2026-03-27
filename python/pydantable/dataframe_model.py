"""Subclass ``DataFrameModel``, annotate fields, get ``RowModel`` and DataFrame ops.

Wraps :class:`pydantable.dataframe.DataFrame` for class-body schemas (FastAPI-style).
Input may be column dicts or row sequences (mappings / Pydantic).
"""

from __future__ import annotations

import contextlib
import html
import sys
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast, get_type_hints

from typing_extensions import Self

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from sqlalchemy.engine import Connection, Engine

from pydantic import BaseModel, ValidationError, create_model

from .dataframe import DataFrame
from .schema import (
    Schema,
    _is_polars_dataframe,
    validate_dataframe_model_field_annotations,
)

def _field_defs_from_annotations(
    annotations: Mapping[str, Any],
    *,
    fill_missing_optional: bool = True,
    field_defaults: Mapping[str, Any] | None = None,
) -> dict[str, tuple[Any, Any]]:
    from .schema import _annotation_nullable_inner

    out: dict[str, tuple[Any, Any]] = {}
    for name, dtype in annotations.items():
        _inner, nullable = _annotation_nullable_inner(dtype)
        if field_defaults is not None and name in field_defaults:
            out[name] = (dtype, field_defaults[name])
            continue
        if nullable:
            out[name] = (dtype, None if fill_missing_optional else ...)
        else:
            out[name] = (dtype, ...)
    return out


# ``str``, ``bytes``, ``memoryview``, etc. are ``Sequence`` but must not be treated
# as a list of row dicts (classic foot-gun).
_ROW_SEQUENCE_EXCLUDES: tuple[type[Any], ...] = (str, bytes, bytearray, memoryview)


def _normalize_input(
    *,
    data: Any,
    row_model: type[BaseModel],
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
) -> tuple[Any, bool]:
    """Return ``(normalized_data, from_row_sequence)``.

    ``from_row_sequence`` is True when input was a sequence of row mappings/models
    so the caller can avoid re-running full per-cell validation downstream.
    """
    expected_fields = list(row_model.model_fields.keys())

    if _is_polars_dataframe(data):
        return data, False

    try:
        import pyarrow as pa  # type: ignore[import-untyped]
    except ImportError:
        pa = None  # type: ignore[assignment]
    if pa is not None:
        if isinstance(data, pa.Table):
            from .io import arrow_table_to_column_dict

            return arrow_table_to_column_dict(data), False
        if isinstance(data, pa.RecordBatch):
            from .io import record_batch_to_column_dict

            return record_batch_to_column_dict(data), False

    if isinstance(data, Mapping):
        # Columnar input path; downstream DataFrame strict validation handles
        # required/extra keys, length, and value type checks.
        return {
            k: list(v) if isinstance(v, (list, tuple)) else v for k, v in data.items()
        }, False  # type: ignore[return-value]

    if isinstance(data, Sequence) and not isinstance(data, _ROW_SEQUENCE_EXCLUDES):
        rows = list(data)
        if not rows:
            return {name: [] for name in expected_fields}, True

        valid_rows: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for idx, row in enumerate(rows):
            if not isinstance(row, (BaseModel, Mapping)):
                if not ignore_errors:
                    raise TypeError(
                        "Row input must be a sequence of mapping objects or "
                        "Pydantic models."
                    )
                failures.append(
                    {
                        "row_index": idx,
                        "row": {"_raw_row": row},
                        "errors": [
                            {
                                "type": "type_error",
                                "loc": (),
                                "msg": (
                                    "Row input must be a mapping object or "
                                    "Pydantic model."
                                ),
                                "input": row,
                            }
                        ],
                    }
                )
                continue
            row_dict = row.model_dump() if isinstance(row, BaseModel) else dict(row)
            try:
                parsed = row_model.model_validate(row)
            except ValidationError as exc:
                if not ignore_errors:
                    raise
                failures.append(
                    {"row_index": idx, "row": row_dict, "errors": exc.errors()}
                )
                continue
            valid_rows.append(parsed.model_dump())

        if failures and on_validation_errors is not None:
            on_validation_errors(failures)

        columns: dict[str, list[Any]] = {name: [] for name in expected_fields}
        for dumped in valid_rows:
            for name in expected_fields:
                columns[name].append(dumped[name])
        return columns, True

    if isinstance(data, Sequence) and isinstance(data, _ROW_SEQUENCE_EXCLUDES):
        raise TypeError(
            "DataFrameModel row input must be a sequence of row dicts or models, "
            f"not {type(data).__name__}. For columnar data use a mapping "
            "`{column: list}`."
        )

    raise TypeError("DataFrameModel input must be a column mapping or row sequence.")


RowT = TypeVar("RowT", bound=BaseModel)
ModelSelf = TypeVar("ModelSelf", bound="DataFrameModel[Any]")
GroupedModelT = TypeVar("GroupedModelT", bound="DataFrameModel[Any]")
AfterModelT = TypeVar("AfterModelT", bound="DataFrameModel[Any]")


class DataFrameModel(Generic[RowT]):
    """Columns on a subclass → generated ``RowModel`` and DataFrame-like methods.

    Annotate every field with a supported column type. :attr:`RowModel` validates
    one row. Data is a column map ``{name: list}``, a PyArrow ``Table`` or
    ``RecordBatch`` (requires ``pyarrow``), a Polars ``DataFrame`` in trusted
    modes, or a sequence of row dicts / models.

    **I/O classmethods:** ``read_*`` / ``aread_*`` use lazy file roots (no full Python
    column lists). ``materialize_*``, ``fetch_sql``, and ``from_sql`` load
    ``dict[str, list]`` then wrap the same lazy :class:`~pydantable.dataframe.DataFrame`
    plan as the constructor. **``export_*``**, **``write_sql``** / **``awrite_sql``**,
    **``read_parquet_url_ctx``** / **``aread_parquet_url_ctx``** delegate to
    :mod:`pydantable.io`.
    """

    RowModel: type[RowT]
    _RowModel_fill_missing_optional: type[BaseModel]
    _RowModel_require_optional: type[BaseModel]
    _SchemaModel: type[Schema]
    _df: DataFrame[Any]
    _dataframe_cls: type[DataFrame[Any]] = DataFrame

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Bridge classes in interface modules reuse the name `DataFrameModel` or
        # expose a typed subclass without their own row schema (user subclasses do).
        if cls.__name__ in ("DataFrameModel", "PandasDataFrameModel"):
            return

        module = sys.modules.get(cls.__module__)
        globalns = vars(module) if module is not None else {}
        eval_ns = dict(vars(typing))
        eval_ns.update(globalns)

        raw_annotations = dict(getattr(cls, "__dict__", {}).get("__annotations__", {}))
        annotations: dict[str, Any] = {}
        for field_name, field_type in raw_annotations.items():
            if isinstance(field_type, str):
                annotations[field_name] = eval(field_type, eval_ns, eval_ns)
            else:
                annotations[field_name] = field_type
        if not annotations:
            raise TypeError("DataFrameModel subclasses must define annotated fields.")

        validate_dataframe_model_field_annotations(cls.__name__, annotations)

        field_defaults: dict[str, Any] = {}
        class_dict = getattr(cls, "__dict__", {})
        for field_name in annotations:
            if field_name in class_dict:
                field_defaults[field_name] = class_dict[field_name]

        field_defs_fill = _field_defs_from_annotations(
            annotations, fill_missing_optional=True, field_defaults=field_defaults
        )
        field_defs_require = _field_defs_from_annotations(
            annotations, fill_missing_optional=False, field_defaults=field_defaults
        )
        # Schema should not implicitly add defaults for optional fields; missing-column
        # behavior is controlled by `fill_missing_optional` at ingest/materialization.
        field_defs_schema = _field_defs_from_annotations(
            annotations, fill_missing_optional=False, field_defaults=field_defaults
        )

        cls._RowModel_fill_missing_optional = create_model(  # type: ignore[call-overload]
            f"{cls.__name__}RowModel",
            __base__=Schema,
            **field_defs_fill,
        )
        cls._RowModel_require_optional = create_model(  # type: ignore[call-overload]
            f"{cls.__name__}RowModelRequireOptional",
            __base__=Schema,
            **field_defs_require,
        )
        cls.RowModel = cls._RowModel_fill_missing_optional
        cls._SchemaModel = create_model(  # type: ignore[call-overload]
            f"{cls.__name__}Schema",
            __base__=Schema,
            **field_defs_schema,
        )

    def __init__(
        self,
        data: Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        """Load columnar data or rows.

        Use ``trusted_mode='shape_only'`` or ``'strict'`` for trusted bulk input
        (layout still validated; ``strict`` adds dtype checks). Default is full
        per-element validation (``trusted_mode='off'`` or omitted). When
        ``ignore_errors=True``, invalid rows are skipped and details can be
        observed via ``on_validation_errors``.

        **Row sequences** are always normalized with :meth:`RowModel.model_validate`
        per row **before** building the inner :class:`~pydantable.dataframe.DataFrame`.
        ``trusted_mode`` does **not** bypass that step: ``trusted_mode='shape_only'``
        still validates each row fully; the flag applies only to the subsequent
        columnar pass (same as column-only input). Use columnar
        ``{name: list}`` input if you need ``shape_only`` / ``strict`` on bulk
        data without per-row Pydantic validation.

        For default ``trusted_mode`` (``off`` / omitted) with row input, the inner
        dataframe uses ``trusted_mode='shape_only'`` so cell values are not
        validated twice after rows are normalized. Columnar mappings use the
        ``trusted_mode`` you pass unchanged.
        """
        row_model = (
            self._RowModel_fill_missing_optional
            if fill_missing_optional
            else self._RowModel_require_optional
        )
        normalized, from_rows = _normalize_input(
            data=data,
            row_model=row_model,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        dataframe_cls = cast("Any", self._dataframe_cls)
        inner_trusted = trusted_mode
        if from_rows and (trusted_mode is None or trusted_mode == "off"):
            inner_trusted = "shape_only"
        self._df = dataframe_cls[self._SchemaModel](
            normalized,
            trusted_mode=inner_trusted,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def _dfm_require_subclass_with_schema(cls) -> None:
        if not getattr(cls, "RowModel", None):
            raise TypeError(
                "I/O classmethods must be called on a concrete DataFrameModel "
                "subclass with annotated columns, not on the bridge "
                f"{cls.__qualname__} base."
            )

    @classmethod
    def _wrap_inner_df(cls, inner: DataFrame[Any]) -> Self:
        obj = cls.__new__(cls)
        obj._df = inner
        return cast("Self", obj)

    @classmethod
    def read_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        """Lazy Parquet read (local path). See :func:`pydantable.io.read_parquet`."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_parquet(
            path,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_parquet_url(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Lazy Parquet via HTTP(S) download to a temp file.

        See :func:`pydantable.io.read_parquet_url`.
        """
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_parquet_url(
            url,
            experimental=experimental,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        """Lazy Arrow IPC file read (local path)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_ipc(
            path,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        """Lazy CSV read (local path)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_csv(
            path,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        """Lazy NDJSON read (local path)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_ndjson(
            path,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_json(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        """Lazy JSON Lines read (local path); same as :meth:`read_ndjson`."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_json(
            path,
            columns=columns,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            **scan_kwargs,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def materialize_parquet(
        cls,
        source: str | bytes | Any,
        *,
        columns: list[str] | None = None,
        engine: str | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Eager Parquet via :func:`pydantable.io.materialize_parquet` (column dict)."""
        cls._dfm_require_subclass_with_schema()
        from .io import materialize_parquet as _mp

        cols = _mp(source, columns=columns, engine=engine)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def materialize_ipc(
        cls,
        source: str | bytes | Any,
        *,
        as_stream: bool = False,
        engine: str | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Eager IPC via :func:`pydantable.io.materialize_ipc`."""
        cls._dfm_require_subclass_with_schema()
        from .io import materialize_ipc as _mi

        cols = _mi(source, as_stream=as_stream, engine=engine)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def materialize_csv(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        use_rap: bool = False,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Eager CSV via :func:`pydantable.io.materialize_csv`."""
        cls._dfm_require_subclass_with_schema()
        from .io import materialize_csv as _mc

        cols = _mc(path, engine=engine, use_rap=use_rap)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def materialize_ndjson(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Eager NDJSON via :func:`pydantable.io.materialize_ndjson`."""
        cls._dfm_require_subclass_with_schema()
        from .io import materialize_ndjson as _mn

        cols = _mn(path, engine=engine)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def materialize_json(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Eager JSON (array or lines) via :func:`pydantable.io.materialize_json`."""
        cls._dfm_require_subclass_with_schema()
        from .io import materialize_json as _mj

        cols = _mj(path, engine=engine)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def fetch_sql(
        cls,
        sql: str,
        bind: str | Engine | Connection,
        *,
        parameters: Mapping[str, Any] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """SQL query → columns via :func:`pydantable.io.fetch_sql` (``[sql]`` extra)."""
        cls._dfm_require_subclass_with_schema()
        from .io import fetch_sql as _fetch_sql

        cols = _fetch_sql(sql, bind, parameters=parameters)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def from_sql(
        cls,
        sql: str,
        bind: str | Engine | Connection,
        *,
        parameters: Mapping[str, Any] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Alias for :meth:`fetch_sql` — SQL query to typed frame."""
        return cls.fetch_sql(
            sql,
            bind,
            parameters=parameters,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    def export_parquet(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None:
        """Eager Parquet write; see :func:`pydantable.io.export_parquet`."""
        cls._dfm_require_subclass_with_schema()
        from .io import export_parquet as _ep

        _ep(path, data, engine=engine)

    @classmethod
    def export_csv(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None:
        """Eager CSV write; see :func:`pydantable.io.export_csv`."""
        cls._dfm_require_subclass_with_schema()
        from .io import export_csv as _ec

        _ec(path, data, engine=engine)

    @classmethod
    def export_ndjson(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None:
        """Eager NDJSON write; see :func:`pydantable.io.export_ndjson`."""
        cls._dfm_require_subclass_with_schema()
        from .io import export_ndjson as _en

        _en(path, data, engine=engine)

    @classmethod
    def export_ipc(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        engine: str | None = None,
    ) -> None:
        """Eager Arrow IPC write; see :func:`pydantable.io.export_ipc`."""
        cls._dfm_require_subclass_with_schema()
        from .io import export_ipc as _ei

        _ei(path, data, engine=engine)

    @classmethod
    def export_json(
        cls,
        path: str | Any,
        data: dict[str, list[Any]],
        *,
        indent: int | None = None,
    ) -> None:
        """Eager JSON array write; see :func:`pydantable.io.export_json`."""
        cls._dfm_require_subclass_with_schema()
        from .io import export_json as _ej

        _ej(path, data, indent=indent)

    @classmethod
    def write_sql(
        cls,
        data: dict[str, list[Any]],
        table_name: str,
        bind: str | Engine | Connection,
        *,
        schema: str | None = None,
        if_exists: str = "append",
    ) -> None:
        """Append/replace rows via :func:`pydantable.io.write_sql` (``[sql]`` extra)."""
        cls._dfm_require_subclass_with_schema()
        from .io import write_sql as _ws

        _ws(data, table_name, bind, schema=schema, if_exists=if_exists)

    @classmethod
    async def awrite_sql(
        cls,
        data: dict[str, list[Any]],
        table_name: str,
        bind: str | Engine | Connection,
        *,
        schema: str | None = None,
        if_exists: str = "append",
        executor: Executor | None = None,
    ) -> None:
        """Async :func:`pydantable.io.awrite_sql`."""
        cls._dfm_require_subclass_with_schema()
        from .io import awrite_sql as _aws

        await _aws(
            data,
            table_name,
            bind,
            schema=schema,
            if_exists=if_exists,
            executor=executor,
        )

    @classmethod
    @contextlib.contextmanager
    def read_parquet_url_ctx(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        **kwargs: Any,
    ):
        """Parquet over HTTP(S); temp file deleted after the block."""
        cls._dfm_require_subclass_with_schema()
        from .io import read_parquet_url_ctx as _ctx

        dataframe_cls = cast("Any", cls._dataframe_cls)
        with _ctx(
            dataframe_cls[cls._SchemaModel],
            url,
            experimental=experimental,
            columns=columns,
            **kwargs,
        ) as inner:
            yield cls._wrap_inner_df(inner)

    @classmethod
    @contextlib.asynccontextmanager
    async def aread_parquet_url_ctx(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        **kwargs: Any,
    ):
        """Async variant of :meth:`read_parquet_url_ctx`."""
        cls._dfm_require_subclass_with_schema()
        from .io import aread_parquet_url_ctx as _actx

        dataframe_cls = cast("Any", cls._dataframe_cls)
        async with _actx(
            dataframe_cls[cls._SchemaModel],
            url,
            experimental=experimental,
            columns=columns,
            executor=executor,
            **kwargs,
        ) as inner:
            yield cls._wrap_inner_df(inner)

    @classmethod
    async def aread_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import aread_parquet as _aread

        root = await _aread(path, columns=columns, executor=executor, **scan_kwargs)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(
            root,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import aread_ipc as _aread

        root = await _aread(path, columns=columns, executor=executor, **scan_kwargs)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(
            root,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import aread_csv as _aread

        root = await _aread(path, columns=columns, executor=executor, **scan_kwargs)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(
            root,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import aread_ndjson as _aread

        root = await _aread(path, columns=columns, executor=executor, **scan_kwargs)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(
            root,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_json(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **scan_kwargs: Any,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import aread_json as _aread

        root = await _aread(path, columns=columns, executor=executor, **scan_kwargs)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(
            root,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    async def amaterialize_parquet(
        cls,
        source: str | bytes | Any,
        *,
        columns: list[str] | None = None,
        engine: str | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import amaterialize_parquet as _amp

        cols = await _amp(source, columns=columns, engine=engine, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def amaterialize_ipc(
        cls,
        source: str | bytes | Any,
        *,
        as_stream: bool = False,
        engine: str | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import amaterialize_ipc as _ami

        cols = await _ami(source, as_stream=as_stream, engine=engine, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def amaterialize_csv(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        use_rap: bool = False,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import amaterialize_csv as _amc

        cols = await _amc(path, engine=engine, use_rap=use_rap, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def amaterialize_ndjson(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import amaterialize_ndjson as _amn

        cols = await _amn(path, engine=engine, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def amaterialize_json(
        cls,
        path: str | Any,
        *,
        engine: str | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import amaterialize_json as _amj

        cols = await _amj(path, engine=engine, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def afetch_sql(
        cls,
        sql: str,
        bind: str | Engine | Connection,
        *,
        parameters: Mapping[str, Any] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        cls._dfm_require_subclass_with_schema()
        from .io import afetch_sql as _afs

        cols = await _afs(sql, bind, parameters=parameters, executor=executor)
        return cls(
            cols,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    @classmethod
    async def afrom_sql(
        cls,
        sql: str,
        bind: str | Engine | Connection,
        *,
        parameters: Mapping[str, Any] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Alias for :meth:`afetch_sql`."""
        return await cls.afetch_sql(
            sql,
            bind,
            parameters=parameters,
            executor=executor,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )

    def write_parquet(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Write the lazy plan to Parquet (no Python column dict materialization)."""
        self._df.write_parquet(path, streaming=streaming, write_kwargs=write_kwargs)

    def write_csv(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        separator: str = ",",
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._df.write_csv(
            path, streaming=streaming, separator=separator, write_kwargs=write_kwargs
        )

    def write_ipc(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._df.write_ipc(
            path,
            streaming=streaming,
            compression=compression,
            write_kwargs=write_kwargs,
        )

    def write_ndjson(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._df.write_ndjson(path, streaming=streaming, write_kwargs=write_kwargs)

    def collect_batches(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
    ) -> list[Any]:
        return self._df.collect_batches(batch_size=batch_size, streaming=streaming)

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._df).split("\n"))
        return f"{type(self).__name__}\n{inner}"

    def _repr_html_(self) -> str:
        """HTML table preview for Jupyter (delegates to inner :class:`DataFrame`)."""
        inner = self._df._repr_html_()
        title = html.escape(type(self).__name__)
        return (
            '<div class="pydantable-render pydantable-render--context" '
            'style="margin:0 0 1rem 0;">'
            '<p style="margin:0 0 10px 0;padding:8px 12px;border-radius:8px;'
            "font:600 12px ui-sans-serif,system-ui,sans-serif;"
            "color:#1e3a8a;background:#eff6ff;border:1px solid #bfdbfe;"
            'letter-spacing:0.02em;">'
            f"<b>{title}</b> (DataFrameModel)</p>{inner}</div>"
        )

    def _repr_mimebundle_(
        self,
        include: Any = None,
        exclude: Any = None,
    ) -> dict[str, Any]:
        return {
            "text/plain": repr(self),
            "text/html": self._repr_html_(),
        }

    @classmethod
    def _derived_model_type(
        cls, field_types: Mapping[str, Any]
    ) -> type[DataFrameModel[Any]]:
        name = f"{cls.__name__}Derived"
        annotations = dict(field_types)
        # Inherit from the originating subclass for better DX/autocomplete and
        # to ensure generated `RowModel` / `Schema` types are aligned.
        derived = type(
            name,
            (cls,),
            {"__annotations__": annotations, "__module__": cls.__module__},
        )
        return cast("type[DataFrameModel[Any]]", derived)

    @classmethod
    def _from_dataframe(
        cls: type[ModelSelf], df: DataFrame[Any]
    ) -> ModelSelf:
        derived_type = cls._derived_model_type(df.schema_fields())
        obj = derived_type.__new__(derived_type)
        obj._df = df
        return cast("ModelSelf", obj)

    def schema_fields(self) -> dict[str, Any]:
        return self._df.schema_fields()

    @staticmethod
    def _expected_schema_fields(model: type[DataFrameModel[Any]]) -> dict[str, Any]:
        """
        Return the expected schema field mapping for `model`.

        We intentionally avoid `typing.get_type_hints(model, ...)` here because that
        evaluates inherited annotations (e.g. `_df: DataFrame[Any]`) which can
        trigger runtime errors via `DataFrame.__class_getitem__`.
        """
        schema = model.schema_model()
        return {
            name: field.annotation
            for name, field in schema.model_fields.items()
            if not name.startswith("_")
        }

    def as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT:
        """
        Re-wrap this lazy pipeline as another `DataFrameModel` subclass.

        This is primarily a static-typing escape hatch for type checkers that
        cannot infer schema-evolving transform returns (e.g. pyright/Pylance).
        """
        if not isinstance(model, type) or not issubclass(model, DataFrameModel):
            raise TypeError("as_model(model=...) expects a DataFrameModel subclass.")
        if validate_schema:
            expected = self._expected_schema_fields(model)
            actual = self.schema_fields()
            if set(expected) != set(actual) or any(
                expected[k] != actual[k] for k in expected if k in actual
            ):
                raise TypeError(
                    "as_model(schema mismatch): expected "
                    f"{sorted(expected)} got {sorted(actual)}"
                )
        obj = model.__new__(model)
        obj._df = self._df
        return cast("AfterModelT", obj)

    @staticmethod
    def _schema_mismatch_details(
        *,
        expected: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> str:
        expected_keys = set(expected)
        actual_keys = set(actual)
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        mismatched: list[str] = []
        for k in sorted(expected_keys & actual_keys):
            if expected[k] != actual[k]:
                mismatched.append(
                    f"{k}: expected={expected[k]!r} actual={actual[k]!r}"
                )
        parts: list[str] = []
        if missing:
            parts.append(f"missing={missing}")
        if extra:
            parts.append(f"extra={extra}")
        if mismatched:
            parts.append("mismatched_types=[" + ", ".join(mismatched) + "]")
        return "; ".join(parts) if parts else "unknown mismatch"

    def try_as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT | None:
        """
        Like :meth:`as_model`, but returns ``None`` on schema mismatch.

        This is useful for “best effort” pipelines where schema evolution is expected
        and mismatches are handled explicitly by the caller.
        """
        if not isinstance(model, type) or not issubclass(model, DataFrameModel):
            raise TypeError("try_as_model(model=...) expects a DataFrameModel subclass.")
        if not validate_schema:
            return self.as_model(model, validate_schema=False)
        expected = self._expected_schema_fields(model)
        actual = self.schema_fields()
        if set(expected) != set(actual) or any(
            expected[k] != actual[k] for k in expected if k in actual
        ):
            return None
        return self.as_model(model, validate_schema=False)

    def assert_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT:
        """
        Like :meth:`as_model`, but raises with a richer schema diff on mismatch.
        """
        if not isinstance(model, type) or not issubclass(model, DataFrameModel):
            raise TypeError("assert_model(model=...) expects a DataFrameModel subclass.")
        if not validate_schema:
            return self.as_model(model, validate_schema=False)
        expected = self._expected_schema_fields(model)
        actual = self.schema_fields()
        if set(expected) != set(actual) or any(
            expected[k] != actual[k] for k in expected if k in actual
        ):
            details = self._schema_mismatch_details(expected=expected, actual=actual)
            raise TypeError(f"assert_model(schema mismatch): {details}")
        return self.as_model(model, validate_schema=False)

    @property
    def columns(self) -> list[str]:
        return self._df.columns

    @property
    def shape(self) -> tuple[int, int]:
        return self._df.shape

    @property
    def empty(self) -> bool:
        return self._df.empty

    @property
    def dtypes(self) -> dict[str, Any]:
        return self._df.dtypes

    def info(self) -> str:
        """Delegate to :meth:`DataFrame.info`."""
        return self._df.info()

    def describe(self) -> str:
        """Delegate to :meth:`DataFrame.describe`."""
        return self._df.describe()

    def explain(
        self,
        *,
        format: Literal["text", "json"] = "text",
        streaming: bool | None = None,
    ) -> str | dict[str, Any]:
        """Delegate to :meth:`DataFrame.explain`."""
        return self._df.explain(format=format, streaming=streaming)

    def value_counts(
        self,
        column: str,
        *,
        normalize: bool = False,
        dropna: bool = True,
    ) -> dict[Any, int | float]:
        """Delegate to :meth:`DataFrame.value_counts`."""
        return self._df.value_counts(column, normalize=normalize, dropna=dropna)

    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
    ) -> Any:
        return self._df.collect(
            as_lists=as_lists,
            as_numpy=as_numpy,
            as_polars=as_polars,
            streaming=streaming,
        )

    def to_dict(self, *, streaming: bool | None = None) -> dict[str, list[Any]]:
        return self._df.to_dict(streaming=streaming)

    def to_polars(self, *, streaming: bool | None = None) -> Any:
        return self._df.to_polars(streaming=streaming)

    def to_arrow(self, *, streaming: bool | None = None) -> Any:
        """
        Materialize as a PyArrow ``Table`` (delegates to :meth:`DataFrame.to_arrow`).
        """
        return self._df.to_arrow(streaming=streaming)

    def __dataframe__(
        self, *, nan_as_null: bool = False, allow_copy: bool = True
    ) -> Any:
        """Delegate dataframe interchange protocol to the inner :class:`DataFrame`."""
        return self._df.__dataframe__(nan_as_null=nan_as_null, allow_copy=allow_copy)

    def __dataframe_consortium_standard__(self, api_version: str | None = None) -> Any:
        """Delegate Consortium Standard entrypoint to the inner :class:`DataFrame`."""
        return self._df.__dataframe_consortium_standard__(api_version=api_version)

    def rows(self) -> list[RowT]:
        """
        Materialize this DataFrame into a list of per-row Pydantic models.

        Same as :meth:`collect` with default arguments (validated against the
        current inner schema type).
        """
        return cast("list[RowT]", self.collect())

    def to_dicts(self) -> list[dict[str, Any]]:
        """
        Return JSON-friendly row dictionaries.

        Uses the generated `RowModel` so field aliases / defaults are
        respected consistently with Pydantic.
        """
        return [row.model_dump() for row in self.rows()]

    async def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """Async :meth:`collect` (delegates to :meth:`DataFrame.acollect`)."""
        return await self._df.acollect(
            as_lists=as_lists,
            as_numpy=as_numpy,
            as_polars=as_polars,
            streaming=streaming,
            executor=executor,
        )

    async def ato_dict(
        self,
        *,
        streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> dict[str, list[Any]]:
        """Async :meth:`to_dict`."""
        return await self._df.ato_dict(streaming=streaming, executor=executor)

    async def ato_polars(
        self,
        *,
        streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """Async :meth:`to_polars`."""
        return await self._df.ato_polars(streaming=streaming, executor=executor)

    async def ato_arrow(
        self,
        *,
        streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """Async :meth:`to_arrow`."""
        return await self._df.ato_arrow(streaming=streaming, executor=executor)

    async def arows(
        self,
        *,
        executor: Executor | None = None,
    ) -> list[RowT]:
        """Async :meth:`rows` (same as ``await acollect()``)."""
        return cast("list[RowT]", await self.acollect(executor=executor))

    async def ato_dicts(
        self,
        *,
        executor: Executor | None = None,
    ) -> list[dict[str, Any]]:
        """Async :meth:`to_dicts`."""
        rows = await self.arows(executor=executor)
        return [row.model_dump() for row in rows]

    def select(self, *cols: Any) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.select(*cols))

    def with_columns(self, **new_columns: Any) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.with_columns(**new_columns))

    def filter(self, condition: Any) -> Self:
        return self._from_dataframe(self._df.filter(condition))

    def sort(
        self, *by: Any, descending: bool | Sequence[bool] = False
    ) -> Self:
        return self._from_dataframe(self._df.sort(*by, descending=descending))

    def unique(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> Self:
        return self._from_dataframe(self._df.unique(subset=subset, keep=keep))

    def distinct(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> Self:
        return self._from_dataframe(self._df.distinct(subset=subset, keep=keep))

    def drop(self, *columns: Any) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.drop(*columns))

    def rename(self, columns: Mapping[str, str]) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.rename(columns))

    def slice(self, offset: int, length: int) -> Self:
        return self._from_dataframe(self._df.slice(offset, length))

    def head(self, n: int = 5) -> Self:
        return self._from_dataframe(self._df.head(n))

    def tail(self, n: int = 5) -> Self:
        return self._from_dataframe(self._df.tail(n))

    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: Sequence[str] | None = None,
    ) -> DataFrameModel[Any]:
        return self._from_dataframe(
            self._df.fill_null(value, strategy=strategy, subset=subset)
        )

    def drop_nulls(self, subset: Sequence[str] | None = None) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.drop_nulls(subset=subset))

    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel[Any]:
        return self._from_dataframe(
            self._df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                variable_name=variable_name,
                value_name=value_name,
            )
        )

    def unpivot(
        self,
        *,
        index: Sequence[str] | None = None,
        on: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel[Any]:
        return self._from_dataframe(
            self._df.unpivot(
                index=index,
                on=on,
                variable_name=variable_name,
                value_name=value_name,
            )
        )

    def pivot(
        self,
        *,
        index: str | Sequence[str],
        columns: Any,
        values: str | Sequence[str],
        aggregate_function: str = "first",
    ) -> DataFrameModel[Any]:
        return self._from_dataframe(
            self._df.pivot(
                index=index,
                columns=columns,
                values=values,
                aggregate_function=aggregate_function,
            )
        )

    def explode(self, columns: str | Sequence[str]) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.explode(columns))

    def unnest(self, columns: str | Sequence[str]) -> DataFrameModel[Any]:
        return self._from_dataframe(self._df.unnest(columns))

    def join(
        self,
        other: DataFrameModel[Any],
        *,
        on: str | Sequence[str] | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> DataFrameModel[Any]:
        if not isinstance(other, DataFrameModel):
            raise TypeError("join(other=...) expects another DataFrameModel instance.")
        return self._from_dataframe(
            self._df.join(
                other._df,
                on=on,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffix=suffix,
            )
        )

    def group_by(self: ModelSelf, *keys: Any) -> GroupedDataFrameModel[ModelSelf]:
        return GroupedDataFrameModel(self._df.group_by(*keys), self.__class__)

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
    ) -> DataFrameModel[Any]:
        return self._from_dataframe(
            self._df.rolling_agg(
                on=on,
                column=column,
                window_size=window_size,
                op=op,
                out_name=out_name,
                by=by,
                min_periods=min_periods,
            )
        )

    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrameModel[ModelSelf]:
        model_type = cast("type[ModelSelf]", self.__class__)
        return DynamicGroupedDataFrameModel(
            self._df.group_by_dynamic(index_column, every=every, period=period, by=by),
            model_type,
        )

    def __getattr__(self, item: str) -> Any:
        # Delegate column refs + API methods to wrapped DataFrame.
        return getattr(self._df, item)

    @classmethod
    def row_model(cls) -> type[RowT]:
        return cls.RowModel

    @classmethod
    def schema_model(cls) -> type[Schema]:
        return cls._SchemaModel

    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrameModel[Any]],
        *,
        how: str = "vertical",
    ) -> DataFrameModel[Any]:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, DataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        return cls._from_dataframe(DataFrame.concat([df._df for df in dfs], how=how))


class GroupedDataFrameModel(Generic[GroupedModelT]):
    """Result of ``DataFrameModel.group_by``; use :meth:`agg` to produce a new model."""

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None:
        self._grouped_df = grouped_df
        self._model_type = model_type

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._grouped_df).split("\n"))
        return f"GroupedDataFrameModel({self._model_type.__name__})\n{inner}"

    def _repr_html_(self) -> str:
        inner = self._grouped_df._repr_html_()
        title = html.escape(self._model_type.__name__)
        return (
            '<div class="pydantable-render pydantable-render--context" '
            'style="margin:0 0 1rem 0;">'
            '<p style="margin:0 0 10px 0;padding:8px 12px;border-radius:8px;'
            "font:600 12px ui-sans-serif,system-ui,sans-serif;"
            "color:#334155;background:#eef2ff;border:1px solid #c7d2fe;"
            'letter-spacing:0.02em;">'
            f"<b>GroupedDataFrameModel({title})</b></p>{inner}</div>"
        )

    def agg(self, **aggregations: Any) -> DataFrameModel[Any]:
        """Same kwargs as :meth:`pydantable.dataframe.GroupedDataFrame.agg`."""
        return self._model_type._from_dataframe(self._grouped_df.agg(**aggregations))


class DynamicGroupedDataFrameModel(Generic[GroupedModelT]):
    """Time-based ``group_by_dynamic`` grouping; call :meth:`agg` to finalize."""

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None:
        self._grouped_df = grouped_df
        self._model_type = model_type

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._grouped_df).split("\n"))
        return f"DynamicGroupedDataFrameModel({self._model_type.__name__})\n{inner}"

    def _repr_html_(self) -> str:
        inner = self._grouped_df._repr_html_()
        title = html.escape(self._model_type.__name__)
        return (
            '<div class="pydantable-render pydantable-render--context" '
            'style="margin:0 0 1rem 0;">'
            '<p style="margin:0 0 10px 0;padding:8px 12px;border-radius:8px;'
            "font:600 12px ui-sans-serif,system-ui,sans-serif;"
            "color:#334155;background:#ecfdf5;border:1px solid #a7f3d0;"
            'letter-spacing:0.02em;">'
            f"<b>DynamicGroupedDataFrameModel({title})</b></p>{inner}</div>"
        )

    def agg(self, **aggregations: Any) -> DataFrameModel[Any]:
        """Same rules as :meth:`pydantable.dataframe.DynamicGroupedDataFrame.agg`."""
        return self._model_type._from_dataframe(self._grouped_df.agg(**aggregations))
