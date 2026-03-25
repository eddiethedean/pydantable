"""Subclass ``DataFrameModel``, annotate fields, get ``RowModel`` and DataFrame ops.

Wraps :class:`pydantable.dataframe.DataFrame` for class-body schemas (FastAPI-style).
Input may be column dicts or row sequences (mappings / Pydantic).
"""

from __future__ import annotations

import html
import sys
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

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
) -> dict[str, tuple[Any, Any]]:
    return {name: (dtype, ...) for name, dtype in annotations.items()}


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


class DataFrameModel:
    """Columns on a subclass → generated ``RowModel`` and DataFrame-like methods.

    Annotate every field with a supported column type. :attr:`RowModel` validates
    one row. Data is a column map ``{name: list}``, a PyArrow ``Table`` or
    ``RecordBatch`` (requires ``pyarrow``), a Polars ``DataFrame`` in trusted
    modes, or a sequence of row dicts / models.

    **I/O classmethods:** ``read_*`` / ``aread_*`` use lazy file roots (no full Python
    column lists). ``materialize_*`` / ``fetch_sql`` load ``dict[str, list]`` then
    wrap the same lazy :class:`~pydantable.dataframe.DataFrame` plan as the constructor.
    """

    RowModel: type[BaseModel]
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

        field_defs = _field_defs_from_annotations(annotations)
        cls.RowModel = create_model(  # type: ignore[call-overload]
            f"{cls.__name__}RowModel",
            __base__=Schema,
            **field_defs,
        )
        cls._SchemaModel = create_model(  # type: ignore[call-overload]
            f"{cls.__name__}Schema",
            __base__=Schema,
            **field_defs,
        )

    def __init__(
        self,
        data: Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        """Load columnar data or rows.

        Use ``trusted_mode='shape_only'`` or ``'strict'`` for trusted bulk input
        (layout still validated; ``strict`` adds dtype checks). Default is full
        per-element validation (``trusted_mode='off'`` or omitted). When
        ``ignore_errors=True``, invalid rows are skipped and details can be
        observed via ``on_validation_errors``.

        Row sequences are validated **per row** with :meth:`RowModel.model_validate`
        (including when you set ``trusted_mode='shape_only'`` on the dataframe—
        that flag currently applies to the inner column pass, not this row step).
        For default ``trusted_mode`` (``off`` / omitted), the inner
        :class:`~pydantable.dataframe.DataFrame` is opened in
        ``trusted_mode='shape_only'`` so cell values are not validated twice.
        Columnar mappings use the ``trusted_mode`` you pass unchanged.
        """
        normalized, from_rows = _normalize_input(
            data=data,
            row_model=self.RowModel,
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
        return cast(Self, obj)

    @classmethod
    def read_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Lazy Parquet read (local path). See :func:`pydantable.io.read_parquet`."""
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_parquet(path, columns=columns)
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_parquet_url(
        cls,
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        **kwargs: Any,
    ) -> Self:
        """Lazy Parquet via HTTP(S) download to a temp file. See :func:`pydantable.io.read_parquet_url`."""
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_parquet_url(
            url, experimental=experimental, columns=columns, **kwargs
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Lazy Arrow IPC file read (local path)."""
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_ipc(path, columns=columns)
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Lazy CSV read (local path)."""
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_csv(path, columns=columns)
        return cls._wrap_inner_df(inner)

    @classmethod
    def read_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        """Lazy NDJSON read (local path)."""
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].read_ndjson(path, columns=columns)
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
        """Eager Parquet → ``dict[str, list]`` via :func:`pydantable.io.materialize_parquet`."""
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
        """SQL → columns via :func:`pydantable.io.fetch_sql` (needs ``pydantable[sql]``)."""
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
    async def aread_parquet(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        from .io import aread_parquet as _aread

        root = await _aread(path, columns=columns, executor=executor)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(root)
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_ipc(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        from .io import aread_ipc as _aread

        root = await _aread(path, columns=columns, executor=executor)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(root)
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_csv(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        from .io import aread_csv as _aread

        root = await _aread(path, columns=columns, executor=executor)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(root)
        return cls._wrap_inner_df(inner)

    @classmethod
    async def aread_ndjson(
        cls,
        path: str | Any,
        *,
        columns: list[str] | None = None,
        executor: Executor | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> Self:
        del trusted_mode, ignore_errors, on_validation_errors
        cls._dfm_require_subclass_with_schema()
        from .io import aread_ndjson as _aread

        root = await _aread(path, columns=columns, executor=executor)
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel]._from_scan_root(root)
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

        cols = await _ami(
            source, as_stream=as_stream, engine=engine, executor=executor
        )
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

        cols = await _amc(
            path, engine=engine, use_rap=use_rap, executor=executor
        )
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

    def write_parquet(
        self, path: str | Any, *, streaming: bool | None = None
    ) -> None:
        """Write the current lazy plan to Parquet without materializing to Python dicts."""
        self._df.write_parquet(path, streaming=streaming)

    def write_csv(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        separator: str = ",",
    ) -> None:
        self._df.write_csv(path, streaming=streaming, separator=separator)

    def write_ipc(
        self,
        path: str | Any,
        *,
        streaming: bool | None = None,
        compression: str | None = None,
    ) -> None:
        self._df.write_ipc(path, streaming=streaming, compression=compression)

    def write_ndjson(
        self, path: str | Any, *, streaming: bool | None = None
    ) -> None:
        self._df.write_ndjson(path, streaming=streaming)

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
    ) -> type[DataFrameModel]:
        name = f"{cls.__name__}Derived"
        annotations = dict(field_types)
        # Inherit from the originating subclass for better DX/autocomplete and
        # to ensure generated `RowModel` / `Schema` types are aligned.
        derived = type(
            name,
            (cls,),
            {"__annotations__": annotations, "__module__": cls.__module__},
        )
        return cast("type[DataFrameModel]", derived)

    @classmethod
    def _from_dataframe(cls, df: DataFrame[Any]) -> DataFrameModel:
        derived_type = cls._derived_model_type(df.schema_fields())
        obj = derived_type.__new__(derived_type)
        obj._df = df
        return obj

    def schema_fields(self) -> dict[str, Any]:
        return self._df.schema_fields()

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

    def rows(self) -> list[BaseModel]:
        """
        Materialize this DataFrame into a list of per-row Pydantic models.

        Same as :meth:`collect` with default arguments (validated against the
        current inner schema type).
        """
        return self.collect()

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
    ) -> list[BaseModel]:
        """Async :meth:`rows` (same as ``await acollect()``)."""
        return await self.acollect(executor=executor)

    async def ato_dicts(
        self,
        *,
        executor: Executor | None = None,
    ) -> list[dict[str, Any]]:
        """Async :meth:`to_dicts`."""
        rows = await self.arows(executor=executor)
        return [row.model_dump() for row in rows]

    def select(self, *cols: Any) -> DataFrameModel:
        return self._from_dataframe(self._df.select(*cols))

    def with_columns(self, **new_columns: Any) -> DataFrameModel:
        return self._from_dataframe(self._df.with_columns(**new_columns))

    def filter(self, condition: Any) -> DataFrameModel:
        return self._from_dataframe(self._df.filter(condition))

    def sort(
        self, *by: Any, descending: bool | Sequence[bool] = False
    ) -> DataFrameModel:
        return self._from_dataframe(self._df.sort(*by, descending=descending))

    def unique(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> DataFrameModel:
        return self._from_dataframe(self._df.unique(subset=subset, keep=keep))

    def distinct(
        self, subset: Sequence[str] | None = None, *, keep: str = "first"
    ) -> DataFrameModel:
        return self._from_dataframe(self._df.distinct(subset=subset, keep=keep))

    def drop(self, *columns: Any) -> DataFrameModel:
        return self._from_dataframe(self._df.drop(*columns))

    def rename(self, columns: Mapping[str, str]) -> DataFrameModel:
        return self._from_dataframe(self._df.rename(columns))

    def slice(self, offset: int, length: int) -> DataFrameModel:
        return self._from_dataframe(self._df.slice(offset, length))

    def head(self, n: int = 5) -> DataFrameModel:
        return self._from_dataframe(self._df.head(n))

    def tail(self, n: int = 5) -> DataFrameModel:
        return self._from_dataframe(self._df.tail(n))

    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: Sequence[str] | None = None,
    ) -> DataFrameModel:
        return self._from_dataframe(
            self._df.fill_null(value, strategy=strategy, subset=subset)
        )

    def drop_nulls(self, subset: Sequence[str] | None = None) -> DataFrameModel:
        return self._from_dataframe(self._df.drop_nulls(subset=subset))

    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel:
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
    ) -> DataFrameModel:
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
    ) -> DataFrameModel:
        return self._from_dataframe(
            self._df.pivot(
                index=index,
                columns=columns,
                values=values,
                aggregate_function=aggregate_function,
            )
        )

    def explode(self, columns: str | Sequence[str]) -> DataFrameModel:
        return self._from_dataframe(self._df.explode(columns))

    def unnest(self, columns: str | Sequence[str]) -> DataFrameModel:
        return self._from_dataframe(self._df.unnest(columns))

    def join(
        self,
        other: DataFrameModel,
        *,
        on: str | Sequence[str] | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> DataFrameModel:
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

    def group_by(self, *keys: Any) -> GroupedDataFrameModel:
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
    ) -> DataFrameModel:
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
    ) -> DynamicGroupedDataFrameModel:
        return DynamicGroupedDataFrameModel(
            self._df.group_by_dynamic(index_column, every=every, period=period, by=by),
            self.__class__,
        )

    def __getattr__(self, item: str) -> Any:
        # Delegate column refs + API methods to wrapped DataFrame.
        return getattr(self._df, item)

    @classmethod
    def row_model(cls) -> type[BaseModel]:
        return cls.RowModel

    @classmethod
    def schema_model(cls) -> type[Schema]:
        return cls._SchemaModel

    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrameModel],
        *,
        how: str = "vertical",
    ) -> DataFrameModel:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, DataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        return cls._from_dataframe(DataFrame.concat([df._df for df in dfs], how=how))


class GroupedDataFrameModel:
    """Result of ``DataFrameModel.group_by``; use :meth:`agg` to produce a new model."""

    def __init__(self, grouped_df: Any, model_type: type[DataFrameModel]) -> None:
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

    def agg(self, **aggregations: Any) -> DataFrameModel:
        """Same kwargs as :meth:`pydantable.dataframe.GroupedDataFrame.agg`."""
        return self._model_type._from_dataframe(self._grouped_df.agg(**aggregations))


class DynamicGroupedDataFrameModel:
    """Time-based ``group_by_dynamic`` grouping; call :meth:`agg` to finalize."""

    def __init__(self, grouped_df: Any, model_type: type[DataFrameModel]) -> None:
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

    def agg(self, **aggregations: Any) -> DataFrameModel:
        """Same rules as :meth:`pydantable.dataframe.DynamicGroupedDataFrame.agg`."""
        return self._model_type._from_dataframe(self._grouped_df.agg(**aggregations))
