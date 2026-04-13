"""Lazy scan and batch iterators for :class:`~pydantable.dataframe.DataFrame`.

Separated from ``_impl.py`` so the core ``DataFrame`` class stays focused on plan
chaining and materialization (single-responsibility split).
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Executor
from typing import Any, Literal

from pydantable.engine import get_default_engine
from pydantable.schema import field_types_for_rust, schema_field_types


def _from_scan_root(
    cls: Any,
    root: Any,
    *,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
) -> Any:
    """Build a lazy frame from a native ``ScanFileRoot`` (see ``DataFrame._from_scan_root``)."""
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


def read_parquet(
    cls: Any,
    path: str | Any,
    *,
    columns: list[str] | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    **scan_kwargs: Any,
) -> Any:
    from pydantable.io import read_parquet as _read_parquet

    return _from_scan_root(
        cls,
        _read_parquet(path, columns=columns, **scan_kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_parquet(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_parquet as _aread_parquet

    root = await _aread_parquet(
        path, columns=columns, executor=executor, **scan_kwargs
    )
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def read_parquet_url(
    cls: Any,
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
) -> Any:
    from pydantable.io import read_parquet_url as _read_parquet_url

    return _from_scan_root(
        cls,
        _read_parquet_url(url, experimental=experimental, columns=columns, **kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_parquet_url(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_parquet_url as _aread_parquet_url

    root = await _aread_parquet_url(
        url,
        experimental=experimental,
        columns=columns,
        executor=executor,
        **kwargs,
    )
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def read_csv(
    cls: Any,
    path: str | Any,
    *,
    columns: list[str] | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    **scan_kwargs: Any,
) -> Any:
    from pydantable.io import read_csv as _read_csv

    return _from_scan_root(
        cls,
        _read_csv(path, columns=columns, **scan_kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_csv(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_csv as _aread_csv

    root = await _aread_csv(path, columns=columns, executor=executor, **scan_kwargs)
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def read_ndjson(
    cls: Any,
    path: str | Any,
    *,
    columns: list[str] | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    **scan_kwargs: Any,
) -> Any:
    from pydantable.io import read_ndjson as _read_ndjson

    return _from_scan_root(
        cls,
        _read_ndjson(path, columns=columns, **scan_kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_ndjson(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_ndjson as _aread_ndjson

    root = await _aread_ndjson(
        path, columns=columns, executor=executor, **scan_kwargs
    )
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def read_json(
    cls: Any,
    path: str | Any,
    *,
    columns: list[str] | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    **scan_kwargs: Any,
) -> Any:
    from pydantable.io import read_json as _read_json

    return _from_scan_root(
        cls,
        _read_json(path, columns=columns, **scan_kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_json(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_json as _aread_json

    root = await _aread_json(
        path, columns=columns, executor=executor, **scan_kwargs
    )
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def read_ipc(
    cls: Any,
    path: str | Any,
    *,
    columns: list[str] | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    **scan_kwargs: Any,
) -> Any:
    from pydantable.io import read_ipc as _read_ipc

    return _from_scan_root(
        cls,
        _read_ipc(path, columns=columns, **scan_kwargs),
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


async def aread_ipc(
    cls: Any,
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
) -> Any:
    from pydantable.io import aread_ipc as _aread_ipc

    root = await _aread_ipc(path, columns=columns, executor=executor, **scan_kwargs)
    return _from_scan_root(
        cls,
        root,
        engine_streaming=engine_streaming,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=on_validation_errors,
    )


def iter_parquet(
    cls: Any,
    path: str | Any,
    *,
    batch_size: int = 65_536,
    columns: list[str] | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
):
    from pydantable.io.iter_file import iter_parquet as _iter

    for cols_dict in _iter(path, batch_size=batch_size, columns=columns):
        yield cls(
            cols_dict,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )


def iter_ipc(
    cls: Any,
    source: str | Any,
    *,
    batch_size: int = 65_536,
    as_stream: bool = False,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
):
    from pydantable.io.iter_file import iter_ipc as _iter

    for cols_dict in _iter(source, batch_size=batch_size, as_stream=as_stream):
        yield cls(
            cols_dict,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )


def iter_csv(
    cls: Any,
    path: str | Any,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
):
    from pydantable.io.iter_file import iter_csv as _iter

    for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
        yield cls(
            cols_dict,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )


def iter_ndjson(
    cls: Any,
    path: str | Any,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
):
    from pydantable.io.iter_file import iter_ndjson as _iter

    for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
        yield cls(
            cols_dict,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )


def iter_json_lines(
    cls: Any,
    path: str | Any,
    *,
    batch_size: int = 65_536,
    encoding: str = "utf-8",
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
):
    from pydantable.io.iter_file import iter_json_lines as _iter

    for cols_dict in _iter(path, batch_size=batch_size, encoding=encoding):
        yield cls(
            cols_dict,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
