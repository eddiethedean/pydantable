"""Compatibility shims for the native extension (Rust + Polars).

Implementation lives on :class:`~pydantable.engine.native.NativePolarsEngine`.
Import :func:`~pydantable.engine.get_default_engine` for new code.

Type annotations use :class:`typing.Any` for plan handles and columnar data because
those objects are produced and consumed by Rust/PyO3; they mirror
:class:`pydantable_protocol.protocols.ExecutionEngine` and are not replaced with
shallow Python types without a portable plan IR (see docs/TYPING.md, policy for
``Any``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from pydantable_native._binding import (  # type: ignore[import-not-found]
        MISSING_SYMBOL_PREFIX as _MISSING_SYMBOL_PREFIX,
    )
    from pydantable_native._binding import (
        load_rust_core as _load_rust_core,
    )
    from pydantable_native._binding import (
        require_rust_core as _require_rust_core,
    )
    from pydantable_native._binding import (
        rust_core_loaded,
        rust_has_async_collect_plan_batches,
        rust_has_async_execute_plan,
    )
except (ImportError, OSError):  # pragma: no cover — optional native binding
    _MISSING_SYMBOL_PREFIX = (
        "The pydantable native extension is present but does not implement "
    )

    def _load_rust_core() -> Any | None:  # type: ignore[no-redef]
        return None

    _require_rust_core = None  # type: ignore[assignment]

    def rust_core_loaded() -> Any | None:  # type: ignore[no-redef]
        return None

    def rust_has_async_collect_plan_batches() -> bool:  # type: ignore[no-redef]
        return False

    def rust_has_async_execute_plan() -> bool:  # type: ignore[no-redef]
        return False


from pydantable.engine import get_default_engine

if TYPE_CHECKING:
    from collections.abc import Sequence

# Legacy alias: mirrors ``pydantable_native._binding`` load state at import time.
_RUST_CORE = rust_core_loaded()


def execute_plan(
    plan: Any,
    data: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
    error_context: str | None = None,
) -> Any:
    return get_default_engine().execute_plan(
        plan,
        data,
        as_python_lists=as_python_lists,
        streaming=streaming,
        error_context=error_context,
    )


async def async_execute_plan(
    plan: Any,
    data: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
    error_context: str | None = None,
) -> Any:
    return await get_default_engine().async_execute_plan(
        plan,
        data,
        as_python_lists=as_python_lists,
        streaming=streaming,
        error_context=error_context,
    )


async def async_collect_plan_batches(
    plan: Any,
    root_data: Any,
    *,
    batch_size: int = 65_536,
    streaming: bool = False,
) -> list[Any]:
    return await get_default_engine().async_collect_plan_batches(
        plan, root_data, batch_size=batch_size, streaming=streaming
    )


def write_parquet(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    write_kwargs: dict[str, Any] | None = None,
    partition_by: list[str] | tuple[str, ...] | None = None,
    mkdir: bool = True,
) -> None:
    return get_default_engine().write_parquet(
        plan,
        root_data,
        path,
        streaming=streaming,
        write_kwargs=write_kwargs,
        partition_by=partition_by,
        mkdir=mkdir,
    )


def write_csv(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    separator: int = ord(","),
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    return get_default_engine().write_csv(
        plan,
        root_data,
        path,
        streaming=streaming,
        separator=separator,
        write_kwargs=write_kwargs,
    )


def write_ipc(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    compression: str | None = None,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    return get_default_engine().write_ipc(
        plan,
        root_data,
        path,
        streaming=streaming,
        compression=compression,
        write_kwargs=write_kwargs,
    )


def write_ndjson(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    return get_default_engine().write_ndjson(
        plan, root_data, path, streaming=streaming, write_kwargs=write_kwargs
    )


def collect_batches(
    plan: Any,
    root_data: Any,
    *,
    batch_size: int = 65_536,
    streaming: bool = False,
) -> list[Any]:
    return get_default_engine().collect_batches(
        plan, root_data, batch_size=batch_size, streaming=streaming
    )


def execute_join(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    left_on: Sequence[str],
    right_on: Sequence[str],
    how: str,
    suffix: str,
    *,
    validate: str | None = None,
    coalesce: bool | None = None,
    join_nulls: bool | None = None,
    maintain_order: str | None = None,
    allow_parallel: bool | None = None,
    force_parallel: bool | None = None,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_join(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        left_on,
        right_on,
        how,
        suffix,
        validate=validate,
        coalesce=coalesce,
        join_nulls=join_nulls,
        maintain_order=maintain_order,
        allow_parallel=allow_parallel,
        force_parallel=force_parallel,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_groupby_agg(
    plan: Any,
    root_data: Any,
    by: Sequence[str],
    aggregations: Any,
    *,
    maintain_order: bool = False,
    drop_nulls: bool = True,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_groupby_agg(
        plan,
        root_data,
        by,
        aggregations,
        maintain_order=maintain_order,
        drop_nulls=drop_nulls,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_concat(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    how: str,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_concat(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        how,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_except_all(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_except_all(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_intersect_all(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_intersect_all(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_melt(
    plan: Any,
    root_data: Any,
    id_vars: Sequence[str],
    value_vars: Sequence[str] | None,
    variable_name: str,
    value_name: str,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_melt(
        plan,
        root_data,
        id_vars,
        value_vars,
        variable_name,
        value_name,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_pivot(
    plan: Any,
    root_data: Any,
    index: Sequence[str],
    columns: str,
    values: Sequence[str],
    aggregate_function: str,
    *,
    pivot_values: Sequence[Any] | None = None,
    sort_columns: bool = False,
    separator: str = "_",
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_pivot(
        plan,
        root_data,
        index,
        columns,
        values,
        aggregate_function,
        pivot_values=pivot_values,
        sort_columns=sort_columns,
        separator=separator,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


def execute_explode(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
    *,
    streaming: bool = False,
    outer: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_explode(
        plan, root_data, columns, streaming=streaming, outer=outer
    )


def execute_posexplode(
    plan: Any,
    root_data: Any,
    list_column: str,
    pos_name: str,
    value_name: str,
    *,
    streaming: bool = False,
    outer: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_posexplode(
        plan,
        root_data,
        list_column,
        pos_name,
        value_name,
        streaming=streaming,
        outer=outer,
    )


def execute_unnest(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
    *,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_unnest(
        plan, root_data, columns, streaming=streaming
    )


def execute_rolling_agg(
    plan: Any,
    root_data: Any,
    on: str,
    column: str,
    window_size: int | str,
    op: str,
    out_name: str,
    by: Sequence[str] | None,
    min_periods: int,
) -> tuple[Any, Any]:
    return get_default_engine().execute_rolling_agg(
        plan,
        root_data,
        on,
        column,
        window_size,
        op,
        out_name,
        by,
        min_periods,
    )


def execute_groupby_dynamic_agg(
    plan: Any,
    root_data: Any,
    index_column: str,
    every: str,
    period: str | None,
    by: Sequence[str] | None,
    aggregations: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    return get_default_engine().execute_groupby_dynamic_agg(
        plan,
        root_data,
        index_column,
        every,
        period,
        by,
        aggregations,
        as_python_lists=as_python_lists,
        streaming=streaming,
    )


__all__ = [
    "_MISSING_SYMBOL_PREFIX",
    "_RUST_CORE",
    "_load_rust_core",
    "_require_rust_core",
    "async_collect_plan_batches",
    "async_execute_plan",
    "collect_batches",
    "execute_concat",
    "execute_except_all",
    "execute_explode",
    "execute_groupby_agg",
    "execute_groupby_dynamic_agg",
    "execute_intersect_all",
    "execute_join",
    "execute_melt",
    "execute_pivot",
    "execute_plan",
    "execute_posexplode",
    "execute_rolling_agg",
    "execute_unnest",
    "rust_has_async_collect_plan_batches",
    "rust_has_async_execute_plan",
    "write_csv",
    "write_ipc",
    "write_ndjson",
    "write_parquet",
]
