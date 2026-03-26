"""Thin Python entry points into ``pydantable._core`` (Rust + Polars).

Each ``execute_*`` runs a logical plan fragment against in-memory data. The
extension may be absent in a source checkout until the module is built with Maturin.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from ._extension import MissingRustExtensionError

_MISSING_SYMBOL_PREFIX = (
    "The pydantable native extension is present but does not implement "
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def _load_rust_core() -> Any | None:
    """
    Import the compiled Rust extension module (if available).

    The package must remain importable without building Rust extensions.
    """
    try:
        from . import _core as rust_core  # type: ignore

        return rust_core
    except ImportError:
        return None


_RUST_CORE = _load_rust_core()


def _require_rust_core() -> Any:
    """Return the loaded extension module or raise :exc:`MissingRustExtensionError`."""
    if _RUST_CORE is None:
        raise MissingRustExtensionError()
    return _RUST_CORE


def _verbose_plan_errors_enabled() -> bool:
    v = os.environ.get("PYDANTABLE_VERBOSE_ERRORS", "").strip().lower()
    return v in ("1", "true", "yes")


def execute_plan(
    plan: Any,
    data: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
    error_context: str | None = None,
) -> Any:
    """Run a full plan to materialized columns (lists or native, per flag).

    ``streaming=True`` requests Polars' streaming collect engine where supported.

    If ``PYDANTABLE_VERBOSE_ERRORS`` is set to a truthy value and
    ``error_context`` is provided, :exc:`ValueError` from the engine is
    re-raised with the context string appended (helps debugging in notebooks).
    """
    rust = _require_rust_core()
    if not hasattr(rust, "execute_plan"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`execute_plan`. "
            "Reinstall or rebuild pydantable. See docs/DEVELOPER.md."
        )
    try:
        return rust.execute_plan(plan, data, as_python_lists, streaming)
    except ValueError as e:
        if _verbose_plan_errors_enabled() and error_context:
            raise ValueError(f"{e}\n[context: {error_context}]") from e
        raise


def write_parquet(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    """Write lazy plan + root to Parquet via Rust (no Python ``dict[str, list]``)."""
    rust = _require_rust_core()
    if not hasattr(rust, "sink_parquet"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`sink_parquet`. See docs/DEVELOPER.md."
        )
    rust.sink_parquet(plan, root_data, path, streaming, write_kwargs)


def write_csv(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    separator: int = ord(","),
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    """Write lazy plan + root to CSV via Rust."""
    rust = _require_rust_core()
    if not hasattr(rust, "sink_csv"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`sink_csv`. See docs/DEVELOPER.md."
        )
    rust.sink_csv(plan, root_data, path, streaming, separator & 0xFF, write_kwargs)


def write_ipc(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    compression: str | None = None,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    """Write lazy plan + root to Arrow IPC file via Rust."""
    rust = _require_rust_core()
    if not hasattr(rust, "sink_ipc"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`sink_ipc`. See docs/DEVELOPER.md."
        )
    rust.sink_ipc(plan, root_data, path, streaming, compression, write_kwargs)


def write_ndjson(
    plan: Any,
    root_data: Any,
    path: str,
    *,
    streaming: bool = False,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    """Write lazy plan + root as newline-delimited JSON via Rust."""
    rust = _require_rust_core()
    if not hasattr(rust, "sink_ndjson"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`sink_ndjson`. See docs/DEVELOPER.md."
        )
    rust.sink_ndjson(plan, root_data, path, streaming, write_kwargs)


def collect_batches(
    plan: Any,
    root_data: Any,
    *,
    batch_size: int = 65_536,
    streaming: bool = False,
) -> list[Any]:
    """Materialize the plan and return a list of Polars ``DataFrame`` chunks (via IPC).

    The engine performs a full collect first, then slices rows—this is not Polars'
    native lazy batch iterator.
    """
    rust = _require_rust_core()
    if not hasattr(rust, "collect_plan_batches"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`collect_plan_batches`. See docs/DEVELOPER.md."
        )
    return list(rust.collect_plan_batches(plan, root_data, batch_size, streaming))


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
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    """Join two plan/data roots; returns ``(new_data, schema_descriptors)``."""
    rust = _require_rust_core()
    if not hasattr(rust, "execute_join"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`execute_join`. See docs/DEVELOPER.md."
        )
    return rust.execute_join(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        list(left_on),
        list(right_on),
        how,
        suffix,
        as_python_lists,
        streaming,
    )


def execute_groupby_agg(
    plan: Any,
    root_data: Any,
    by: Sequence[str],
    aggregations: Any,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    """Group and aggregate; returns materialized data and output schema descriptors."""
    rust = _require_rust_core()
    if not hasattr(rust, "execute_groupby_agg"):
        raise MissingRustExtensionError(
            f"{_MISSING_SYMBOL_PREFIX}`execute_groupby_agg`. See docs/DEVELOPER.md."
        )
    return rust.execute_groupby_agg(
        plan, root_data, list(by), aggregations, as_python_lists, streaming
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
    """Concatenate two frames (e.g. vertical stack)."""
    rust = _require_rust_core()
    return rust.execute_concat(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        how,
        as_python_lists,
        streaming,
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
    """Unpivot to long format (melt)."""
    rust = _require_rust_core()
    return rust.execute_melt(
        plan,
        root_data,
        list(id_vars),
        None if value_vars is None else list(value_vars),
        variable_name,
        value_name,
        as_python_lists,
        streaming,
    )


def execute_pivot(
    plan: Any,
    root_data: Any,
    index: Sequence[str],
    columns: str,
    values: Sequence[str],
    aggregate_function: str,
    *,
    as_python_lists: bool = False,
    streaming: bool = False,
) -> tuple[Any, Any]:
    """Pivot with aggregation."""
    rust = _require_rust_core()
    return rust.execute_pivot(
        plan,
        root_data,
        list(index),
        columns,
        list(values),
        aggregate_function,
        as_python_lists,
        streaming,
    )


def execute_explode(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
    *,
    streaming: bool = False,
) -> tuple[Any, Any]:
    """Explode list columns to one row per element."""
    rust = _require_rust_core()
    return rust.execute_explode(plan, root_data, list(columns), streaming)


def execute_unnest(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
    *,
    streaming: bool = False,
) -> tuple[Any, Any]:
    """Unnest struct columns into top-level fields."""
    rust = _require_rust_core()
    return rust.execute_unnest(plan, root_data, list(columns), streaming)


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
    """Rolling window aggregation along a time or index column."""
    rust = _require_rust_core()
    return rust.execute_rolling_agg(
        plan, root_data, on, column, window_size, op, out_name, by, min_periods
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
    """Time-bucket group-by with aggregations."""
    rust = _require_rust_core()
    return rust.execute_groupby_dynamic_agg(
        plan,
        root_data,
        index_column,
        every,
        period,
        by,
        aggregations,
        as_python_lists,
        streaming,
    )
