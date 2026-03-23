"""Thin Python entry points into ``pydantable._core`` (Rust + Polars).

Each ``execute_*`` runs a logical plan fragment against in-memory data. The
extension may be absent in a source checkout until the module is built with Maturin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    """Return the loaded extension module or raise :exc:`NotImplementedError`."""
    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust extension is not available. "
            "Build the PyO3 module so `pydantable._core` can be imported."
        )
    return _RUST_CORE


def execute_plan(plan: Any, data: Any, *, as_python_lists: bool = False) -> Any:
    """Run a full plan to materialized columns (lists or native, per flag)."""
    rust = _require_rust_core()
    if not hasattr(rust, "execute_plan"):
        raise NotImplementedError("Rust extension does not implement `execute_plan`.")
    return rust.execute_plan(plan, data, as_python_lists)


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
) -> tuple[Any, Any]:
    """Join two plan/data roots; returns ``(new_data, schema_descriptors)``."""
    rust = _require_rust_core()
    if not hasattr(rust, "execute_join"):
        raise NotImplementedError("Rust extension does not implement `execute_join`.")
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
    )


def execute_groupby_agg(
    plan: Any,
    root_data: Any,
    by: Sequence[str],
    aggregations: Any,
    *,
    as_python_lists: bool = False,
) -> tuple[Any, Any]:
    """Group and aggregate; returns materialized data and output schema descriptors."""
    rust = _require_rust_core()
    if not hasattr(rust, "execute_groupby_agg"):
        raise NotImplementedError(
            "Rust extension does not implement `execute_groupby_agg`."
        )
    return rust.execute_groupby_agg(
        plan, root_data, list(by), aggregations, as_python_lists
    )


def execute_concat(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    how: str,
    *,
    as_python_lists: bool = False,
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
    )


def execute_explode(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
) -> tuple[Any, Any]:
    """Explode list columns to one row per element."""
    rust = _require_rust_core()
    return rust.execute_explode(plan, root_data, list(columns))


def execute_unnest(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
) -> tuple[Any, Any]:
    """Unnest struct columns into top-level fields."""
    rust = _require_rust_core()
    return rust.execute_unnest(plan, root_data, list(columns))


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
) -> tuple[Any, Any]:
    """Time-bucket group-by with aggregations."""
    rust = _require_rust_core()
    return rust.execute_groupby_dynamic_agg(
        plan, root_data, index_column, every, period, by, aggregations, as_python_lists
    )
