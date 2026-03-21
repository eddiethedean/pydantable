"""Rust extension (`pydantable._core`): the only execution engine."""

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
    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust extension is not available. "
            "Build the PyO3 module so `pydantable._core` can be imported."
        )
    return _RUST_CORE


def execute_plan(plan: Any, data: Any) -> Any:
    rust = _require_rust_core()
    if not hasattr(rust, "execute_plan"):
        raise NotImplementedError("Rust extension does not implement `execute_plan`.")
    return rust.execute_plan(plan, data)


def execute_join(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    left_on: Sequence[str],
    right_on: Sequence[str],
    how: str,
    suffix: str,
) -> tuple[Any, Any]:
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
    )


def execute_groupby_agg(
    plan: Any,
    root_data: Any,
    by: Sequence[str],
    aggregations: Any,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    if not hasattr(rust, "execute_groupby_agg"):
        raise NotImplementedError(
            "Rust extension does not implement `execute_groupby_agg`."
        )
    return rust.execute_groupby_agg(plan, root_data, list(by), aggregations)


def execute_concat(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    how: str,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    return rust.execute_concat(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        how,
    )


def execute_melt(
    plan: Any,
    root_data: Any,
    id_vars: Sequence[str],
    value_vars: Sequence[str] | None,
    variable_name: str,
    value_name: str,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    return rust.execute_melt(
        plan,
        root_data,
        list(id_vars),
        None if value_vars is None else list(value_vars),
        variable_name,
        value_name,
    )


def execute_pivot(
    plan: Any,
    root_data: Any,
    index: Sequence[str],
    columns: str,
    values: Sequence[str],
    aggregate_function: str,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    return rust.execute_pivot(
        plan,
        root_data,
        list(index),
        columns,
        list(values),
        aggregate_function,
    )


def execute_explode(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    return rust.execute_explode(plan, root_data, list(columns))


def execute_unnest(
    plan: Any,
    root_data: Any,
    columns: Sequence[str],
) -> tuple[Any, Any]:
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
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    return rust.execute_groupby_dynamic_agg(
        plan, root_data, index_column, every, period, by, aggregations
    )
