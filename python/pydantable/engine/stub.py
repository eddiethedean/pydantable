"""Minimal execution engine for testing registry and protocol wiring.

Not suitable for real workloads: most operations raise
:class:`~pydantable.errors.UnsupportedEngineOperationError`.
"""

from __future__ import annotations

from typing import Any

from pydantable.errors import unsupported_engine_operation

from .protocols import stub_engine_capabilities

_STUB_PLAN = object()

_RAISE_METHODS = (
    "make_literal",
    "plan_with_columns",
    "expr_is_global_agg",
    "expr_global_default_alias",
    "plan_global_select",
    "plan_select",
    "plan_filter",
    "plan_sort",
    "plan_unique",
    "plan_duplicate_mask",
    "plan_drop_duplicate_groups",
    "plan_drop",
    "plan_rename",
    "plan_rolling_agg",
    "plan_slice",
    "plan_with_row_count",
    "plan_fill_null",
    "plan_drop_nulls",
    "execute_plan",
    "collect_batches",
    "execute_join",
    "execute_groupby_agg",
    "execute_concat",
    "execute_except_all",
    "execute_intersect_all",
    "execute_melt",
    "execute_pivot",
    "execute_explode",
    "execute_posexplode",
    "execute_unnest",
    "execute_rolling_agg",
    "execute_groupby_dynamic_agg",
    "write_parquet",
    "write_csv",
    "write_ipc",
    "write_ndjson",
    # async_* are bound below (must be coroutine functions)
)


def _raise(name: str) -> Any:
    raise unsupported_engine_operation(
        backend="stub",
        operation=name,
        hint="Use NativePolarsEngine or set_default_engine(...) with a real backend.",
    )


class StubExecutionEngine:
    """Placeholder backend: use only for tests and API experiments."""

    __slots__ = ()

    @property
    def rust_core(self) -> Any:
        raise unsupported_engine_operation(
            backend="stub",
            operation="expression_runtime",
            required_capability="rust_expression_runtime",
            hint="Use NativePolarsEngine or set_expression_runtime().",
        )

    @property
    def capabilities(self) -> Any:
        return stub_engine_capabilities()

    def make_plan(self, field_types: Any) -> Any:
        return _STUB_PLAN

    def has_async_execute_plan(self) -> bool:
        return False

    def has_async_collect_plan_batches(self) -> bool:
        return False


def _bind(name: str):
    def _method(self: StubExecutionEngine, *args: Any, **kwargs: Any) -> Any:
        return _raise(name)

    _method.__name__ = name
    _method.__qualname__ = f"StubExecutionEngine.{name}"
    return _method


for _m in _RAISE_METHODS:
    setattr(StubExecutionEngine, _m, _bind(_m))


async def _async_execute_plan(self: StubExecutionEngine, *a: Any, **k: Any) -> Any:
    return _raise("async_execute_plan")


async def _async_collect_plan_batches(
    self: StubExecutionEngine, *a: Any, **k: Any
) -> Any:
    return _raise("async_collect_plan_batches")


StubExecutionEngine.async_execute_plan = _async_execute_plan  # type: ignore[attr-defined, method-assign]
StubExecutionEngine.async_collect_plan_batches = (  # type: ignore[attr-defined, method-assign]
    _async_collect_plan_batches
)
