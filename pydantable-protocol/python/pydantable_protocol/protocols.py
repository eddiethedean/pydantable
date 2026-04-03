"""Structural protocols and capability flags for pydantable execution engines.

These definitions are published in a **zero-dependency** distribution so
third-party backends (for example a SQL engine package) can implement
:class:`ExecutionEngine` and type-check against it **without** installing
``pydantable``.

Unsupported calls should raise
:class:`~pydantable_protocol.exceptions.UnsupportedEngineOperationError`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class EngineCapabilities:
    """Feature flags for an execution engine (native, stub, or future backends)."""

    backend: Literal["native", "stub", "custom"]
    extension_loaded: bool
    has_execute_plan: bool
    has_async_execute_plan: bool
    has_async_collect_plan_batches: bool
    has_sink_parquet: bool
    has_sink_csv: bool
    has_sink_ipc: bool
    has_sink_ndjson: bool
    has_collect_plan_batches: bool
    has_execute_join: bool
    has_execute_groupby_agg: bool


def stub_engine_capabilities() -> EngineCapabilities:
    """Capabilities for minimal stub / test-double execution engines."""

    return EngineCapabilities(
        backend="stub",
        extension_loaded=False,
        has_execute_plan=False,
        has_async_execute_plan=False,
        has_async_collect_plan_batches=False,
        has_sink_parquet=False,
        has_sink_csv=False,
        has_sink_ipc=False,
        has_sink_ndjson=False,
        has_collect_plan_batches=False,
        has_execute_join=False,
        has_execute_groupby_agg=False,
    )


@runtime_checkable
class PlanExecutor(Protocol):
    """Execute a logical plan against root data."""

    def execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any: ...

    async def async_execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any: ...

    async def async_collect_plan_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]: ...

    def collect_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]: ...

    def execute_join(
        self,
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
    ) -> tuple[Any, Any]: ...

    def execute_groupby_agg(
        self,
        plan: Any,
        root_data: Any,
        by: Sequence[str],
        aggregations: Any,
        *,
        maintain_order: bool = False,
        drop_nulls: bool = True,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_concat(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        how: str,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_except_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_intersect_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_melt(
        self,
        plan: Any,
        root_data: Any,
        id_vars: Sequence[str],
        value_vars: Sequence[str] | None,
        variable_name: str,
        value_name: str,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_pivot(
        self,
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
    ) -> tuple[Any, Any]: ...

    def execute_explode(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_posexplode(
        self,
        plan: Any,
        root_data: Any,
        list_column: str,
        pos_name: str,
        value_name: str,
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
    ) -> tuple[Any, Any]: ...

    def execute_rolling_agg(
        self,
        plan: Any,
        root_data: Any,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None,
        min_periods: int,
    ) -> tuple[Any, Any]: ...

    def execute_groupby_dynamic_agg(
        self,
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
    ) -> tuple[Any, Any]: ...


@runtime_checkable
class SinkWriter(Protocol):
    """Write lazy plan + root to on-disk formats."""

    def write_parquet(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
        partition_by: list[str] | tuple[str, ...] | None = None,
        mkdir: bool = True,
    ) -> None: ...

    def write_csv(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        separator: int = ord(","),
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...

    def write_ipc(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...

    def write_ndjson(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None: ...


@runtime_checkable
class ExecutionEngine(PlanExecutor, SinkWriter, Protocol):
    """Full drop-in engine: logical plan transforms, execution, sinks, capabilities."""

    @property
    def capabilities(self) -> EngineCapabilities: ...

    def make_plan(self, field_types: Any) -> Any: ...

    def has_async_execute_plan(self) -> bool: ...

    def has_async_collect_plan_batches(self) -> bool: ...

    def make_literal(self, *, value: Any) -> Any: ...

    def plan_with_columns(self, plan: Any, columns: dict[str, Any]) -> Any: ...

    def expr_is_global_agg(self, expr: Any) -> bool: ...

    def expr_global_default_alias(self, expr: Any) -> Any: ...

    def plan_global_select(self, plan: Any, items: list[tuple[str, Any]]) -> Any: ...

    def plan_select(self, plan: Any, projects: list[str]) -> Any: ...

    def plan_filter(self, plan: Any, condition_expr: Any) -> Any: ...

    def plan_sort(
        self,
        plan: Any,
        keys: list[str],
        desc: list[bool],
        nulls_last: list[bool],
        maintain_order: bool,
    ) -> Any: ...

    def plan_unique(
        self,
        plan: Any,
        subset: list[str] | None,
        keep: str,
        maintain_order: bool,
    ) -> Any: ...

    def plan_duplicate_mask(
        self, plan: Any, subset: list[str] | None, keep: str
    ) -> Any: ...

    def plan_drop_duplicate_groups(
        self, plan: Any, subset: list[str] | None
    ) -> Any: ...

    def plan_drop(self, plan: Any, columns: list[str]) -> Any: ...

    def plan_rename(self, plan: Any, rename_map: Mapping[str, str]) -> Any: ...

    def plan_slice(self, plan: Any, offset: int, length: int) -> Any: ...

    def plan_with_row_count(self, plan: Any, name: str, offset: int) -> Any: ...

    def plan_fill_null(
        self,
        plan: Any,
        subset: list[str] | None,
        value: Any,
        strategy: str | None,
    ) -> Any: ...

    def plan_drop_nulls(
        self,
        plan: Any,
        subset: list[str] | None,
        how: str,
        threshold: int | None,
    ) -> Any: ...

    def plan_rolling_agg(
        self,
        plan: Any,
        column: str,
        window_size: int,
        min_periods: int,
        op: str,
        out_name: str,
        partition_by: Sequence[str] | None = None,
    ) -> Any: ...
