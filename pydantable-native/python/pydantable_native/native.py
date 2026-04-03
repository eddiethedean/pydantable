"""Default execution engine: Rust extension + Polars (``pydantable_native._core``)."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from pydantable_protocol.exceptions import MissingRustExtensionError

from ._binding import (
    MISSING_SYMBOL_PREFIX,
    require_rust_core,
    rust_has_async_collect_plan_batches,
    rust_has_async_execute_plan,
)
from ._trace import span
from .capabilities import native_engine_capabilities

if TYPE_CHECKING:
    from collections.abc import Sequence


def _verbose_plan_errors_enabled() -> bool:
    v = os.environ.get("PYDANTABLE_VERBOSE_ERRORS", "").strip().lower()
    return v in ("1", "true", "yes")


class NativePolarsEngine:
    """Native backend: logical plans and execution via ``pydantable_native._core``."""

    __slots__ = ()

    @property
    def rust_core(self) -> Any:
        """The loaded PyO3 extension module (``make_plan``, ``execute_plan``, …)."""
        return require_rust_core()

    def make_plan(self, field_types: Any) -> Any:
        return self.rust_core.make_plan(field_types)

    def has_async_execute_plan(self) -> bool:
        return rust_has_async_execute_plan()

    def has_async_collect_plan_batches(self) -> bool:
        return rust_has_async_collect_plan_batches()

    def make_literal(self, *, value: Any) -> Any:
        return self.rust_core.make_literal(value=value)

    def plan_with_columns(self, plan: Any, columns: dict[str, Any]) -> Any:
        return self.rust_core.plan_with_columns(plan, columns)

    def expr_is_global_agg(self, expr: Any) -> bool:
        return self.rust_core.expr_is_global_agg(expr)

    def expr_global_default_alias(self, expr: Any) -> Any:
        return self.rust_core.expr_global_default_alias(expr)

    def plan_global_select(self, plan: Any, items: list[tuple[str, Any]]) -> Any:
        return self.rust_core.plan_global_select(plan, items)

    def plan_select(self, plan: Any, projects: list[str]) -> Any:
        return self.rust_core.plan_select(plan, projects)

    def plan_filter(self, plan: Any, condition_expr: Any) -> Any:
        return self.rust_core.plan_filter(plan, condition_expr)

    def plan_sort(
        self,
        plan: Any,
        keys: list[str],
        desc: list[bool],
        nulls_last: list[bool],
        maintain_order: bool,
    ) -> Any:
        return self.rust_core.plan_sort(plan, keys, desc, nulls_last, maintain_order)

    def plan_unique(
        self,
        plan: Any,
        subset: list[str] | None,
        keep: str,
        maintain_order: bool,
    ) -> Any:
        return self.rust_core.plan_unique(plan, subset, keep, maintain_order)

    def plan_duplicate_mask(
        self, plan: Any, subset: list[str] | None, keep: str
    ) -> Any:
        return self.rust_core.plan_duplicate_mask(plan, subset, keep)

    def plan_drop_duplicate_groups(self, plan: Any, subset: list[str] | None) -> Any:
        return self.rust_core.plan_drop_duplicate_groups(plan, subset)

    def plan_drop(self, plan: Any, columns: list[str]) -> Any:
        return self.rust_core.plan_drop(plan, columns)

    def plan_rename(self, plan: Any, rename_map: dict[str, str]) -> Any:
        return self.rust_core.plan_rename(plan, rename_map)

    def plan_slice(self, plan: Any, offset: int, length: int) -> Any:
        return self.rust_core.plan_slice(plan, offset, length)

    def plan_with_row_count(self, plan: Any, name: str, offset: int) -> Any:
        return self.rust_core.plan_with_row_count(plan, name, offset)

    def plan_fill_null(
        self,
        plan: Any,
        subset: list[str] | None,
        value: Any,
        strategy: str | None,
    ) -> Any:
        return self.rust_core.plan_fill_null(plan, subset, value, strategy)

    def plan_drop_nulls(
        self,
        plan: Any,
        subset: list[str] | None,
        how: str,
        threshold: int | None,
    ) -> Any:
        return self.rust_core.plan_drop_nulls(plan, subset, how, threshold)

    def plan_rolling_agg(
        self,
        plan: Any,
        column: str,
        window_size: int,
        min_periods: int,
        op: str,
        out_name: str,
        partition_by: list[str] | None = None,
    ) -> Any:
        return self.rust_core.plan_rolling_agg(
            plan,
            column,
            window_size,
            min_periods,
            op,
            out_name,
            partition_by,
        )

    def execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any:
        rust = require_rust_core()
        if not hasattr(rust, "execute_plan"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`execute_plan`. "
                "Reinstall or rebuild pydantable-native. See docs/DEVELOPER.md."
            )
        with span(
            "execute_plan",
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
            error_context=error_context,
        ):
            try:
                return rust.execute_plan(plan, data, as_python_lists, streaming)
            except ValueError as e:
                if _verbose_plan_errors_enabled() and error_context:
                    raise ValueError(f"{e}\n[context: {error_context}]") from e
                raise

    async def async_execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any:
        rust = require_rust_core()
        if not hasattr(rust, "async_execute_plan"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`async_execute_plan`. "
                "Rebuild pydantable-native from source. See docs/DEVELOPER.md."
            )
        with span(
            "async_execute_plan",
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
            error_context=error_context,
        ):
            try:
                return await rust.async_execute_plan(
                    plan, data, as_python_lists, streaming
                )
            except ValueError as e:
                if _verbose_plan_errors_enabled() and error_context:
                    raise ValueError(f"{e}\n[context: {error_context}]") from e
                raise

    async def async_collect_plan_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]:
        rust = require_rust_core()
        if not hasattr(rust, "async_collect_plan_batches"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`async_collect_plan_batches`. "
                "Rebuild pydantable-native from source. See docs/DEVELOPER.md."
            )
        with span(
            "async_collect_plan_batches",
            batch_size=int(batch_size),
            streaming=bool(streaming),
        ):
            return await rust.async_collect_plan_batches(
                plan, root_data, batch_size, streaming
            )

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
    ) -> None:
        rust = require_rust_core()
        if not hasattr(rust, "sink_parquet"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`sink_parquet`. See docs/DEVELOPER.md."
            )
        pb = list(partition_by) if partition_by else None
        with span(
            "sink_parquet",
            streaming=bool(streaming),
            path=str(path),
            partition_by=pb,
            mkdir=bool(mkdir),
        ):
            rust.sink_parquet(plan, root_data, path, streaming, write_kwargs, pb, mkdir)

    def write_csv(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        separator: int = ord(","),
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        rust = require_rust_core()
        if not hasattr(rust, "sink_csv"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`sink_csv`. See docs/DEVELOPER.md."
            )
        with span("sink_csv", streaming=bool(streaming), path=str(path)):
            rust.sink_csv(
                plan, root_data, path, streaming, separator & 0xFF, write_kwargs
            )

    def write_ipc(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        rust = require_rust_core()
        if not hasattr(rust, "sink_ipc"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`sink_ipc`. See docs/DEVELOPER.md."
            )
        with span(
            "sink_ipc",
            streaming=bool(streaming),
            path=str(path),
            compression=compression,
        ):
            rust.sink_ipc(plan, root_data, path, streaming, compression, write_kwargs)

    def write_ndjson(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        rust = require_rust_core()
        if not hasattr(rust, "sink_ndjson"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`sink_ndjson`. See docs/DEVELOPER.md."
            )
        with span("sink_ndjson", streaming=bool(streaming), path=str(path)):
            rust.sink_ndjson(plan, root_data, path, streaming, write_kwargs)

    def collect_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]:
        rust = require_rust_core()
        if not hasattr(rust, "collect_plan_batches"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`collect_plan_batches`. See docs/DEVELOPER.md."
            )
        with span(
            "collect_plan_batches",
            batch_size=int(batch_size),
            streaming=bool(streaming),
        ):
            return list(
                rust.collect_plan_batches(plan, root_data, batch_size, streaming)
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        if not hasattr(rust, "execute_join"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`execute_join`. See docs/DEVELOPER.md."
            )
        with span(
            "execute_join",
            how=how,
            suffix=suffix,
            validate=validate,
            coalesce=coalesce,
            join_nulls=join_nulls,
            maintain_order=maintain_order,
            allow_parallel=allow_parallel,
            force_parallel=force_parallel,
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_join(
                left_plan,
                left_root_data,
                right_plan,
                right_root_data,
                list(left_on),
                list(right_on),
                how,
                suffix,
                validate,
                coalesce,
                join_nulls,
                maintain_order,
                allow_parallel,
                force_parallel,
                as_python_lists,
                streaming,
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        if not hasattr(rust, "execute_groupby_agg"):
            raise MissingRustExtensionError(
                f"{MISSING_SYMBOL_PREFIX}`execute_groupby_agg`. See docs/DEVELOPER.md."
            )
        with span(
            "execute_groupby_agg",
            by=list(by),
            maintain_order=bool(maintain_order),
            drop_nulls=bool(drop_nulls),
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_groupby_agg(
                plan,
                root_data,
                list(by),
                aggregations,
                bool(maintain_order),
                bool(drop_nulls),
                as_python_lists,
                streaming,
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_concat",
            how=how,
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_concat(
                left_plan,
                left_root_data,
                right_plan,
                right_root_data,
                how,
                as_python_lists,
                streaming,
            )

    def execute_except_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_except_all",
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_except_all(
                left_plan,
                left_root_data,
                right_plan,
                right_root_data,
                as_python_lists,
                streaming,
            )

    def execute_intersect_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_intersect_all",
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_intersect_all(
                left_plan,
                left_root_data,
                right_plan,
                right_root_data,
                as_python_lists,
                streaming,
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_melt",
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_pivot",
            aggregate_function=aggregate_function,
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
            return rust.execute_pivot(
                plan,
                root_data,
                list(index),
                columns,
                list(values),
                aggregate_function,
                None if pivot_values is None else list(pivot_values),
                bool(sort_columns),
                str(separator),
                as_python_lists,
                streaming,
            )

    def execute_explode(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_explode",
            columns=list(columns),
            streaming=bool(streaming),
            outer=bool(outer),
        ):
            return rust.execute_explode(
                plan, root_data, list(columns), streaming, outer
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_posexplode",
            list_column=list_column,
            pos_name=pos_name,
            value_name=value_name,
            streaming=bool(streaming),
            outer=bool(outer),
        ):
            return rust.execute_posexplode(
                plan,
                root_data,
                str(list_column),
                str(pos_name),
                str(value_name),
                streaming,
                outer,
            )

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span("execute_unnest", columns=list(columns), streaming=bool(streaming)):
            return rust.execute_unnest(plan, root_data, list(columns), streaming)

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_rolling_agg",
            on=on,
            column=column,
            window_size=window_size,
            agg_op=op,
            out_name=out_name,
            min_periods=int(min_periods),
        ):
            return rust.execute_rolling_agg(
                plan, root_data, on, column, window_size, op, out_name, by, min_periods
            )

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
    ) -> tuple[Any, Any]:
        rust = require_rust_core()
        with span(
            "execute_groupby_dynamic_agg",
            index_column=index_column,
            every=every,
            period=period,
            as_python_lists=bool(as_python_lists),
            streaming=bool(streaming),
        ):
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

    @property
    def capabilities(self) -> Any:
        """Feature flags for the loaded native extension."""
        return native_engine_capabilities()
