"""Execution engine: native Polars planning + Mongo collection materialization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantable_native.capabilities import native_engine_capabilities
from pydantable_native.native import NativePolarsEngine
from pydantable_protocol.protocols import EngineCapabilities

from entei_core._materialize import materialize_root_data


class EnteiPydantableEngine(NativePolarsEngine):
    """Pydantable engine that delegates planning to the native Rust core and runs
    execution on in-memory column dicts. If root data is ``MongoRoot``,
    documents are read from the collection (full scan for this version) and
    converted to ``dict[str, list]`` before calling the native executor.

    Keep the **process default** engine as ``NativePolarsEngine`` so
    ``get_expression_runtime`` can build ``Expr`` trees;
    pass ``engine=EnteiPydantableEngine()`` only on frames that use a
    ``MongoRoot`` (or plain columnar dicts).
    """

    @property
    def capabilities(self) -> EngineCapabilities:
        c = native_engine_capabilities()
        return EngineCapabilities(
            backend="custom",
            extension_loaded=c.extension_loaded,
            has_execute_plan=c.has_execute_plan,
            has_async_execute_plan=c.has_async_execute_plan,
            has_async_collect_plan_batches=c.has_async_collect_plan_batches,
            has_sink_parquet=c.has_sink_parquet,
            has_sink_csv=c.has_sink_csv,
            has_sink_ipc=c.has_sink_ipc,
            has_sink_ndjson=c.has_sink_ndjson,
            has_collect_plan_batches=c.has_collect_plan_batches,
            has_execute_join=c.has_execute_join,
            has_execute_groupby_agg=c.has_execute_groupby_agg,
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
        return super().execute_plan(
            plan,
            materialize_root_data(data),
            as_python_lists=as_python_lists,
            streaming=streaming,
            error_context=error_context,
        )

    async def async_execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any:
        return await super().async_execute_plan(
            plan,
            materialize_root_data(data),
            as_python_lists=as_python_lists,
            streaming=streaming,
            error_context=error_context,
        )

    async def async_collect_plan_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]:
        return await super().async_collect_plan_batches(
            plan,
            materialize_root_data(root_data),
            batch_size=batch_size,
            streaming=streaming,
        )

    def collect_batches(
        self,
        plan: Any,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]:
        return super().collect_batches(
            plan,
            materialize_root_data(root_data),
            batch_size=batch_size,
            streaming=streaming,
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
        super().write_parquet(
            plan,
            materialize_root_data(root_data),
            path,
            streaming=streaming,
            write_kwargs=write_kwargs,
            partition_by=partition_by,
            mkdir=mkdir,
        )

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
        super().write_csv(
            plan,
            materialize_root_data(root_data),
            path,
            streaming=streaming,
            separator=separator,
            write_kwargs=write_kwargs,
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
        super().write_ipc(
            plan,
            materialize_root_data(root_data),
            path,
            streaming=streaming,
            compression=compression,
            write_kwargs=write_kwargs,
        )

    def write_ndjson(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().write_ndjson(
            plan,
            materialize_root_data(root_data),
            path,
            streaming=streaming,
            write_kwargs=write_kwargs,
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
        return super().execute_join(
            left_plan,
            materialize_root_data(left_root_data),
            right_plan,
            materialize_root_data(right_root_data),
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
        return super().execute_groupby_agg(
            plan,
            materialize_root_data(root_data),
            by,
            aggregations,
            maintain_order=maintain_order,
            drop_nulls=drop_nulls,
            as_python_lists=as_python_lists,
            streaming=streaming,
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
        return super().execute_concat(
            left_plan,
            materialize_root_data(left_root_data),
            right_plan,
            materialize_root_data(right_root_data),
            how,
            as_python_lists=as_python_lists,
            streaming=streaming,
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
        return super().execute_except_all(
            left_plan,
            materialize_root_data(left_root_data),
            right_plan,
            materialize_root_data(right_root_data),
            as_python_lists=as_python_lists,
            streaming=streaming,
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
        return super().execute_intersect_all(
            left_plan,
            materialize_root_data(left_root_data),
            right_plan,
            materialize_root_data(right_root_data),
            as_python_lists=as_python_lists,
            streaming=streaming,
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
        return super().execute_melt(
            plan,
            materialize_root_data(root_data),
            id_vars,
            value_vars,
            variable_name,
            value_name,
            as_python_lists=as_python_lists,
            streaming=streaming,
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
        return super().execute_pivot(
            plan,
            materialize_root_data(root_data),
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
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]:
        return super().execute_explode(
            plan,
            materialize_root_data(root_data),
            columns,
            streaming=streaming,
            outer=outer,
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
        return super().execute_posexplode(
            plan,
            materialize_root_data(root_data),
            list_column,
            pos_name,
            value_name,
            streaming=streaming,
            outer=outer,
        )

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        return super().execute_unnest(
            plan,
            materialize_root_data(root_data),
            columns,
            streaming=streaming,
        )

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
        return super().execute_rolling_agg(
            plan,
            materialize_root_data(root_data),
            on,
            column,
            window_size,
            op,
            out_name,
            by,
            min_periods,
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
        return super().execute_groupby_dynamic_agg(
            plan,
            materialize_root_data(root_data),
            index_column,
            every,
            period,
            by,
            aggregations,
            as_python_lists=as_python_lists,
            streaming=streaming,
        )
