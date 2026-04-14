"""Mongo-oriented execution engine: native plans + :class:`entei_core.MongoRoot`
materialization.

The **entei-core** distribution supplies :class:`~entei_core.mongo_root.MongoRoot` and
columnar scan helpers; this module implements :class:`EnteiPydantableEngine` on the
pydantable side (same pattern as lazy SQL facades + **moltres-core**).
"""

from __future__ import annotations

import importlib
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantable.engine import NativePolarsEngine, native_engine_capabilities
from pydantable.errors import UnsupportedEngineOperationError

if TYPE_CHECKING:
    from collections.abc import Sequence


def _materialize_root_data(data: Any) -> Any:
    entei_core = importlib.import_module("entei_core")
    return entei_core.materialize_root_data(data)


async def _amaterialize_root_data(data: Any) -> Any:
    # Async Beanie root support lives in pydantable (no entei-core required).
    try:
        from pydantable.mongo_entei import BeanieAsyncRoot
    except Exception:
        BeanieAsyncRoot = None  # type: ignore[assignment]
    if BeanieAsyncRoot is not None and isinstance(data, BeanieAsyncRoot):
        from pydantable.io.beanie import afetch_beanie

        return await afetch_beanie(
            data.document_cls,
            criteria=data.criteria,
            fields=list(data.fields) if data.fields is not None else None,
            fetch_links=data.fetch_links,
            nesting_depth=data.nesting_depth,
            nesting_depths_per_field=data.nesting_depths_per_field,
            flatten=data.flatten,
            id_column=data.id_column,
        )
    return _materialize_root_data(data)


if NativePolarsEngine is None:

    class EnteiPydantableEngine:  # type: ignore[no-redef]
        """Placeholder when ``pydantable-native`` is not installed."""

        __slots__ = ()

        def __init__(self, *_a: Any, **_k: Any) -> None:
            from pydantable._extension import MissingRustExtensionError

            raise MissingRustExtensionError(
                "EnteiPydantableEngine requires pydantable-native (native extension). "
                "Reinstall pydantable or install pydantable-native."
            )

else:

    class EnteiPydantableEngine(NativePolarsEngine):  # type: ignore[misc, no-redef]
        """Delegate planning/execution to :class:`NativePolarsEngine`.

        Materializes Mongo roots at execution time.
        """

        __slots__ = ()

        @property
        def capabilities(self) -> Any:
            return replace(native_engine_capabilities(), backend="custom")

        def execute_plan(
            self,
            plan: Any,
            data: Any,
            *,
            as_python_lists: bool = False,
            streaming: bool = False,
            error_context: str | None = None,
        ) -> Any:
            # Async Beanie roots are async-only.
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization. "
                    "Use `await df.acollect()` / `await df.ato_dict()` (or call "
                    "EnteiDataFrame.from_beanie(...) with a sync database)."
                )
            return super().execute_plan(
                plan,
                _materialize_root_data(data),
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
                await _amaterialize_root_data(data),
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
                await _amaterialize_root_data(root_data),
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
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(root_data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization. "
                    "Use `await df.acollect()` / `await df.astream()`."
                )
            return super().collect_batches(
                plan,
                _materialize_root_data(root_data),
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
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(root_data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization and "
                    "do not support sync lazy sinks. Materialize async to a "
                    "column dict, then "
                    "export/write."
                )
            super().write_parquet(
                plan,
                _materialize_root_data(root_data),
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
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(root_data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization and "
                    "do not support sync lazy sinks. Materialize async to a "
                    "column dict, then "
                    "export/write."
                )
            super().write_csv(
                plan,
                _materialize_root_data(root_data),
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
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(root_data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization and "
                    "do not support sync lazy sinks. Materialize async to a "
                    "column dict, then "
                    "export/write."
                )
            super().write_ipc(
                plan,
                _materialize_root_data(root_data),
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
            try:
                from pydantable.mongo_entei import BeanieAsyncRoot
            except Exception:
                BeanieAsyncRoot = None  # type: ignore[assignment]
            if BeanieAsyncRoot is not None and isinstance(root_data, BeanieAsyncRoot):
                raise UnsupportedEngineOperationError(
                    "Beanie-backed Mongo roots require async materialization and "
                    "do not support sync lazy sinks. Materialize async to a "
                    "column dict, then "
                    "export/write."
                )
            super().write_ndjson(
                plan,
                _materialize_root_data(root_data),
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
                _materialize_root_data(left_root_data),
                right_plan,
                _materialize_root_data(right_root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(left_root_data),
                right_plan,
                _materialize_root_data(right_root_data),
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
                _materialize_root_data(left_root_data),
                right_plan,
                _materialize_root_data(right_root_data),
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
                _materialize_root_data(left_root_data),
                right_plan,
                _materialize_root_data(right_root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
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
                _materialize_root_data(root_data),
                index_column,
                every,
                period,
                by,
                aggregations,
                as_python_lists=as_python_lists,
                streaming=streaming,
            )


__all__ = ["EnteiPydantableEngine"]
