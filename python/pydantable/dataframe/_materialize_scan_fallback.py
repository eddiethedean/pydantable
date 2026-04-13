"""Engine materialization with scan-root recovery for missing optional columns."""

from __future__ import annotations

import functools
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel  # noqa: TC002
from pydantic_core import PydanticUndefined

from pydantable.schema import _annotation_nullable_inner, field_types_for_rust

if TYPE_CHECKING:
    from pydantable.engine.protocols import ExecutionEngine

from ._execution_handle import _materialize_in_thread
from ._scan import _extract_missing_scan_column_from_engine_error, _is_scan_file_root


def _optional_scan_recovery_limit(initial_field_count: int) -> int:
    """Upper bound on execute/replan rounds (one optional column dropped per round)."""
    return max(initial_field_count + 2, 8)


def materialize_with_optional_scan_fallback_sync(
    engine: ExecutionEngine,
    *,
    plan: Any,
    root_data: Any,
    field_types: dict[str, Any],
    current_schema_type: type[BaseModel],
    io_validation_fill_missing_optional: bool,
    streaming: bool,
    error_context: str,
) -> dict[str, list[Any]]:
    """Run ``execute_plan``; drop missing optional columns on scan roots when needed."""
    ft = dict(field_types)
    pl = plan
    limit = _optional_scan_recovery_limit(len(ft))
    rnd = 0
    while True:
        rnd += 1
        if rnd > limit:
            raise RuntimeError(
                "Optional scan column recovery exceeded iteration bound "
                f"({rnd} > {limit}); error_context={error_context!r}"
            )
        try:
            return engine.execute_plan(
                pl,
                root_data,
                as_python_lists=True,
                streaming=streaming,
                error_context=error_context,
            )
        except ValueError as e:
            if not _is_scan_file_root(root_data):
                raise
            missing_col = _extract_missing_scan_column_from_engine_error(str(e))
            if not missing_col:
                raise
            ann = ft.get(missing_col)
            if ann is None:
                raise
            _, nullable = _annotation_nullable_inner(ann)
            if not nullable:
                raise
            if not io_validation_fill_missing_optional:
                fi = current_schema_type.model_fields.get(missing_col)
                default = (
                    getattr(fi, "default", PydanticUndefined)
                    if fi is not None
                    else PydanticUndefined
                )
                if default is PydanticUndefined:
                    raise ValueError(
                        "Missing optional columns (configured as error): "
                        f"{[missing_col]}"
                    ) from e
            ft.pop(missing_col, None)
            pl = engine.make_plan(field_types_for_rust(ft))


async def materialize_with_optional_scan_fallback_async(
    engine: ExecutionEngine,
    *,
    plan: Any,
    root_data: Any,
    field_types: dict[str, Any],
    current_schema_type: type[BaseModel],
    io_validation_fill_missing_optional: bool,
    streaming: bool,
    error_context: str,
    executor: Executor | None,
) -> dict[str, list[Any]]:
    """Async ``async_execute_plan`` with the same optional-column recovery loop."""
    if not engine.has_async_execute_plan():
        return await _materialize_in_thread(
            functools.partial(
                materialize_with_optional_scan_fallback_sync,
                engine,
                plan=plan,
                root_data=root_data,
                field_types=dict(field_types),
                current_schema_type=current_schema_type,
                io_validation_fill_missing_optional=io_validation_fill_missing_optional,
                streaming=streaming,
                error_context=error_context,
            ),
            executor=executor,
        )

    ft = dict(field_types)
    pl = plan
    limit = _optional_scan_recovery_limit(len(ft))
    rnd = 0
    while True:
        rnd += 1
        if rnd > limit:
            raise RuntimeError(
                "Optional scan column recovery exceeded iteration bound "
                f"({rnd} > {limit}); error_context={error_context!r}"
            )
        try:
            return await engine.async_execute_plan(
                pl,
                root_data,
                as_python_lists=True,
                streaming=streaming,
                error_context=error_context,
            )
        except ValueError as e:
            if not _is_scan_file_root(root_data):
                raise
            missing_col = _extract_missing_scan_column_from_engine_error(str(e))
            if not missing_col:
                raise
            ann = ft.get(missing_col)
            if ann is None:
                raise
            _, nullable = _annotation_nullable_inner(ann)
            if not nullable:
                raise
            if not io_validation_fill_missing_optional:
                fi = current_schema_type.model_fields.get(missing_col)
                default = (
                    getattr(fi, "default", PydanticUndefined)
                    if fi is not None
                    else PydanticUndefined
                )
                if default is PydanticUndefined:
                    raise ValueError(
                        "Missing optional columns (configured as error): "
                        f"{[missing_col]}"
                    ) from e
            ft.pop(missing_col, None)
            pl = engine.make_plan(field_types_for_rust(ft))
