"""Optional-column scan recovery: iteration guard and limits."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantable.dataframe._materialize_scan_fallback import (
    _optional_scan_recovery_limit,
    materialize_with_optional_scan_fallback_sync,
)
from pydantable.schema import schema_field_types
from pydantic import BaseModel, ConfigDict


def test_optional_scan_recovery_limit_scales_with_schema_width() -> None:
    assert _optional_scan_recovery_limit(1) >= 8
    assert _optional_scan_recovery_limit(20) == 22


class _ThreeOptional(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    a: int | None
    b: int | None
    c: int | None


def test_scan_fallback_raises_when_recovery_exceeds_iteration_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard against pathological replan/execute loops."""
    monkeypatch.setattr(
        "pydantable.dataframe._materialize_scan_fallback._optional_scan_recovery_limit",
        lambda _n: 2,
    )

    class ScanFileRoot:
        __module__ = "pydantable_native._core"

    root = ScanFileRoot()
    ft = dict(schema_field_types(_ThreeOptional))
    plan = object()

    eng = MagicMock()
    eng.make_plan.return_value = plan
    n = {"calls": 0}

    def execute_plan(
        _plan: object,
        _data: object,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str = "",
    ) -> dict[str, list[Any]]:
        n["calls"] += 1
        if n["calls"] == 1:
            raise ValueError("ColumnNotFoundError: 'a'")
        if n["calls"] == 2:
            raise ValueError("ColumnNotFoundError: 'b'")
        return {"id": [1]}

    eng.execute_plan.side_effect = execute_plan

    with pytest.raises(RuntimeError, match="exceeded iteration bound"):
        materialize_with_optional_scan_fallback_sync(
            eng,
            plan=plan,
            root_data=root,
            field_types=ft,
            current_schema_type=_ThreeOptional,
            io_validation_fill_missing_optional=True,
            streaming=False,
            error_context="test",
        )
