"""PlanFrame datetime extraction lowering via ``planframe_adapter.expr`` (issue #3)."""

from __future__ import annotations

from datetime import date, datetime

from planframe.expr import api as pf
from pydantable import DataFrameModel
from pydantable.planframe_adapter.execute import execute_frame


class _Dates(DataFrameModel):
    d: date


class _Datetimes(DataFrameModel):
    ts: datetime


def _run_with_column(m: DataFrameModel, pf_expr: object) -> dict[str, list[object]]:
    out = execute_frame(m._pf.with_columns(out=pf_expr))
    return out.to_dict()


def test_planframe_dt_year_month_day_on_date() -> None:
    m = _Dates({"d": [date(2024, 3, 15), date(2025, 12, 1)]})
    assert _run_with_column(m, pf.DtYear(pf.col("d")))["out"] == [2024, 2025]
    assert _run_with_column(m, pf.DtMonth(pf.col("d")))["out"] == [3, 12]
    assert _run_with_column(m, pf.DtDay(pf.col("d")))["out"] == [15, 1]


def test_planframe_dt_year_month_day_on_datetime() -> None:
    m = _Datetimes(
        {
            "ts": [
                datetime(2024, 7, 20, 14, 30, 0),
                datetime(2023, 1, 2, 0, 0, 0),
            ]
        }
    )
    assert _run_with_column(m, pf.DtYear(pf.col("ts")))["out"] == [2024, 2023]
    assert _run_with_column(m, pf.DtMonth(pf.col("ts")))["out"] == [7, 1]
    assert _run_with_column(m, pf.DtDay(pf.col("ts")))["out"] == [20, 2]
