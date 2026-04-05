"""Cover ``rust_engine`` shims by delegating to a mocked default engine."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pydantable.rust_engine as rust_engine
import pytest


@pytest.fixture
def mock_engine(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    eng = MagicMock()
    eng.execute_plan.return_value = None
    eng.async_execute_plan = AsyncMock(return_value=None)
    eng.async_collect_plan_batches = AsyncMock(return_value=[])
    eng.write_parquet.return_value = None
    eng.write_csv.return_value = None
    eng.write_ipc.return_value = None
    eng.write_ndjson.return_value = None
    eng.collect_batches.return_value = []
    eng.execute_join.return_value = (None, None)
    eng.execute_groupby_agg.return_value = (None, None)
    eng.execute_concat.return_value = (None, None)
    eng.execute_except_all.return_value = (None, None)
    eng.execute_intersect_all.return_value = (None, None)
    eng.execute_melt.return_value = (None, None)
    eng.execute_pivot.return_value = (None, None)
    eng.execute_explode.return_value = (None, None)
    eng.execute_posexplode.return_value = (None, None)
    eng.execute_unnest.return_value = (None, None)
    eng.execute_rolling_agg.return_value = (None, None)
    eng.execute_groupby_dynamic_agg.return_value = (None, None)

    monkeypatch.setattr(rust_engine, "get_default_engine", lambda: eng)
    return eng


def test_rust_engine_sync_writes_and_execute_plan(mock_engine: MagicMock) -> None:
    rust_engine.execute_plan("p", "d", as_python_lists=True, streaming=True)
    mock_engine.execute_plan.assert_called_once()

    rust_engine.write_parquet("p", "d", "/x", streaming=True, mkdir=False)
    rust_engine.write_csv("p", "d", "/x", separator=ord(";"))
    rust_engine.write_ipc("p", "d", "/x", compression="zstd")
    rust_engine.write_ndjson("p", "d", "/x")
    rust_engine.collect_batches("p", "d", batch_size=100)

    assert mock_engine.write_parquet.called
    assert mock_engine.collect_batches.called


@pytest.mark.asyncio
async def test_rust_engine_async_plan_helpers(mock_engine: MagicMock) -> None:
    await rust_engine.async_execute_plan("p", "d", error_context="e")
    await rust_engine.async_collect_plan_batches("p", "d", batch_size=10)
    mock_engine.async_execute_plan.assert_awaited()
    mock_engine.async_collect_plan_batches.assert_awaited()


def test_rust_engine_execute_join_merge_variants(mock_engine: MagicMock) -> None:
    rust_engine.execute_join(
        "lp",
        "ld",
        "rp",
        "rd",
        ["a"],
        ["b"],
        "inner",
        "_r",
        validate="1:1",
        coalesce=True,
    )
    rust_engine.execute_groupby_agg(
        "p",
        "d",
        ["k"],
        {},
        maintain_order=True,
        drop_nulls=False,
    )
    rust_engine.execute_concat("lp", "ld", "rp", "rd", "vertical")
    rust_engine.execute_except_all("lp", "ld", "rp", "rd")
    rust_engine.execute_intersect_all("lp", "ld", "rp", "rd")
    assert mock_engine.execute_join.called


def test_rust_engine_execute_reshape_ops(mock_engine: MagicMock) -> None:
    rust_engine.execute_melt(
        "p",
        "d",
        ["id"],
        ["v"],
        "var",
        "val",
    )
    rust_engine.execute_pivot(
        "p",
        "d",
        ["i"],
        "c",
        ["v"],
        "first",
        pivot_values=None,
        sort_columns=True,
    )
    rust_engine.execute_explode("p", "d", ["c"], outer=True)
    rust_engine.execute_posexplode("p", "d", "c", "pos", "v", outer=False)
    rust_engine.execute_unnest("p", "d", ["s"])
    rust_engine.execute_rolling_agg(
        "p",
        "d",
        "ts",
        "v",
        2,
        "sum",
        "out",
        ["g"],
        1,
    )
    rust_engine.execute_groupby_dynamic_agg(
        "p",
        "d",
        "ts",
        "1h",
        "2h",
        ["g"],
        {},
    )
    assert mock_engine.execute_melt.called
