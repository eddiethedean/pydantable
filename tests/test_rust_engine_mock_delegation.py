"""``rust_engine`` shims: one delegation smoke test plus native roundtrips."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pydantable.rust_engine as rust_engine
import pytest
from pydantable import DataFrame, Schema
from pydantable.engine.protocols import native_engine_capabilities


class _Row(Schema):
    x: int


@pytest.fixture
def mock_engine(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    eng = MagicMock()
    eng.execute_plan.return_value = {"x": [1]}
    eng.async_execute_plan = AsyncMock(return_value={"x": [1]})
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


def test_rust_engine_delegates_sync_async_and_reshape_to_default_engine(
    mock_engine: MagicMock,
) -> None:
    """Smoke test: shims forward calls (and kwargs) to ``get_default_engine()``."""
    rust_engine.execute_plan(
        "p", "d", as_python_lists=True, streaming=True, error_context="ctx"
    )
    mock_engine.execute_plan.assert_called_once_with(
        "p",
        "d",
        as_python_lists=True,
        streaming=True,
        error_context="ctx",
    )

    rust_engine.write_parquet("p", "d", "/x", streaming=True, mkdir=False)
    mock_engine.write_parquet.assert_called_with(
        "p",
        "d",
        "/x",
        streaming=True,
        write_kwargs=None,
        partition_by=None,
        mkdir=False,
    )

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
    rust_engine.execute_melt("p", "d", ["id"], ["v"], "var", "val")
    assert mock_engine.execute_join.called
    assert mock_engine.execute_melt.called


@pytest.mark.asyncio
async def test_rust_engine_async_delegates_with_kwargs(mock_engine: MagicMock) -> None:
    await rust_engine.async_execute_plan("p", "d", error_context="e", streaming=True)
    mock_engine.async_execute_plan.assert_awaited_once_with(
        "p",
        "d",
        as_python_lists=False,
        streaming=True,
        error_context="e",
    )


def test_rust_engine_execute_plan_native_roundtrip() -> None:
    if not native_engine_capabilities().extension_loaded:
        pytest.skip("Native extension not installed")

    df = DataFrame[_Row]({"x": [1, 2, 3]})
    expected = df.to_dict()
    out = rust_engine.execute_plan(
        df._rust_plan,
        df._root_data,
        as_python_lists=True,
    )
    assert out == expected


def test_rust_engine_execute_plan_native_after_filter() -> None:
    if not native_engine_capabilities().extension_loaded:
        pytest.skip("Native extension not installed")

    df = DataFrame[_Row]({"x": [1, 2, 3, 4]})
    filtered = df.filter(df.x >= 3)
    expected = filtered.to_dict()
    out = rust_engine.execute_plan(
        filtered._rust_plan,
        filtered._root_data,
        as_python_lists=True,
    )
    assert out == expected


@pytest.mark.asyncio
async def test_rust_engine_async_execute_plan_native_roundtrip() -> None:
    if not native_engine_capabilities().extension_loaded:
        pytest.skip("Native extension not installed")
    if not native_engine_capabilities().has_async_execute_plan:
        pytest.skip("Native async_execute_plan not available")

    df = DataFrame[_Row]({"x": [10, 20]})
    expected = df.to_dict()
    out = await rust_engine.async_execute_plan(
        df._rust_plan,
        df._root_data,
        as_python_lists=True,
    )
    assert out == expected


def test_rust_engine_write_parquet_native_roundtrip(tmp_path) -> None:
    if not native_engine_capabilities().extension_loaded:
        pytest.skip("Native extension not installed")

    df = DataFrame[_Row]({"x": [1, 2]})
    path = tmp_path / "out.parquet"
    rust_engine.write_parquet(df._rust_plan, df._root_data, str(path))
    back = DataFrame[_Row].read_parquet(str(path)).to_dict()
    assert back == {"x": [1, 2]}
