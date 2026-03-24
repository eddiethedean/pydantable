"""Async materialization APIs (acollect / ato_dict / ato_polars)."""

from __future__ import annotations

import concurrent.futures
from unittest import mock

import pytest
from pydantable import DataFrameModel


class Tiny(DataFrameModel):
    x: int


@pytest.mark.asyncio
async def test_acollect_default_rows() -> None:
    df = Tiny({"x": [1, 2]})
    rows = await df.acollect()
    assert [r.x for r in rows] == [1, 2]


@pytest.mark.asyncio
async def test_acollect_as_lists() -> None:
    df = Tiny({"x": [1, 2]})
    col = await df.acollect(as_lists=True)
    assert col == {"x": [1, 2]}


@pytest.mark.asyncio
async def test_ato_dict() -> None:
    df = Tiny({"x": [1, 2]})
    col = await df.ato_dict()
    assert col == {"x": [1, 2]}


@pytest.mark.asyncio
async def test_ato_polars() -> None:
    pl = pytest.importorskip("polars")
    df = Tiny({"x": [1, 2]})
    out = await df.ato_polars()
    assert isinstance(out, pl.DataFrame)
    assert out.to_dict(as_series=False) == {"x": [1, 2]}


@pytest.mark.asyncio
async def test_acollect_custom_executor() -> None:
    df = Tiny({"x": [1, 2]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        col = await df.acollect(as_lists=True, executor=ex)
    assert col == {"x": [1, 2]}


@pytest.mark.asyncio
async def test_arows_ato_dicts() -> None:
    df = Tiny({"x": [1, 2]})
    rows = await df.arows()
    assert [r.x for r in rows] == [1, 2]
    dicts = await df.ato_dicts()
    assert dicts == [{"x": 1}, {"x": 2}]


@pytest.mark.asyncio
async def test_dataframe_acollect_core() -> None:
    from pydantable import DataFrame
    from pydantable.schema import Schema

    class S(Schema):
        x: int

    df = DataFrame[S]({"x": [3]})
    rows = await df.acollect()
    assert len(rows) == 1 and rows[0].x == 3


@pytest.mark.asyncio
async def test_ato_polars_import_error_propagates() -> None:
    df = Tiny({"x": [1]})
    with mock.patch(
        "pydantable.dataframe.importlib.import_module",
        side_effect=ImportError("no polars"),
    ), pytest.raises(ImportError, match="polars is required"):
        await df.ato_polars()
