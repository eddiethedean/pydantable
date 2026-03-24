"""Async materialization APIs (acollect / ato_dict / ato_polars).

**ato_arrow** is covered in ``tests/test_arrow_interchange.py`` (0.16.0), not here.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import warnings
from unittest import mock

import pytest
from pydantable import DataFrameModel


class Tiny(DataFrameModel):
    x: int


class TwoCol(DataFrameModel):
    a: int
    b: int


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
    with (
        mock.patch(
            "pydantable.dataframe._impl.importlib.import_module",
            side_effect=ImportError("no polars"),
        ),
        pytest.raises(ImportError, match="polars is required"),
    ):
        await df.ato_polars()


@pytest.mark.asyncio
async def test_acollect_empty_frame() -> None:
    df = Tiny({"x": []})
    assert await df.acollect() == []
    assert await df.acollect(as_lists=True) == {"x": []}
    assert await df.ato_dict() == {"x": []}


@pytest.mark.asyncio
async def test_acollect_matches_sync_after_transforms() -> None:
    df = TwoCol({"a": [1, 2, 3], "b": [10, 20, 30]})
    chained = df.filter(df.a >= 2).with_columns(sum_ab=df.a + df.b)
    sync_rows = chained.collect()
    async_rows = await chained.acollect()
    assert [r.model_dump() for r in sync_rows] == [r.model_dump() for r in async_rows]
    sync_d = chained.to_dict()
    async_d = await chained.ato_dict()
    assert sync_d == async_d


@pytest.mark.asyncio
async def test_gather_parallel_acollect_independent_frames() -> None:
    d1 = Tiny({"x": [1, 2]})
    d2 = Tiny({"x": [10, 20, 30]})
    r1, r2 = await asyncio.gather(d1.acollect(as_lists=True), d2.ato_dict())
    assert r1 == {"x": [1, 2]}
    assert r2 == {"x": [10, 20, 30]}


@pytest.mark.asyncio
async def test_acollect_as_numpy() -> None:
    np = pytest.importorskip("numpy")
    df = Tiny({"x": [1, 2, 3]})
    out = await df.acollect(as_numpy=True)
    assert isinstance(out["x"], np.ndarray)
    assert out["x"].tolist() == [1, 2, 3]


@pytest.mark.asyncio
async def test_acollect_rejects_numpy_and_lists_together() -> None:
    df = Tiny({"x": [1]})
    with pytest.raises(ValueError, match="as_numpy=True and as_lists=True"):
        await df.acollect(as_numpy=True, as_lists=True)


@pytest.mark.asyncio
async def test_acollect_deprecated_as_polars_branch() -> None:
    pl = pytest.importorskip("polars")
    df = Tiny({"x": [7]})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        pl_out = await df.acollect(as_polars=True)
        dict_out = await df.acollect(as_polars=False)
    assert any("as_polars is deprecated" in str(x.message) for x in w)
    assert isinstance(pl_out, pl.DataFrame)
    assert pl_out.to_dict(as_series=False) == {"x": [7]}
    assert dict_out == {"x": [7]}


@pytest.mark.asyncio
async def test_ato_polars_uses_custom_executor() -> None:
    pl = pytest.importorskip("polars")
    df = Tiny({"x": [4, 5]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        out = await df.ato_polars(executor=ex)
    assert isinstance(out, pl.DataFrame)
    assert out.to_dict(as_series=False) == {"x": [4, 5]}


@pytest.mark.asyncio
async def test_ato_dict_uses_custom_executor() -> None:
    df = TwoCol({"a": [1], "b": [2]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        d = await df.ato_dict(executor=ex)
    assert d == {"a": [1], "b": [2]}


@pytest.mark.asyncio
async def test_ato_dict_after_pyarrow_map_column() -> None:
    pa = pytest.importorskip("pyarrow")
    from pydantable import DataFrame
    from pydantable.schema import Schema

    class M(Schema):
        m: dict[str, int]

    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("u", 11), ("v", 22)]], type=mt)
    df = DataFrame[M]({"m": arr}, trusted_mode="strict")
    col = await df.ato_dict()
    assert col == {"m": [{"u": 11, "v": 22}]}
