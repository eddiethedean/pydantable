"""Async materialization APIs (acollect / ato_dict / ato_polars / submit / astream).

**ato_arrow** is covered in ``tests/test_arrow_interchange.py`` (0.16.0), not here.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import warnings
from unittest import mock

import pytest
from pydantable import DataFrame, DataFrameModel
from pydantable.schema import Schema


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


class SSchema(Schema):
    x: int


@pytest.mark.asyncio
async def test_dataframe_acollect_core() -> None:
    df = DataFrame[SSchema]({"x": [3]})
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
    assert any("2.0.0" in str(x.message) for x in w)
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


class MapSchema(Schema):
    m: dict[str, int]


@pytest.mark.asyncio
async def test_ato_dict_after_pyarrow_map_column() -> None:
    pa = pytest.importorskip("pyarrow")
    mt = pa.map_(pa.string(), pa.int64())
    arr = pa.array([[("u", 11), ("v", 22)]], type=mt)
    df = DataFrame[MapSchema]({"m": arr}, trusted_mode="strict")
    col = await df.ato_dict()
    assert col == {"m": [{"u": 11, "v": 22}]}


@pytest.mark.asyncio
async def test_submit_result_matches_collect() -> None:
    df = Tiny({"x": [1, 2]})
    handle = df.submit()
    assert not handle.done()
    rows = await handle.result()
    assert handle.done()
    assert [r.x for r in rows] == [1, 2]


@pytest.mark.asyncio
async def test_submit_as_lists() -> None:
    df = Tiny({"x": [7, 8]})
    handle = df.submit(as_lists=True)
    col = await handle.result()
    assert col == {"x": [7, 8]}


@pytest.mark.asyncio
async def test_submit_with_executor() -> None:
    df = TwoCol({"a": [1], "b": [2]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        handle = df.submit(as_lists=True, executor=ex)
        col = await handle.result()
    assert col == {"a": [1], "b": [2]}


@pytest.mark.asyncio
async def test_gather_submit_handles() -> None:
    d1 = Tiny({"x": [1]})
    d2 = Tiny({"x": [2]})
    h1 = d1.submit(as_lists=True)
    h2 = d2.submit(as_lists=True)
    r1, r2 = await asyncio.gather(h1.result(), h2.result())
    assert r1 == {"x": [1]}
    assert r2 == {"x": [2]}


@pytest.mark.asyncio
async def test_astream_yields_column_dict_chunks() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": list(range(5))})
    chunks: list[dict[str, list[int]]] = []
    async for batch in df.astream(batch_size=2):
        chunks.append(batch)
    assert len(chunks) == 3
    merged: list[int] = []
    for b in chunks:
        merged.extend(b["x"])
    assert merged == list(range(5))


@pytest.mark.asyncio
async def test_astream_matches_collect_batches_shapes() -> None:
    pytest.importorskip("polars")
    df = TwoCol({"a": [1, 2, 3], "b": [4, 5, 6]})
    async_chunks = [c async for c in df.astream(batch_size=2)]
    sync_batches = df.collect_batches(batch_size=2)
    assert len(async_chunks) == len(sync_batches)
    for ac, pl_df in zip(async_chunks, sync_batches, strict=True):
        assert ac == pl_df.to_dict(as_series=False)


@pytest.mark.asyncio
async def test_submit_propagates_collect_validation_error() -> None:
    df = Tiny({"x": [1]})
    handle = df.submit(as_numpy=True, as_lists=True)
    with pytest.raises(ValueError, match="as_numpy=True and as_lists=True"):
        await handle.result()


@pytest.mark.asyncio
async def test_submit_as_numpy() -> None:
    np = pytest.importorskip("numpy")
    df = Tiny({"x": [1, 2]})
    handle = df.submit(as_numpy=True)
    out = await handle.result()
    assert isinstance(out["x"], np.ndarray)
    assert out["x"].tolist() == [1, 2]


@pytest.mark.asyncio
async def test_execution_handle_result_idempotent() -> None:
    df = Tiny({"x": [9]})
    handle = df.submit(as_lists=True)
    col1 = await handle.result()
    col2 = await handle.result()
    assert col1 == col2 == {"x": [9]}


@pytest.mark.asyncio
async def test_dataframe_submit_and_astream() -> None:
    pytest.importorskip("polars")
    df = DataFrame[SSchema]({"x": [1, 2, 3]})
    handle = df.submit(as_lists=True)
    assert await handle.result() == {"x": [1, 2, 3]}
    chunks = [c async for c in df.astream(batch_size=2)]
    assert len(chunks) == 2
    assert chunks[0]["x"] + chunks[1]["x"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_astream_empty_frame_no_chunks() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": []})
    chunks = [c async for c in df.astream(batch_size=10)]
    assert chunks == []


@pytest.mark.asyncio
async def test_astream_single_row_one_chunk() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [42]})
    chunks = [c async for c in df.astream(batch_size=100)]
    assert len(chunks) == 1
    assert chunks[0] == {"x": [42]}


@pytest.mark.asyncio
async def test_astream_batch_size_one() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [1, 2, 3]})
    chunks = [c async for c in df.astream(batch_size=1)]
    assert [c["x"] for c in chunks] == [[1], [2], [3]]


@pytest.mark.asyncio
async def test_astream_raises_import_error_when_polars_missing() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [1]})

    _real_import = importlib.import_module

    def _fail_polars(name: str, *a: object, **kw: object) -> object:
        if name == "polars":
            raise ImportError("no polars")
        return _real_import(name, *a, **kw)

    with (
        mock.patch(
            "pydantable.dataframe._impl.importlib.import_module",
            side_effect=_fail_polars,
        ),
        pytest.raises(ImportError, match="polars is required for astream"),
    ):
        async for _ in df.astream():
            pass


@pytest.mark.asyncio
async def test_astream_engine_streaming_smoke() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [1, 2]})
    chunks = [c async for c in df.astream(batch_size=1, engine_streaming=False)]
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_ato_arrow_matches_sync() -> None:
    pytest.importorskip("pyarrow")
    df = Tiny({"x": [1, 2]})
    sync_t = df.to_arrow()
    async_t = await df.ato_arrow()
    assert sync_t.equals(async_t)


@pytest.mark.asyncio
async def test_acollect_when_async_execute_plan_disabled_uses_thread_fallback() -> None:
    df = Tiny({"x": [1, 2, 3]})
    with mock.patch(
        "pydantable.dataframe._impl.rust_has_async_execute_plan",
        return_value=False,
    ):
        col = await df.acollect(as_lists=True)
    assert col == {"x": [1, 2, 3]}


@pytest.mark.asyncio
async def test_astream_when_async_batches_disabled_uses_thread_fallback() -> None:
    pytest.importorskip("polars")
    df = TwoCol({"a": [1, 2], "b": [3, 4]})
    with mock.patch(
        "pydantable.dataframe._impl.rust_has_async_collect_plan_batches",
        return_value=False,
    ):
        chunks = [c async for c in df.astream(batch_size=1)]
    assert len(chunks) == 2
    assert chunks[0] == {"a": [1], "b": [3]}
    assert chunks[1] == {"a": [2], "b": [4]}


@pytest.mark.asyncio
async def test_gather_mixed_acollect_and_submit() -> None:
    d1 = Tiny({"x": [1]})
    d2 = Tiny({"x": [2]})
    h = d2.submit(as_lists=True)
    col, fut_result = await asyncio.gather(d1.acollect(as_lists=True), h.result())
    assert col == {"x": [1]}
    assert fut_result == {"x": [2]}


@pytest.mark.asyncio
async def test_execution_handle_cancel_after_done_returns_false() -> None:
    df = Tiny({"x": [1]})
    h = df.submit(as_lists=True)
    await h.result()
    assert h.done()
    assert h.cancel() is False


@pytest.mark.asyncio
async def test_astream_with_executor_for_row_conversion() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [1, 2]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        chunks = [c async for c in df.astream(batch_size=1, executor=ex)]
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_acollect_thread_fallback_matches_sync_after_filter() -> None:
    """When async_execute_plan is unavailable, ato_dict still matches to_dict."""
    df = TwoCol({"a": [1, 2, 3], "b": [10, 20, 30]})
    chained = df.filter(df.a >= 2)
    with mock.patch(
        "pydantable.dataframe._impl.rust_has_async_execute_plan",
        return_value=False,
    ):
        async_d = await chained.ato_dict()
    assert async_d == chained.to_dict()
