"""Async materialization APIs (acollect / ato_dict / ato_polars / submit / astream).

**stream** (sync chunked dicts) is covered here alongside **astream**.

**ato_arrow** is covered in ``tests/test_arrow_interchange.py`` (0.16.0), not here.

**Event loop (non-blocking):** tests whose names contain ``yields_event_loop`` assert
that another asyncio task runs *before* the slow step finishes (ordering on a
shared ``order`` list). They cover:

- ``acollect`` / ``ato_dict`` / ``ato_polars`` / ``ato_arrow`` / ``arows`` (native
  ``async_execute_plan`` on :class:`~pydantable.engine.native.NativePolarsEngine`
  when present; else skipped)
- ``acollect`` thread fallback (``asyncio.to_thread``)
- ``AwaitableDataFrameModel.acollect`` after ``aread_parquet``
- ``astream`` (native ``async_collect_plan_batches`` or thread fallback)
- ``ExecutionHandle.result`` after ``submit`` (background thread + ``await``)
- ``pydantable.io`` ``_run_io`` (``asyncio.to_thread``), via ``aread_parquet`` with a
  stub ``read_parquet`` (``amaterialize_*``, ``aexport_*``, ``afetch_sql``, ``aiter_*``
  share the same ``_run_io`` helper).

**Concurrent async:** tests whose names contain ``concurrent`` run *multiple*
independent awaitables in parallel (typically ``asyncio.gather``) and assert each
result matches its own frame/source (no cross-talk). This is distinct from
``yields_event_loop`` (interleaving with a single observer task). Most use
``_in_memory_engine_mock_for_concurrent_tests`` and
``_astream_collect_batches_mock_for_concurrent_tests`` so they run without a matching
native ``execute_plan`` binary; ``ScanFileRoot`` integration is optional (skipped when
unavailable).

**Monkeypatching:** frames materialize via ``self._engine``; tests that replace
execution patch :class:`~pydantable.engine.native.NativePolarsEngine` at class scope
(or pass a custom ``engine=``).

**Concurrency (deterministic):** avoid wall-clock overlap assertions, which are
flaky on CI due to OS scheduling jitter. Prefer event-based synchronization
tests that prove two operations can be in-flight at once.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import threading
import time
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

import pydantable.io as io_mod
import pytest
from pydantable import DataFrame, DataFrameModel
from pydantable.awaitable_dataframe_model import AwaitableDataFrameModel
from pydantable.engine import get_default_engine
from pydantable.engine.native import NativePolarsEngine
from pydantable.rust_engine import (
    rust_has_async_collect_plan_batches,
    rust_has_async_execute_plan,
)
from pydantable.schema import Schema


class Tiny(DataFrameModel):
    x: int


class TwoCol(DataFrameModel):
    a: int
    b: int


def _native_bound_async_execute_plan() -> Any:
    eng = get_default_engine()
    assert isinstance(eng, NativePolarsEngine)
    return eng.async_execute_plan


def _native_bound_async_collect_plan_batches() -> Any:
    eng = get_default_engine()
    assert isinstance(eng, NativePolarsEngine)
    return eng.async_collect_plan_batches


@contextmanager
def _in_memory_engine_mock_for_concurrent_tests() -> Any:
    """Sync ``execute_plan`` in a thread; return column dict from in-memory root.

    Lets concurrent ``gather`` tests run without a matching native ``execute_plan``
    binary (CI / dev wheels); only for in-memory ``dict[str, list]`` roots.
    """

    impl = importlib.import_module("pydantable.dataframe._impl")

    def fake_execute(
        self: object,
        plan: object,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> dict[str, list[Any]]:
        if not isinstance(data, dict):
            raise AssertionError(
                "concurrent in-memory mock expects dict column root data"
            )
        return {str(k): list(v) for k, v in data.items()}

    with (
        mock.patch.object(
            NativePolarsEngine, "has_async_execute_plan", return_value=False
        ),
        mock.patch.object(NativePolarsEngine, "execute_plan", fake_execute),
    ):
        yield impl


def _delayed_async_execute_plan_with_order(
    real: Any,
    order: list[str],
) -> Any:
    """Wrap a bound ``NativePolarsEngine.async_execute_plan`` (see ``real``)."""

    async def delayed(
        self: object,
        plan: object,
        data: object,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> object:
        order.append("engine_start")
        await asyncio.sleep(0.12)
        order.append("engine_pre_real")
        out = await real(
            plan,
            data,
            as_python_lists=as_python_lists,
            streaming=streaming,
            error_context=error_context,
        )
        order.append("engine_done")
        return out

    return delayed


def _delayed_async_collect_plan_batches_with_order(
    real: Any,
    order: list[str],
) -> Any:
    """Wrap a bound ``NativePolarsEngine.async_collect_plan_batches``."""

    async def delayed(
        self: object,
        plan: object,
        root_data: object,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> object:
        order.append("batch_start")
        await asyncio.sleep(0.12)
        order.append("batch_pre_real")
        out = await real(
            plan,
            root_data,
            batch_size=batch_size,
            streaming=streaming,
        )
        order.append("batch_done")
        return out

    return delayed


async def _wait_threading_event(ev: threading.Event, *, timeout: float) -> None:
    """Wait for a ``threading.Event`` without ``asyncio.to_thread(ev.wait)``.

    Using the default thread pool to block on ``Event.wait`` can starve the same
    pool that ``acollect`` uses for ``asyncio.to_thread(materialize)``, deadlocking
    when the pool is small (e.g. under load or xdist).
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not ev.is_set():
        if loop.time() >= deadline:
            raise TimeoutError
        await asyncio.sleep(0.01)


def _delayed_run_io_with_order(real: Any, order: list[str]) -> Any:
    """Wrap ``pydantable.io._run_io`` with an await for async I/O tests."""

    async def delayed(
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any] | None = None,
        *,
        executor: concurrent.futures.Executor | None = None,
    ) -> Any:
        order.append("io_start")
        await asyncio.sleep(0.12)
        order.append("io_pre_real")
        out = await real(fn, args, kwargs, executor=executor)
        order.append("io_done")
        return out

    return delayed


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
    df = Tiny({"x": [7]})
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        await df.acollect(as_polars=True)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        await df.acollect(as_polars=False)  # type: ignore[call-arg]


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


def test_stream_yields_column_dict_chunks() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": list(range(5))})
    chunks = list(df.stream(batch_size=2))
    assert len(chunks) == 3
    merged: list[int] = []
    for b in chunks:
        merged.extend(b["x"])
    assert merged == list(range(5))


def test_stream_matches_collect_batches_shapes() -> None:
    pytest.importorskip("polars")
    df = TwoCol({"a": [1, 2, 3], "b": [4, 5, 6]})
    stream_chunks = list(df.stream(batch_size=2))
    sync_batches = df.collect_batches(batch_size=2)
    assert len(stream_chunks) == len(sync_batches)
    for sc, pl_df in zip(stream_chunks, sync_batches, strict=True):
        assert sc == pl_df.to_dict(as_series=False)


@pytest.mark.asyncio
async def test_stream_parity_with_astream() -> None:
    pytest.importorskip("polars")
    df = TwoCol({"a": [1, 2, 3], "b": [4, 5, 6]})
    async_chunks = [c async for c in df.astream(batch_size=2)]
    sync_chunks = list(df.stream(batch_size=2))
    assert async_chunks == sync_chunks


def test_stream_empty_frame_no_chunks() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": []})
    assert list(df.stream(batch_size=10)) == []


def test_stream_engine_streaming_smoke() -> None:
    pytest.importorskip("polars")
    df = Tiny({"x": [1, 2]})
    chunks = list(df.stream(batch_size=1, engine_streaming=False))
    assert len(chunks) == 2


def test_dataframe_submit_and_stream() -> None:
    pytest.importorskip("polars")
    df = DataFrame[SSchema]({"x": [1, 2, 3]})
    handle = df.submit(as_lists=True)
    assert asyncio.run(handle.result()) == {"x": [1, 2, 3]}
    chunks = list(df.stream(batch_size=2))
    assert len(chunks) == 2
    assert chunks[0]["x"] + chunks[1]["x"] == [1, 2, 3]


def test_stream_raises_import_error_when_polars_missing() -> None:
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
        pytest.raises(ImportError, match="polars is required for stream"),
    ):
        list(df.stream())


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
    with mock.patch.object(
        NativePolarsEngine, "has_async_execute_plan", return_value=False
    ):
        col = await df.acollect(as_lists=True)
    assert col == {"x": [1, 2, 3]}


@pytest.mark.asyncio
async def test_astream_when_async_batches_disabled_uses_thread_fallback() -> None:
    pytest.importorskip("polars")
    df = TwoCol({"a": [1, 2], "b": [3, 4]})
    with mock.patch.object(
        NativePolarsEngine, "has_async_collect_plan_batches", return_value=False
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
    with mock.patch.object(
        NativePolarsEngine, "has_async_execute_plan", return_value=False
    ):
        async_d = await chained.ato_dict()
    assert async_d == chained.to_dict()


@pytest.mark.asyncio
async def test_acollect_yields_event_loop_native_async_execute_plan() -> None:
    """Other asyncio tasks must run while ``await async_execute_plan`` is in flight."""
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": list(range(50))})
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        await asyncio.wait_for(
            asyncio.gather(df.acollect(as_lists=True), observer()),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_acollect_yields_event_loop_thread_fallback() -> None:
    """Blocking engine work runs in a thread; the loop can still run other tasks."""
    msf = importlib.import_module("pydantable.dataframe._materialize_scan_fallback")
    real_mit = msf._materialize_in_thread
    order: list[str] = []

    async def mit_with_slow_sync(
        fn: object,
        *,
        executor: concurrent.futures.Executor | None,
    ) -> object:
        order.append("mit_start")

        def slow() -> object:
            order.append("thread_block_start")
            time.sleep(0.12)
            order.append("thread_block_end")
            assert callable(fn)
            return fn()  # type: ignore[operator]

        out = await real_mit(slow, executor=executor)
        order.append("mit_done")
        return out

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1, 2, 3]})
    with (
        mock.patch.object(
            NativePolarsEngine, "has_async_execute_plan", return_value=False
        ),
        mock.patch.object(msf, "_materialize_in_thread", mit_with_slow_sync),
        mock.patch.object(
            NativePolarsEngine,
            "execute_plan",
            lambda *_a, **_k: {"x": [1, 2, 3]},
        ),
    ):
        col = await asyncio.wait_for(
            asyncio.gather(df.acollect(as_lists=True), observer()),
            timeout=30.0,
        )
    assert col[0] == {"x": [1, 2, 3]}
    assert order.index("observer") < order.index("mit_done")


@pytest.mark.asyncio
async def test_ato_dict_yields_event_loop_native_async_execute_plan() -> None:
    """``ato_dict`` uses the same async materialization path as ``acollect``."""
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = TwoCol({"a": [1], "b": [2]})
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        got = await asyncio.wait_for(
            asyncio.gather(df.ato_dict(), observer()),
            timeout=30.0,
        )
    assert got[0] == {"a": [1], "b": [2]}
    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_aread_chain_acollect_yields_event_loop_native_async(
    tmp_path: Path,
) -> None:
    """``AwaitableDataFrameModel.acollect`` must not monopolize the event loop."""
    from pydantable.io import export_parquet

    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    path = tmp_path / "ev.pq"
    export_parquet(path, {"x": [1, 2]})

    class UserDF(DataFrameModel):
        x: int

    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        await asyncio.wait_for(
            asyncio.gather(adf.acollect(as_lists=True), observer()),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_ato_polars_yields_event_loop_native_async_execute_plan() -> None:
    pytest.importorskip("polars")
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1, 2]})
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        got = await asyncio.wait_for(
            asyncio.gather(df.ato_polars(), observer()),
            timeout=30.0,
        )
    assert got[0].to_dict(as_series=False) == {"x": [1, 2]}
    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_ato_arrow_yields_event_loop_native_async_execute_plan() -> None:
    pytest.importorskip("pyarrow")
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [3]})
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        got = await asyncio.wait_for(
            asyncio.gather(df.ato_arrow(), observer()),
            timeout=30.0,
        )
    assert got[0].to_pydict() == {"x": [3]}
    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_arows_yields_event_loop_native_async_execute_plan() -> None:
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    order: list[str] = []
    delayed = _delayed_async_execute_plan_with_order(
        _native_bound_async_execute_plan(), order
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1]})
    with mock.patch.object(NativePolarsEngine, "async_execute_plan", delayed):
        rows, _ = await asyncio.wait_for(
            asyncio.gather(df.arows(), observer()),
            timeout=30.0,
        )
    assert [r.x for r in rows] == [1]
    assert order.index("observer") < order.index("engine_done")


@pytest.mark.asyncio
async def test_astream_yields_event_loop_native_async_collect_batches() -> None:
    pytest.importorskip("polars")
    if not rust_has_async_collect_plan_batches():
        pytest.skip("Rust extension has no async_collect_plan_batches")

    order: list[str] = []
    delayed = _delayed_async_collect_plan_batches_with_order(
        _native_bound_async_collect_plan_batches(),
        order,
    )

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1, 2]})

    async def first_chunk() -> None:
        async for _ in df.astream(batch_size=1):
            break

    with mock.patch.object(NativePolarsEngine, "async_collect_plan_batches", delayed):
        await asyncio.wait_for(
            asyncio.gather(first_chunk(), observer()),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("batch_done")


@pytest.mark.asyncio
async def test_astream_yields_event_loop_thread_fallback() -> None:
    """``astream`` without native async batches uses ``asyncio.to_thread``."""
    pytest.importorskip("polars")
    pl = importlib.import_module("polars")
    impl = importlib.import_module("pydantable.dataframe._impl")
    real_mit = impl._materialize_in_thread
    order: list[str] = []

    async def mit_with_slow_sync(
        fn: object,
        *,
        executor: concurrent.futures.Executor | None,
    ) -> object:
        order.append("mit_start")

        def slow() -> object:
            time.sleep(0.12)
            assert callable(fn)
            return fn()  # type: ignore[operator]

        out = await real_mit(slow, executor=executor)
        order.append("mit_done")
        return out

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1]})
    fake_batch = pl.DataFrame({"x": [1]})

    async def first_chunk() -> None:
        async for _ in df.astream(batch_size=1):
            break

    with (
        mock.patch.object(
            NativePolarsEngine,
            "has_async_collect_plan_batches",
            return_value=False,
        ),
        mock.patch.object(impl, "_materialize_in_thread", mit_with_slow_sync),
        mock.patch.object(
            NativePolarsEngine,
            "collect_batches",
            lambda *_a, **_k: [fake_batch],
        ),
    ):
        await asyncio.wait_for(
            asyncio.gather(first_chunk(), observer()),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("mit_done")


@pytest.mark.asyncio
async def test_submit_result_yields_event_loop_while_background_collect() -> None:
    """``await handle.result()`` must not spin the GIL; other tasks can run."""
    order: list[str] = []

    def slow_execute(
        *_a: object,
        **_k: object,
    ) -> dict[str, list[int]]:
        time.sleep(0.12)
        order.append("engine_slept")
        return {"x": [1]}

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    df = Tiny({"x": [1]})
    with mock.patch.object(NativePolarsEngine, "execute_plan", slow_execute):
        h = df.submit(as_lists=True)
        await asyncio.wait_for(
            asyncio.gather(h.result(), observer()),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("engine_slept")


@pytest.mark.asyncio
async def test_io_aread_parquet_yields_event_loop() -> None:
    """``pydantable.io`` async readers use ``_run_io`` → ``asyncio.to_thread``."""
    order: list[str] = []
    delayed = _delayed_run_io_with_order(io_mod._run_io, order)

    async def observer() -> None:
        await asyncio.sleep(0)
        order.append("observer")

    def fake_read_parquet(*_a: object, **_k: object) -> dict[str, str]:
        return {"stub": "root"}

    with (
        mock.patch.object(io_mod, "_run_io", delayed),
        mock.patch.object(io_mod, "read_parquet", fake_read_parquet),
    ):
        await asyncio.wait_for(
            asyncio.gather(
                io_mod.aread_parquet("/tmp/pydantable_io_nb_test.pq"),
                observer(),
            ),
            timeout=30.0,
        )

    assert order.index("observer") < order.index("io_done")


# --- Concurrent execution (parallel gather; independent correct results) ---


@contextmanager
def _astream_collect_batches_mock_for_concurrent_tests() -> Any:
    """Thread-path ``astream`` without native ``collect_plan_batches`` (Polars)."""
    impl = importlib.import_module("pydantable.dataframe._impl")
    pl = pytest.importorskip("polars")

    def fake_collect_batches(
        self: object,
        plan: object,
        root_data: Any,
        *,
        batch_size: int = 65_536,
        streaming: bool = False,
    ) -> list[Any]:
        return [pl.DataFrame(root_data)]

    with (
        mock.patch.object(
            NativePolarsEngine,
            "has_async_collect_plan_batches",
            return_value=False,
        ),
        mock.patch.object(NativePolarsEngine, "collect_batches", fake_collect_batches),
    ):
        yield impl


@pytest.mark.asyncio
async def test_concurrent_acollect_runs_engine_work_in_parallel_threads() -> None:
    """Avoid wall-clock assertions: prove concurrency with synchronization.

    When native async execution is unavailable, `acollect()` offloads blocking work
    to threads. Two concurrent `acollect()` calls should be able to enter the
    patched engine function before either returns (subject to executor capacity).
    """
    started = threading.Event()
    both_started = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    n_started = 0

    def slow_execute(
        self: object,
        plan: object,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> dict[str, list[Any]]:
        nonlocal n_started
        if not isinstance(data, dict):
            raise AssertionError("expected in-memory dict root")
        with lock:
            n_started += 1
            started.set()
            if n_started >= 2:
                both_started.set()
        # Block until the test releases both calls; this makes overlap detectable
        # without relying on wall-clock ratios.
        release.wait(timeout=5.0)
        return {str(k): list(v) for k, v in data.items()}

    d1 = Tiny({"x": [1]})
    d2 = Tiny({"x": [2]})
    with (
        mock.patch.object(
            NativePolarsEngine, "has_async_execute_plan", return_value=False
        ),
        mock.patch.object(NativePolarsEngine, "execute_plan", slow_execute),
    ):
        # Schedule work before waiting on ``started``: a bare ``gather`` object is
        # not run until awaited, so nothing would enter ``slow_execute`` otherwise.
        async def _both() -> tuple[Any, Any]:
            return await asyncio.gather(
                d1.acollect(as_lists=True), d2.acollect(as_lists=True)
            )

        t = asyncio.create_task(_both())
        try:
            # Wait until at least one call entered, then until both entered.
            await asyncio.wait_for(_wait_threading_event(started, timeout=2.0), 2.5)
            await asyncio.wait_for(
                _wait_threading_event(both_started, timeout=2.0), 2.5
            )
        finally:
            release.set()
        c1, c2 = await asyncio.wait_for(t, timeout=5.0)
    assert c1 == {"x": [1]} and c2 == {"x": [2]}


@pytest.mark.asyncio
async def test_concurrent_native_async_execute_plan_overlaps() -> None:
    """Keep a small overlap check for native async, but avoid strict wall ratios."""
    if not rust_has_async_execute_plan():
        pytest.skip("Rust extension has no async_execute_plan (Tokio bridge)")

    real = _native_bound_async_execute_plan()
    gate = asyncio.Event()
    entered = 0
    lock = asyncio.Lock()

    async def gated_async_execute_plan(
        self: object,
        plan: object,
        data: object,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> object:
        nonlocal entered
        async with lock:
            entered += 1
            if entered >= 2:
                gate.set()
        # ensure both coroutines can reach this point before proceeding
        await gate.wait()
        return await real(
            plan,
            data,
            as_python_lists=as_python_lists,
            streaming=streaming,
            error_context=error_context,
        )

    d1 = Tiny({"x": [1]})
    d2 = Tiny({"x": [2]})
    with mock.patch.object(
        NativePolarsEngine, "async_execute_plan", gated_async_execute_plan
    ):
        c1, c2 = await asyncio.wait_for(
            asyncio.gather(d1.acollect(as_lists=True), d2.acollect(as_lists=True)),
            timeout=10.0,
        )
    assert c1 == {"x": [1]} and c2 == {"x": [2]}


#
# NOTE: wall-clock overlap tests for concurrent native async_execute_plan were removed
# because they are sensitive to OS scheduling jitter (especially on macOS runners) and
# cause release-blocking flakes unrelated to correctness. See
# ``test_concurrent_native_async_execute_plan_overlaps`` above for gather-based coverage
# without timing assertions.


@pytest.mark.asyncio
async def test_concurrent_acollect_many_independent_frames() -> None:
    """Many ``acollect`` coroutines in flight; each result matches its model."""
    with _in_memory_engine_mock_for_concurrent_tests():
        frames = [Tiny({"x": [i, i + 1]}) for i in range(0, 16, 2)]
        cols = await asyncio.gather(*[f.acollect(as_lists=True) for f in frames])
    assert len(cols) == len(frames)
    for i, col in enumerate(cols):
        assert col == {"x": [i * 2, i * 2 + 1]}


@pytest.mark.asyncio
async def test_concurrent_mixed_async_terminals_same_gather() -> None:
    """Different async terminals on different frames in one ``gather``."""
    with _in_memory_engine_mock_for_concurrent_tests():
        d1 = Tiny({"x": [1, 2]})
        d2 = Tiny({"x": [3, 4]})
        d3 = Tiny({"x": [5]})
        lists_out, dict_out, rows = await asyncio.gather(
            d1.acollect(as_lists=True),
            d2.ato_dict(),
            d3.arows(),
        )
    assert lists_out == {"x": [1, 2]}
    assert dict_out == {"x": [3, 4]}
    assert [r.x for r in rows] == [5]


@pytest.mark.asyncio
async def test_concurrent_ato_polars_and_ato_arrow() -> None:
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")
    with _in_memory_engine_mock_for_concurrent_tests():
        d1 = Tiny({"x": [10, 20]})
        d2 = Tiny({"x": [30]})
        pl_df, pa_tbl = await asyncio.gather(d1.ato_polars(), d2.ato_arrow())
    assert pl_df.to_dict(as_series=False) == {"x": [10, 20]}
    assert pa_tbl.to_pydict() == {"x": [30]}


@pytest.mark.asyncio
async def test_concurrent_astream_independent_frames() -> None:
    with _astream_collect_batches_mock_for_concurrent_tests():
        d1 = Tiny({"x": [1, 2, 3]})
        d2 = Tiny({"x": [4, 5]})

        async def chunks(df: Tiny) -> list[dict[str, list[int]]]:
            return [c async for c in df.astream(batch_size=1)]

        a, b = await asyncio.gather(chunks(d1), chunks(d2))
    # Stub returns one Polars frame per collect; chunks match each independent frame.
    assert a == [{"x": [1, 2, 3]}]
    assert b == [{"x": [4, 5]}]


@pytest.mark.asyncio
async def test_concurrent_submit_handles_independent_frames() -> None:
    with _in_memory_engine_mock_for_concurrent_tests():
        frames = [Tiny({"x": [i]}) for i in range(5)]
        handles = [f.submit(as_lists=True) for f in frames]
        cols = await asyncio.gather(*[h.result() for h in handles])
    assert [c["x"] for c in cols] == [[0], [1], [2], [3], [4]]


@pytest.mark.asyncio
async def test_concurrent_io_aread_parquet_stubbed() -> None:
    """Parallel ``aread_parquet`` calls complete; stubs avoid ScanFileRoot."""
    scan_stub = object()

    def fake_read_parquet(*_a: object, **_k: object) -> object:
        return scan_stub

    with mock.patch.object(io_mod, "read_parquet", fake_read_parquet):
        outs = await asyncio.gather(
            io_mod.aread_parquet("/tmp/c1.pq"),
            io_mod.aread_parquet("/tmp/c2.pq"),
            io_mod.aread_parquet("/tmp/c3.pq"),
        )
    assert outs == [scan_stub, scan_stub, scan_stub]


@pytest.mark.asyncio
async def test_concurrent_awaitable_chains_acollect() -> None:
    """Parallel ``AwaitableDataFrameModel`` chains; each ``acollect`` stays isolated."""
    specs = [{"x": [0, 1]}, {"x": [10, 11]}, {"x": [20, 21]}]
    chains: list[AwaitableDataFrameModel[Any]] = []
    for spec in specs:

        async def _get(s: dict[str, list[int]] = spec) -> Tiny:
            return Tiny(s)

        chains.append(AwaitableDataFrameModel(_get))

    with _in_memory_engine_mock_for_concurrent_tests():
        cols = await asyncio.gather(*[c.acollect(as_lists=True) for c in chains])
    assert cols[0] == {"x": [0, 1]}
    assert cols[1] == {"x": [10, 11]}
    assert cols[2] == {"x": [20, 21]}


@pytest.mark.asyncio
async def test_concurrent_aread_parquet_chains_acollect_integration(
    tmp_path: Path,
) -> None:
    """Real lazy file roots in parallel when the extension supports ``ScanFileRoot``."""
    try:
        import pydantable_native._core as rust  # type: ignore[import-not-found]
    except ImportError:
        pytest.skip("native extension not importable")
    if not hasattr(rust, "ScanFileRoot"):
        pytest.skip("ScanFileRoot not available")

    from pydantable.io import export_parquet

    paths: list[Path] = []
    for i in range(3):
        p = tmp_path / f"c{i}.pq"
        export_parquet(p, {"x": [i * 10, i * 10 + 1]})
        paths.append(p)

    class UserDF(DataFrameModel):
        x: int

    chains = [UserDF.aread_parquet(p, trusted_mode="shape_only") for p in paths]
    cols = await asyncio.gather(*[c.acollect(as_lists=True) for c in chains])
    assert cols[0] == {"x": [0, 1]}
    assert cols[1] == {"x": [10, 11]}
    assert cols[2] == {"x": [20, 21]}
