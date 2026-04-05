from __future__ import annotations

import asyncio
import threading

from pydantable import DataFrame, Schema


class Small(Schema):
    x: int


def test_submit_cancel_before_start_does_not_crash_thread() -> None:
    """
    Regression: cancelling a submit() handle immediately should not cause the
    background thread to raise InvalidStateError when setting results.
    """
    df = DataFrame[Small]({"x": list(range(10))})
    h = df.submit(as_lists=True)
    _ = h.cancel()
    # Give the background thread a brief window to run (or skip) without
    # producing unhandled exceptions that pytest would surface.
    asyncio.run(asyncio.sleep(0.05))


def test_submit_cancel_await_does_not_cancel_engine_work(monkeypatch) -> None:
    """
    Cancelling an asyncio task awaiting ExecutionHandle.result() should not cancel
    the underlying concurrent future (engine work keeps running).
    """
    df = DataFrame[Small]({"x": list(range(10))})

    started = threading.Event()
    release = threading.Event()

    orig_collect = df.collect

    def slow_collect(*args, **kwargs):
        started.set()
        # Block until the test cancels the awaiting task.
        release.wait(timeout=5)
        return orig_collect(*args, **kwargs)

    monkeypatch.setattr(df, "collect", slow_collect)
    h = df.submit(as_lists=True)

    async def _run():
        t = asyncio.create_task(h.result())
        await asyncio.to_thread(started.wait, 2)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        # Allow background work to complete, then ensure result is still available.
        release.set()
        out = await asyncio.wait_for(h.result(), timeout=2)
        assert out == {"x": list(range(10))}

    import pytest

    asyncio.run(_run())
