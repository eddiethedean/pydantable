"""Deferred materialization via thread-pool futures and asyncio bridging."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Executor, Future
from typing import Any


class ExecutionHandle:
    """Background :meth:`DataFrame.submit` job; await :meth:`result`."""

    __slots__ = ("_fut",)

    def __init__(self, fut: Future[Any]) -> None:
        self._fut = fut

    def done(self) -> bool:
        return self._fut.done()

    def cancel(self) -> bool:
        """Cancel the wait only; in-flight engine work may still complete."""
        return self._fut.cancel()

    async def result(self) -> Any:
        # Cancellation of the awaiting task should *not* cancel the underlying
        # concurrent future (and therefore should not attempt to cancel engine work).
        return await asyncio.shield(asyncio.wrap_future(self._fut))


async def _materialize_in_thread(
    fn: Callable[[], Any],
    *,
    executor: Executor | None,
) -> Any:
    """Run a no-arg callable for blocking Rust/Polars work off the event loop."""
    loop = asyncio.get_running_loop()
    if executor is not None:
        return await loop.run_in_executor(executor, fn)
    return await asyncio.to_thread(fn)
