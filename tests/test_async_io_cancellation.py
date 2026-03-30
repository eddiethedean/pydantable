from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_aiter_from_iter_consumer_can_stop_without_deadlock() -> None:
    """
    Regression: the producer thread must not deadlock if the consumer stops early.

    Without a cancellation-aware bridge, a fast producer can block forever trying
    to enqueue into a bounded asyncio.Queue after the consumer exits.
    """
    from pydantable.io import _aiter_from_iter

    def producer():
        i = 0
        while True:
            i += 1
            yield {"x": [i]}

    agen = _aiter_from_iter(producer(), executor=None)
    try:
        first = await asyncio.wait_for(agen.__anext__(), timeout=1.0)
        assert first["x"][0] == 1
    finally:
        # Stop early like a client disconnect / route returning after first chunk.
        await asyncio.wait_for(agen.aclose(), timeout=1.0)
