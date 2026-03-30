"""Test helpers for FastAPI apps using pydantable."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from fastapi import FastAPI

from pydantable.fastapi import executor_lifespan, register_exception_handlers


def fastapi_app_with_executor(
    *,
    max_workers: int | None = 4,
    thread_name_prefix: str = "pydantable-test",
    register_handlers: bool = False,
) -> FastAPI:
    """Build a FastAPI app with :func:`pydantable.fastapi.executor_lifespan`.

    Use with :func:`fastapi_test_client` so ``app.state.executor`` is set during
    requests.
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with executor_lifespan(
            app,
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        ):
            yield

    app = FastAPI(lifespan=lifespan)
    if register_handlers:
        register_exception_handlers(app)
    return app


@contextmanager
def fastapi_test_client(app: FastAPI) -> Any:
    """Context manager around :class:`fastapi.testclient.TestClient` that runs lifespan.

    Required for routes that use :func:`pydantable.fastapi.get_executor`.
    """
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        yield client
