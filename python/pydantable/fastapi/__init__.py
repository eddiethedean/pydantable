"""Optional FastAPI helpers (install ``pydantable[fastapi]``).

``import pydantable`` does not require FastAPI; import this submodule when you use
FastAPI in your service.
"""

from __future__ import annotations

from collections.abc import AsyncIterator  # noqa: TC003
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI  # noqa: TC002
from pydantic import ValidationError
from starlette.requests import Request  # noqa: TC002

from pydantable._extension import MissingRustExtensionError

__all__ = [
    "MissingRustExtensionError",
    "executor_lifespan",
    "get_executor",
    "register_exception_handlers",
]


@asynccontextmanager
async def executor_lifespan(
    app: FastAPI,
    *,
    max_workers: int | None = None,
    thread_name_prefix: str = "pydantable",
) -> AsyncIterator[None]:
    """Attach a thread pool executor to ``app.state.executor``.

    Use with FastAPI's ``lifespan`` so ``acollect(executor=...)`` and
    ``pydantable.io`` helpers can share a dedicated pool instead of the default
    ``asyncio`` thread pool.

    Example::

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            async with executor_lifespan(app, max_workers=4):
                yield

        app = FastAPI(lifespan=lifespan)
    """
    executor = ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
    )
    app.state.executor = executor
    try:
        yield
    finally:
        executor.shutdown(wait=True)


def get_executor(request: Request) -> Executor | None:
    """Return ``app.state.executor`` when set by :func:`executor_lifespan`.

    Use with ``Depends(get_executor)`` and pass the result to
    ``acollect(executor=...)``.
    """
    return getattr(request.app.state, "executor", None)


def register_exception_handlers(app: FastAPI) -> None:
    """Register HTTP-friendly handlers for common pydantable / Pydantic errors.

    - :exc:`~pydantable.MissingRustExtensionError` → **503** (native extension missing).
    - :exc:`pydantic.ValidationError` → **422** with ``detail`` as Pydantic's error
      list.

    Inbound request body validation is usually handled by FastAPI as
    :exc:`fastapi.exceptions.RequestValidationError` (**422**) before your route runs.
    This handler covers :exc:`~pydantic.ValidationError` raised inside handlers (for
    example manual ``model_validate``). Do **not** register a blanket handler for
    :exc:`ValueError` from the engine — map those explicitly in your routes.

    Idempotent for duplicate registration: re-calling on the same app replaces handlers
    for the same exception types (Starlette/FastAPI behavior).
    """
    from fastapi.responses import JSONResponse

    @app.exception_handler(MissingRustExtensionError)
    async def _missing_rust(
        request: Request, exc: MissingRustExtensionError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc)},
        )

    @app.exception_handler(ValidationError)
    async def _pydantic_validation(
        request: Request,
        exc: ValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )
