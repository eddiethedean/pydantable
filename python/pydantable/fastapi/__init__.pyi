"""Typing stubs for :mod:`pydantable.fastapi`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from concurrent.futures import Executor
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import StreamingResponse

from pydantable._extension import MissingRustExtensionError

def executor_lifespan(
    app: FastAPI,
    *,
    max_workers: int | None = ...,
    thread_name_prefix: str = ...,
) -> AbstractAsyncContextManager[None]: ...

def get_executor(request: Request) -> Executor | None: ...

async def ndjson_chunk_bytes(
    chunks: AsyncIterator[dict[str, Any]],
) -> AsyncIterator[bytes]: ...

def ndjson_streaming_response(
    chunks: AsyncIterator[dict[str, Any]],
    *,
    media_type: str = ...,
) -> StreamingResponse: ...

def register_exception_handlers(app: FastAPI) -> None: ...

__all__ = [
    "MissingRustExtensionError",
    "executor_lifespan",
    "get_executor",
    "ndjson_chunk_bytes",
    "ndjson_streaming_response",
    "register_exception_handlers",
]
