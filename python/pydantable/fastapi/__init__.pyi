"""Typing stubs for :mod:`pydantable.fastapi`."""

from __future__ import annotations

from concurrent.futures import Executor
from contextlib import AbstractAsyncContextManager

from fastapi import FastAPI
from starlette.requests import Request

from pydantable._extension import MissingRustExtensionError

def executor_lifespan(
    app: FastAPI,
    *,
    max_workers: int | None = ...,
    thread_name_prefix: str = ...,
) -> AbstractAsyncContextManager[None]: ...

def get_executor(request: Request) -> Executor | None: ...

def register_exception_handlers(app: FastAPI) -> None: ...

__all__ = [
    "MissingRustExtensionError",
    "executor_lifespan",
    "get_executor",
    "register_exception_handlers",
]
