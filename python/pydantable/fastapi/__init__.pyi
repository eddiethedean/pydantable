"""Typing stubs for :mod:`pydantable.fastapi`."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from concurrent.futures import Executor
from contextlib import AbstractAsyncContextManager
from typing import Any, Literal, TypeVar

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import StreamingResponse

from pydantable._extension import MissingRustExtensionError
from pydantable.dataframe_model import DataFrameModel
from pydantable.errors import ColumnLengthMismatchError, PydantableUserError
from pydantable.ingest_errors import IngestRowFailure, IngestValidationErrorDetail

_DFM = TypeVar("_DFM", bound=DataFrameModel[Any])

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
def columnar_body_model(
    row_model: type[BaseModel],
    *,
    model_name: str | None = ...,
    json_schema_extra: dict[str, Any] | None = ...,
    example: dict[str, list[Any]] | None = ...,
    generate_examples: bool = ...,
    input_key_mode: Literal["python", "aliases", "both"] = ...,
) -> type[BaseModel]: ...
def columnar_body_model_from_dataframe_model(
    model_cls: type[DataFrameModel[Any]],
    *,
    model_name: str | None = ...,
    json_schema_extra: dict[str, Any] | None = ...,
    example: dict[str, list[Any]] | None = ...,
    generate_examples: bool = ...,
    input_key_mode: Literal["python", "aliases", "both"] = ...,
) -> type[BaseModel]: ...
def columnar_dependency(
    model_cls: type[_DFM],
    *,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = ...,
    fill_missing_optional: bool = ...,
    ignore_errors: bool = ...,
    validation_profile: str | None = ...,
    json_schema_extra: dict[str, Any] | None = ...,
    example: dict[str, list[Any]] | None = ...,
    generate_examples: bool = ...,
    input_key_mode: Literal["python", "aliases", "both"] = ...,
) -> Callable[..., _DFM]: ...
def rows_dependency(
    model_cls: type[_DFM],
    *,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = ...,
    fill_missing_optional: bool = ...,
    ignore_errors: bool = ...,
    validation_profile: str | None = ...,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = ...,
) -> Callable[..., _DFM]: ...

def ingest_error_response(
    failures: object,
    *,
    status_code: int = ...,
    title: str = ...,
) -> Any: ...

__all__ = [
    "ColumnLengthMismatchError",
    "IngestRowFailure",
    "IngestValidationErrorDetail",
    "MissingRustExtensionError",
    "PydantableUserError",
    "columnar_body_model",
    "columnar_body_model_from_dataframe_model",
    "columnar_dependency",
    "executor_lifespan",
    "get_executor",
    "ingest_error_response",
    "ndjson_chunk_bytes",
    "ndjson_streaming_response",
    "register_exception_handlers",
    "rows_dependency",
]
