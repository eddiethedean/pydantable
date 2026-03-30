"""Tests for :mod:`pydantable.fastapi` and the golden-path example app."""

from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantable import DataFrameModel, MissingRustExtensionError
from pydantable.fastapi import (
    executor_lifespan,
    get_executor,
    ndjson_chunk_bytes,
    ndjson_streaming_response,
    register_exception_handlers,
)
from pydantic import BaseModel


async def test_ndjson_chunk_bytes_yields_lines() -> None:
    async def chunks():
        yield {"x": [1, 2]}
        yield {"y": [3]}

    out = []
    async for line in ndjson_chunk_bytes(chunks()):
        out.append(line)

    assert len(out) == 2
    assert json.loads(out[0].decode()) == {"x": [1, 2]}
    assert json.loads(out[1].decode()) == {"y": [3]}


async def test_ndjson_chunk_bytes_empty_chunks() -> None:
    async def empty():
        if False:  # pragma: no cover - async generator with no yields
            yield {}

    out = [b async for b in ndjson_chunk_bytes(empty())]
    assert out == []


async def test_ndjson_chunk_bytes_unicode_and_null() -> None:
    async def chunks():
        yield {"msg": ["héllo", "世界"], "flag": [True, False]}
        yield {"n": [None, 1]}

    lines = [json.loads(b.decode("utf-8")) async for b in ndjson_chunk_bytes(chunks())]
    assert lines[0]["msg"] == ["héllo", "世界"]
    assert lines[1]["n"] == [None, 1]


def test_ndjson_streaming_response_custom_media_type() -> None:
    app = FastAPI()

    async def one():
        yield {"z": [1]}

    @app.get("/s")
    async def s():
        return ndjson_streaming_response(one(), media_type="application/jsonlines")

    client = TestClient(app)
    r = client.get("/s")
    assert r.status_code == 200
    assert "jsonlines" in r.headers.get("content-type", "")
    assert json.loads(r.text.strip()) == {"z": [1]}


class _NdjsonStreamDF(DataFrameModel):
    k: int


async def test_ndjson_chunk_bytes_with_real_dataframe_astream() -> None:
    """``astream`` chunks through NDJSON bytes (same contract as streaming routes)."""
    df = _NdjsonStreamDF({"k": [1, 2, 3, 4]}, trusted_mode="shape_only")
    decoded: list[dict[str, list]] = []
    async for raw in ndjson_chunk_bytes(df.astream(batch_size=2, executor=None)):
        decoded.append(json.loads(raw.decode("utf-8")))
    assert len(decoded) == 2
    assert decoded[0]["k"] == [1, 2]
    assert decoded[1]["k"] == [3, 4]


def test_ndjson_streaming_response_route() -> None:
    app = FastAPI()

    async def chunks():
        yield {"a": [1]}
        yield {"b": [2, 3]}

    @app.get("/stream")
    async def stream():
        return ndjson_streaming_response(chunks())

    client = TestClient(app)
    r = client.get("/stream")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("application/x-ndjson")
    lines = [ln for ln in r.text.strip().split("\n") if ln]
    assert [json.loads(x) for x in lines] == [{"a": [1]}, {"b": [2, 3]}]


def test_register_exception_handlers_twice_is_ok() -> None:
    app = FastAPI()
    register_exception_handlers(app)
    register_exception_handlers(app)


def test_validation_error_handler_returns_422() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    class M(BaseModel):
        x: int

    @app.get("/val")
    async def val() -> None:
        M.model_validate({"x": "not-int"})

    client = TestClient(app)
    r = client.get("/val")
    assert r.status_code == 422
    body = r.json()
    assert "detail" in body
    assert isinstance(body["detail"], list)


def test_missing_rust_extension_handler_returns_503() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/needs-core")
    async def needs_core() -> None:
        raise MissingRustExtensionError("simulated missing wheel")

    client = TestClient(app)
    r = client.get("/needs-core")
    assert r.status_code == 503
    assert r.json() == {"detail": "simulated missing wheel"}


def test_request_body_validation_422_untouched_by_custom_handlers() -> None:
    """Invalid JSON bodies still get FastAPI's 422 (before our route runs)."""
    app = FastAPI()
    register_exception_handlers(app)

    class _Row(BaseModel):
        id: int

    @app.post("/rows")
    async def rows(items: list[_Row]) -> list[_Row]:
        return items

    client = TestClient(app)
    r = client.post("/rows", json=[{"id": "not-an-int"}])
    assert r.status_code == 422
    # Distinct from our pydantic.ValidationError handler (different error shape key).
    assert "detail" in r.json()


def test_lifespan_attaches_thread_pool_executor_to_app_state() -> None:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with executor_lifespan(app, max_workers=2, thread_name_prefix="pt-test"):
            yield

    app = FastAPI(lifespan=lifespan)
    with TestClient(app):
        ex = app.state.executor
        assert isinstance(ex, ThreadPoolExecutor)
        assert ex._max_workers == 2


class _ExecutorTestDF(DataFrameModel):
    id: int


class _NoLifespanDF(DataFrameModel):
    n: int


def test_get_executor_is_none_without_lifespan() -> None:
    """acollect(executor=None) is valid when no pool was attached."""

    app = FastAPI()

    @app.post("/x", response_model=list[_NoLifespanDF.RowModel])
    async def x(rows: list[_NoLifespanDF.RowModel], ex=Depends(get_executor)):  # noqa: B008
        assert ex is None
        return await _NoLifespanDF(rows).acollect(executor=ex)

    client = TestClient(app)
    r = client.post("/x", json=[{"n": 1}])
    assert r.status_code == 200
    assert r.json() == [{"n": 1}]


def test_executor_depends_and_acollect() -> None:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with executor_lifespan(app, max_workers=2, thread_name_prefix="pt-test"):
            yield

    app = FastAPI(lifespan=lifespan)

    @app.post("/u", response_model=list[_ExecutorTestDF.RowModel])
    async def u(
        rows: list[_ExecutorTestDF.RowModel],
        ex=Depends(get_executor),  # noqa: B008
    ):
        assert isinstance(ex, ThreadPoolExecutor)
        return await _ExecutorTestDF(rows).acollect(executor=ex)

    with TestClient(app) as client:
        r = client.post("/u", json=[{"id": 1}])
    assert r.status_code == 200
    assert r.json() == [{"id": 1}]


def test_executor_lifespan_shutdowns_pool() -> None:
    mock_pool = MagicMock()
    mock_pool.shutdown = MagicMock()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        with patch(
            "pydantable.fastapi.ThreadPoolExecutor",
            return_value=mock_pool,
        ):
            async with executor_lifespan(app):
                yield

    app = FastAPI(lifespan=lifespan)
    with TestClient(app):
        pass
    mock_pool.shutdown.assert_called_once_with(wait=True)


def test_golden_path_example_app() -> None:
    root = Path(__file__).resolve().parents[1]
    ex_dir = str(root / "docs" / "examples" / "fastapi")
    sys.path.insert(0, ex_dir)
    try:
        import golden_path_app
    finally:
        sys.path.remove(ex_dir)

    with TestClient(golden_path_app.app) as client:
        assert client.get("/health").json() == {"status": "ok"}

        r = client.post(
            "/api/v1/users",
            json=[{"id": 1, "age": 20}, {"id": 2, "age": None}],
        )
        assert r.status_code == 200
        assert r.json() == [
            {"id": 1, "age": 20},
            {"id": 2, "age": None},
        ]

        rs = client.get("/api/v1/users/stream")
        assert rs.status_code == 200
        assert rs.headers.get("content-type", "").startswith("application/x-ndjson")
        lines = [ln for ln in rs.text.strip().split("\n") if ln]
        assert len(lines) >= 1
        merged_id: list[int] = []
        merged_age: list = []
        for line in lines:
            obj = json.loads(line)
            assert isinstance(obj, dict)
            assert "id" in obj and "age" in obj
            merged_id.extend(obj["id"])
            merged_age.extend(obj["age"])
        assert merged_id == [1, 2, 3]
        assert merged_age == [10, None, 40]
