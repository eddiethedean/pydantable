"""FastAPI `TestClient` smoke tests for `DataFrameModel` routes (optional deps).

This file intentionally spans multiple release themes:

- **0.14+:** sync routes, OpenAPI, columnar JSON bodies (integration smoke).
- **0.15+:** async routes using async materialization (`acollect`, `ato_dict`).
- **0.16+:** multipart Parquet upload + `read_parquet` in an async handler.

See `docs/DEVELOPER.md` (release ↔ tests map) for a maintainer-facing index.
"""

from __future__ import annotations

import json
from io import BytesIO

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from pydantable import DataFrameModel
from pydantic import BaseModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


# --- 0.14.0+: sync TestClient, OpenAPI, columnar bodies ---


def test_testclient_row_list_body_and_openapi() -> None:
    app = FastAPI()

    @app.post("/users", response_model=list[UserRow])
    def create_users(rows: list[UserDF.RowModel]):
        df = UserDF(rows)
        return df.filter(df.id > 0).collect()

    client = TestClient(app)
    r = client.post("/users", json=[{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert r.status_code == 200
    assert r.json() == [{"id": 1, "age": 20}, {"id": 2, "age": None}]

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    assert "/users" in openapi.json().get("paths", {})


def test_testclient_columnar_dict_body() -> None:
    app = FastAPI()

    @app.post("/bulk")
    def bulk(body: dict[str, list]):
        df = UserDF(body, trusted_mode="shape_only")
        return df.to_dict()

    client = TestClient(app)
    r = client.post("/bulk", json={"id": [1, 2], "age": [10, None]})
    assert r.status_code == 200
    assert r.json() == {"id": [1, 2], "age": [10, None]}


# --- 0.15.0+: async routes (async materialization) ---


def test_testclient_async_acollect_and_ato_dict() -> None:
    app = FastAPI()

    @app.post("/users-async", response_model=list[UserRow])
    async def create_users_async(rows: list[UserDF.RowModel]):
        df = UserDF(rows)
        return await df.acollect()

    @app.post("/bulk-async")
    async def bulk_async(body: dict[str, list]):
        df = UserDF(body, trusted_mode="shape_only")
        return await df.ato_dict()

    client = TestClient(app)
    r = client.post("/users-async", json=[{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert r.status_code == 200
    assert r.json() == [{"id": 1, "age": 20}, {"id": 2, "age": None}]

    r2 = client.post("/bulk-async", json={"id": [1, 2], "age": [10, None]})
    assert r2.status_code == 200
    assert r2.json() == {"id": [1, 2], "age": [10, None]}


def test_streaming_response_after_ato_dict() -> None:
    """Smoke: ``StreamingResponse`` after ``ato_dict`` (see ``docs/FASTAPI.md``)."""

    app = FastAPI()

    @app.get("/bulk-stream")
    async def bulk_stream() -> StreamingResponse:
        df = UserDF({"id": [1, 2], "age": [10, None]}, trusted_mode="shape_only")
        col = await df.ato_dict()
        payload = json.dumps(col).encode()

        def chunks() -> list[bytes]:
            return [payload]

        return StreamingResponse(iter(chunks()), media_type="application/json")

    client = TestClient(app)
    r = client.get("/bulk-stream")
    assert r.status_code == 200
    assert json.loads(r.content.decode()) == {"id": [1, 2], "age": [10, None]}


def test_row_list_invalid_type_is_422() -> None:
    app = FastAPI()

    @app.post("/users", response_model=list[UserRow])
    def create_users(rows: list[UserDF.RowModel]):
        df = UserDF(rows)
        return df.collect()

    client = TestClient(app)
    r = client.post("/users", json=[{"id": "not-an-int", "age": 20}])
    assert r.status_code == 422


# --- 0.16.0+: multipart Parquet + read_parquet ---


def test_multipart_parquet_upload() -> None:
    pytest.importorskip("python_multipart")
    pytest.importorskip("pyarrow")

    import pyarrow as pa
    import pyarrow.parquet as pq
    from pydantable import read_parquet

    app = FastAPI()

    @app.post("/upload")
    async def upload_parquet(file: UploadFile):
        raw = await file.read()
        cols = read_parquet(raw)
        df = UserDF(cols, trusted_mode="shape_only")
        return df.to_dict()

    buf = BytesIO()
    pq.write_table(pa.Table.from_pydict({"id": [1, 2], "age": [30, None]}), buf)
    client = TestClient(app)
    r = client.post(
        "/upload",
        files={"file": ("data.parquet", buf.getvalue(), "application/octet-stream")},
    )
    assert r.status_code == 200
    assert r.json() == {"id": [1, 2], "age": [30, None]}
