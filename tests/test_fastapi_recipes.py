"""FastAPI `TestClient` smoke tests for `DataFrameModel` routes (optional deps)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantable import DataFrameModel
from pydantic import BaseModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


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
