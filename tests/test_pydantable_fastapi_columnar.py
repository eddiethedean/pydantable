"""Tests for columnar OpenAPI, Depends factories, and pydantable.testing.fastapi."""

from __future__ import annotations

from concurrent.futures import Executor
from typing import Annotated

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantable import ColumnLengthMismatchError, DataFrameModel
from pydantable.fastapi import (
    columnar_body_model,
    columnar_body_model_from_dataframe_model,
    columnar_dependency,
    get_executor,
    ingest_error_response,
    register_exception_handlers,
    rows_dependency,
)
from pydantable.testing.fastapi import fastapi_app_with_executor, fastapi_test_client
from pydantic import BaseModel, Field


class UserDF(DataFrameModel):
    id: int
    age: int | None = None


class AliasDF(DataFrameModel):
    id: int = Field(validation_alias="userId")
    age: int | None = None


class InnerRow(BaseModel):
    x: int


class NestedDF(DataFrameModel):
    inner: InnerRow


def test_columnar_body_model_cache_returns_same_class() -> None:
    a = columnar_body_model_from_dataframe_model(UserDF)
    b = columnar_body_model_from_dataframe_model(UserDF)
    assert a is b


def test_columnar_body_model_via_row_model_matches_dataframe_factory() -> None:
    from_df = columnar_body_model_from_dataframe_model(UserDF)
    from_row = columnar_body_model(UserDF.RowModel, model_name=from_df.__name__)
    assert from_df is from_row


def test_columnar_body_model_json_schema_lists() -> None:
    Col = columnar_body_model_from_dataframe_model(UserDF)
    schema = Col.model_json_schema()
    props = schema["properties"]
    assert "id" in props
    assert props["id"].get("type") == "array"
    assert props["age"].get("type") == "array"


def test_columnar_body_model_example_in_schema() -> None:
    Col = columnar_body_model_from_dataframe_model(
        UserDF,
        example={"id": [1, 2], "age": [10, None]},
    )
    schema = Col.model_json_schema()
    assert schema.get("example") == {"id": [1, 2], "age": [10, None]}


def test_columnar_dependency_422_missing_required_column() -> None:
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/col", json={"age": [1, 2]})
        assert r.status_code == 422


def test_columnar_dependency_strict_mode_accepts_valid_integers() -> None:
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[
            UserDF,
            Depends(columnar_dependency(UserDF, trusted_mode="strict")),
        ],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/col", json={"id": [1], "age": [42]})
        assert r.status_code == 200
        assert r.json() == {"id": [1], "age": [42]}


def test_columnar_dependency_validation_profile_trusted_upstream() -> None:
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[
            UserDF,
            Depends(columnar_dependency(UserDF, validation_profile="trusted_upstream")),
        ],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        # strict mode would reject strings; trusted_upstream (shape_only) should accept.
        r = client.post("/col", json={"id": ["1"], "age": [None]})
        assert r.status_code == 200


def test_register_handlers_column_length_mismatch_from_direct_raise() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/direct")
    def direct() -> None:
        raise ColumnLengthMismatchError("test detail")

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.get("/direct")
        assert r.status_code == 400
        assert r.json()["detail"] == "test detail"


def test_ingest_error_response_returns_structured_payload() -> None:
    failures = [
        {
            "row_index": 0,
            "row": {"id": "bad"},
            "errors": [{"type": "int_parsing", "loc": ("id",), "msg": "bad", "input": "bad"}],
        }
    ]
    resp = ingest_error_response(failures, status_code=422, title="Bad rows")
    assert resp.status_code == 422
    # Starlette JSONResponse stores bytes body; decode to check shape.
    import json

    payload = json.loads(resp.body.decode("utf-8"))
    assert payload["title"] == "Bad rows"
    assert isinstance(payload["failures"], list)
    assert payload["failures"][0]["row_index"] == 0


def test_columnar_dependency_422_invalid_list_element_type() -> None:
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/col", json={"id": ["not-int"], "age": [None]})
        assert r.status_code == 422


def test_columnar_dependency_length_mismatch_returns_400_with_handlers() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.post("/col")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.post("/col", json={"id": [1, 2], "age": [3]})
        assert r.status_code == 400
        assert "same length" in r.json()["detail"]


def test_columnar_dependency_length_mismatch_is_500_when_unhandled() -> None:
    """Without handlers, :exc:`ColumnLengthMismatchError` surfaces as **500**."""
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.post("/col", json={"id": [1, 2], "age": [3]})
        assert r.status_code == 500


def test_register_handlers_does_not_catch_generic_valueerror() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    def boom() -> None:
        raise ValueError("generic engine error")

    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/boom").status_code == 500


def test_nested_columnar_round_trip() -> None:
    app = FastAPI()

    @app.post("/n")
    def route(
        df: Annotated[NestedDF, Depends(columnar_dependency(NestedDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post(
            "/n",
            json={"inner": [{"x": 1}, {"x": 2}]},
        )
        assert r.status_code == 200
        assert r.json() == {"inner": [{"x": 1}, {"x": 2}]}


def test_rows_dependency_422_bad_row() -> None:
    app = FastAPI()

    @app.post("/rows")
    def route(
        df: Annotated[UserDF, Depends(rows_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/rows", json=[{"id": "not-int", "age": None}])
        assert r.status_code == 422


def test_fastapi_app_with_executor_register_handlers() -> None:
    app = fastapi_app_with_executor(register_handlers=True)

    @app.get("/ok")
    def ok() -> str:
        return "ok"

    with fastapi_test_client(app) as client:
        assert client.get("/ok").status_code == 200


def test_columnar_dependency_round_trip() -> None:
    app = FastAPI()

    @app.post("/col")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/col", json={"id": [1, 2], "age": [3, None]})
        assert r.status_code == 200
        assert r.json() == {"id": [1, 2], "age": [3, None]}


def test_columnar_dependency_validation_alias() -> None:
    app = FastAPI()

    @app.post("/alias")
    def route(
        df: Annotated[AliasDF, Depends(columnar_dependency(AliasDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post("/alias", json={"userId": [1, 2], "age": [10, 20]})
        assert r.status_code == 200
        assert r.json() == {"id": [1, 2], "age": [10, 20]}


def test_rows_dependency() -> None:
    app = FastAPI()

    @app.post("/rows")
    def route(
        df: Annotated[UserDF, Depends(rows_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    with fastapi_test_client(app) as client:
        r = client.post(
            "/rows",
            json=[{"id": 1, "age": 2}, {"id": 3, "age": None}],
        )
        assert r.status_code == 200
        assert r.json() == {"id": [1, 3], "age": [2, None]}


def test_openapi_includes_columnar_schema() -> None:
    app = FastAPI()

    @app.post("/c")
    def route(
        df: Annotated[UserDF, Depends(columnar_dependency(UserDF))],
    ) -> dict[str, list]:
        return df.to_dict()

    schema = app.openapi()
    paths = schema["paths"]["/c"]["post"]
    ref = paths["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    assert ref.startswith("#/components/schemas/")
    name = ref.split("/")[-1]
    body_schema = schema["components"]["schemas"][name]
    assert body_schema["properties"]["id"]["type"] == "array"


def test_fastapi_app_with_executor_and_get_executor() -> None:
    app = fastapi_app_with_executor(max_workers=2)

    @app.get("/ex")
    def route(
        ex: Annotated[Executor | None, Depends(get_executor)],
    ) -> bool:
        return ex is not None

    with fastapi_test_client(app) as client:
        r = client.get("/ex")
        assert r.status_code == 200
        assert r.json() is True
