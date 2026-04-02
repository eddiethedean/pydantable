from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantable import DataFrameModel
from pydantable.fastapi import columnar_dependency
from pydantic import Field


class Users(DataFrameModel):
    user_id: int = Field(validation_alias="userId")


def test_columnar_dependency_input_key_mode_aliases_maps_keys() -> None:
    app = FastAPI()

    @app.post("/ingest")
    def ingest(
        df: Annotated[
            Users,
            Depends(columnar_dependency(Users, input_key_mode="aliases")),
        ],
    ) -> dict:
        return df.to_dict()

    with TestClient(app) as client:
        r = client.post("/ingest", json={"userId": [1, 2]})
        assert r.status_code == 200
        assert r.json() == {"user_id": [1, 2]}


def test_columnar_dependency_input_key_mode_python_rejects_alias_key() -> None:
    app = FastAPI()

    @app.post("/ingest")
    def ingest(
        df: Annotated[
            Users,
            Depends(columnar_dependency(Users, input_key_mode="python")),
        ],
    ) -> dict:
        return df.to_dict()

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.post("/ingest", json={"userId": [1]})
        assert r.status_code == 422


def test_columnar_dependency_input_key_mode_aliases_rejects_python_key() -> None:
    app = FastAPI()

    @app.post("/ingest")
    def ingest(
        df: Annotated[
            Users,
            Depends(columnar_dependency(Users, input_key_mode="aliases")),
        ],
    ) -> dict:
        return df.to_dict()

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.post("/ingest", json={"user_id": [1]})
        assert r.status_code == 422


def test_columnar_dependency_input_key_mode_both_rejects_conflict() -> None:
    app = FastAPI()

    @app.post("/ingest")
    def ingest(
        df: Annotated[
            Users,
            Depends(columnar_dependency(Users, input_key_mode="both")),
        ],
    ) -> dict:
        return df.to_dict()

    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.post("/ingest", json={"userId": [1], "user_id": [1]})
        # ValueError inside dependency becomes 500 unless app maps it; just assert fail.
        assert r.status_code >= 400
