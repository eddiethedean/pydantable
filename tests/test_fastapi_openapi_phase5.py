from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import Field

from pydantable import DataFrameModel
from pydantable.fastapi import columnar_dependency, rows_dependency


class Users(DataFrameModel):
    user_id: int = Field(description="User id", examples=[1])
    email: str = Field(description="Email address", examples=["a@example.com"])


def test_openapi_columnar_body_has_descriptions_and_examples() -> None:
    app = FastAPI()

    @app.post("/ingest/columnar")
    def ingest_columnar(
        df: Annotated[
            Users,
            Depends(
                columnar_dependency(
                    Users,
                    generate_examples=True,
                )
            ),
        ]
    ) -> dict:
        return df.to_dict()

    @app.post("/ingest/rows")
    def ingest_rows(
        df: Annotated[
            Users,
            Depends(rows_dependency(Users)),
        ]
    ) -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        spec = client.get("/openapi.json").json()

    # Locate the request body schema for the columnar endpoint and assert
    # field-level description + examples were propagated.
    req_schema = (
        spec["paths"]["/ingest/columnar"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
    )
    ref = req_schema["$ref"]
    name = ref.split("/")[-1]
    model_schema = spec["components"]["schemas"][name]
    props = model_schema["properties"]

    assert props["user_id"]["description"] == "User id"
    assert props["email"]["description"] == "Email address"
    assert props["user_id"]["examples"] == [1]
    assert props["email"]["examples"] == ["a@example.com"]

