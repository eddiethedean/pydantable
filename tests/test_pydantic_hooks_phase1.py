from __future__ import annotations

from typing import Annotated, Any

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pydantable import DataFrameModel, Schema


Email = Annotated[str, Field(json_schema_extra={"pydantable": {"pii": True}})]


class _RowBase(Schema):
    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("email", check_fields=False)
    @classmethod
    def _lower_email(cls, v: str) -> str:
        return v.lower()


class _UsersRowWins(DataFrameModel):
    # Nested Row should take precedence over __row_base__.
    class Row(Schema):
        model_config = ConfigDict(str_strip_whitespace=True)

        @field_validator("email", check_fields=False)
        @classmethod
        def _lower_email_nested(cls, v: str) -> str:
            return v.lower()

    __row_base__ = _RowBase

    id: int
    email: str


class _UsersRowBaseOnly(DataFrameModel):
    __row_base__ = _RowBase

    id: int
    email: str


def test_row_base_precedence_nested_row_wins() -> None:
    # Use a value that requires the base's `str_strip_whitespace=True`.
    df = _UsersRowWins({"id": [1], "email": ["  A@EXAMPLE.COM  "]})
    rows = df.collect()
    assert rows[0].email == "a@example.com"


def test_row_base_applies_when_only___row_base__() -> None:
    df = _UsersRowBaseOnly({"id": [1], "email": ["  A@EXAMPLE.COM  "]})
    rows = df.collect()
    assert rows[0].email == "a@example.com"


def test_to_dicts_model_dump_kwargs_passthrough_by_alias() -> None:
    class _Aliased(DataFrameModel):
        id: int = Field(alias="user_id")
        note: str | None

    df = _Aliased({"id": [1], "note": [None]})
    out = df.to_dicts(by_alias=True, exclude_none=True)
    assert out == [{"user_id": 1}]


def test_column_policy_reading_from_json_schema_extra() -> None:
    class _PolicyDF(DataFrameModel):
        id: int
        email: Email

    assert _PolicyDF.column_policy("email") == {"pii": True}
    assert _PolicyDF.column_policies() == {"email": {"pii": True}}
    assert _PolicyDF.column_policy("id") == {}
    with pytest.raises(KeyError):
        _PolicyDF.column_policy("missing")


def test_json_schema_helpers_return_dicts() -> None:
    class _DF(DataFrameModel):
        id: int
        name: str

    row_schema = _DF.row_json_schema()
    schema_schema = _DF.schema_json_schema()
    assert isinstance(row_schema, dict)
    assert isinstance(schema_schema, dict)
    assert "properties" in row_schema
    assert "properties" in schema_schema


def test_to_dicts_passthrough_exclude_defaults_and_async_ato_dicts() -> None:
    class DF(DataFrameModel):
        id: int
        note: str | None = None

    df = DF({"id": [1], "note": [None]})
    assert df.to_dicts(exclude_defaults=True) == [{"id": 1}]

    async def _run() -> None:
        out = await df.ato_dicts(exclude_defaults=True)
        assert out == [{"id": 1}]

    import asyncio

    asyncio.run(_run())

