from __future__ import annotations

from pydantic import Field

from pydantable import DataFrameModel


class Users(DataFrameModel):
    user_id: int
    email: str = Field(json_schema_extra={"pydantable": {"redact": True}})


def test_to_dicts_redact_applies_policy() -> None:
    df = Users({"user_id": [1], "email": ["a@example.com"]})
    out = df.to_dicts(redact=True)
    assert out == [{"user_id": 1, "email": "***"}]


def test_to_dicts_redact_default_from_policy() -> None:
    class DF(DataFrameModel):
        __pydantable__ = {"redact": True}
        email: str = Field(json_schema_extra={"pydantable": {"redact": True}})

    df = DF({"email": ["x"]})
    assert df.to_dicts() == [{"email": "***"}]

