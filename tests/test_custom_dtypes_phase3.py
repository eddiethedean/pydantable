from __future__ import annotations

from typing import Any

import pytest
from pydantic_core import CoreSchema, core_schema

from pydantable import DataFrameModel
from pydantable.dtypes import register_scalar, reset_registry_for_tests


class ULID(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Any
    ) -> CoreSchema:
        def coerce(v: object) -> ULID:
            if isinstance(v, ULID):
                return v
            if isinstance(v, str):
                return ULID(v.strip())
            raise TypeError("ULID expects str")

        return core_schema.no_info_after_validator_function(
            coerce,
            core_schema.str_schema(),
        )


def test_register_scalar_rejects_bad_inputs() -> None:
    reset_registry_for_tests()
    with pytest.raises(TypeError):
        register_scalar("not-a-type", base="str")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        register_scalar(ULID, base="uuid")  # not supported by registry in Phase 3


def test_custom_scalar_is_supported_and_validates_under_off() -> None:
    reset_registry_for_tests()
    register_scalar(ULID, base="str")

    class DF(DataFrameModel):
        id: ULID

    df = DF({"id": ["  abc  "]})
    rows = df.collect()
    assert isinstance(rows[0].id, ULID)
    assert rows[0].id == "abc"


def test_custom_scalar_shape_only_accepts_like_base() -> None:
    reset_registry_for_tests()
    register_scalar(ULID, base="str")

    class DF(DataFrameModel):
        id: ULID

    df = DF({"id": ["x"]}, trusted_mode="shape_only")
    assert df.to_dict() == {"id": ["x"]}


def test_custom_scalar_identity_preserved_on_select() -> None:
    reset_registry_for_tests()
    register_scalar(ULID, base="str")

    class DF(DataFrameModel):
        id: ULID
        other: int

    df = DF({"id": ["a"], "other": [1]})
    projected = df.select("id")
    assert projected.schema_fields()["id"] is ULID

