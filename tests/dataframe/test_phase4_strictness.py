from __future__ import annotations

from typing import ClassVar

import pytest
from pydantable import DataFrameModel
from pydantable.validation_profiles import (
    register_validation_profile,
    reset_validation_profiles_for_tests,
)
from pydantic import BaseModel, Field, ValidationError


class Inner(BaseModel):
    n: int


def test_column_strictness_strict_rejects_coercion() -> None:
    class DF(DataFrameModel):
        x: int = Field(json_schema_extra={"pydantable": {"strictness": "strict"}})

    with pytest.raises(ValidationError):
        DF({"x": ["1"]})


def test_column_strictness_off_skips_element_validation_but_enforces_nullability() -> (
    None
):
    class DF(DataFrameModel):
        x: int = Field(json_schema_extra={"pydantable": {"strictness": "off"}})

    # Element validation is skipped, but the Rust/Polars engine still requires
    # engine-compatible dtypes for execution.
    df = DF({"x": [1]})
    assert df.to_dict() == {"x": [1]}

    with pytest.raises(ValueError):
        DF({"x": [None]})


def test_nested_strictness_strict_applies_to_struct_column() -> None:
    class DF(DataFrameModel):
        inner: Inner = Field(
            json_schema_extra={"pydantable": {"nested_strictness": "strict"}}
        )

    with pytest.raises(ValidationError):
        DF({"inner": [{"n": "1"}]})


def test_profile_defaults_apply_when_inherit() -> None:
    reset_validation_profiles_for_tests()
    register_validation_profile(
        "strict_cols",
        {
            "column_strictness_default": "strict",
            "nested_strictness_default": "strict",
        },
    )

    class DF(DataFrameModel):
        __pydantable__: ClassVar[dict[str, object]] = {
            "validation_profile": "strict_cols"
        }
        x: int
        inner: Inner

    with pytest.raises(ValidationError):
        DF({"x": ["1"], "inner": [{"n": "1"}]})


def test_nested_strictness_strict_applies_to_list_and_dict_values() -> None:
    class DF(DataFrameModel):
        xs: list[int] = Field(
            json_schema_extra={"pydantable": {"nested_strictness": "strict"}}
        )
        m: dict[str, int] = Field(
            json_schema_extra={"pydantable": {"nested_strictness": "strict"}}
        )

    with pytest.raises(ValidationError):
        DF({"xs": [["1"]], "m": [{"a": "1"}]})
