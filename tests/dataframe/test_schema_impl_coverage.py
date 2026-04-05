"""Behavioral tests for ``schema/_impl`` branches not covered elsewhere."""

from __future__ import annotations

from enum import Enum
from typing import Literal
from unittest.mock import patch

import pytest  # noqa: TC002
from pydantable.schema import is_supported_scalar_column_annotation
from pydantable.schema._impl import (
    _check_literal_column_args,
    _is_supported_column_annotation_inner,
    _is_supported_non_null_scalar_type,
    _shape_only_drift_warnings_enabled,
)
from pydantic import BaseModel


class _Color(Enum):
    RED = 1
    BLUE = 2


def test_literal_column_args_empty() -> None:
    assert not _check_literal_column_args(())


def test_literal_column_args_mixed_primitive_kinds() -> None:
    assert not _check_literal_column_args((1, "a"))


def test_literal_column_args_bool_and_int_distinct() -> None:
    assert not _check_literal_column_args((True, 1))


def test_supported_scalar_literal_int_only() -> None:
    assert is_supported_scalar_column_annotation(Literal[1, 2, 3])


def test_supported_optional_literal() -> None:
    assert is_supported_scalar_column_annotation(Literal["a", "b"] | None)


def test_enum_non_null_scalar() -> None:
    assert _is_supported_non_null_scalar_type(_Color)


def test_shape_only_env_disables_drift_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS", "1")
    assert _shape_only_drift_warnings_enabled() is False
    monkeypatch.delenv("PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS", raising=False)
    assert _shape_only_drift_warnings_enabled() is True


@patch(
    "pydantable.schema._impl.get_type_hints",
    side_effect=KeyError("missing"),
)
def test_keyerror_from_get_type_hints_unsupported_model(_mock: object) -> None:
    class M(BaseModel):
        x: int

    assert not _is_supported_column_annotation_inner(M, _model_stack=set())


@patch(
    "pydantable.schema._impl.get_type_hints",
    side_effect=AttributeError("attr"),
)
def test_attributeerror_from_get_type_hints_unsupported_model(_mock: object) -> None:
    class M(BaseModel):
        x: int

    assert not _is_supported_column_annotation_inner(M, _model_stack=set())


@patch(
    "pydantable.schema._impl.get_type_hints",
    side_effect=RecursionError("deep"),
)
def test_recursionerror_from_get_type_hints_unsupported_model(_mock: object) -> None:
    class M(BaseModel):
        x: int

    assert not _is_supported_column_annotation_inner(M, _model_stack=set())
