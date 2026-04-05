"""Regression: narrowed exceptions around ``get_type_hints`` in schema helpers."""

from __future__ import annotations

from unittest.mock import patch

from pydantable.schema._impl import _is_supported_column_annotation_inner
from pydantic import BaseModel


class Simple(BaseModel):
    x: int


def test_supported_nested_model_still_recognized() -> None:
    assert _is_supported_column_annotation_inner(Simple, _model_stack=set())


@patch(
    "pydantable.schema._impl.get_type_hints",
    side_effect=NameError("unresolved forward ref"),
)
def test_nameerror_from_get_type_hints_marks_model_unsupported(_mock: object) -> None:
    class M(BaseModel):
        x: int

    assert not _is_supported_column_annotation_inner(M, _model_stack=set())


@patch(
    "pydantable.schema._impl.get_type_hints",
    side_effect=TypeError("bad annotation"),
)
def test_typeerror_from_get_type_hints_marks_model_unsupported(_mock: object) -> None:
    class M(BaseModel):
        x: int

    assert not _is_supported_column_annotation_inner(M, _model_stack=set())
