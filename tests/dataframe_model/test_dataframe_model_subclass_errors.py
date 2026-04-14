from __future__ import annotations

import pytest
from pydantable import DataFrameModel


def test_dataframe_model_subclass_requires_annotated_fields() -> None:
    with pytest.raises(TypeError, match="must define annotated fields"):

        class NoFieldsDF(DataFrameModel):
            pass


def test_dataframe_model_string_annotation_unknown_name_errors_cleanly() -> None:
    with pytest.raises(TypeError, match="unsupported type"):

        class UnknownAnnDF(DataFrameModel):
            x: "NotARealType"  # noqa: UP037,F821


def test_dataframe_model_nested_row_must_be_basemodel_subclass() -> None:
    with pytest.raises(TypeError, match=r"Row must be a Pydantic BaseModel subclass"):

        class BadRowDF(DataFrameModel):
            x: int
            Row = object()  # type: ignore[assignment]


def test_dataframe_model_row_base_must_be_basemodel_subclass() -> None:
    with pytest.raises(TypeError, match=r"__row_base__ must be a Pydantic BaseModel"):

        class BadRowBaseDF(DataFrameModel):
            x: int
            __row_base__ = object()  # type: ignore[assignment]
