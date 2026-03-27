from __future__ import annotations

import enum
import uuid
from decimal import Decimal

import pytest
from pydantable.pyspark.sql.types import (
    ArrayType,
    DataType,
    IntegerType,
    StructField,
    StructType,
    annotation_to_data_type,
)
from pydantic import BaseModel


def test_data_type_to_annotation_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        DataType().to_annotation()


def test_array_and_struct_tokens_reject_to_annotation() -> None:
    with pytest.raises(NotImplementedError, match="ArrayType"):
        ArrayType(IntegerType()).to_annotation()
    with pytest.raises(NotImplementedError, match="StructType"):
        StructType([StructField("a", IntegerType())]).to_annotation()


def test_annotation_to_data_type_stdlib_scalars() -> None:
    class Color(enum.Enum):
        red = "r"

    assert annotation_to_data_type(Color).typeName == "string"
    assert annotation_to_data_type(uuid.UUID).typeName == "string"
    assert annotation_to_data_type(Decimal).typeName == "string"


def test_annotation_to_data_type_optional_and_list() -> None:
    opt_int = annotation_to_data_type(int | None)
    assert isinstance(opt_int, IntegerType)
    assert opt_int.nullable is True

    lst = annotation_to_data_type(list[str])
    assert isinstance(lst, ArrayType)
    assert lst.element_type.typeName == "string"


def test_annotation_to_data_type_nested_basemodel() -> None:
    class Inner(BaseModel):
        score: float

    class Outer(BaseModel):
        name: str
        inner: Inner

    st = annotation_to_data_type(Outer)
    assert isinstance(st, StructType)
    assert {f.name for f in st.fields} >= {"name", "inner"}


def test_fallback_annotation_becomes_string() -> None:
    class Opaque:
        pass

    assert annotation_to_data_type(Opaque).typeName == "string"
