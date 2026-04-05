"""Exercise ``pyspark.sql.types`` tokens and ``annotation_to_data_type`` branches."""

from __future__ import annotations

import enum
import uuid
from decimal import Decimal

import pytest
from pydantic import BaseModel

from pydantable.pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    annotation_to_data_type,
)


class _E(enum.Enum):
    A = "a"


def test_data_type_repr_and_subclasses() -> None:
    assert "IntegerType" in repr(IntegerType())
    assert "nullable" in repr(IntegerType(nullable=True))
    assert "long" in LongType().typeName.lower() or "Long" in repr(LongType())


def test_to_annotation_nullable_variants() -> None:
    assert IntegerType(nullable=False).to_annotation() is int
    assert LongType(nullable=True).to_annotation() == int | None
    assert DoubleType(nullable=True).to_annotation() == float | None
    assert StringType(nullable=True).to_annotation() == str | None
    assert BooleanType(nullable=True).to_annotation() == bool | None


def test_array_and_struct_repr() -> None:
    at = ArrayType(IntegerType(), nullable=True)
    r = repr(at)
    assert "ArrayType" in r and "nullable" in r
    st = StructType([StructField("x", IntegerType())], nullable=True)
    assert "StructType" in repr(st) and "x" in repr(st)


def test_array_struct_to_annotation_unsupported() -> None:
    with pytest.raises(NotImplementedError):
        ArrayType(IntegerType()).to_annotation()
    with pytest.raises(NotImplementedError):
        StructType([]).to_annotation()


def test_annotation_to_data_type_primitives_and_specials() -> None:
    assert isinstance(annotation_to_data_type(int), IntegerType)
    assert isinstance(annotation_to_data_type(float), DoubleType)
    assert isinstance(annotation_to_data_type(str), StringType)
    assert isinstance(annotation_to_data_type(bool), BooleanType)
    assert isinstance(annotation_to_data_type(_E), StringType)
    assert isinstance(annotation_to_data_type(uuid.UUID), StringType)
    assert isinstance(annotation_to_data_type(Decimal), StringType)


def test_annotation_to_data_type_optional_inner_branches() -> None:
    assert annotation_to_data_type(int | None).nullable is True
    assert isinstance(annotation_to_data_type(int | None), IntegerType)
    assert isinstance(annotation_to_data_type(float | None), DoubleType)
    assert isinstance(annotation_to_data_type(str | None), StringType)
    assert isinstance(annotation_to_data_type(bool | None), BooleanType)


def test_annotation_to_data_type_optional_struct_and_array() -> None:
    class M(BaseModel):
        k: str

    st_ann = annotation_to_data_type(M)
    assert isinstance(st_ann, StructType)

    opt_struct = annotation_to_data_type(M | None)
    assert isinstance(opt_struct, StructType)
    assert opt_struct.nullable is True

    arr = annotation_to_data_type(list[int])
    assert isinstance(arr, ArrayType)

    opt_arr = annotation_to_data_type(list[int] | None)
    assert isinstance(opt_arr, ArrayType)
    assert opt_arr.nullable is True
