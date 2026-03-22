from __future__ import annotations

import enum
import uuid
from contextlib import suppress
from decimal import Decimal
from dataclasses import dataclass
from types import NoneType
from typing import Any, get_args, get_origin, get_type_hints

from pydantic import BaseModel


class DataType:
    """Spark-like type token for schema ergonomics (not a JVM DataType)."""

    typeName: str = "abstract"
    nullable: bool = False

    def __repr__(self) -> str:
        n = "nullable " if self.nullable else ""
        return f"{n}{self.__class__.__name__}()"

    def to_annotation(self) -> Any:
        raise NotImplementedError


class IntegerType(DataType):
    typeName = "integer"

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = nullable

    def to_annotation(self) -> Any:
        return int | None if self.nullable else int


class LongType(DataType):
    typeName = "long"

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = nullable

    def to_annotation(self) -> Any:
        return int | None if self.nullable else int


class DoubleType(DataType):
    typeName = "double"

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = nullable

    def to_annotation(self) -> Any:
        return float | None if self.nullable else float


class StringType(DataType):
    typeName = "string"

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = nullable

    def to_annotation(self) -> Any:
        return str | None if self.nullable else str


class BooleanType(DataType):
    typeName = "boolean"

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = nullable

    def to_annotation(self) -> Any:
        return bool | None if self.nullable else bool


@dataclass(frozen=True)
class StructField:
    """Spark-like field descriptor (name + pydantable DataType)."""

    name: str
    dataType: DataType


class ArrayType(DataType):
    """Spark-like array/list token (element dtype + nullability)."""

    typeName = "array"

    def __init__(self, element_type: DataType, *, nullable: bool = False) -> None:
        self.element_type = element_type
        self.nullable = nullable

    def __repr__(self) -> str:
        n = "nullable " if self.nullable else ""
        return f"{n}ArrayType({self.element_type!r})"

    def to_annotation(self) -> Any:
        raise NotImplementedError("ArrayType.to_annotation is not supported.")


class StructType(DataType):
    """Nested record or simple struct schema view (not JVM PySpark StructType)."""

    typeName = "struct"

    def __init__(self, fields: list[StructField], *, nullable: bool = False) -> None:
        self.fields = list(fields)
        self.nullable = nullable

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    def __repr__(self) -> str:
        n = "nullable " if self.nullable else ""
        inner = ", ".join(f"{sf.name}: {sf.dataType!r}" for sf in self.fields)
        return f"{n}StructType([{inner}])"

    def to_annotation(self) -> Any:
        raise NotImplementedError("StructType.to_annotation is not supported.")


def annotation_to_data_type(annotation: Any) -> DataType:
    """Best-effort map from a Pydantic-style annotation to a DataType token."""
    if annotation is int:
        return IntegerType(nullable=False)
    if annotation is float:
        return DoubleType(nullable=False)
    if annotation is str:
        return StringType(nullable=False)
    if annotation is bool:
        return BooleanType(nullable=False)
    if (
        isinstance(annotation, type)
        and issubclass(annotation, enum.Enum)
        and annotation is not enum.Enum
    ):
        return StringType(nullable=False)
    if annotation is uuid.UUID:
        return StringType(nullable=False)
    if annotation is Decimal:
        return StringType(nullable=False)

    args = tuple(get_args(annotation))
    if len(args) >= 2 and NoneType in args:
        non_none = [a for a in args if a is not NoneType]
        if len(non_none) == 1:
            inner = annotation_to_data_type(non_none[0])
            if isinstance(inner, IntegerType):
                return IntegerType(nullable=True)
            if isinstance(inner, LongType):
                return LongType(nullable=True)
            if isinstance(inner, DoubleType):
                return DoubleType(nullable=True)
            if isinstance(inner, StringType):
                return StringType(nullable=True)
            if isinstance(inner, BooleanType):
                return BooleanType(nullable=True)
            if isinstance(inner, StructType):
                return StructType(inner.fields, nullable=True)
            if isinstance(inner, ArrayType):
                return ArrayType(inner.element_type, nullable=True)

    origin = get_origin(annotation)
    if origin is list:
        la = get_args(annotation)
        if len(la) == 1:
            return ArrayType(annotation_to_data_type(la[0]))

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        hints: dict[str, Any] = {}
        with suppress(Exception):
            hints = get_type_hints(annotation, include_extras=True)
        fields: list[StructField] = []
        for n, finfo in annotation.model_fields.items():
            ann = hints.get(n)
            if ann is None:
                ann = finfo.annotation
            if ann is None:
                continue
            fields.append(StructField(n, annotation_to_data_type(ann)))
        return StructType(fields)

    return StringType(nullable=False)
