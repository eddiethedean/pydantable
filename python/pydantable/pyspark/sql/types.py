from __future__ import annotations

from dataclasses import dataclass
from types import NoneType
from typing import Any, get_args


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


class StructType:
    """Simple struct schema view (not PySpark StructType)."""

    def __init__(self, fields: list[StructField]) -> None:
        self.fields = list(fields)

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    def __repr__(self) -> str:
        inner = ", ".join(f"{sf.name}: {sf.dataType!r}" for sf in self.fields)
        return f"StructType([{inner}])"


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
    return StringType(nullable=False)
