"""Strict trusted mode checks for PyArrow column buffers (0.13.x)."""

from __future__ import annotations

import enum
from decimal import Decimal

import pytest
from pydantable import DataFrame, Schema

pa = pytest.importorskip("pyarrow")


class IntCol(Schema):
    i: int


class DecCol(Schema):
    d: Decimal


class Color(enum.Enum):
    a = "a"
    b = "b"


class EnumCol(Schema):
    c: Color


def test_strict_pyarrow_int64_array() -> None:
    arr = pa.array([1, 2, 3], type=pa.int64())
    df = DataFrame[IntCol]({"i": arr}, trusted_mode="strict")
    assert df.to_dict() == {"i": [1, 2, 3]}


def test_strict_pyarrow_int_chunked_array() -> None:
    ca = pa.chunked_array(
        [pa.array([1, 2], type=pa.int64()), pa.array([3], type=pa.int64())]
    )
    df = DataFrame[IntCol]({"i": ca}, trusted_mode="strict")
    assert df.to_dict() == {"i": [1, 2, 3]}


def test_strict_pyarrow_int_rejects_float64() -> None:
    arr = pa.array([1.0, 2.0], type=pa.float64())
    with pytest.raises(ValueError, match="strict trusted"):
        DataFrame[IntCol]({"i": arr}, trusted_mode="strict")


def test_strict_pyarrow_decimal128() -> None:
    arr = pa.array(
        [Decimal("1.50"), Decimal("2.50")],
        type=pa.decimal128(12, 2),
    )
    df = DataFrame[DecCol]({"d": arr}, trusted_mode="strict")
    out = df.to_dict()["d"]
    assert out[0] == Decimal("1.50")
    assert out[1] == Decimal("2.50")


def test_strict_pyarrow_decimal_rejects_int64() -> None:
    arr = pa.array([1, 2], type=pa.int64())
    with pytest.raises(ValueError, match="strict trusted"):
        DataFrame[DecCol]({"d": arr}, trusted_mode="strict")


def test_strict_pyarrow_enum_accepts_string_arrow_buffer() -> None:
    """Strict allows Arrow utf8 for string enums (construction-only check)."""
    arr = pa.array(["a", "b", "a"], type=pa.string())
    df = DataFrame[EnumCol]({"c": arr}, trusted_mode="strict")
    assert "c" in df.schema_fields()


def test_strict_pyarrow_enum_rejects_float() -> None:
    arr = pa.array([1.0, 2.0], type=pa.float64())
    with pytest.raises(ValueError, match="strict trusted"):
        DataFrame[EnumCol]({"c": arr}, trusted_mode="strict")
