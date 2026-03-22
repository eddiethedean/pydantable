from decimal import Decimal

import pytest
from pydantic import ValidationError

from pydantable import DataFrame, Schema
from pydantable.schema import (
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_scalar_column_annotation,
)


class Row(Schema):
    x: Decimal


def test_decimal_scalar_supported():
    assert is_supported_scalar_column_annotation(Decimal)
    assert is_supported_scalar_column_annotation(Decimal | None)


def test_decimal_descriptor_roundtrip():
    d = {"base": "decimal", "nullable": False}
    assert dtype_descriptor_to_annotation(d) is Decimal
    assert descriptor_matches_column_annotation(d, Decimal)


def test_dataframe_decimal_roundtrip():
    v = Decimal("12.345678901")
    df = DataFrame[Row]({"x": [v]})
    out = df.to_dict()["x"][0]
    assert isinstance(out, Decimal)
    assert out == v


def test_dataframe_decimal_validation():
    with pytest.raises(ValidationError):
        DataFrame[Row]({"x": ["nope"]})
