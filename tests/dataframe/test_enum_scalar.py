import enum
from typing import Any

import pytest
from pydantable import DataFrame, Schema
from pydantable.schema import (
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_scalar_column_annotation,
)
from pydantic import ValidationError


class Color(enum.Enum):
    red = "red"
    blue = "blue"


class Row(Schema):
    c: Color


def test_enum_scalar_supported_annotation():
    assert is_supported_scalar_column_annotation(Color)
    assert is_supported_scalar_column_annotation(Color | None)


def test_enum_descriptor_roundtrip():
    d = {"base": "enum", "nullable": False}
    assert dtype_descriptor_to_annotation(d) is Any
    assert descriptor_matches_column_annotation(d, Color)


def test_dataframe_enum_roundtrip():
    df = DataFrame[Row]({"c": [Color.red, Color.blue]})
    assert df.to_dict()["c"] == [Color.red, Color.blue]


def test_dataframe_enum_validation_rejects_bad_cell():
    with pytest.raises(ValidationError):
        DataFrame[Row]({"c": ["nope"]})


def test_enum_filter_eq_member_literal():
    df = DataFrame[Row]({"c": [Color.red, Color.blue, Color.red]})
    out = df.filter(df.c == Color.blue).collect(as_lists=True)
    assert out["c"] == [Color.blue]


def test_enum_isin_accepts_wire_str_literal():
    df = DataFrame[Row]({"c": [Color.red, Color.blue, Color.red]})
    out = df.filter(df.c.isin("red")).collect(as_lists=True)
    assert out["c"] == [Color.red, Color.red]
