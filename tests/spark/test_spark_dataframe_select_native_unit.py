from __future__ import annotations

import pytest
from pydantable.schema import Schema
from pydantable.spark_dataframe import SparkDataFrame


class Row(Schema):
    x: int
    y: int


class _JC:
    def __init__(self, s: str) -> None:
        self._s = s

    def toString(self) -> str:
        return self._s


class _Col:
    def __init__(self, s: str) -> None:
        self._jc = _JC(s)


def test_spark_dataframe_select_native_accepts_strings() -> None:
    df = SparkDataFrame[Row]({"x": [1], "y": [2]})
    out = df.select_native("x").to_dict()
    assert out == {"x": [1]}


def test_spark_dataframe_select_native_accepts_simple_column_ref_stub() -> None:
    df = SparkDataFrame[Row]({"x": [1], "y": [2]})
    out = df.select_native(_Col("x")).to_dict()
    assert out == {"x": [1]}


def test_spark_dataframe_select_native_rejects_non_simple_column_expression() -> None:
    df = SparkDataFrame[Row]({"x": [1], "y": [2]})
    with pytest.raises(TypeError, match="simple column"):
        df.select_native(_Col("x \\+ 1"))
