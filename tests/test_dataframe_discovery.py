"""Tests for columns, shape, info, describe on core DataFrame."""

from __future__ import annotations

from pydantable import DataFrame, DataFrameModel
from pydantic import BaseModel


class _T(BaseModel):
    x: int
    y: float


def test_columns_shape_dtypes() -> None:
    df = DataFrame[_T]({"x": [1, 2], "y": [1.0, 2.0]})
    assert df.columns == ["x", "y"]
    assert df.shape == (2, 2)
    assert not df.empty
    assert "x" in df.dtypes


def test_info_contains_shape_line() -> None:
    df = DataFrame[_T]({"x": [1], "y": [2.0]})
    s = df.info()
    assert "shape (root buffer)" in s
    assert "dtypes:" in s


def test_describe_numeric() -> None:
    df = DataFrame[_T]({"x": [1, 2, 3], "y": [1.0, 2.0, 3.0]})
    d = df.describe()
    assert "x:" in d and "mean=" in d


def test_dataframe_model_delegates() -> None:
    class M(DataFrameModel):
        x: int

    m = M({"x": [1, 2]})
    assert m.columns == ["x"]
    assert m.shape[0] == 2
    assert "schema" in m.info().lower() or "Schema" in m.info()


def test_pyspark_show_and_summary() -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": [1, 2, 3]})
    s = df.summary()
    assert "a:" in s and "count=3" in s
