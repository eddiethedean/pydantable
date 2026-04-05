"""Parametrized smoke tests for shared pandas / pyspark façade methods."""

from __future__ import annotations

import pytest
from pydantable import Schema
from pydantable.pyspark import DataFrameModel as SparkDM


class _P(Schema):
    a: int


class _SparkMini(SparkDM):
    a: int


def _assert_head_tail(df: object) -> None:
    assert df.head(2).collect(as_lists=True)["a"] == [1, 2]
    assert df.tail(2).collect(as_lists=True)["a"] == [4, 5]


def test_pandas_facade_head_tail_roundtrip() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_P]({"a": [1, 2, 3, 4, 5]})
    _assert_head_tail(df)


def test_pyspark_facade_head_tail_roundtrip() -> None:
    df = _SparkMini({"a": [1, 2, 3, 4, 5]})
    _assert_head_tail(df)


def test_pandas_facade_sort_values_desc() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_P]({"a": [3, 1, 2]})
    out = df.sort_values("a", ascending=False).collect(as_lists=True)
    assert out["a"] == [3, 2, 1]


def test_pyspark_facade_sort_asc() -> None:
    df = _SparkMini({"a": [3, 1, 2]})
    out = df.sort("a").collect(as_lists=True)
    assert out["a"] == [1, 2, 3]


class _TwoCol(Schema):
    a: int
    b: int


class _SparkTwo(SparkDM):
    a: int
    b: int


def test_pandas_facade_drop_column_by_name() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_TwoCol]({"a": [1, 2], "b": [3, 4]})
    out = df.drop(columns=["b"]).collect(as_lists=True)
    assert out == {"a": [1, 2]}


def test_pyspark_facade_drop_column_by_name() -> None:
    df = _SparkTwo({"a": [1, 2], "b": [3, 4]})
    out = df.drop("b").collect(as_lists=True)
    assert out == {"a": [1, 2]}


def test_pandas_facade_rename_positional_mapping() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_TwoCol]({"a": [1], "b": [2]})
    out = df.rename({"a": "alpha"}).collect(as_lists=True)
    assert out == {"alpha": [1], "b": [2]}


def test_pandas_facade_rename_columns_kwarg() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_TwoCol]({"a": [1], "b": [2]})
    out = df.rename(columns={"b": "beta"}).collect(as_lists=True)
    assert out == {"a": [1], "beta": [2]}


def test_pandas_facade_rename_missing_column_errors_raise() -> None:
    pytest.importorskip("pandas")
    from pydantable.pandas import DataFrame

    df = DataFrame[_TwoCol]({"a": [1], "b": [2]})
    with pytest.raises(KeyError, match="not found"):
        df.rename(columns={"missing": "x"}, errors="raise")
