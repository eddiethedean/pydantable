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
