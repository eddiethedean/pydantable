"""Focused tests for ``dataframe/_impl`` and ``dataframe_model`` error paths."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, DataFrameModel, Schema
from pydantic import ValidationError


class _S(Schema):
    id: int
    name: str | None


class _M(DataFrameModel):
    id: int


def test_dataframe_collect_requires_columns_present() -> None:
    df = DataFrame[_S]({"id": [1], "name": ["a"]})
    assert df.collect(as_lists=True)["id"] == [1]


def test_dataframe_model_rejects_extra_columns_in_rows() -> None:
    with pytest.raises(ValidationError, match="extra"):
        _M([{"id": 1, "extra": 2}])


def test_dataframe_head_zero_returns_empty_columns() -> None:
    df = DataFrame[_S]({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    out = df.head(0).collect(as_lists=True)
    assert out == {"id": [], "name": []}


def test_dataframe_with_row_count_rejects_empty_name() -> None:
    df = DataFrame[_S]({"id": [1], "name": ["a"]})
    with pytest.raises(TypeError, match="non-empty string"):
        df.with_row_count("")
