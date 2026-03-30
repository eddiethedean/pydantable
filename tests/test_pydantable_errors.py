"""Tests for :mod:`pydantable.errors` and schema ingest errors."""

from __future__ import annotations

import pytest
from pydantable import ColumnLengthMismatchError, DataFrameModel, PydantableUserError


class _ShapeDF(DataFrameModel):
    a: int
    b: int


def test_column_length_mismatch_is_user_error_and_value_error() -> None:
    with pytest.raises(ColumnLengthMismatchError) as ei:
        _ShapeDF({"a": [1, 2], "b": [3]})
    assert isinstance(ei.value, PydantableUserError)
    assert isinstance(ei.value, ValueError)
    assert "same length" in str(ei.value)


def test_valid_equal_lengths_still_construct() -> None:
    df = _ShapeDF({"a": [1, 2], "b": [3, 4]})
    assert df.to_dict() == {"a": [1, 2], "b": [3, 4]}
