"""Parametrized getitem validation for pandas- and pyspark-shaped facades."""

from __future__ import annotations

import pytest
from pydantable import Schema


class _Row(Schema):
    a: int
    b: int


@pytest.mark.parametrize(
    ("facade", "factory"),
    [
        (
            "pandas",
            lambda: __import__("pydantable.pandas", fromlist=["DataFrame"]).DataFrame[
                _Row
            ]({"a": [1], "b": [2]}),
        ),
        (
            "pyspark",
            lambda: __import__("pydantable.pyspark", fromlist=["DataFrame"]).DataFrame[
                _Row
            ]({"a": [1], "b": [2]}),
        ),
    ],
)
def test_facade_getitem_rejects_empty_and_invalid_keys(
    facade: str, factory: object
) -> None:
    df = factory()  # type: ignore[operator]
    with pytest.raises(ValueError, match="non-empty"):
        df[[]]
    with pytest.raises(TypeError, match="supports"):
        df[object()]  # type: ignore[index]
