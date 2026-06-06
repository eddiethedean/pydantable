"""Public Schema / DataFrameModel acceptance rules for edge dtypes."""

from __future__ import annotations

from enum import Enum
from typing import Literal

import pytest
from pydantable import DataFrameModel
from pydantic import BaseModel


class _Color(str, Enum):
    RED = "red"
    BLUE = "blue"


class _Inner(BaseModel):
    score: int


def test_dataframe_accepts_literal_int_column() -> None:
    class Row(DataFrameModel):
        tag: Literal[1, 2, 3]

    df = Row({"tag": [1, 2, 1]})
    assert df.to_dict() == {"tag": [1, 2, 1]}


def test_dataframe_accepts_optional_literal_and_str_enum() -> None:
    class Row(DataFrameModel):
        label: Literal["a", "b"] | None
        color: _Color

    df = Row({"label": ["a", None], "color": [_Color.RED, _Color.BLUE]})
    assert df.to_dict() == {"label": ["a", None], "color": ["red", "blue"]}


def test_dataframe_rejects_mixed_literal_kinds_at_class_body() -> None:
    with pytest.raises(TypeError, match="Literal"):

        class Bad(DataFrameModel):
            x: Literal[1, "a"]  # type: ignore[misc]

        _ = Bad


def test_dataframe_accepts_nested_basemodel_column() -> None:
    class Outer(DataFrameModel):
        inner: _Inner

    df = Outer({"inner": [{"score": 1}, {"score": 2}]})
    assert df.to_dict()["inner"] == [{"score": 1}, {"score": 2}]
