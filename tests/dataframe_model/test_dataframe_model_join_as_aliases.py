"""join_as keyword aliases and validation (DataFrameModel)."""

from __future__ import annotations

import pytest
from pydantable import DataFrameModel


class Left(DataFrameModel):
    id: int
    x: int


class Right(DataFrameModel):
    id: int
    y: int


class Joined(DataFrameModel):
    id: int
    x: int
    y: int


def test_join_as_after_model_alias_matches_positional() -> None:
    left = Left({"id": [1], "x": [10]})
    right = Right({"id": [1], "y": [20]})
    on = [left.col.id]
    a = left.join_as(right, Joined, on=on)
    b = left.join_as(other=right, after_model=Joined, on=on)
    c = left.join_as(other=right, model=Joined, on=on)
    assert a.to_dict() == b.to_dict() == c.to_dict()


def test_join_as_model_and_after_model_disagree() -> None:
    class OtherJoined(DataFrameModel):
        id: int
        x: int
        y: int

    left = Left({"id": [1], "x": [10]})
    right = Right({"id": [1], "y": [20]})
    on = [left.col.id]
    with pytest.raises(TypeError, match="disagree"):
        left.join_as(right, Joined, after_model=OtherJoined, on=on)
