"""join_as keyword aliases and validation (DataFrame)."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema


class Left(Schema):
    id: int
    x: int


class Right(Schema):
    id: int
    y: int


class Joined(Schema):
    id: int
    x: int
    y: int


def test_join_as_schema_alias_matches_positional() -> None:
    left = DataFrame[Left]({"id": [1], "x": [10]})
    right = DataFrame[Right]({"id": [1], "y": [20]})
    on = [left.col.id]
    a = left.join_as(Joined, right, on=on)
    b = left.join_as(other=right, schema=Joined, on=on)
    c = left.join_as(other=right, after_schema_type=Joined, on=on)
    assert a.to_dict() == b.to_dict() == c.to_dict()


def test_join_as_after_schema_type_and_schema_disagree() -> None:
    class OtherJoined(Schema):
        id: int
        x: int
        y: int

    left = DataFrame[Left]({"id": [1], "x": [10]})
    right = DataFrame[Right]({"id": [1], "y": [20]})
    on = [left.col.id]
    with pytest.raises(TypeError, match="disagree"):
        left.join_as(Joined, right, schema=OtherJoined, on=on)
