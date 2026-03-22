"""Global aggregates in select (Phase D) — single-row results."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import global_mean, global_sum
from pydantable.schema import Schema


class T(Schema):
    id: int
    v: int


def test_select_global_sum_mean_single_row() -> None:
    df = DataFrame[T]({"id": [1, 2, 3], "v": [10, 20, 30]})
    out = df.select(global_sum(df.v), global_mean(df.v)).collect(as_lists=True)
    assert out == {"sum_v": [60], "mean_v": [20.0]}


def test_select_global_sum_named() -> None:
    df = DataFrame[T]({"id": [1, 2], "v": [5, 15]})
    out = df.select(total=global_sum(df.v)).collect(as_lists=True)
    assert out == {"total": [20]}
