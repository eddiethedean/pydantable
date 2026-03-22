"""Window expressions (row_number, window sum) via Polars lowering."""

from __future__ import annotations

from pydantable import DataFrame
from pydantable.expressions import row_number, window_sum
from pydantable.schema import Schema
from pydantable.window_spec import Window


class W(Schema):
    g: int
    v: int


def test_row_number_over_partition_order() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.with_columns(rn=row_number().over(w)).collect(as_lists=True)
    assert out["rn"] == [1, 2, 1]


def test_window_sum_over_partition() -> None:
    df = DataFrame[W]({"g": [1, 1, 2], "v": [10, 20, 30]})
    w = Window.partitionBy("g").spec()
    out = df.with_columns(s=window_sum(df.v).over(w)).collect(as_lists=True)
    assert out["s"] == [30, 30, 30]
