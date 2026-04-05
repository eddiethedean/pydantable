from __future__ import annotations

import pytest
from pydantable.window_spec import Window


def test_partition_by_requires_columns() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        Window.partitionBy()


def test_order_by_requires_columns() -> None:
    with pytest.raises(ValueError, match="orderBy"):
        Window.partitionBy("a").orderBy()


def test_order_by_ascending_length_mismatch() -> None:
    with pytest.raises(ValueError, match="ascending length"):
        Window.partitionBy("a").orderBy("x", "y", ascending=[True])


def test_order_by_nulls_last_length_mismatch() -> None:
    with pytest.raises(ValueError, match="nulls_last length"):
        Window.partitionBy("a").orderBy("x", "y", nulls_last=[True])


def test_partition_only_spec_and_frames() -> None:
    b = Window.partitionBy("k")
    s = b.spec()
    assert s.partition_by == ("k",)
    assert s.order_by == ()

    r = b.rowsBetween(0, 2)
    assert r.frame_kind == "rows"
    assert r.frame_start == 0
    assert r.frame_end == 2

    g = b.rangeBetween(-1, 1)
    assert g.frame_kind == "range"
    assert g.frame_start == -1
    assert g.frame_end == 1
