"""Phase E: multi-file Parquet directory scan with ``allow_missing_columns=True``."""

from __future__ import annotations

import pytest

pytest.importorskip("pydantable._core")

from pydantable import DataFrameModel
from pydantable.io import export_parquet


class XY(DataFrameModel):
    x: int
    y: int | None


def test_read_parquet_directory_allow_missing_columns_unions_nulls(tmp_path) -> None:
    """Mismatched columns: second file lacks ``y``; nulls fill under union."""
    export_parquet(tmp_path / "a.parquet", {"x": [1], "y": [10]})
    export_parquet(tmp_path / "b.parquet", {"x": [2]})
    df = XY.read_parquet(
        str(tmp_path),
        trusted_mode="shape_only",
        glob=True,
        allow_missing_columns=True,
    )
    assert df.to_dict() == {"x": [1, 2], "y": [10, None]}


def test_read_parquet_directory_without_allow_missing_columns_errors_on_collect(
    tmp_path,
) -> None:
    """Lazy scan may succeed; materialization fails without union of missing columns."""
    export_parquet(tmp_path / "a.parquet", {"x": [1], "y": [10]})
    export_parquet(tmp_path / "b.parquet", {"x": [2]})
    df = XY.read_parquet(
        str(tmp_path),
        trusted_mode="shape_only",
        glob=True,
        allow_missing_columns=False,
    )
    with pytest.raises(ValueError, match="column y"):
        df.to_dict()
