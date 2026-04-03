"""Phase B1: Parquet lazy scan kwargs (hive, row index, include_file_paths)."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable_native._core")

from pydantable import DataFrameModel
from pydantable.io import export_parquet


class HivePart(DataFrameModel):
    p: str
    x: int


class RowIdx(DataFrameModel):
    idx: int
    x: int


class WithPath(DataFrameModel):
    path: str
    x: int


def test_read_parquet_hive_partitioned_directory(tmp_path) -> None:
    part_dir = tmp_path / "p=hello"
    part_dir.mkdir(parents=True)
    f = part_dir / "data.parquet"
    export_parquet(f, {"x": [1, 2]})
    df = HivePart.read_parquet(
        str(tmp_path),
        trusted_mode="shape_only",
        hive_partitioning=True,
    )
    d = df.to_dict()
    assert "p" in d
    assert d["p"] == ["hello", "hello"]
    assert d["x"] == [1, 2]


def test_read_parquet_row_index_name(tmp_path) -> None:
    path = tmp_path / "one.pq"
    export_parquet(path, {"x": [10, 20]})
    df = RowIdx.read_parquet(
        str(path),
        trusted_mode="shape_only",
        row_index_name="idx",
        row_index_offset=0,
    )
    d = df.to_dict()
    assert d["idx"] == [0, 1]
    assert d["x"] == [10, 20]


def test_read_parquet_include_file_paths(tmp_path) -> None:
    path = tmp_path / "one.pq"
    export_parquet(path, {"x": [1]})
    df = WithPath.read_parquet(
        str(path),
        trusted_mode="shape_only",
        include_file_paths="path",
    )
    d = df.to_dict()
    assert "path" in d
    assert len(d["path"]) == 1
    assert str(path) in d["path"][0] or d["path"][0].endswith("one.pq")


class OnlyX(DataFrameModel):
    x: int


def test_read_parquet_unknown_scan_kw_raises(tmp_path) -> None:
    path = tmp_path / "x.pq"
    export_parquet(path, {"x": [1]})
    with pytest.raises(ValueError, match=re.escape("unknown scan_kw key")):
        OnlyX.read_parquet(
            str(path),
            trusted_mode="shape_only",
            not_a_valid_parquet_scan_kw=True,
        ).to_dict()


def test_row_index_offset_without_name_errors(tmp_path) -> None:
    path = tmp_path / "x.pq"
    export_parquet(path, {"x": [1]})
    with pytest.raises(ValueError, match="row_index_offset requires row_index_name"):
        OnlyX.read_parquet(
            str(path),
            trusted_mode="shape_only",
            row_index_offset=1,
        ).to_dict()
