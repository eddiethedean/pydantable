"""Phase B2: CSV lazy scan — directory, glob, hive not applied, unknown kwargs."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable_native._core")

from pydantable import DataFrameModel
from pydantable.io import export_csv


class XY(DataFrameModel):
    x: int
    y: int


class XOnly(DataFrameModel):
    x: int


class RowIdx(DataFrameModel):
    idx: int
    x: int


class WithPath(DataFrameModel):
    path: str
    x: int


def test_read_csv_directory_concatenates_files(tmp_path) -> None:
    export_csv(tmp_path / "a.csv", {"x": [1, 2], "y": [10, 20]})
    export_csv(tmp_path / "b.csv", {"x": [3], "y": [30]})
    df = XY.read_csv(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert d["x"] == [1, 2, 3]
    assert d["y"] == [10, 20, 30]


def test_read_csv_glob_star_csv(tmp_path) -> None:
    export_csv(tmp_path / "a.csv", {"x": [1], "y": [2]})
    export_csv(tmp_path / "b.csv", {"x": [3], "y": [4]})
    pattern = str(tmp_path / "*.csv")
    df = XY.read_csv(pattern, trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert sorted(zip(d["x"], d["y"], strict=True)) == [(1, 2), (3, 4)]


def test_read_csv_hive_style_path_does_not_add_partition_column(tmp_path) -> None:
    part = tmp_path / "p=hello"
    part.mkdir(parents=True)
    export_csv(part / "a.csv", {"x": [1]})
    df = XOnly.read_csv(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert "p" not in d
    assert d["x"] == [1]


def test_read_csv_unknown_scan_kw_raises(tmp_path) -> None:
    export_csv(tmp_path / "one.csv", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match=re.escape("unknown scan_kw key")):
        XY.read_csv(
            str(tmp_path / "one.csv"),
            trusted_mode="shape_only",
            not_a_valid_csv_scan_kw=True,
        ).to_dict()


def test_read_csv_row_index_name(tmp_path) -> None:
    export_csv(tmp_path / "one.csv", {"x": [10, 20]})
    df = RowIdx.read_csv(
        str(tmp_path / "one.csv"),
        trusted_mode="shape_only",
        row_index_name="idx",
        row_index_offset=0,
    )
    d = df.to_dict()
    assert d["idx"] == [0, 1]
    assert d["x"] == [10, 20]


def test_read_csv_include_file_paths(tmp_path) -> None:
    export_csv(tmp_path / "one.csv", {"x": [1]})
    df = WithPath.read_csv(
        str(tmp_path / "one.csv"),
        trusted_mode="shape_only",
        include_file_paths="path",
    )
    d = df.to_dict()
    assert "path" in d
    assert len(d["path"]) == 1
    assert str(tmp_path / "one.csv") in d["path"][0] or d["path"][0].endswith("one.csv")


def test_row_index_offset_without_name_errors(tmp_path) -> None:
    export_csv(tmp_path / "x.csv", {"x": [1]})
    with pytest.raises(ValueError, match="row_index_offset requires row_index_name"):
        RowIdx.read_csv(
            str(tmp_path / "x.csv"),
            trusted_mode="shape_only",
            row_index_offset=1,
        ).to_dict()
