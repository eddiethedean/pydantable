"""Phase B4: IPC lazy scan — directory, glob, unknown kwargs, lineage."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable_native._core")

from pydantable import DataFrameModel
from pydantable.io import export_ipc


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


def test_read_ipc_directory_concatenates_files(tmp_path) -> None:
    export_ipc(tmp_path / "a.arrow", {"x": [1, 2], "y": [10, 20]})
    export_ipc(tmp_path / "b.arrow", {"x": [3], "y": [30]})
    df = XY.read_ipc(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert d["x"] == [1, 2, 3]
    assert d["y"] == [10, 20, 30]


def test_read_ipc_glob_star_arrow(tmp_path) -> None:
    export_ipc(tmp_path / "a.arrow", {"x": [1], "y": [2]})
    export_ipc(tmp_path / "b.arrow", {"x": [3], "y": [4]})
    pattern = str(tmp_path / "*.arrow")
    df = XY.read_ipc(pattern, trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert sorted(zip(d["x"], d["y"], strict=True)) == [(1, 2), (3, 4)]


def test_read_ipc_hive_style_path_does_not_add_partition_column(tmp_path) -> None:
    """Hive-style dir: no partition column (same idea as CSV/NDJSON B-tests)."""
    part = tmp_path / "p=hello"
    part.mkdir(parents=True)
    export_ipc(part / "a.arrow", {"x": [1]})
    df = XOnly.read_ipc(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert "p" not in d
    assert d["x"] == [1]


def test_read_ipc_unknown_scan_kw_raises(tmp_path) -> None:
    export_ipc(tmp_path / "one.arrow", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match=re.escape("unknown scan_kw key")):
        XY.read_ipc(
            str(tmp_path / "one.arrow"),
            trusted_mode="shape_only",
            not_a_valid_ipc_scan_kw=True,
        ).to_dict()


def test_read_ipc_row_index_name(tmp_path) -> None:
    export_ipc(tmp_path / "one.arrow", {"x": [10, 20]})
    df = RowIdx.read_ipc(
        str(tmp_path / "one.arrow"),
        trusted_mode="shape_only",
        row_index_name="idx",
        row_index_offset=0,
    )
    d = df.to_dict()
    assert d["idx"] == [0, 1]
    assert d["x"] == [10, 20]


def test_read_ipc_include_file_paths(tmp_path) -> None:
    export_ipc(tmp_path / "one.arrow", {"x": [1]})
    df = WithPath.read_ipc(
        str(tmp_path / "one.arrow"),
        trusted_mode="shape_only",
        include_file_paths="path",
    )
    d = df.to_dict()
    assert "path" in d
    assert len(d["path"]) == 1
    assert str(tmp_path / "one.arrow") in d["path"][0] or d["path"][0].endswith(
        "one.arrow"
    )


def test_read_ipc_row_index_offset_without_name_errors(tmp_path) -> None:
    export_ipc(tmp_path / "x.arrow", {"x": [1]})
    with pytest.raises(ValueError, match="row_index_offset requires row_index_name"):
        RowIdx.read_ipc(
            str(tmp_path / "x.arrow"),
            trusted_mode="shape_only",
            row_index_offset=1,
        ).to_dict()


def test_read_ipc_glob_false_single_file(tmp_path) -> None:
    export_ipc(tmp_path / "one.arrow", {"x": [1], "y": [2]})
    df = XY.read_ipc(
        str(tmp_path / "one.arrow"),
        trusted_mode="shape_only",
        glob=False,
    )
    d = df.to_dict()
    assert d["x"] == [1]
    assert d["y"] == [2]
