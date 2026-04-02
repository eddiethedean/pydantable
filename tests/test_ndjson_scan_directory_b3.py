"""Phase B3: NDJSON lazy scan — directory, glob, hive not applied, unknown kwargs."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable._core")

from pydantable import DataFrameModel
from pydantable.io import export_ndjson


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


def test_read_ndjson_directory_concatenates_files(tmp_path) -> None:
    export_ndjson(tmp_path / "a.jsonl", {"x": [1, 2], "y": [10, 20]})
    export_ndjson(tmp_path / "b.jsonl", {"x": [3], "y": [30]})
    df = XY.read_ndjson(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert d["x"] == [1, 2, 3]
    assert d["y"] == [10, 20, 30]


def test_read_ndjson_glob_star_jsonl(tmp_path) -> None:
    export_ndjson(tmp_path / "a.jsonl", {"x": [1], "y": [2]})
    export_ndjson(tmp_path / "b.jsonl", {"x": [3], "y": [4]})
    pattern = str(tmp_path / "*.jsonl")
    df = XY.read_ndjson(pattern, trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert sorted(zip(d["x"], d["y"], strict=True)) == [(1, 2), (3, 4)]


def test_read_ndjson_hive_style_path_does_not_add_partition_column(tmp_path) -> None:
    part = tmp_path / "p=hello"
    part.mkdir(parents=True)
    export_ndjson(part / "a.jsonl", {"x": [1]})
    df = XOnly.read_ndjson(str(tmp_path), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert "p" not in d
    assert d["x"] == [1]


def test_read_ndjson_unknown_scan_kw_raises(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match=re.escape("unknown scan_kw key")):
        XY.read_ndjson(
            str(tmp_path / "one.jsonl"),
            trusted_mode="shape_only",
            not_a_valid_ndjson_scan_kw=True,
        ).to_dict()


def test_read_ndjson_glob_false_raises(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match="glob=False is not supported"):
        XY.read_ndjson(
            str(tmp_path / "one.jsonl"),
            trusted_mode="shape_only",
            glob=False,
        ).to_dict()


def test_read_ndjson_row_index_name(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [10, 20]})
    df = RowIdx.read_ndjson(
        str(tmp_path / "one.jsonl"),
        trusted_mode="shape_only",
        row_index_name="idx",
        row_index_offset=0,
    )
    d = df.to_dict()
    assert d["idx"] == [0, 1]
    assert d["x"] == [10, 20]


def test_read_ndjson_include_file_paths(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [1]})
    df = WithPath.read_ndjson(
        str(tmp_path / "one.jsonl"),
        trusted_mode="shape_only",
        include_file_paths="path",
    )
    d = df.to_dict()
    assert "path" in d
    assert len(d["path"]) == 1
    assert str(tmp_path / "one.jsonl") in d["path"][0] or d["path"][0].endswith(
        "one.jsonl"
    )


def test_read_ndjson_row_index_offset_without_name_errors(tmp_path) -> None:
    export_ndjson(tmp_path / "x.jsonl", {"x": [1]})
    with pytest.raises(ValueError, match="row_index_offset requires row_index_name"):
        RowIdx.read_ndjson(
            str(tmp_path / "x.jsonl"),
            trusted_mode="shape_only",
            row_index_offset=1,
        ).to_dict()


def test_read_ndjson_mixed_extension_glob_jsonl_only_sees_jsonl(tmp_path) -> None:
    """``*.jsonl`` glob does not match ``.ndjson`` files (Polars expansion)."""
    export_ndjson(tmp_path / "a.jsonl", {"x": [1], "y": [2]})
    export_ndjson(tmp_path / "b.ndjson", {"x": [9], "y": [9]})
    df = XY.read_ndjson(str(tmp_path / "*.jsonl"), trusted_mode="shape_only", glob=True)
    d = df.to_dict()
    assert d["x"] == [1]
    assert d["y"] == [2]
