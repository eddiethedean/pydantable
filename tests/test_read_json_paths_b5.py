"""Phase B5: read_json paths match read_ndjson (JSON Lines lazy scan alias)."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable._core")

from pydantable import DataFrameModel
from pydantable.io import export_ndjson, read_json, read_ndjson


class XY(DataFrameModel):
    x: int
    y: int


def test_read_json_same_scan_root_as_read_ndjson(tmp_path) -> None:
    export_ndjson(tmp_path / "a.jsonl", {"x": [1], "y": [2]})
    p = str(tmp_path / "a.jsonl")
    r1 = read_ndjson(p)
    r2 = read_json(p)
    assert type(r1) is type(r2)
    assert getattr(r1, "path", None) == getattr(r2, "path", None)


def test_read_json_directory_matches_ndjson(tmp_path) -> None:
    export_ndjson(tmp_path / "a.jsonl", {"x": [1, 2], "y": [10, 20]})
    export_ndjson(tmp_path / "b.jsonl", {"x": [3], "y": [30]})
    j = XY.read_json(str(tmp_path), trusted_mode="shape_only", glob=True)
    n = XY.read_ndjson(str(tmp_path), trusted_mode="shape_only", glob=True)
    assert j.to_dict() == n.to_dict()
    assert j.to_dict()["x"] == [1, 2, 3]


def test_read_json_unknown_scan_kw_raises(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match=re.escape("unknown scan_kw key")):
        XY.read_json(
            str(tmp_path / "one.jsonl"),
            trusted_mode="shape_only",
            not_a_valid_json_scan_kw=True,
        ).to_dict()


def test_read_json_glob_false_raises(tmp_path) -> None:
    export_ndjson(tmp_path / "one.jsonl", {"x": [1], "y": [2]})
    with pytest.raises(ValueError, match="glob=False is not supported"):
        XY.read_json(
            str(tmp_path / "one.jsonl"),
            trusted_mode="shape_only",
            glob=False,
        ).to_dict()
