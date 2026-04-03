"""Phase D1: hive-style partitioned Parquet writes via ``DataFrame.write_parquet``."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("pydantable_native._core")

from pydantable import DataFrameModel


class HivePart(DataFrameModel):
    p: str
    x: int


def test_write_parquet_partition_by_hive_layout(tmp_path) -> None:
    root = tmp_path / "dataset"
    df = HivePart({"p": ["a", "a", "b"], "x": [1, 2, 3]})
    df.write_parquet(str(root), partition_by=["p"])
    assert (root / "p=a" / "00000000.parquet").is_file()
    assert (root / "p=b" / "00000000.parquet").is_file()
    back = HivePart.read_parquet(
        str(root),
        trusted_mode="shape_only",
        glob=True,
        hive_partitioning=True,
    )
    d = back.to_dict()
    assert sorted(zip(d["p"], d["x"], strict=True)) == [("a", 1), ("a", 2), ("b", 3)]


def test_write_parquet_partition_by_requires_directory_not_file(tmp_path) -> None:
    f = tmp_path / "single.parquet"
    f.write_bytes(b"not parquet")
    df = HivePart({"p": ["x"], "x": [1]})
    with pytest.raises(
        ValueError, match="partition_by requires path to be a directory"
    ):
        df.write_parquet(str(f), partition_by=["p"])


def test_write_parquet_partition_unknown_column(tmp_path) -> None:
    df = HivePart({"p": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="unknown column"):
        df.write_parquet(str(tmp_path / "ds"), partition_by=["missing"])


def test_write_parquet_mkdir_false_missing_root(tmp_path) -> None:
    root = tmp_path / "nope" / "ds"
    df = HivePart({"p": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="mkdir=False"):
        df.write_parquet(str(root), partition_by=["p"], mkdir=False)


def test_write_parquet_unknown_write_kw_raises(tmp_path) -> None:
    path = tmp_path / "out.parquet"
    df = HivePart({"p": ["a"], "x": [1]})
    with pytest.raises(ValueError, match=re.escape("unknown write_kw key")):
        df.write_parquet(
            str(path),
            write_kwargs={"not_a_valid_parquet_write_kw": True},
        )
