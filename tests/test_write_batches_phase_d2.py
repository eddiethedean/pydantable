"""Phase D2: ``write_*_batches`` file vs directory and append semantics."""

from __future__ import annotations

from pathlib import Path

import pytest

from pydantable.io.write_batches import (
    write_csv_batches,
    write_ndjson_batches,
    write_parquet_batches,
)


def test_write_csv_batches_append_trunc(tmp_path: Path) -> None:
    p = tmp_path / "t.csv"
    write_csv_batches(p, [{"a": [1]}], mode="w")
    write_csv_batches(p, [{"a": [2]}], mode="a", write_header=False)
    text = p.read_text(encoding="utf-8")
    assert "1" in text and "2" in text


def test_write_ndjson_batches_append(tmp_path: Path) -> None:
    p = tmp_path / "t.jsonl"
    write_ndjson_batches(p, [{"a": [1]}], mode="w")
    write_ndjson_batches(p, [{"a": [2]}], mode="a")
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


@pytest.mark.parametrize(
    "writer",
    [write_parquet_batches, write_csv_batches, write_ndjson_batches],
)
def test_write_batches_rejects_existing_directory(tmp_path: Path, writer) -> None:
    if writer is write_parquet_batches:
        pytest.importorskip("pyarrow.parquet")
    d = tmp_path / "dir"
    d.mkdir()
    with pytest.raises(ValueError, match="not a directory"):
        writer(str(d), iter([{"a": [1]}]))
