from __future__ import annotations

from pathlib import Path

import pytest

from pydantable.io import (
    iter_csv,
    iter_ndjson,
    write_csv_batches,
    write_ndjson_batches,
)


def test_csv_iter_and_write_batches_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.csv"
    src.write_text("a,b\n1,x\n2,y\n3,z\n", encoding="utf-8")
    out = tmp_path / "out.csv"
    write_csv_batches(out, iter_csv(src, batch_size=2), mode="w")
    # Round-trip through the iterator again (values remain strings).
    batches = list(iter_csv(out, batch_size=10))
    assert len(batches) == 1
    assert batches[0]["a"] == ["1", "2", "3"]
    assert batches[0]["b"] == ["x", "y", "z"]


def test_ndjson_iter_and_write_batches_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.ndjson"
    src.write_text('{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n{"a":3,"b":"z"}\n', encoding="utf-8")
    out = tmp_path / "out.ndjson"
    write_ndjson_batches(out, iter_ndjson(src, batch_size=2), mode="w")
    batches = list(iter_ndjson(out, batch_size=10))
    assert len(batches) == 1
    assert batches[0]["a"] == [1, 2, 3]
    assert batches[0]["b"] == ["x", "y", "z"]


def test_parquet_iter_and_write_batches(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    from pydantable.io import iter_parquet, write_parquet_batches

    table = pa.Table.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    src = tmp_path / "src.parquet"
    pq.write_table(table, src)

    out = tmp_path / "out.parquet"
    write_parquet_batches(out, iter_parquet(src, batch_size=2), compression=None)
    batches = list(iter_parquet(out, batch_size=10))
    assert len(batches) == 1
    assert batches[0]["a"] == [1, 2, 3]
    assert batches[0]["b"] == ["x", "y", "z"]

