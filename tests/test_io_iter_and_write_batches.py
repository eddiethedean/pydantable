from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest

from pydantable.io import (
    aiter_csv,
    iter_csv,
    iter_json_array,
    iter_json_lines,
    iter_ndjson,
    write_csv_batches,
    write_ndjson_batches,
)
from pydantable.io.batches import ensure_rectangular, iter_concat_batches


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


def test_parquet_iter_respects_columns_and_batch_size(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    from pydantable.io import iter_parquet

    table = pa.Table.from_pydict(
        {"a": [1, 2, 3, 4, 5], "b": ["u", "v", "w", "x", "y"], "c": [0.1, 0.2, 0.3, 0.4, 0.5]}
    )
    src = tmp_path / "wide.parquet"
    pq.write_table(table, src)

    batches = list(iter_parquet(src, batch_size=2, columns=["a", "b"]))
    assert len(batches) == 3
    assert all(set(b) == {"a", "b"} for b in batches)
    assert [b["a"] for b in batches] == [[1, 2], [3, 4], [5]]
    assert sum(len(b["b"]) for b in batches) == 5


def test_ipc_stream_roundtrip_matches_flags(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    from pydantable.io import iter_ipc, write_ipc_batches

    path = tmp_path / "s.arrow"
    batches_in = [{"a": [1, 2], "b": ["x", "y"]}, {"a": [3], "b": ["z"]}]
    write_ipc_batches(path, iter(batches_in), as_stream=True)
    read_back = list(iter_ipc(path, batch_size=65536, as_stream=True))
    merged = iter_concat_batches(iter(read_back))
    assert merged["a"] == [1, 2, 3]
    assert merged["b"] == ["x", "y", "z"]

    path_file = tmp_path / "f.arrow"
    write_ipc_batches(path_file, iter(batches_in), as_stream=False)
    read_file = list(iter_ipc(path_file, as_stream=False))
    assert iter_concat_batches(iter(read_file)) == merged


def test_ipc_file_vs_stream_mismatch_is_not_auto_detected(tmp_path: Path) -> None:
    """Readers must pass as_stream= consistently with how the file was written."""
    pytest.importorskip("pyarrow")
    from pydantable.io import iter_ipc, write_ipc_batches

    path = tmp_path / "only_stream.arrow"
    write_ipc_batches(path, iter([{"x": [1]}]), as_stream=True)
    with pytest.raises(Exception):
        list(iter_ipc(path, as_stream=False))


def test_json_array_roundtrip_and_validation(tmp_path: Path) -> None:
    path = tmp_path / "arr.json"
    path.write_text(
        json.dumps([{"k": 1, "s": "a"}, {"k": 2, "s": "b"}, {"k": 3}]),
        encoding="utf-8",
    )
    batches = list(iter_json_array(path, batch_size=2))
    assert len(batches) == 2
    assert batches[0]["s"] == ["a", "b"]
    # Last chunk is only the final object; keys are unioned within the batch (here just ``k``).
    assert batches[1] == {"k": [3]}

    bad = tmp_path / "not_array.json"
    bad.write_text('{"k":1}', encoding="utf-8")
    with pytest.raises(ValueError, match="top-level array"):
        list(iter_json_array(bad))


def test_iter_json_lines_alias_matches_ndjson(tmp_path: Path) -> None:
    p = tmp_path / "x.ndjson"
    p.write_text('{"a":1}\n{"a":2}\n', encoding="utf-8")
    assert list(iter_json_lines(p, batch_size=10)) == list(iter_ndjson(p, batch_size=10))


def test_csv_and_ndjson_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        next(iter(iter_csv(StringIO("a\n1"), batch_size=0)))
    with pytest.raises(ValueError, match="batch_size"):
        next(iter(iter_ndjson(StringIO("{}\n"), batch_size=-1)))


def test_ndjson_rejects_non_object_lines() -> None:
    with pytest.raises(ValueError, match="JSON objects"):
        list(iter_ndjson(StringIO("[1,2]\n"), batch_size=10))


def test_csv_short_rows_and_header_only(tmp_path: Path) -> None:
    p = tmp_path / "short.csv"
    p.write_text("a,b\n1\n2,extra\n", encoding="utf-8")
    batch = next(iter_csv(p, batch_size=10))
    assert batch["a"] == ["1", "2"]
    assert batch["b"] == [None, "extra"]

    empty_cols = tmp_path / "headers_only.csv"
    empty_cols.write_text("x,y\n", encoding="utf-8")
    assert list(iter_csv(empty_cols)) == []

    totally_empty = tmp_path / "empty.csv"
    totally_empty.write_text("", encoding="utf-8")
    assert list(iter_csv(totally_empty)) == []


def test_write_csv_batches_append_and_invalid_mode(tmp_path: Path) -> None:
    base = tmp_path / "a.csv"
    write_csv_batches(base, iter([{"u": [1], "v": ["a"]}]), mode="w")
    write_csv_batches(base, iter([{"u": [2], "v": ["b"]}]), mode="a", write_header=False)
    text = base.read_text(encoding="utf-8")
    lines = [ln for ln in text.strip().split("\n") if ln]
    assert lines == ["u,v", "1,a", "2,b"]

    with pytest.raises(ValueError, match="mode"):
        write_csv_batches(base, iter([{"u": [1]}]), mode="wb")  # type: ignore[arg-type]


def test_write_csv_batches_skips_empty_batches(tmp_path: Path) -> None:
    out = tmp_path / "b.csv"

    def gen() -> object:
        yield {"a": []}  # skipped
        yield {"a": [1, 2]}

    write_csv_batches(out, gen(), mode="w")
    rows = list(iter_csv(out, batch_size=10))
    assert rows[0]["a"] == ["1", "2"]


def test_ensure_rectangular_and_concat() -> None:
    assert ensure_rectangular({}) == {}
    ensure_rectangular({"a": [1], "b": [2]})

    with pytest.raises(ValueError, match="same length"):
        ensure_rectangular({"a": [1], "b": [1, 2]})

    # Column keys are fixed from the first batch; extra keys in later batches are ignored.
    merged = iter_concat_batches(iter([{"x": [1, 2]}, {"x": [3], "y": ["a"]}]))
    assert merged == {"x": [1, 2, 3]}

    missing_second_col = iter_concat_batches(
        iter([{"x": [1, 2], "y": [10, 20]}, {"x": [3]}])
    )
    assert missing_second_col["x"] == [1, 2, 3]
    assert missing_second_col["y"] == [10, 20]

    assert iter_concat_batches(iter(())) == {}


@pytest.mark.asyncio
async def test_aiter_csv_matches_iter_csv(tmp_path: Path) -> None:
    p = tmp_path / "c.csv"
    p.write_text("a,b\n1,x\n2,y\n", encoding="utf-8")
    sync_batches = list(iter_csv(p, batch_size=1))
    async_batches = []
    async for b in aiter_csv(p, batch_size=1):
        async_batches.append(b)
    assert async_batches == sync_batches

