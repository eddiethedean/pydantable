"""``iter_chain_batches`` chains per-file batch iterators."""

from __future__ import annotations

from pathlib import Path

from pydantable.io import export_ndjson, iter_chain_batches, iter_ndjson


def test_iter_chain_batches_two_files(tmp_path: Path) -> None:
    export_ndjson(tmp_path / "a.jsonl", {"x": [1, 2]})
    export_ndjson(tmp_path / "b.jsonl", {"x": [3]})
    paths = sorted(tmp_path.glob("*.jsonl"))
    batches = list(iter_chain_batches(paths, iter_ndjson))
    assert len(batches) == 2
    assert batches[0]["x"] == [1, 2]
    assert batches[1]["x"] == [3]
