"""Large NDJSON: lazy scan vs chunked ``iter_ndjson`` (Phase D patterns).

Demonstrates (1) typed lazy ``read_ndjson`` → filter → ``collect()`` and
(2) batched ``iter_ndjson`` without a full ``LazyFrame`` plan.

Needs ``pydantable._core``. Run::

    python docs/examples/io/large_ndjson_patterns.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import iter_ndjson


class LogLine(DataFrameModel):
    """One JSON object per line."""

    level: str
    msg: str


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "app.ndjson"
        path.write_text(
            '{"level":"info","msg":"start"}\n'
            '{"level":"error","msg":"bad"}\n'
            '{"level":"info","msg":"done"}\n',
            encoding="utf-8",
        )

        # Lazy: transforms stay on Polars LazyFrame until collect.
        df = LogLine.read_ndjson(str(path))
        errs = df.filter(df.level == "error")
        rows = errs.collect()
        assert len(rows) == 1
        assert rows[0].msg == "bad"

        # Chunked eager batches: bounded dict[str, list] slices (no lazy plan).
        batches = list(iter_ndjson(str(path), batch_size=2))
        assert len(batches) == 2
        assert batches[0]["level"] == ["info", "error"]

    print("large_ndjson_patterns: ok")


if __name__ == "__main__":
    main()
