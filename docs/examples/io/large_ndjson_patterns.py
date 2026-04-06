"""Large NDJSON: typed lazy ``read_ndjson`` → filter → ``collect()``.

Transforms stay on the Polars :class:`~polars.LazyFrame` until ``collect``/``to_dict``.

Needs ``pydantable._core``. Run::

    python docs/examples/io/large_ndjson_patterns.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


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
        errs = df.filter(df.col.level == "error")
        rows = errs.collect()
        assert len(rows) == 1
        assert rows[0].msg == "bad"

    print("large_ndjson_patterns: ok")


if __name__ == "__main__":
    main()
