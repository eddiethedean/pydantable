"""Lazy Parquet overview: export → read → filter → collect (needs ``pydantable._core``).

Run from the repository root::

    python docs/examples/io/overview_roundtrip.py

With a source checkout and no editable install, use::

    PYTHONPATH=python python docs/examples/io/overview_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrame
from pydantable.io import export_parquet, materialize_parquet
from pydantic import BaseModel


class Row(BaseModel):
    x: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "data.parquet"
        export_parquet(path, {"x": [1, 2, 3]})

        df = DataFrame[Row].read_parquet(str(path))
        filtered = df.filter(df.x > 1)
        rows = filtered.collect()
        assert [r.x for r in rows] == [2, 3]

        eager = materialize_parquet(path)
        assert eager["x"] == [1, 2, 3]

    print("overview_roundtrip: ok")


if __name__ == "__main__":
    main()
