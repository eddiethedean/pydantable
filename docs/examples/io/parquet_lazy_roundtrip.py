"""Lazy Parquet read/write with ``write_kwargs`` (needs ``pydantable._core``).

Run::

    python docs/examples/io/parquet_lazy_roundtrip.py
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
        src = Path(td) / "in.parquet"
        dst = Path(td) / "out.parquet"
        export_parquet(src, {"x": [7]})

        df = DataFrame[Row].read_parquet(str(src))
        df.write_parquet(str(dst), write_kwargs={"compression": "snappy"})

        got = materialize_parquet(dst)
        assert got["x"] == [7]

    print("parquet_lazy_roundtrip: ok")


if __name__ == "__main__":
    main()
