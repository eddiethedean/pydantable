"""Lazy Parquet read/write with ``write_kwargs`` (needs ``pydantable._core``).

Run::

    python docs/examples/io/parquet_lazy_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Row(DataFrameModel):
    x: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "in.parquet"
        dst = Path(td) / "out.parquet"
        Row({"x": [7]}).write_parquet(str(src))

        df = Row.read_parquet(str(src))
        df.write_parquet(str(dst), write_kwargs={"compression": "snappy"})

        got = Row.materialize_parquet(dst)
        assert got.to_dict()["x"] == [7]

    print("parquet_lazy_roundtrip: ok")


if __name__ == "__main__":
    main()
