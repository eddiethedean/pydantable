"""Lazy multi-file Parquet: write shard files, then one lazy scan with ``glob=True``.

Uses :meth:`~pydantable.dataframe_model.DataFrameModel.read_parquet` with a directory
path so Polars concatenates shards under a single :class:`~polars.LazyFrame` plan until
:meth:`~pydantable.dataframe.DataFrame.to_dict`.

Needs ``pydantable._core`` and PyArrow. Run::

    python docs/examples/io/iter_glob_parquet_batches.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Row(DataFrameModel):
    """Small demo schema shared across shard files."""

    n: int


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        Row({"n": [0, 1]}).write_parquet(str(base / "part_0.parquet"))
        Row({"n": [2]}).write_parquet(str(base / "part_1.parquet"))

        lazy = Row.read_parquet(str(base), trusted_mode="shape_only", glob=True)
        merged = lazy.to_dict()
        assert merged["n"] == [0, 1, 2]

    print("iter_glob_parquet_batches: ok")


if __name__ == "__main__":
    main()
