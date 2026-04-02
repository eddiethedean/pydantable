"""Batched reads over several Parquet files: glob, then ``iter_parquet`` per file.

``iter_parquet`` takes **one** path per call. For multiple files, expand paths in
Python (``Path.glob``, ``glob.glob``, or a list) and iterate each file—or use
:func:`pydantable.io.iter_chain_batches` to chain per-file iterators.

For **lazy** multi-file concat, use :func:`pydantable.io.read_parquet` with
``glob=True`` and :meth:`~pydantable.dataframe.DataFrame.to_dict`.

Needs ``pydantable._core`` and PyArrow. Run::

    python docs/examples/io/iter_glob_parquet_batches.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import export_parquet, iter_chain_batches, iter_parquet


class Row(DataFrameModel):
    """Small demo schema shared across shard files."""

    n: int


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        export_parquet(base / "part_0.parquet", {"n": [0, 1]})
        export_parquet(base / "part_1.parquet", {"n": [2]})

        paths = sorted(base.glob("part_*.parquet"))
        batch_count_loop = sum(1 for _ in iter_chain_batches(paths, iter_parquet))
        batch_count_explicit = sum(1 for p in paths for _ in iter_parquet(p))
        assert batch_count_loop == batch_count_explicit

        lazy = Row.read_parquet(str(base), trusted_mode="shape_only", glob=True)
        merged = lazy.to_dict()
        assert merged["n"] == [0, 1, 2]

    print("iter_glob_parquet_batches: ok")


if __name__ == "__main__":
    main()
