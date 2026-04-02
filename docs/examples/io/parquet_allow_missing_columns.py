"""Multi-file Parquet: missing columns across files with ``allow_missing_columns=True``.

Two small Parquet files in a temp directory — one omits column ``y`` — then
``read_parquet(..., glob=True, allow_missing_columns=True)`` and ``to_dict()``.

Needs ``pydantable._core``. Run::

    python docs/examples/io/parquet_allow_missing_columns.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import export_parquet


class Row(DataFrameModel):
    x: int
    y: int | None


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        export_parquet(base / "a.parquet", {"x": [1], "y": [10]})
        export_parquet(base / "b.parquet", {"x": [2]})
        df = Row.read_parquet(
            str(base),
            trusted_mode="shape_only",
            glob=True,
            allow_missing_columns=True,
        )
        assert df.to_dict() == {"x": [1, 2], "y": [10, None]}
    print("parquet_allow_missing_columns: ok")


if __name__ == "__main__":
    main()
