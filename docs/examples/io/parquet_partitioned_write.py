"""Hive-style partitioned Parquet: ``write_parquet(..., partition_by=...)`` → read with ``hive_partitioning``.

Needs ``pydantable._core``. Run::

    python docs/examples/io/parquet_partitioned_write.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Event(DataFrameModel):
    """Demo fact table with a string partition key."""

    region: str
    n: int


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "events"
        df = Event({"region": ["east", "east", "west"], "n": [1, 2, 3]})
        df.write_parquet(str(root), partition_by=["region"])
        assert (root / "region=east" / "00000000.parquet").is_file()
        assert (root / "region=west" / "00000000.parquet").is_file()
        back = Event.read_parquet(
            str(root),
            trusted_mode="shape_only",
            glob=True,
            hive_partitioning=True,
        )
        got = sorted(zip(back.to_dict()["region"], back.to_dict()["n"], strict=True))
        assert got == [("east", 1), ("east", 2), ("west", 3)]

    print("parquet_partitioned_write: ok")


if __name__ == "__main__":
    main()
