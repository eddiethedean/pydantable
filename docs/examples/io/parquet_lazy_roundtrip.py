"""Lazy Parquet: snapshot in → rewrite with ``write_kwargs`` (Snappy) → materialize.

Typical for archiving daily aggregates: read yesterday's file, optionally re-write with
explicit compression for downstream consumers.

Needs ``pydantable._core``. Run::

    python docs/examples/io/parquet_lazy_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class DailyRevenue(DataFrameModel):
    """Single row per region/day in a finance mart."""

    revenue_cents: int


def main() -> None:
    with tempfile.TemporaryDirectory() as archive:
        incoming = Path(archive) / "revenue_2025-03-24.parquet"
        outgoing = Path(archive) / "revenue_2025-03-24_snappy.parquet"
        # $1.25M for the day, stored as integer cents (finance systems often do this).
        DailyRevenue({"revenue_cents": [125_000_000]}).write_parquet(str(incoming))

        df = DailyRevenue.read_parquet(str(incoming))
        df.write_parquet(str(outgoing), write_kwargs={"compression": "snappy"})

        got = DailyRevenue.materialize_parquet(outgoing)
        assert got.to_dict()["revenue_cents"] == [125_000_000]

    print("parquet_lazy_roundtrip: ok")


if __name__ == "__main__":
    main()
