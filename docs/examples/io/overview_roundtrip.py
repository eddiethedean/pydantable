"""Lazy Parquet: write order lines → read back → filter → collect (needs ``pydantable._core``).

Mirrors a common pipeline: land Parquet in a staging folder, scan lazily, filter, then
either iterate rows or materialize the full slice for tests/QA.

Run from the repository root::

    python docs/examples/io/overview_roundtrip.py

With a source checkout and no editable install, use::

    PYTHONPATH=python python docs/examples/io/overview_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class OrderLine(DataFrameModel):
    """Warehouse pick line: one SKU line on an order."""

    line_id: int
    quantity: int


def main() -> None:
    with tempfile.TemporaryDirectory() as staging:
        # e.g. ``s3://bucket/staging/2025-03-25/order_lines.parquet`` in production
        parquet_path = Path(staging) / "order_lines.parquet"
        OrderLine(
            {
                "line_id": [101, 102, 103],
                "quantity": [1, 4, 2],
            }
        ).write_parquet(str(parquet_path))

        df = OrderLine.read_parquet(str(parquet_path))
        # Multi-unit lines only (same idea as HAVING quantity > 1 in SQL)
        multi = df.filter(df.quantity > 1)
        rows = multi.collect()
        assert [r.line_id for r in rows] == [102, 103]
        assert [r.quantity for r in rows] == [4, 2]

        eager = OrderLine.materialize_parquet(parquet_path)
        assert eager.to_dict() == {
            "line_id": [101, 102, 103],
            "quantity": [1, 4, 2],
        }

    print("overview_roundtrip: ok")


if __name__ == "__main__":
    main()
