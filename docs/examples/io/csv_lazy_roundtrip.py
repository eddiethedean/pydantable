"""Lazy CSV: European-style ``;`` separator (common ERP exports) and lazy ``write_csv``.

Needs ``pydantable._core``. Run::

    python docs/examples/io/csv_lazy_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import materialize_csv


class InventorySnapshot(DataFrameModel):
    """SKU-level stock row from a vendor CSV (semicolon-delimited)."""

    sku: int
    qty_on_hand: int


def main() -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        # Many EU exports use ';' because ',' is the decimal separator in locale.
        erp_export = Path(data_dir) / "stock_export.csv"
        normalized = Path(data_dir) / "stock_utf8_comma.csv"
        erp_export.write_text("sku;qty_on_hand\n1001;42\n", encoding="utf-8")

        df = InventorySnapshot.read_csv(str(erp_export), separator=";")
        df.write_csv(str(normalized))

        got = InventorySnapshot(materialize_csv(normalized))
        d = got.to_dict()
        assert [int(x) for x in d["sku"]] == [1001]
        assert [int(x) for x in d["qty_on_hand"]] == [42]

    print("csv_lazy_roundtrip: ok")


if __name__ == "__main__":
    main()
