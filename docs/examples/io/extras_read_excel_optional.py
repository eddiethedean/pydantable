"""Optional ``read_excel`` demo (``pydantable[excel]`` / openpyxl). Always exits 0.

``read_excel`` lives in :mod:`pydantable.io.extras` (no ``DataFrameModel`` shim yet).

Run::

    python docs/examples/io/extras_read_excel_optional.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path


def main() -> None:
    try:
        from openpyxl import Workbook
    except ImportError:
        print("extras_read_excel: skip (pip install 'pydantable[excel]')")
        return

    from pydantable import DataFrameModel
    from pydantable.io.extras import read_excel

    class RegionalHeadcount(DataFrameModel):
        """HR export: one row per region (integers avoid Excel float quirks in asserts)."""

        region_code: int
        headcount: int

    with tempfile.TemporaryDirectory() as td:
        xlsx = Path(td) / "FY25_Q1_headcount_by_region.xlsx"
        wb = Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["region_code", "headcount"])
        ws.append([10, 42])
        wb.save(xlsx)
        wb.close()

        got = RegionalHeadcount(read_excel(xlsx, experimental=True))
        d = got.to_dict()
        assert [int(x) for x in d["region_code"]] == [10]
        assert [int(x) for x in d["headcount"]] == [42]

    print("extras_read_excel: ok")


if __name__ == "__main__":
    main()
