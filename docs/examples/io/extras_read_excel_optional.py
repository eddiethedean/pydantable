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

    class Row(DataFrameModel):
        u: int
        v: int

    with tempfile.TemporaryDirectory() as td:
        xlsx = Path(td) / "book.xlsx"
        wb = Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["u", "v"])
        ws.append([1, 2])
        wb.save(xlsx)
        wb.close()

        got = Row(read_excel(xlsx, experimental=True))
        assert [int(x) for x in got.to_dict()["u"]] == [1]
        assert got.to_dict()["v"] == [2]

    print("extras_read_excel: ok")


if __name__ == "__main__":
    main()
