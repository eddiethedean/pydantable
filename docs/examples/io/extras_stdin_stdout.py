"""CSV round-trip with lazy :meth:`~pydantable.dataframe_model.DataFrameModel.read_csv`
+ :meth:`~pydantable.dataframe.DataFrame.to_dict` and ``write_csv``.

For **true** stdin/stdout streaming helpers, see :doc:`IO_EXTRAS` (optional extras).

Run::

    python docs/examples/io/extras_stdin_stdout.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Shipment(DataFrameModel):
    """Inbound CSV from a carrier (numeric codes in text-heavy exports)."""

    order_id: int
    carton_id: int


class ShipmentStr(DataFrameModel):
    """Same layout written back out as strings (labels / IDs)."""

    order_id: str
    carton_id: str


def main() -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write("order_id,carton_id\n44021,90001\n")
        path = f.name
    try:
        d = Shipment.read_csv(path).to_dict()
        assert [int(x) for x in d["order_id"]] == [44021]
        assert [int(x) for x in d["carton_id"]] == [90001]
    finally:
        os.unlink(path)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as out:
        out_path = out.name
    try:
        ShipmentStr({"order_id": ["44021"], "carton_id": ["90001"]}).write_csv(out_path)
        body = Path(out_path).read_text(encoding="utf-8")
        assert "order_id" in body and "44021" in body
    finally:
        os.unlink(out_path)

    print("extras_stdin_stdout: ok")


if __name__ == "__main__":
    main()
