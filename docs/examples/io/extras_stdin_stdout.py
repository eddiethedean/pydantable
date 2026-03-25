"""CSV round-trip with ``DataFrameModel.materialize_csv`` / ``write_csv``.

For **true** stdin/stdout streaming helpers, use :mod:`pydantable.io.extras`.

Run::

    python docs/examples/io/extras_stdin_stdout.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Row(DataFrameModel):
    c: int
    d: int


class RowStr(DataFrameModel):
    c: str
    d: str


def main() -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write("c,d\n9,10\n")
        path = f.name
    try:
        tbl = Row.materialize_csv(path)
        d = tbl.to_dict()
        assert [int(x) for x in d["c"]] == [9]
        assert [int(x) for x in d["d"]] == [10]
    finally:
        os.unlink(path)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as out:
        out_path = out.name
    try:
        RowStr({"c": ["x"], "d": ["y"]}).write_csv(out_path)
        body = Path(out_path).read_text(encoding="utf-8")
        assert "c" in body and "x" in body
    finally:
        os.unlink(out_path)

    print("extras_stdin_stdout: ok")


if __name__ == "__main__":
    main()
