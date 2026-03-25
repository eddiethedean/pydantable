"""Lazy CSV: ``separator`` scan kwarg and lazy ``write_csv``.

Needs ``pydantable._core``. Run::

    python docs/examples/io/csv_lazy_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrame
from pydantable.io import materialize_csv
from pydantic import BaseModel


class Row(BaseModel):
    a: int
    b: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "semi.csv"
        dst = Path(td) / "out.csv"
        src.write_text("a;b\n1;2\n", encoding="utf-8")

        df = DataFrame[Row].read_csv(str(src), separator=";")
        df.write_csv(str(dst))

        got = materialize_csv(dst)
        assert [int(x) for x in got["a"]] == [1]
        assert [int(x) for x in got["b"]] == [2]

    print("csv_lazy_roundtrip: ok")


if __name__ == "__main__":
    main()
