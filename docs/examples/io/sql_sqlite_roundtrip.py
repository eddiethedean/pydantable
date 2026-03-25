"""SQLite: ``DataFrameModel.fetch_sql`` and :func:`pydantable.io.write_sql`.

``write_sql`` is only on :mod:`pydantable.io` (no instance/class shim yet).

Run::

    python docs/examples/io/sql_sqlite_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import write_sql
from sqlalchemy import create_engine, text


class Tbl(DataFrameModel):
    n: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        with eng.begin() as conn:
            conn.execute(text("CREATE TABLE t (n INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (5)"))

        got = Tbl.fetch_sql("SELECT n FROM t", eng)
        assert got.to_dict() == {"n": [5]}

        write_sql({"n": [6]}, "t", eng, if_exists="append")
        got2 = Tbl.fetch_sql("SELECT n FROM t ORDER BY n", eng)
        assert got2.to_dict()["n"] == [5, 6]

    print("sql_sqlite_roundtrip: ok")


if __name__ == "__main__":
    main()
