"""SQLite: ``fetch_sql`` + model, and ``write_sql`` (:mod:`pydantable.io`).

Run::

    python docs/examples/io/sql_sqlite_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import fetch_sql, write_sql
from sqlalchemy import create_engine, text


class OrderLine(DataFrameModel):
    """Row shape matching ``order_lines`` in the app database."""

    line_total_cents: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        try:
            with eng.begin() as conn:
                conn.execute(
                    text("CREATE TABLE order_lines (line_total_cents INTEGER NOT NULL)")
                )
                conn.execute(text("INSERT INTO order_lines VALUES (4999)"))

            got = OrderLine(fetch_sql("SELECT line_total_cents FROM order_lines", eng))
            assert got.to_dict() == {"line_total_cents": [4999]}

            write_sql(
                {"line_total_cents": [12_50]},
                "order_lines",
                eng,
                if_exists="append",
            )
            sql = "SELECT line_total_cents FROM order_lines ORDER BY line_total_cents"
            got2 = OrderLine(fetch_sql(sql, eng))
            assert got2.to_dict()["line_total_cents"] == [12_50, 4999]
        finally:
            # Windows file locks can block tempdir cleanup unless handles are released.
            eng.dispose()

    print("sql_sqlite_roundtrip: ok")


if __name__ == "__main__":
    main()
