"""SQLite via SQLAlchemy: ``fetch_sql`` / ``write_sql`` (needs ``pydantable[sql]``).

Run::

    python docs/examples/io/sql_sqlite_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable.io import fetch_sql, write_sql
from sqlalchemy import create_engine, text


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        with eng.begin() as conn:
            conn.execute(text("CREATE TABLE t (n INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (5)"))

        got = fetch_sql("SELECT n FROM t", eng)
        assert got == {"n": [5]}

        write_sql({"n": [6]}, "t", eng, if_exists="append")
        got2 = fetch_sql("SELECT n FROM t ORDER BY n", eng)
        assert got2["n"] == [5, 6]

    print("sql_sqlite_roundtrip: ok")


if __name__ == "__main__":
    main()
