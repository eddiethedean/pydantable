"""SQLite: streaming SQL batches with :func:`pydantable.io.iter_sql`.

Run::

    python docs/examples/io/sql_sqlite_streaming.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable.io import iter_sql
from sqlalchemy import create_engine, text


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        try:
            with eng.begin() as conn:
                conn.execute(text("CREATE TABLE t (n INTEGER NOT NULL)"))
                conn.execute(
                    text(
                        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000) "
                        "INSERT INTO t SELECT x FROM seq"
                    )
                )

            total = 0
            for batch in iter_sql(
                "SELECT n FROM t WHERE n >= :min_n ORDER BY n",
                eng,
                parameters={"min_n": 10},
                batch_size=128,
            ):
                # Each batch is a dict[str, list] — process and discard incrementally.
                total += len(batch["n"])
            assert total == 991
        finally:
            eng.dispose()

    print("sql_sqlite_streaming: ok")


if __name__ == "__main__":
    main()

