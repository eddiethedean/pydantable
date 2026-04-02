"""SQLite: streaming batches with :func:`pydantable.io.iter_sqlmodel`.

Mirrors the raw-SQL streaming example using a **SQLModel** ``table=True`` model.
Install **pydantable[sql]**.

Run::

    python docs/examples/io/sql_sqlite_sqlmodel_streaming.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable.io import iter_sqlmodel, write_sqlmodel
from sqlalchemy import create_engine
from sqlmodel import Field, SQLModel


class Row(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    n: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        try:
            n = 500
            write_sqlmodel(
                {"id": list(range(1, n + 1)), "n": list(range(1, n + 1))},
                Row,
                eng,
                if_exists="replace",
                replace_ok=True,
            )
            total = 0
            for batch in iter_sqlmodel(
                Row,
                eng,
                where=Row.n >= 10,
                order_by=[Row.n],
                batch_size=100,
            ):
                total += len(batch["n"])
            assert total == n - 9
        finally:
            eng.dispose()

    print("sql_sqlite_sqlmodel_streaming: ok")


if __name__ == "__main__":
    main()
