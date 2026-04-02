"""SQLite: SQLModel table + :class:`~pydantable.DataFrameModel` round-trip.

Uses :func:`pydantable.io.write_sqlmodel` / :func:`pydantable.io.fetch_sqlmodel` and
:class:`~pydantable.DataFrameModel` classmethods. Install **pydantable[sql]**.

Run::

    python docs/examples/io/sql_sqlite_sqlmodel_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import fetch_sqlmodel, write_sqlmodel
from sqlalchemy import create_engine
from sqlmodel import Field, SQLModel


class Widget(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    label: str


class WidgetDF(DataFrameModel):
    id: int | None
    label: str


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "app.db"
        eng = create_engine(f"sqlite:///{db}")
        try:
            write_sqlmodel(
                {"id": [1], "label": ["first"]},
                Widget,
                eng,
                if_exists="replace",
                replace_ok=True,
            )
            df = WidgetDF.fetch_sqlmodel(Widget, eng, order_by=[Widget.id])
            assert df.to_dict() == {"id": [1], "label": ["first"]}

            write_sqlmodel(
                {"id": [2], "label": ["second"]},
                Widget,
                eng,
                if_exists="append",
            )
            got = fetch_sqlmodel(Widget, eng, order_by=[Widget.id])
            assert got["label"] == ["first", "second"]
        finally:
            eng.dispose()

    print("sql_sqlite_sqlmodel_roundtrip: ok")


if __name__ == "__main__":
    main()
