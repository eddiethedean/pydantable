"""NDJSON lazy read and eager materialize (needs ``pydantable._core``).

Run::

    python docs/examples/io/ndjson_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Row(DataFrameModel):
    a: int
    b: str


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "rows.ndjson"
        path.write_text(
            '{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n',
            encoding="utf-8",
        )

        df = Row.read_ndjson(str(path))
        rows = df.collect()
        assert [r.a for r in rows] == [1, 2]
        assert [r.b for r in rows] == ["x", "y"]

        path2 = Path(td) / "round.ndjson"
        Row({"a": [3], "b": ["z"]}).write_ndjson(str(path2))
        got = Row.materialize_ndjson(path2)
        assert got.to_dict()["a"] == [3]
        assert got.to_dict()["b"] == ["z"]

    print("ndjson_roundtrip: ok")


if __name__ == "__main__":
    main()
