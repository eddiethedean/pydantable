"""NDJSON lazy read and eager materialize (needs ``pydantable._core``).

Run::

    python docs/examples/io/ndjson_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrame
from pydantable.io import export_ndjson, materialize_ndjson
from pydantic import BaseModel


class Row(BaseModel):
    a: int
    b: str


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "rows.ndjson"
        path.write_text(
            '{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n',
            encoding="utf-8",
        )

        df = DataFrame[Row].read_ndjson(str(path))
        rows = df.collect()
        assert [r.a for r in rows] == [1, 2]
        assert [r.b for r in rows] == ["x", "y"]

        path2 = Path(td) / "round.ndjson"
        export_ndjson(path2, {"a": [3], "b": ["z"]})
        got = materialize_ndjson(path2)
        assert got["a"] == [3]
        assert got["b"] == ["z"]

    print("ndjson_roundtrip: ok")


if __name__ == "__main__":
    main()
