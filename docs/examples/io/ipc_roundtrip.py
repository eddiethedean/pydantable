"""Arrow IPC file: write, lazy read/write, materialize.

Needs ``pydantable._core``. Run::

    python docs/examples/io/ipc_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class Row(DataFrameModel):
    z: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "in.arrow"
        dst = Path(td) / "out.arrow"
        Row({"z": [10, 20]}).write_ipc(str(src))

        df = Row.read_ipc(str(src))
        df.write_ipc(str(dst))

        got = Row.materialize_ipc(dst)
        assert [int(x) for x in got.to_dict()["z"]] == [10, 20]

    print("ipc_roundtrip: ok")


if __name__ == "__main__":
    main()
