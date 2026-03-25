"""Arrow IPC file: export, lazy read/write, materialize.

Needs ``pydantable._core``. Run::

    python docs/examples/io/ipc_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrame
from pydantable.io import export_ipc, materialize_ipc
from pydantic import BaseModel


class Row(BaseModel):
    z: int


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "in.arrow"
        dst = Path(td) / "out.arrow"
        export_ipc(src, {"z": [10, 20]})

        df = DataFrame[Row].read_ipc(str(src))
        df.write_ipc(str(dst))

        got = materialize_ipc(dst)
        assert [int(x) for x in got["z"]] == [10, 20]

    print("ipc_roundtrip: ok")


if __name__ == "__main__":
    main()
