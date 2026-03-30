"""Arrow IPC: hand off a columnar batch between processes (Feather/IPC on disk).

Needs ``pydantable._core``. Run::

    python docs/examples/io/ipc_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel
from pydantable.io import materialize_ipc


class SensorReading(DataFrameModel):
    """Two samples from a batch job writing IPC for a downstream consumer."""

    sensor_id: int
    celsius: int


def main() -> None:
    with tempfile.TemporaryDirectory() as scratch:
        from_worker = Path(scratch) / "batch_17.arrow"
        to_consumer = Path(scratch) / "batch_17_copy.arrow"
        SensorReading({"sensor_id": [1, 2], "celsius": [21, 22]}).write_ipc(
            str(from_worker)
        )

        df = SensorReading.read_ipc(str(from_worker))
        df.write_ipc(str(to_consumer))

        got = SensorReading(materialize_ipc(to_consumer))
        assert [int(x) for x in got.to_dict()["sensor_id"]] == [1, 2]
        assert [int(x) for x in got.to_dict()["celsius"]] == [21, 22]

    print("ipc_roundtrip: ok")


if __name__ == "__main__":
    main()
