"""NDJSON: append-only API / audit log → lazy scan; round-trip via ``write_ndjson``.

Each line is one JSON object (common for log shipping and CDC-style exports).

Needs ``pydantable._core``. Run::

    python docs/examples/io/ndjson_roundtrip.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel


class ApiAccessEvent(DataFrameModel):
    """One request line from an edge log (NDJSON)."""

    status: int
    path: str


def main() -> None:
    with tempfile.TemporaryDirectory() as logs:
        access_log = Path(logs) / "access-20250325.ndjson"
        access_log.write_text(
            '{"status": 200, "path": "/v1/health"}\n'
            '{"status": 404, "path": "/v1/missing"}\n',
            encoding="utf-8",
        )

        df = ApiAccessEvent.read_ndjson(str(access_log))
        rows = df.collect()
        assert [r.status for r in rows] == [200, 404]
        assert [r.path for r in rows] == ["/v1/health", "/v1/missing"]

        replay = Path(logs) / "replay.ndjson"
        ApiAccessEvent({"status": [500], "path": ["/v1/checkout"]}).write_ndjson(
            str(replay)
        )
        got = ApiAccessEvent.materialize_ndjson(replay)
        assert got.to_dict() == {"status": [500], "path": ["/v1/checkout"]}

    print("ndjson_roundtrip: ok")


if __name__ == "__main__":
    main()
