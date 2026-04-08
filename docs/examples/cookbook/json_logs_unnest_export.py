"""Cookbook: NDJSON logs → filter → unnest → lazy ``write_ndjson`` (see RTD cookbook).

Run from repo root::

    PYTHONPATH=python python docs/examples/cookbook/json_logs_unnest_export.py

Needs ``pydantable._core``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel, Schema


class Meta(Schema):
    """Nested object carried on each log line."""

    region: str
    code: int


class LogLine(DataFrameModel):
    """One NDJSON object per line."""

    event: str
    meta: Meta


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "events.ndjson"
        src.write_text(
            '{"event":"ping","meta":{"region":"us","code":1}}\n'
            '{"event":"pong","meta":{"region":"eu","code":2}}\n',
            encoding="utf-8",
        )

        df = LogLine.read_ndjson(str(src))
        us_only = df.filter(df.meta.struct_field("region") == "us")
        # PlanFrame-first API: unnest requires explicit field list.
        flat = us_only.unnest("meta", fields=["region", "code"])
        out_path = Path(tmp) / "flat.ndjson"
        flat.write_ndjson(str(out_path))

        text = out_path.read_text(encoding="utf-8")
        assert "us" in text and ("meta_region" in text or "region" in text)

    print("json_logs_unnest_export: ok")


if __name__ == "__main__":
    main()
