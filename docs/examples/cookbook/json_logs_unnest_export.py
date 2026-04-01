"""Cookbook: NDJSON logs → filter → unnest → ``export_json`` (see RTD cookbook).

Run from repo root::

    PYTHONPATH=python python docs/examples/cookbook/json_logs_unnest_export.py

Needs ``pydantable._core``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pydantable import DataFrameModel, Schema
from pydantable.io import export_json


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
        flat = us_only.unnest("meta")
        out_path = Path(tmp) / "flat.json"
        export_json(out_path, flat.collect(as_lists=True))

        text = out_path.read_text(encoding="utf-8")
        assert '"meta_region": "us"' in text or "meta_region" in text

    print("json_logs_unnest_export: ok")


if __name__ == "__main__":
    main()
