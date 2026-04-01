# JSON Lines logs: read → unnest → export

Append-only **NDJSON** (one JSON object per line) is a common log and CDC shape. This recipe uses a **lazy** scan so transforms run on Polars before materialization, then **unnests** a nested struct field for flat columns, and writes **JSON** out again.

## Recipe

```python
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
```

## Notes

- **Lazy read:** `read_ndjson` keeps a scan root until `collect` / `write_*` / `export_*` materializes (see {doc}`/EXECUTION` and {doc}`/IO_JSON`).
- **Unnest naming:** columns become `meta_region`, `meta_code`, … per {doc}`/INTERFACE_CONTRACT`.
- **Selectors:** to pick all struct columns before unnesting, use `s.structs()` as in {doc}`/SELECTORS` (**Nested structs**).
- **Egress:** `export_json` writes one JSON **array** of row objects; use `DataFrame.write_ndjson` if you need **JSON Lines** output instead ({doc}`/IO_NDJSON`).

## See also

{doc}`/IO_JSON` · {doc}`/IO_NDJSON` · {doc}`/SELECTORS` · {doc}`/ROADMAP_1_10_JSON_STRUCT`
