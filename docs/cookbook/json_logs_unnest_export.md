# JSON Lines logs: read → unnest → write NDJSON

Append-only **NDJSON** (one JSON object per line) is a common log and CDC shape. This recipe uses a **lazy** scan so transforms run on Polars before materialization, then **unnests** a nested struct field for flat columns, and writes **NDJSON** with **`write_ndjson`** (lazy pipeline sink).

## Recipe

The runnable script lives in the repository at `docs/examples/cookbook/json_logs_unnest_export.py` (same code as below).


--8<-- "examples/cookbook/json_logs_unnest_export.py"

### Example output

From the repository root, with the extension built:

```bash
PYTHONPATH=python python docs/examples/cookbook/json_logs_unnest_export.py
```

```text
json_logs_unnest_export: ok
```

## Notes

- **Lazy read / write:** `read_ndjson` keeps a scan root until `collect` / `write_ndjson` / other terminals (see [EXECUTION](/EXECUTION.md) and [IO_JSON](/IO_JSON.md)).
- **Unnest naming:** columns become `meta_region`, `meta_code`, … per [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md).
- **Selectors:** to pick all struct columns before unnesting, use `s.structs()` as in [SELECTORS](/SELECTORS.md) (**Nested structs**).
- **Egress:** this recipe uses **`write_ndjson`** ([IO_NDJSON](/IO_NDJSON.md)). For a single JSON **array** file, use **`DataFrameModel.export_json`** (eager column dict → file; see [IO_JSON](/IO_JSON.md)).

## See also

[IO_JSON](/IO_JSON.md) · [IO_NDJSON](/IO_NDJSON.md) · [SELECTORS](/SELECTORS.md) · [CHANGELOG](/CHANGELOG.md) (**1.10.0**)
