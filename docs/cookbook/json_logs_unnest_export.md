# JSON Lines logs: read → unnest → write NDJSON

Append-only **NDJSON** (one JSON object per line) is a common log and CDC shape. This recipe uses a **lazy** scan so transforms run on Polars before materialization, then **unnests** a nested struct field for flat columns, and writes **NDJSON** with **`write_ndjson`** (lazy pipeline sink).

## Recipe

The runnable script lives in the repository at `docs/examples/cookbook/json_logs_unnest_export.py` (same code as below).

```{literalinclude} ../examples/cookbook/json_logs_unnest_export.py
:language: python
```

### Example output

From the repository root, with the extension built:

```bash
PYTHONPATH=python python docs/examples/cookbook/json_logs_unnest_export.py
```

```text
json_logs_unnest_export: ok
```

## Notes

- **Lazy read / write:** `read_ndjson` keeps a scan root until `collect` / `write_ndjson` / other terminals (see {doc}`/EXECUTION` and {doc}`/IO_JSON`).
- **Unnest naming:** columns become `meta_region`, `meta_code`, … per {doc}`/INTERFACE_CONTRACT`.
- **Egress:** this recipe uses **`write_ndjson`** ({doc}`/IO_NDJSON`). For a single JSON **array** file, use **`DataFrameModel.export_json`** (eager column dict → file; see {doc}`/IO_JSON`).

## See also

{doc}`/IO_JSON` · {doc}`/IO_NDJSON` · {doc}`/CHANGELOG` (**1.10.0**)
