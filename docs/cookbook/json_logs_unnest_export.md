# JSON Lines logs: read → unnest → export

Append-only **NDJSON** (one JSON object per line) is a common log and CDC shape. This recipe uses a **lazy** scan so transforms run on Polars before materialization, then **unnests** a nested struct field for flat columns, and writes **JSON** out again.

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

- **Lazy read:** `read_ndjson` keeps a scan root until `collect` / `write_*` / `export_*` materializes (see {doc}`/EXECUTION` and {doc}`/IO_JSON`).
- **Unnest naming:** columns become `meta_region`, `meta_code`, … per {doc}`/INTERFACE_CONTRACT`.
- **Selectors:** to pick all struct columns before unnesting, use `s.structs()` as in {doc}`/SELECTORS` (**Nested structs**).
- **Egress:** `export_json` writes one JSON **array** of row objects; use `DataFrame.write_ndjson` if you need **JSON Lines** output instead ({doc}`/IO_NDJSON`).

## See also

{doc}`/IO_JSON` · {doc}`/IO_NDJSON` · {doc}`/SELECTORS` · {doc}`/changelog` (**1.10.0**)
