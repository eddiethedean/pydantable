# NDJSON I/O (newline-delimited JSON)

**Primary:** **`DataFrame[Schema].read_ndjson`**, **`write_ndjson`**, and **`DataFrameModel`** methods. **Secondary:** **`pydantable.io`**.

Each line of the file is one JSON object; the scanner infers or aligns columns across lines.

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

- **`DataFrame[Schema].read_ndjson(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_ndjson(...)`**, **`await MyModel.aread_ndjson(..., executor=None)`**
- **`materialize_ndjson`**, **`await amaterialize_ndjson`** from **`pydantable.io`**, then **`MyModel(cols)`**

### `pydantable.io`

- **`read_ndjson`**, **`aread_ndjson`**
- **`materialize_ndjson`**, **`amaterialize_ndjson`**
- **`fetch_ndjson_url`** — HTTP(S) → temp file → read
- **`iter_ndjson`**, **`iter_json_lines`** (alias), **`aiter_ndjson`**, **`aiter_json_lines`**, **`write_ndjson_batches`** — JSON-object lines batched into **`dict[str, list]`** ({doc}`IO_OVERVIEW`).

**`scan_kwargs`:** **`low_memory`**, **`rechunk`**, **`ignore_errors`**, **`n_rows`**, **`infer_schema_length`**, **`glob`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Paths, directories, and `glob`

Use **`glob=True`** (or omit it) when reading a **directory** or a **glob pattern** so your call matches **Parquet** / **CSV** lazy reads. Polars **0.53** builds NDJSON lazy scans with **`UnifiedScanArgs { glob: true, … }`** internally; **glob expansion cannot be disabled** from the **`LazyJsonLineReader`** API. Passing **`glob=False`** raises **`ValueError`** from pydantable.

**Hive-style partitions** are **disabled** for NDJSON in Polars **0.53** (no partition columns from paths). A single glob such as **`*.jsonl`** only matches that extension; use another pattern or a second read for **`.ndjson`** files. Details: {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`.

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_ndjson(path, *, write_kwargs=..., streaming=...)`**
- **`model.write_ndjson(...)`**

**`write_kwargs`:** **`json_format`** (**`"lines"`** / **`"json"`**). See {doc}`DATA_IO_SOURCES`.

### `pydantable.io`

- **`export_ndjson`**, **`aexport_ndjson`**
- **`write_ndjson_batches`** — stream many batches to one NDJSON file.

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/ndjson_roundtrip.py
```

```{literalinclude} examples/io/ndjson_roundtrip.py
:language: python
```

Large-file patterns (lazy scan; optional **`iter_ndjson`** batches in {doc}`IO_JSON`): **`python docs/examples/io/large_ndjson_patterns.py`**.

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_HTTP` · {doc}`EXECUTION`
