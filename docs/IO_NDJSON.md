# NDJSON I/O (newline-delimited JSON)

**Primary:** **`DataFrame[Schema].read_ndjson`**, **`write_ndjson`**, and **`DataFrameModel`** methods. **Secondary:** **`pydantable.io`**.

Each line of the file is one JSON object; the scanner infers or aligns columns across lines.

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

- **`DataFrame[Schema].read_ndjson(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_ndjson(...)`**, **`await MyModel.aread_ndjson(..., executor=None)`**
- **`MyModel.materialize_ndjson`**, **`await MyModel.amaterialize_ndjson`**

### `pydantable.io`

- **`read_ndjson`**, **`aread_ndjson`**
- **`materialize_ndjson`**, **`amaterialize_ndjson`**
- **`fetch_ndjson_url`** — HTTP(S) → temp file → read

**`scan_kwargs`:** **`low_memory`**, **`rechunk`**, **`ignore_errors`**, **`n_rows`**, **`infer_schema_length`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_ndjson(path, *, write_kwargs=..., streaming=...)`**
- **`model.write_ndjson(...)`**

**`write_kwargs`:** **`json_format`** (**`"lines"`** / **`"json"`**). See {doc}`DATA_IO_SOURCES`.

### `pydantable.io`

- **`export_ndjson`**, **`aexport_ndjson`**

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/ndjson_roundtrip.py
```

```{literalinclude} examples/io/ndjson_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_HTTP` · {doc}`EXECUTION`
