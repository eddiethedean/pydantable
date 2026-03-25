# NDJSON I/O (newline-delimited JSON)

**Module:** `pydantable.io` · **Typed frames:** `DataFrame[Schema].read_ndjson`, `write_ndjson` (and `DataFrameModel` equivalents).

Each line of the file is one JSON object; the scanner infers or aligns columns across lines.

## Read (sources)

### Lazy local file → `ScanFileRoot`

- **`read_ndjson(path, *, columns=None, **scan_kwargs)`**
- **`aread_ndjson(...)`**

**`scan_kwargs`** include **`low_memory`**, **`rechunk`**, **`ignore_errors`**, **`n_rows`**, **`infer_schema_length`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Eager `dict[str, list]`

- **`materialize_ndjson(path, *, engine=None)`**
- **`amaterialize_ndjson(...)`**

**`engine="auto"`** tries Rust, then a small pure-Python line parser.

### HTTP(S)

- **`fetch_ndjson_url(url, *, experimental=True, **kwargs)`** — download to a temp file, then Rust NDJSON read; temp file removed after read.

## Write (targets)

### Lazy plan → file

- **`DataFrame[Schema].write_ndjson(path, *, write_kwargs=None)`**

**`write_kwargs`** may include **`json_format`** (**`"lines"`** or **`"json"`**). See {doc}`DATA_IO_SOURCES`.

### Eager column dict → file

- **`export_ndjson(path, data, *, engine=None)`**
- **`aexport_ndjson(...)`**

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
