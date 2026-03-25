# CSV I/O

**Module:** `pydantable.io` · **Typed frames:** `DataFrame[Schema].read_csv`, `write_csv` (and `DataFrameModel` equivalents).

## Read (sources)

### Lazy local file → `ScanFileRoot`

- **`read_csv(path, *, columns=None, **scan_kwargs)`**
- **`aread_csv(...)`**

**`scan_kwargs`** map to Polars CSV scan options, for example **`separator`**, **`has_header`**, **`skip_rows`**, **`skip_lines`**, **`n_rows`**, **`infer_schema_length`**, **`ignore_errors`**, **`low_memory`**, **`rechunk`**, **`glob`**, **`cache`**, **`quote_char`**, **`eol_char`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Eager `dict[str, list]`

- **`materialize_csv(path, *, engine=None, use_rap=False)`**
- **`amaterialize_csv(...)`**

**`engine="auto"`** tries Rust, then falls back to the stdlib **`csv`** module. **`use_rap=True`** uses **`aread_csv_rap`** only when **no** asyncio event loop is running; inside async code, **`await aread_csv_rap(path)`** from **`pydantable.io.rap_support`** instead.

### HTTP(S)

- **`fetch_csv_url(url, *, experimental=True, **kwargs)`** — downloads to a temp file, prefers Rust CSV read, then stdlib fallback. Temp file is removed after read.

## Write (targets)

### Lazy plan → file

- **`DataFrame[Schema].write_csv(path, *, separator=None, compression=None, write_kwargs=None)`**

**`write_kwargs`** may include **`include_header`**, **`include_bom`**. Top-level **`separator`** / **`compression`** are forwarded where the Rust sink supports them. See {doc}`DATA_IO_SOURCES`.

### Eager column dict → file

- **`export_csv(path, data, *, engine=None)`**
- **`aexport_csv(...)`**

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/csv_lazy_roundtrip.py
```

```{literalinclude} examples/io/csv_lazy_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_HTTP` · {doc}`EXECUTION`
