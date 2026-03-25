# Parquet I/O

**Module:** `pydantable.io` · **Typed frames:** `DataFrame[Schema].read_parquet`, `read_parquet_url`, `write_parquet` (and `DataFrameModel` equivalents).

## Read (sources)

### Lazy local file → `ScanFileRoot`

- **`read_parquet(path, *, columns=None, **scan_kwargs)`**
- **`aread_parquet(...)`** — same, via `asyncio.to_thread` (optional **`executor=`**).

Use when you want a **Polars lazy scan** inside the Rust plan without loading the file into Python lists. Optional **`columns`** limits projected fields.

**`scan_kwargs`** (Polars scan): for example **`n_rows`**, **`low_memory`**, **`rechunk`**, **`use_statistics`**, **`cache`**, **`glob`**, **`allow_missing_columns`**, **`parallel`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### HTTP(S) → temp file → lazy root

- **`read_parquet_url(url, *, experimental=True, columns=None, **kwargs)`**
- **`aread_parquet_url(...)`**

**`kwargs`** are passed to **`fetch_bytes`** (e.g. **`headers`**, **`timeout`**), **not** to the Parquet scanner. There is **no** `scan_kwargs` on this path today; the file is scanned with defaults after download.

The temp **`.parquet`** file is **not** deleted automatically. Delete it after **`write_*`**, **`collect()`**, etc. Details: {doc}`DATA_IO_SOURCES` (**`read_parquet_url` temp-file lifecycle**).

### Eager `dict[str, list]`

- **`materialize_parquet(source, *, columns=None, engine=None)`**
- **`amaterialize_parquet(...)`**

**`engine`:** **`"auto"`** (default) uses Rust for **local file paths** when **`columns is None`**; otherwise PyArrow. **`"rust"`** / **`"pyarrow"`** force one implementation. Override default with env **`PYDANTABLE_IO_ENGINE`**.

- **`fetch_parquet_url(url, *, experimental=True, columns=None, **kwargs)`** — download and decode in one step (PyArrow on bytes).

## Write (targets)

### Lazy plan → file (Rust / Polars)

- **`DataFrame[Schema].write_parquet(path, *, compression=None, write_kwargs=None)`**

**`write_kwargs`** may include **`compression`**, **`row_group_size`**, **`data_page_size`**, **`statistics`**, **`parallel`** (see {doc}`DATA_IO_SOURCES`). Unknown keys raise **`ValueError`**.

### Eager column dict → file

- **`export_parquet(path, data, *, engine=None)`**
- **`aexport_parquet(...)`**

Uses Rust when available, else **`pyarrow.parquet`** (install **`pydantable[arrow]`** for the fallback path).

## Runnable examples

Run conventions: see {doc}`IO_OVERVIEW` (**Runnable example**). Scripts live under **`docs/examples/io/`**.

**Eager round-trip and lazy filter** — `overview_roundtrip.py` (also embedded on {doc}`IO_OVERVIEW`).

**Lazy read → lazy write with `write_kwargs`**:

```bash
python docs/examples/io/parquet_lazy_roundtrip.py
```

```{literalinclude} examples/io/parquet_lazy_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`EXECUTION` · {doc}`DATA_IO_SOURCES`
