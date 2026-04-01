# Parquet I/O

**Primary:** **`DataFrame[Schema].read_parquet`**, **`read_parquet_url`**, **`write_parquet`**, and **`DataFrameModel`** classmethods / instance methods below. **Secondary:** **`pydantable.io`** — same behavior without a typed frame (returns **`ScanFileRoot`** or **`dict[str, list]`** / writes from raw dicts).

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

**Lazy — local file**

- **`DataFrame[Schema].read_parquet(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_parquet(...)`** — classmethod; optional **`trusted_mode`** / validation kwargs (see {doc}`DATAFRAMEMODEL`).
- **`await MyModel.aread_parquet(..., executor=None)`**

**Lazy — HTTP(S) Parquet**

- **`DataFrame[Schema].read_parquet_url(url, *, experimental=True, columns=None, **kwargs)`** — **`kwargs`** go to **`fetch_bytes`**, not the Parquet scanner.
- **`MyModel.read_parquet_url(...)`** — same.
- **`DataFrameModel`** does **not** define **`aread_parquet_url`**; use **`pydantable.io.aread_parquet_url`** and **`DataFrame[Schema]`** if you need async HTTP.

**Eager — column dict in memory**

- **`materialize_parquet`** / **`await amaterialize_parquet`** from **`pydantable.io`**, then **`MyModel(cols, ...)`**
- **`DataFrame[Schema](cols)`** from any **`dict[str, list]`** (including **`materialize_parquet`** from **`pydantable.io`**).

The temp file for **`read_parquet_url`** is **not** deleted automatically; see {doc}`DATA_IO_SOURCES` (**`read_parquet_url` temp-file lifecycle**).

### `pydantable.io` (module functions)

**Lazy**

- **`read_parquet(path, *, columns=None, **scan_kwargs)`** → **`ScanFileRoot`**
- **`aread_parquet(...)`** — **`asyncio.to_thread`** (optional **`executor=`**)
- **`read_parquet_url`**, **`aread_parquet_url`**

**Eager**

- **`materialize_parquet(source, *, columns=None, engine=None)`**, **`amaterialize_parquet`**
- **`fetch_parquet_url`** — download and decode in one step (PyArrow on bytes)

**Batched (`dict[str, list]`, PyArrow)**

- **`iter_parquet(path, *, batch_size=..., columns=None)`** / **`aiter_parquet`** — yield rectangular column dicts (optional **`columns`** projection).
- **`write_parquet_batches(path, batches, *, compression=None)`** — append multiple batches as row groups in one file.

**`scan_kwargs`:** for example **`n_rows`**, **`low_memory`**, **`rechunk`**, **`use_statistics`**, **`cache`**, **`glob`**, **`allow_missing_columns`**, **`parallel`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Paths, directories, and `glob`

Lazy **`read_parquet`** uses Polars **`scan_parquet`**; **`glob`** is forwarded via **`scan_kwargs`** (Polars **`ScanArgsParquet::default()`** uses **`glob: true`**). **Hive-style partitions** are tunable via **`hive_partitioning`** / **`hive_start_idx`** / **`try_parse_hive_dates`**; **`include_file_paths`** adds a source path column; **`row_index_name`** / **`row_index_offset`** add a row index. **`HiveOptions.schema`** (partition dtype overrides) and **`ScanArgsParquet.schema`** are still **not** exposed—see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>` and {doc}`ROADMAP_1_11_LOCAL_IO` Phase B1+.

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_parquet(path, *, compression=None, write_kwargs=None, streaming=...)`**
- **`model.write_parquet(...)`** — same.

**`write_kwargs`** may include **`compression`**, **`row_group_size`**, **`data_page_size`**, **`statistics`**, **`parallel`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### `pydantable.io`

- **`export_parquet(path, data, *, engine=None)`**, **`aexport_parquet`** — eager **`dict[str, list]`** → file (Rust when available, else PyArrow with **`pydantable[arrow]`**).
- **`write_parquet_batches`** — many batches → one Parquet file (PyArrow; see {doc}`IO_OVERVIEW` batch section).

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
