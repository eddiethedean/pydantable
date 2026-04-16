# Parquet I/O

**Primary:** **`DataFrame[Schema].read_parquet`**, **`read_parquet_url`**, **`write_parquet`**, and **`DataFrameModel`** classmethods / instance methods below. **Secondary:** **`pydantable.io`** — same behavior without a typed frame (returns **`ScanFileRoot`** or **`dict[str, list]`** / writes from raw dicts).

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

**Lazy — local file**

- **`DataFrame[Schema].read_parquet(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_parquet(...)`** — classmethod; optional **`trusted_mode`** / validation kwargs (see [DATAFRAMEMODEL](/DATAFRAMEMODEL.md)).
- **`await MyModel.aread_parquet(..., executor=None)`**

**Lazy — HTTP(S) Parquet**

- **`DataFrame[Schema].read_parquet_url(url, *, experimental=True, columns=None, **kwargs)`** — **`kwargs`** go to **`fetch_bytes`**, not the Parquet scanner.
- **`MyModel.read_parquet_url(...)`** — same.
- **`DataFrameModel`** does **not** define **`aread_parquet_url`**; use **`pydantable.io.aread_parquet_url`** and **`DataFrame[Schema]`** if you need async HTTP.

**Eager — column dict in memory**

- **`materialize_parquet`** / **`await amaterialize_parquet`** from **`pydantable.io`**, then **`MyModel(cols, ...)`**
- **`DataFrame[Schema](cols)`** from any **`dict[str, list]`** (including **`materialize_parquet`** from **`pydantable.io`**).

The temp file for **`read_parquet_url`** is **not** deleted automatically; see [DATA_IO_SOURCES](/DATA_IO_SOURCES.md) (**`read_parquet_url` temp-file lifecycle**).

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
- **`write_parquet_batches(path, batches, *, compression=None)`** — append multiple batches as row groups in **one** Parquet file (not a dataset directory; see [IO_OVERVIEW](/IO_OVERVIEW.md) **Batched column dict I/O**).

**`scan_kwargs`:** for example **`n_rows`**, **`low_memory`**, **`rechunk`**, **`use_statistics`**, **`cache`**, **`glob`**, **`allow_missing_columns`**, **`parallel`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**. Unknown keys raise **`ValueError`**. See [DATA_IO_SOURCES](/DATA_IO_SOURCES.md).

### Paths, directories, and `glob`

Lazy **`read_parquet`** uses Polars **`scan_parquet`**; **`glob`** is forwarded via **`scan_kwargs`** (Polars **`ScanArgsParquet::default()`** uses **`glob: true`**). **Hive-style partitions** are tunable via **`hive_partitioning`** / **`hive_start_idx`** / **`try_parse_hive_dates`**; **`include_file_paths`** adds a source path column; **`row_index_name`** / **`row_index_offset`** add a row index. **`HiveOptions.schema`** (partition dtype overrides) and **`ScanArgsParquet.schema`** are still **not** exposed—see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`.

### Multi-file Parquet: columns, dtypes, and `allow_missing_columns`

When **`path`** is a **directory** or **glob**, Polars **unifies** the Parquet schemas across files into one **lazy** schema. If some files omit a column that appears in others, the scan can **fail** unless you set **`allow_missing_columns=True`** in **`scan_kwargs`** (forwarded to Polars **`ScanArgsParquet.allow_missing_columns`**). With **`allow_missing_columns=True`**, missing physical columns are typically filled with **null** for rows coming from files that do not define that column.

**Typed `DataFrameModel`:** Cell validation runs at **materialization** (**`to_dict()`**, **`collect()`**, …). Declare optional columns as **`T | None`** (or use **`Field(default=...)`**) when a column may be absent or null after the union. If the engine reports a **missing column** that is still optional in your model, pydantable may **retry** materialization after narrowing the plan—see [EXECUTION](/EXECUTION.md) and the **`_materialize_columns_with_missing_optional_fallback`** path in the implementation.

**Normalizing dtypes** after a heterogeneous dataset: use **`Expr.cast(...)`**, **`strptime`**, and related helpers so the plan matches your schema; see [SUPPORTED_TYPES](/SUPPORTED_TYPES.md) (**Cast** and **Type-specific `Expr` methods**). pydantable does **not** emit cross-file **schema drift warnings** by default; behavior follows Polars for the pinned version—see [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) (**Local lazy file scans**). For application-level checks (e.g. compare PyArrow file schemas before building a lazy plan), use your own code or [PLAN_AND_PLUGINS](/PLAN_AND_PLUGINS.md) (**`pydantable.observe`**).

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_parquet(path, *, compression=None, write_kwargs=None, streaming=..., partition_by=None, mkdir=True)`**
- **`model.write_parquet(...)`** — same.

**`write_kwargs`** may include **`compression`**, **`row_group_size`**, **`data_page_size`**, **`statistics`**, **`parallel`**. Unknown keys raise **`ValueError`**. See [DATA_IO_SOURCES](/DATA_IO_SOURCES.md).

### Partitioned (hive-style) Parquet output

When **`partition_by`** is a non-empty list of column names, **`path`** is the **dataset root directory** (not a single `*.parquet` file). The lazy plan is collected once, then rows are split with Polars **`partition_by_stable`**; each group is written under **`col=value/.../00000000.parquet`**, and **partition columns are omitted** from the data files (read back with **`read_parquet(..., hive_partitioning=True)`** as usual). String partition values are sanitized for path segments (`/` and `\` replaced). **`mkdir=True`** creates the root directory (and shard directories) as needed; use **`mkdir=False`** only when the root directory already exists. This path **materializes** the full result before sharding (same as a non-partitioned **`write_parquet`**); it is not a streaming multi-file Polars sink. A failed run may leave a **partial** dataset on disk—see [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) (**Writes**).

### `pydantable.io`

- **`export_parquet(path, data, *, engine=None)`**, **`aexport_parquet`** — eager **`dict[str, list]`** → file (Rust when available, else PyArrow with **`pydantable[arrow]`**).
- **`write_parquet_batches`** — many batches → one Parquet file (PyArrow; see [IO_OVERVIEW](/IO_OVERVIEW.md) batch section).

## Runnable examples

Run conventions: see [IO_OVERVIEW](/IO_OVERVIEW.md) (**Runnable example**). Scripts live under **`docs/examples/io/`**.

**Eager round-trip and lazy filter** — `overview_roundtrip.py` (also embedded on [IO_OVERVIEW](/IO_OVERVIEW.md)).

**Lazy read → lazy write with `write_kwargs`**:

```bash
python docs/examples/io/parquet_lazy_roundtrip.py
```


--8<-- "examples/io/parquet_lazy_roundtrip.py"

**Partitioned write → hive read**:

```bash
python docs/examples/io/parquet_partitioned_write.py
```


--8<-- "examples/io/parquet_partitioned_write.py"

## See also

[IO_OVERVIEW](/IO_OVERVIEW.md) · [EXECUTION](/EXECUTION.md) · [DATA_IO_SOURCES](/DATA_IO_SOURCES.md)
