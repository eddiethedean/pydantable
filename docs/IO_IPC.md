# Arrow IPC / Feather file I/O

**Primary:** **`DataFrame[Schema].read_ipc`**, **`write_ipc`**, and **`DataFrameModel`** methods. **Secondary:** **`pydantable.io`**.

This covers **Arrow IPC file** (`.arrow` / `.feather`-style single file), not arbitrary **streaming IPC** on a socket unless you materialize through PyArrow yourself.

**Batch iterators / writers (1.5.0+):** **`iter_ipc`** and **`write_ipc_batches`** take **`as_stream=`**. Use the **same** value on read and write: on-disk IPC **file** format is **`as_stream=False`**; IPC **stream** bytes are **`as_stream=True`** (the **`write_ipc_batches`** default). See {doc}`IO_OVERVIEW` (**Batched column dict I/O**).

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

- **`DataFrame[Schema].read_ipc(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_ipc(...)`**, **`await MyModel.aread_ipc(..., executor=None)`**
- **`materialize_ipc`**, **`await amaterialize_ipc`** from **`pydantable.io`**, then **`MyModel(cols)`** — **`as_stream`**, **`engine`**

### `pydantable.io`

- **`read_ipc`**, **`aread_ipc`**
- **`materialize_ipc`**, **`amaterialize_ipc`**

**`scan_kwargs`:** forwarded to **`IpcScanOptions`** (**`record_batch_statistics`**) and **`UnifiedScanArgs`** (**`glob`**, **`cache`**, **`rechunk`**, **`n_rows`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**). Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Paths, directories, and multi-file

Lazy **`read_ipc`** uses Polars **`LazyFrame::scan_ipc`** with pydantable-built **`IpcScanOptions`** and **`UnifiedScanArgs`** (defaults match Polars **`Default`**: **`glob: true`**, hive options **enabled**). Tune **`glob`** / hive / lineage kwargs like other **`read_*`** roots—see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`.

**`as_stream=False`** (default): local file paths can use Rust; otherwise PyArrow. **`as_stream=True`** uses PyArrow stream decoding. Install **`pydantable[arrow]`** when the path goes through PyArrow.

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_ipc(path, *, compression=..., write_kwargs=..., streaming=...)`**
- **`model.write_ipc(...)`**

IPC sink options are intentionally narrow: use top-level **`compression=`**. **Non-empty `write_kwargs`** is rejected.

### `pydantable.io`

- **`export_ipc`**, **`aexport_ipc`**
- **`iter_ipc`**, **`aiter_ipc`**, **`write_ipc_batches`** — rectangular **`dict[str, list]`** batches (PyArrow); **`as_stream`** must match how the bytes were produced.

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/ipc_roundtrip.py
```

```{literalinclude} examples/io/ipc_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`EXECUTION` · {doc}`SUPPORTED_TYPES` (Arrow interchange)
