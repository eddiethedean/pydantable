# Choosing an I/O API

Use this page to pick the right entry point. Execution semantics (lazy collect vs streaming) are in {doc}`EXECUTION`.

## Quick reference

| Goal | Use | Returns / effect |
|------|-----|-------------------|
| **Scan a local file without loading full columns into Python** | **`MyModel.read_*`** or **`DataFrame[Schema].read_*`** (default) | **`ScanFileRoot`** → lazy plan |
| **Run transforms on a big file, then write without a giant dict** | **`MyModel.read_*`** → … → **`DataFrame.write_parquet`** (or **`write_*`**) | File on disk |
| **Load everything eagerly into a typed frame** | **`pydantable.io.materialize_*`**, **`fetch_sql`**, **`await afetch_sql`**, then **`MyModel(cols)`** | **`DataFrameModel`** / **`DataFrame`** |
| **Raw `dict[str, list]` only** (utilities) | **`pydantable.io.materialize_*`**, **`fetch_sql`**, **`fetch_*_url`**, **`read_from_object_store`** | Column dict |
| **Persist columns to a file** | **`MyModel.export_*`** / **`await MyModel.aexport_*`** | File on disk |
| **Query or load from a database** | **`fetch_sql`** / **`await afetch_sql`** / **`iter_sql`** / **`aiter_sql`** + **`MyModel(...)`**; **`MyModel.write_sql`** / **`await MyModel.awrite_sql`** | Typed frame or none |
| **HTTP(S) download** | **`fetch_*_url`** (eager dicts) or **`MyModel.read_parquet_url`** / **`await MyModel.aread_parquet_url`** (lazy temp file — {doc}`IO_HTTP`) | Varies |
| **Object-store URIs (`s3://`, …)** | **`read_from_object_store`** (**`[cloud]`**) | Column dict |
| **Tier-2 readers (Excel, Delta, …)** | **`pydantable.io.extras`** | Column dict or helpers |

**Module reference:** every symbol also exists on **`pydantable.io`**; see {doc}`IO_OVERVIEW` (**Module reference**).

## Multi-file, directories, and globs

| Goal | Use | Notes |
|------|-----|--------|
| **Typed lazy pipeline** over a **directory**, **glob**, or **hive-style dataset** | **`MyModel.read_*`** / **`DataFrame[Schema].read_*`** with **`**scan_kwargs`** (e.g. **`glob`** for Parquet/CSV) | Preferred for large data; scanning is delegated to Polars—see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`. |
| **Eager `dict[str, list]`** from **multiple files** | **`materialize_*`** per file in a loop, or **defer** to lazy **`read_*`** | **`materialize_*`** is oriented to **single sources**; multi-file concat is often clearer as a lazy **`read_*`** then **`to_dict()`**. |
| **Bounded-memory Python batches** without a **`DataFrame`** plan | **`iter_*` / `aiter_*`** over **one path per call** | Expand globs in Python, then one **`iter_*`** per file (or **`iter_chain_batches`**); for many files, lazy **`read_*`** is often simpler—see {doc}`IO_OVERVIEW` (**Batched column dict I/O** → **Multi-file paths, globs, and memory**) and **`docs/examples/io/iter_glob_parquet_batches.py`**. |

## Engine selection (`materialize_parquet` and friends)

Set **`PYDANTABLE_IO_ENGINE`** to **`auto`** (default), **`rust`**, or **`pyarrow`** where supported.

### Parquet (`materialize_parquet`)

- **`auto` / `rust`:** local **file path** and **`columns is None`** → Rust fast path when the extension is built.
- **PyArrow path:** **`columns`** is set, or **`source`** is **`bytes`** / **`BinaryIO`**, or Rust fails and **`auto`** falls back.

### CSV / NDJSON (`materialize_csv`, `materialize_ndjson`)

- **`auto` / `rust`:** try Rust readers for a **local path**; on failure **`auto`** falls back to stdlib **`csv`** or JSON lines parsing in Python.

### IPC (`materialize_ipc`)

- **`auto` / `rust`:** local on-disk IPC **file** with **`as_stream=False`**; streams and buffers use PyArrow.

See implementation notes in **`pydantable.io`** module docstrings for edge cases.

## Typed frame first vs `pydantable.io` only

- **Default:** **`DataFrameModel`** / **`DataFrame[Schema]`** classmethods (**`read_*`**, **`materialize_*`**, **`fetch_sql`**, …) for **validation** and **typed** **`Expr`** pipelines.
- **`pydantable.io`** only when you need untyped **`dict[str, list]`**, **`ScanFileRoot`** without wrapping, or **`extras`** — scripts, tests, notebooks, internal glue.

See {doc}`DATAFRAMEMODEL` and {doc}`IO_OVERVIEW`.
