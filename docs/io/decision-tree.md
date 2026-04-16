# Choosing an I/O API

Use this page to pick the right entry point. Execution semantics (lazy collect vs streaming) are in [EXECUTION](/user-guide/execution/).

## Quick reference

| Goal | Use | Returns / effect |
|------|-----|-------------------|
| **Scan a local file without loading full columns into Python** | **`MyModel.read_*`** or **`DataFrame[Schema].read_*`** (default) | **`ScanFileRoot`** → lazy plan |
| **Run transforms on a big file, then write without a giant dict** | **`MyModel.read_*`** → … → **`DataFrame.write_parquet`** (or **`write_*`**) | File on disk |
| **Load everything eagerly into a typed frame** | **`pydantable.io.materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`await afetch_sqlmodel`** / **`await afetch_sql_raw`**, then **`MyModel(cols)`** | **`DataFrameModel`** / **`DataFrame`** |
| **Raw `dict[str, list]` only** (utilities) | **`pydantable.io.materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`fetch_*_url`**, **`read_from_object_store`** | Column dict |
| **Persist columns to a file** | **`MyModel.export_*`** / **`await MyModel.aexport_*`** | File on disk |
| **Query or load from a database** | **`fetch_sqlmodel`** / **`fetch_sql_raw`** (async: **`afetch_*`**) / **`iter_sqlmodel`** / **`iter_sql_raw`** + **`MyModel(...)`**; **`MyModel.fetch_sqlmodel`** / **`write_sqlmodel`** (deprecated unprefixed SQL names: [IO_SQL](/io/sql/)) | Typed frame or none |
| **Lazy typed transforms on SQL** (plans compiled to SQL; swap-in engine) | **`SqlDataFrame`** / **`SqlDataFrameModel`** with **`sql_config=`** / **`sql_engine=`** — install **`pydantable[sql]`** ([SQL_ENGINE](/integrations/engines/sql/)) | Lazy **`DataFrame`** / **`DataFrameModel`** |
| **Lazy typed transforms on a MongoDB collection** (swap-in engine) | **Sync DB:** **`MongoDataFrame[Schema].from_beanie(Doc, database=sync_db)`** or **`from_collection(coll)`** ([MONGO_ENGINE](/integrations/engines/mongo/)). **Async-only lazy (no sync client):** **`from_beanie_async(Doc, …)`** or **`from_beanie_async(prebuilt_query)`** ([BEANIE](/integrations/engines/beanie/)). | Lazy **`DataFrame`** / **`DataFrameModel`** |
| **Eager MongoDB column dict** (read / write **`dict[str, list]`**, no **`DataFrame`**) | **Driver-level:** **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** (sync **`Collection`**) or **`await afetch_mongo`** / **`await aiter_mongo`** / **`await awrite_mongo`** (**`AsyncCollection`** uses native async — **`is_async_mongo_collection`**, [MONGO_ENGINE](/integrations/engines/mongo/)). **ODM-aware:** **`afetch_beanie`** / **`awrite_beanie`** ([BEANIE](/integrations/engines/beanie/)). **`pydantable[mongo]`** ships **PyMongo** + **Beanie**. | Column dict batches or insert count |
| **HTTP(S) download** | **`fetch_*_url`** (eager dicts) or **`MyModel.read_parquet_url`** / **`await MyModel.aread_parquet_url`** (lazy temp file — [IO_HTTP](/io/http/)) | Varies |
| **Object-store URIs (`s3://`, …)** | **`read_from_object_store`** (**`[cloud]`**) | Column dict |
| **Tier-2 readers (Excel, Delta, …)** | **`pydantable.io.extras`** | Column dict or helpers |

**Module reference:** every symbol also exists on **`pydantable.io`**; see [IO_OVERVIEW](/io/overview/) (**Module reference**).

## Multi-file, directories, and globs

| Goal | Use | Notes |
|------|-----|--------|
| **Typed lazy pipeline** over a **directory**, **glob**, or **hive-style dataset** | **`MyModel.read_*`** / **`DataFrame[Schema].read_*`** with **`**scan_kwargs`** (e.g. **`glob`** for Parquet/CSV) | Preferred for large data; scanning is delegated to Polars—see {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`. |
| **Eager `dict[str, list]`** from **multiple files** | **`materialize_*`** per file in a loop, or **defer** to lazy **`read_*`** | **`materialize_*`** is oriented to **single sources**; multi-file concat is often clearer as a lazy **`read_*`** then **`to_dict()`**. |
| **Bounded-memory Python batches** without a **`DataFrame`** plan | **`iter_*` / `aiter_*`** over **one path per call** | Expand globs in Python, then one **`iter_*`** per file (or **`iter_chain_batches`**); for many files, lazy **`read_*`** is often simpler—see [IO_OVERVIEW](/io/overview/) (**Batched column dict I/O** → **Multi-file paths, globs, and memory**) and **`docs/examples/io/iter_glob_parquet_batches.py`**. |
| **Write a hive-style partitioned Parquet dataset** (directory of **`col=value/...`** shards) | **`DataFrame.write_parquet`** / **`DataFrameModel.write_parquet`** with **`partition_by`** | **`path`** is the dataset root; see [IO_PARQUET](/io/parquet/) (**Partitioned (hive-style) Parquet output**). For a **single** Parquet file from **`dict[str, list]`** batches, use **`export_parquet`** or **`write_parquet_batches`**. |
| **Parquet files disagree on column names** (multi-file / glob scan) | **`read_parquet(..., allow_missing_columns=True, ...)`** ( **`scan_kwargs`** ) + optional **`Expr.cast`** / optional model fields | Polars unions schemas; missing columns become null when allowed—see [IO_PARQUET](/io/parquet/) (**Multi-file Parquet: columns, dtypes, and `allow_missing_columns`**). |

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

- **Default:** **`DataFrameModel`** / **`DataFrame[Schema]`** classmethods (**`read_*`**, **`materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, …) for **validation** and **typed** **`Expr`** pipelines.
- **`pydantable.io`** only when you need untyped **`dict[str, list]`**, **`ScanFileRoot`** without wrapping, or **`extras`** — scripts, tests, notebooks, internal glue.

See [DATAFRAMEMODEL](/user-guide/dataframemodel/) and [IO_OVERVIEW](/io/overview/).
