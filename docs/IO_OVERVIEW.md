# Data I/O by format (overview)

**Primary API:** **`DataFrame[Schema]`** and **`DataFrameModel`** — instance and classmethods for lazy reads, eager materialization, and lazy writes (Rust-backed where documented). **Secondary:** **`pydantable.io`** module functions — same operations without a typed frame (return **`ScanFileRoot`**, **`dict[str, list]`**, or write from raw column dicts).

For **execution semantics** (lazy vs collect, Rust engine), see {doc}`EXECUTION`. For **roadmap-style** “what to support next,” see {doc}`DATA_IO_SOURCES`.

## Primary API: `DataFrame` and `DataFrameModel`

| What | `DataFrame[Schema]` | `DataFrameModel` subclass |
|------|------------------------|----------------------------|
| **Lazy local file** | **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`** | **`MyModel.read_*`** — classmethods; same **`**scan_kwargs`** |
| **Lazy Parquet URL** | **`read_parquet_url`** | **`MyModel.read_parquet_url`** — **`**kwargs`** for **`fetch_bytes`** only |
| **Async lazy reads** | Use **`pydantable.io.aread_*`** and wrap, or **`DataFrame`** from root — **DataFrameModel** has **`aread_parquet`**, **`aread_csv`**, **`aread_ndjson`**, **`aread_ipc`** (optional **`executor=`**). **No** **`aread_parquet_url`** on the model — use **`io.aread_parquet_url`** + **`DataFrame`** if needed. |
| **Eager reads** | Constructor **`DataFrame[Schema](cols)`** from **`dict[str, list]`** | **`materialize_*`**, **`fetch_sql`**, **`amaterialize_*`**, **`afetch_sql`** classmethods |
| **Lazy writes** | **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** | Same **instance** methods on **`model`** — **`streaming`**, **`write_kwargs`**, etc. |

Optional validation on eager paths: **`trusted_mode`**, **`ignore_errors`**, **`on_validation_errors`** (see {doc}`DATAFRAMEMODEL`).

**Not on the model:** **`export_*`**, **`write_sql`**, **`awrite_sql`**, and **`pydantable.io.extras`** — call those module functions with a **`dict[str, list]`**, or build **`DataFrame` / `DataFrameModel`** from the result.

## Module functions (`pydantable.io`)

Use these when you want **`ScanFileRoot`**, raw column dicts, or file I/O without constructing **`DataFrame[Schema]`** first (e.g. scripts, tests, or glue code).

| Layer | Role |
|-------|------|
| **`read_*` / `aread_*`** | Lazy **local file** scan → **`ScanFileRoot`** → Polars **`LazyFrame`** in the Rust plan (no full column lists in Python). |
| **`read_parquet_url` / `aread_parquet_url`** | HTTP(S) download to a **temp Parquet file**, then same lazy root (you must delete the temp path when done). |
| **`materialize_*` / `amaterialize_*`** | Eager **`dict[str, list]`** (Rust and/or PyArrow / stdlib, depending on path). |
| **`fetch_*_url`**, **`fetch_sql`**, **`read_from_object_store`**, **`pydantable.io.extras`** | Other sources that return **`dict[str, list]`** (or build column dicts) for **`DataFrameModel`** / constructors. |
| **`export_*` / `aexport_*`**, **`write_sql` / `awrite_sql`** | Eager writes from an in-memory column dict or your own pipeline. |

## One page per source or target family

| Topic | Guide |
|-------|--------|
| **Parquet** (files, URLs, lazy write) | {doc}`IO_PARQUET` |
| **CSV** | {doc}`IO_CSV` |
| **NDJSON** (newline-delimited JSON) | {doc}`IO_NDJSON` |
| **Arrow IPC / Feather file** | {doc}`IO_IPC` |
| **HTTP(S), object stores** | {doc}`IO_HTTP` |
| **SQL** (SQLAlchemy) | {doc}`IO_SQL` |
| **Excel, Delta, Avro, ORC, cloud warehouses, Kafka, stdin/stdout** | {doc}`IO_EXTRAS` |

## Runnable example

From the repository root, with **`pydantable._core`** built (**`maturin develop`**, **`pip install -e .`**, or a wheel):

```bash
python docs/examples/io/overview_roundtrip.py
```

From a source tree without installing the package, set **`PYTHONPATH=python`** (path to the **`python/`** directory that contains **`pydantable`**).

```{literalinclude} examples/io/overview_roundtrip.py
:language: python
```

## Lazy **`scan_kwargs`** and sink **`write_kwargs`**

Optional Polars scan/write options are accepted as **`**scan_kwargs`** on lazy file reads and **`write_kwargs={...}`** on lazy file writes (same on **`DataFrame`** / **`DataFrameModel`** and on **`pydantable.io`** lazy paths). Allowed keys are validated in Rust; unknown keys raise **`ValueError`**. The full matrix lives in {doc}`DATA_IO_SOURCES` (**Lazy read `**scan_kwargs` and write `write_kwargs`**).
