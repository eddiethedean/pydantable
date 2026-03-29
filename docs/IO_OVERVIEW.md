# Data I/O by format (overview)

**Primary API:** **`DataFrame[Schema]`** and **`DataFrameModel`** — instance and classmethods for lazy reads, eager materialization, and lazy writes (Rust-backed where documented). **Secondary:** **`pydantable.io`** module functions — same operations without a typed frame (return **`ScanFileRoot`**, **`dict[str, list]`**, or write from raw column dicts).

For **execution semantics** (lazy vs collect, Rust engine), see {doc}`EXECUTION`. For **roadmap-style** “what to support next,” see {doc}`DATA_IO_SOURCES`. **Which API should I call?** See {doc}`IO_DECISION_TREE`.

(full-api-in-pydantableio)=
## Full API in `pydantable.io`

The **`pydantable`** package re-exports a small subset of I/O (Parquet-focused lazy reads and common helpers). **Every** public I/O function lives under **`pydantable.io`** — use **`from pydantable.io import …`** for CSV/NDJSON/IPC **`materialize_*`**, **`export_*`**, **`fetch_*_url`**, **`write_sql`**, **`extras`**, and async mirrors.

## Primary API: `DataFrame` and `DataFrameModel`

| What | `DataFrame[Schema]` | `DataFrameModel` subclass |
|------|------------------------|----------------------------|
| **Lazy local file** | **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** | **`MyModel.read_*`** — classmethods; same **`**scan_kwargs`** |
| **Lazy Parquet URL** | **`read_parquet_url`** | **`MyModel.read_parquet_url`** — **`**kwargs`** for **`fetch_bytes`** only |
| **Temp Parquet URL cleanup** | Build frame inside **`read_parquet_url_ctx`** ( **`io`** ) or use **`DataFrameModel.read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** | Same — context managers unlink the download when the block exits ({doc}`IO_HTTP`) |
| **Async lazy reads** | **`DataFrame[Schema].aread_*`** (mirrors `DataFrameModel.aread_*`), or `await pydantable.io.aread_*` and build from `ScanFileRoot`. | **`aread_parquet`**, **`aread_csv`**, **`aread_ndjson`**, **`aread_ipc`**, **`aread_json`**, optional **`executor=`**; URL temp file without ctx: **`io.aread_parquet_url`**. |
| **Eager reads** | Constructor **`DataFrame[Schema](cols)`** from **`dict[str, list]`** | **`materialize_*`**, **`fetch_sql`**, **`amaterialize_*`**, **`afetch_sql`** classmethods |
| **Lazy writes** | **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** | Same **instance** methods on **`model`** — **`streaming`**, **`write_kwargs`**, etc. |

**Ingest validation options:** `trusted_mode`, `fill_missing_optional`, `ignore_errors`, `on_validation_errors` apply on **constructors** and on **typed lazy reads** (`DataFrame[Schema].read_*` / `aread_*`, `DataFrameModel.read_*` / `aread_*`) at **materialization time** (`to_dict()` / `collect()` / `to_arrow()` / `to_polars()`).

- `trusted_mode=None` / `"off"`: full per-cell validation (default).
- `ignore_errors=True` (only meaningful when `trusted_mode` is `"off"`): invalid rows are skipped and `on_validation_errors` receives one batch payload.
- `trusted_mode="shape_only"` / `"strict"`: skip per-cell validation (still enforces shape + nullability; `"strict"` adds dtype-compat checks). `ignore_errors` does not skip rows in these modes.
- `fill_missing_optional=True` (default): missing optional columns are filled with `None` during ingest/materialization.
- `fill_missing_optional=False`: missing optional columns/fields raise an error unless the schema field has an explicit default; explicit defaults are used in that case.

See {doc}`DATAFRAMEMODEL` for the detailed ingest contract.

**Glue I/O on `DataFrameModel`:** classmethods **`export_*`**, **`write_sql`** / **`awrite_sql`**, **`from_sql`** / **`afrom_sql`** delegate to **`pydantable.io`** (same signatures as the module functions). **`pydantable.io.extras`** remains module-only.

## Engine matrix (`materialize_*`)

**`PYDANTABLE_IO_ENGINE`:** **`auto`** (default), **`rust`**, or **`pyarrow`** where supported.

| Function | Rust path (typical) | PyArrow / fallback |
|----------|---------------------|------------------------|
| **`materialize_parquet`** | Local file path, **`columns is None`** | **`columns`** set, or **`bytes`** / **`BinaryIO`** source, or `auto` fallback |
| **`materialize_csv`** | Local path | stdlib **`csv`** if Rust fails under **`auto`** |
| **`materialize_ndjson`** | Local path | Python JSON lines if Rust fails under **`auto`** |
| **`materialize_ipc`** | Local IPC file, **`as_stream=False`** | Streams, **`as_stream=True`**, buffers |

Details: {doc}`IO_DECISION_TREE` (**Engine selection**).

## Module functions (`pydantable.io`)

Use these when you want **`ScanFileRoot`**, raw column dicts, or file I/O without constructing **`DataFrame[Schema]`** first (e.g. scripts, tests, or glue code).

| Layer | Role |
|-------|------|
| **`read_*` / `aread_*`** | Lazy **local file** scan → **`ScanFileRoot`** → Polars **`LazyFrame`** in the Rust plan (no full column lists in Python). |
| **`read_parquet_url` / `aread_parquet_url`** | HTTP(S) download to a **temp Parquet file**, then same lazy root — prefer **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** for automatic cleanup ({doc}`IO_HTTP`). |
| **`materialize_*` / `amaterialize_*`** | Eager **`dict[str, list]`** (Rust and/or PyArrow / stdlib, depending on path). |
| **`fetch_*_url`**, **`fetch_sql`**, **`read_from_object_store`**, **`pydantable.io.extras`** | Other sources that return **`dict[str, list]`** (or build column dicts) for **`DataFrameModel`** / constructors. |
| **`export_*` / `aexport_*`**, **`write_sql` / `awrite_sql`** | Eager writes from an in-memory column dict or your own pipeline. |

## Batched column dict I/O (`iter_*`, `write_*_batches`, `aiter_*`)

For **bounded memory** pipelines in plain Python (outside the Rust **`LazyFrame`** plan), **`pydantable.io`** exposes **iterators** that yield **`dict[str, list]`** in chunks, plus **writers** that append many such batches to one file.

- **Contract:** each yielded batch is **rectangular**: every column list has the same length. Helpers **`ensure_rectangular`** and **`iter_concat_batches`** live in **`pydantable.io.batches`**.
- **Core formats:** **`iter_parquet`**, **`iter_ipc`**, **`iter_csv`**, **`iter_ndjson`** (**`iter_json_lines`** is an alias), **`iter_json_array`** — and **`write_parquet_batches`**, **`write_ipc_batches`**, **`write_csv_batches`**, **`write_ndjson_batches`**. **Parquet**, **IPC**, and **JSON-array** batch paths need **`pydantable[arrow]`** (PyArrow). **CSV** / **NDJSON** use the stdlib (plus **`json`**).
- **IPC file vs stream:** **`iter_ipc`** / **`write_ipc_batches`** take **`as_stream=`**. Defaults differ (**reader** assumes on-disk **file** format; **writer** defaults to **stream** format). For a round-trip, pass the **same** flag on read and write (see {doc}`IO_IPC`).
- **Async:** **`aiter_parquet`**, **`aiter_ipc`**, **`aiter_csv`**, **`aiter_ndjson`**, **`aiter_json_lines`**, **`aiter_json_array`** mirror the sync iterators (thread offload). **`aiter_sql`** streams SQL batches similarly ({doc}`IO_SQL`).
- **Extras:** **`iter_excel`**, **`iter_delta`**, **`iter_avro`**, **`iter_orc`**, **`iter_bigquery`**, **`iter_snowflake`**, **`iter_kafka_json`** — same column-dict batch shape where the underlying library allows streaming; see {doc}`IO_EXTRAS`.
- **Top-level imports:** many of these names are also re-exported from **`pydantable`** alongside **`DataFrame`** / **`DataFrameModel`** for quick scripts.

This layer is **orthogonal** to **lazy **`read_*`** / **`write_*`** on **`DataFrame`**: use **`read_*`** when you want the Rust engine and Polars planning; use **`iter_*`** when you already have a **pull**-style batch loop in Python or need a format PyArrow reads without building a **`ScanFileRoot`**.

## One page per source or target family

| Topic | Guide |
|-------|--------|
| **Parquet** (files, URLs, lazy write) | {doc}`IO_PARQUET` |
| **CSV** | {doc}`IO_CSV` |
| **NDJSON** (newline-delimited JSON) | {doc}`IO_NDJSON` |
| **JSON** (array of objects; lazy + materialize) | {doc}`IO_JSON` |
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
