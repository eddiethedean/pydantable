# Data I/O by format (overview)

**Default (application code):** use **`DataFrame[Schema]`** and **`DataFrameModel`** for **lazy** **`read_*`** / **`aread_*`**, **`export_*`**, **`write_sql`** / **`awrite_sql`**, and lazy **`write_*`** (Rust-backed where documented). For **eager** column dicts, call **`pydantable.io`** (**`materialize_*`**, **`fetch_sql`**, **`iter_sql`**, …) and pass **`dict[str, list]`** into **`MyModel(...)`** / **`DataFrame[Schema](...)`** — **`DataFrameModel`** does not wrap eager loaders anymore.

**Utilities (`pydantable.io`):** **`materialize_*`**, **`fetch_sql`**, **`iter_sql`**, URL helpers, and format-specific readers that return **`dict[str, list]`** or **`ScanFileRoot`** for **`DataFrame`** construction.

For **execution semantics** (lazy vs collect, Rust engine), see {doc}`EXECUTION`. For **roadmap-style** “what to support next,” see {doc}`DATA_IO_SOURCES`. **Which API should I call?** See {doc}`IO_DECISION_TREE`.

(full-api-in-pydantableio)=
## Module reference: `pydantable.io`

All public I/O **symbols** are defined under **`pydantable.io`** (and re-exported in small subsets from **`pydantable`** for Parquet-focused quick scripts). Import **`from pydantable.io import …`** when you need **`ScanFileRoot`**, untyped **`dict[str, list]`**, **`fetch_*_url`**, **`pydantable.io.extras`**, or names not mirrored on **`DataFrameModel`**.

## Primary API: `DataFrame` and `DataFrameModel`

| What | `DataFrame[Schema]` | `DataFrameModel` subclass |
|------|------------------------|----------------------------|
| **Lazy local file** | **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** | **`MyModel.read_*`** — classmethods; same **`**scan_kwargs`** |
| **Lazy Parquet URL** | **`read_parquet_url`** | **`MyModel.read_parquet_url`** — **`**kwargs`** for **`fetch_bytes`** only |
| **Temp Parquet URL cleanup** | Build frame inside **`read_parquet_url_ctx`** ( **`io`** ) or use **`DataFrameModel.read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** | Same — context managers unlink the download when the block exits ({doc}`IO_HTTP`) |
| **Async lazy reads** | **`DataFrame[Schema].aread_*`** (mirrors **`MyModel.aread_*`**) | **`await MyModel.aread_parquet(...)`**, **`aread_csv`**, …, optional **`executor=`**; URL without ctx: **`aread_parquet_url`** (prefer **`aread_parquet_url_ctx`**) |
| **Eager reads** | Constructor **`DataFrame[Schema](cols)`** from **`dict[str, list]`** | Use **`pydantable.io`** (**`materialize_*`**, **`fetch_sql`**, …) then **`MyModel(cols)`** |
| **Lazy writes** | **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** | Same **instance** methods on **`model`** — **`streaming`**, **`write_kwargs`**, etc. |

**Ingest validation options:** `trusted_mode`, `fill_missing_optional`, `ignore_errors`, `on_validation_errors` apply on **constructors** and on **typed lazy reads** (`DataFrame[Schema].read_*` / `aread_*`, `DataFrameModel.read_*` / `aread_*`) at **materialization time** (`to_dict()` / `collect()` / `to_arrow()` / `to_polars()`).

- `trusted_mode=None` / `"off"`: full per-cell validation (default).
- `ignore_errors=True` (only meaningful when `trusted_mode` is `"off"`): invalid rows are skipped and `on_validation_errors` receives one batch payload.
- `trusted_mode="shape_only"` / `"strict"`: skip per-cell validation (still enforces shape + nullability; `"strict"` adds dtype-compat checks). `ignore_errors` does not skip rows in these modes.
- `fill_missing_optional=True` (default): missing optional columns are filled with `None` during ingest/materialization.
- `fill_missing_optional=False`: missing optional columns/fields raise an error unless the schema field has an explicit default; explicit defaults are used in that case.

See {doc}`DATAFRAMEMODEL` for the detailed ingest contract.

**`DataFrameModel`** classmethods call into **`pydantable.io`** internally; signatures match the module where applicable. **`pydantable.io.extras`** is module-only (no **`DataFrameModel`** wrappers).

## Engine matrix (`materialize_*`)

**`PYDANTABLE_IO_ENGINE`:** **`auto`** (default), **`rust`**, or **`pyarrow`** where supported.

```{note}
Some eager **Rust** I/O paths (especially **`export_*`** / column-dict writes) require the optional **`polars`** Python package at runtime. If you force `engine="rust"` without that extra installed, you may get an `ImportError`. Using `engine="auto"` will fall back where a pure-Python / PyArrow path exists.
```

| Function | Rust path (typical) | PyArrow / fallback |
|----------|---------------------|------------------------|
| **`materialize_parquet`** | Local file path, **`columns is None`** | **`columns`** set, or **`bytes`** / **`BinaryIO`** source, or `auto` fallback |
| **`materialize_csv`** | Local path | stdlib **`csv`** if Rust fails under **`auto`** |
| **`materialize_ndjson`** | Local path | Python JSON lines if Rust fails under **`auto`** |
| **`materialize_ipc`** | Local IPC file, **`as_stream=False`** | Streams, **`as_stream=True`**, buffers |

Details: {doc}`IO_DECISION_TREE` (**Engine selection**).

## Module functions (`pydantable.io`)

Use **`pydantable.io`** when you need **`ScanFileRoot`**, raw column dicts, eager **`materialize_*`**, or **`fetch_sql`** / **`iter_sql`**. **Application code** typically uses **`MyModel.read_*`** / **`aread_*`** for lazy files and **`pydantable.io` + constructor** when an eager **`dict[str, list]`** is required.

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
