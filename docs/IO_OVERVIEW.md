# Data I/O by format (overview)

**Default (application code):** use **`DataFrame[Schema]`** and **`DataFrameModel`** for **lazy** **`read_*`** / **`aread_*`**, **`export_*`**, SQL (**`write_sqlmodel`** / **`awrite_sqlmodel`**, or deprecated **`write_sql`** / **`awrite_sql`**), and lazy **`write_*`** (Rust-backed where documented). For **eager** column dicts, import **`materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`iter_sqlmodel`** / **`iter_sql_raw`**, … **from `pydantable`** and pass **`dict[str, list]`** into **`MyModel(...)`** / **`DataFrame[Schema](...)`**. (These names are implemented in **`pydantable.io`** but **you should not import `pydantable.io` in application code** — use the package root.) SQL naming and deprecations: {doc}`IO_SQL`.

**Same functions** (**`materialize_*`**, **`fetch_sqlmodel`**, URL helpers, iterators, …) are defined in **`pydantable.io`** for the library’s own layering; end users rely on **`from pydantable import …`** or **`DataFrame` / `DataFrameModel`** methods.

For **execution semantics** (lazy vs collect, Rust engine), see {doc}`EXECUTION`. For **roadmap-style** “what to support next,” see {doc}`DATA_IO_SOURCES`. **Polars 0.53 scan kwargs vs pydantable** (paths, globs, hive): {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`. **Which API should I call?** See {doc}`IO_DECISION_TREE`.

(full-api-in-pydantableio)=
## Internal layout: `pydantable.io`

The **`pydantable.io`** package holds the concrete implementations; **application code** imports from **`pydantable`** (see the root **`__init__.py`**) or calls **`DataFrame` / `DataFrameModel`** classmethods. Only **extension authors** or **contributors** should import **`pydantable.io`** directly (e.g. **`ScanFileRoot`**, **`pydantable.io.extras`**, batch helpers in **`pydantable.io.batches`** when not re-exported).

## Primary API: `DataFrame` and `DataFrameModel`

| What | `DataFrame[Schema]` | `DataFrameModel` subclass |
|------|------------------------|----------------------------|
| **Lazy local file** | **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** | **`MyModel.read_*`** — classmethods; same **`**scan_kwargs`** |
| **Lazy Parquet URL** | **`read_parquet_url`** | **`MyModel.read_parquet_url`** — **`**kwargs`** for **`fetch_bytes`** only |
| **Temp Parquet URL cleanup** | Build frame inside **`read_parquet_url_ctx`** ( **`io`** ) or use **`DataFrameModel.read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** | Same — context managers unlink the download when the block exits ({doc}`IO_HTTP`) |
| **Async lazy reads** | **`DataFrame[Schema].aread_*`** (mirrors **`MyModel.aread_*`**) | **`await MyModel.aread_parquet(...)`**, **`aread_csv`**, …, optional **`executor=`**; URL without ctx: **`aread_parquet_url`** (prefer **`aread_parquet_url_ctx`**) |
| **Eager reads** | Constructor **`DataFrame[Schema](cols)`** from **`dict[str, list]`** | **`from pydantable import materialize_*`, `fetch_sqlmodel`, …** then **`MyModel(cols)`** |
| **Lazy writes** | **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** | Same **instance** methods on **`model`** — **`streaming`**, **`write_kwargs`**, etc. |

**Ingest validation options:** `trusted_mode`, `fill_missing_optional`, `ignore_errors`, `on_validation_errors` apply on **constructors** and on **typed lazy reads** (`DataFrame[Schema].read_*` / `aread_*`, `DataFrameModel.read_*` / `aread_*`) at **materialization time** (`to_dict()` / `collect()` / `to_arrow()` / `to_polars()`).

- `trusted_mode=None` / `"off"`: full per-cell validation (default).
- `ignore_errors=True` (only meaningful when `trusted_mode` is `"off"`): invalid rows are skipped and `on_validation_errors` receives one batch payload.
- `trusted_mode="shape_only"` / `"strict"`: skip per-cell validation (still enforces shape + nullability; `"strict"` adds dtype-compat checks). `ignore_errors` does not skip rows in these modes.
- `fill_missing_optional=True` (default): missing optional columns are filled with `None` during ingest/materialization.
- `fill_missing_optional=False`: missing optional columns/fields raise an error unless the schema field has an explicit default; explicit defaults are used in that case.

See {doc}`DATAFRAMEMODEL` for the detailed ingest contract.

**`DataFrameModel`** classmethods call the same implementations as **`pydantable.io`** internally. **`pydantable.io.extras`** (Excel, …) has no **`DataFrameModel`** wrapper — prefer **`materialize_*`** / **`iter_*`** from **`pydantable`** where re-exported, or see {doc}`IO_EXTRAS` for advanced cases.

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

## Public imports (`from pydantable import …`)

Use **`from pydantable import …`** for eager **`materialize_*`**, SQL **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`iter_*`**, and the same names documented in {doc}`IO_SQL`. **Lazy files:** **`MyModel.read_*`** / **`aread_*`**. Only import **`pydantable.io`** directly if you need **`ScanFileRoot`**, **`pydantable.io.extras`**, or symbols not on the root package.

| Layer | Role |
|-------|------|
| **`read_*` / `aread_*`** | Lazy **local file** scan → **`ScanFileRoot`** → Polars **`LazyFrame`** in the Rust plan (no full column lists in Python). |
| **`read_parquet_url` / `aread_parquet_url`** | HTTP(S) download to a **temp Parquet file**, then same lazy root — prefer **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** for automatic cleanup ({doc}`IO_HTTP`). |
| **`materialize_*` / `amaterialize_*`** | Eager **`dict[str, list]`** (Rust and/or PyArrow / stdlib, depending on path). |
| **`fetch_*_url`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`read_from_object_store`**, **`pydantable.io.extras`** | Other sources that return **`dict[str, list]`** — import **`fetch_*`** from **`pydantable`** where re-exported; **`object_store`** / **`extras`** may still require **`pydantable.io`** (see {doc}`IO_EXTRAS`). |
| **`export_*` / `aexport_*`**, **`write_sqlmodel`** / **`write_sql_raw`** (deprecated: **`write_sql`**) | Eager writes from an in-memory column dict or your own pipeline. |

## Batched column dict I/O (`iter_*`, `write_*_batches`, `aiter_*`)

For **bounded memory** pipelines in plain Python (outside the Rust **`LazyFrame`** plan), import **`iter_parquet`**, **`iter_csv`**, … **from `pydantable`** (chunked **`dict[str, list]`**) plus batch writers re-exported from the root package.

- **Contract:** each yielded batch is **rectangular**. Helpers **`ensure_rectangular`** and **`iter_concat_batches`** live in **`pydantable.io.batches`** (import that path only if you need those helpers; otherwise prefer lazy **`read_*`**).
- **Core formats:** **`iter_parquet`**, **`iter_ipc`**, **`iter_csv`**, **`iter_ndjson`** (**`iter_json_lines`** is an alias), **`iter_json_array`** — and **`write_parquet_batches`**, **`write_ipc_batches`**, **`write_csv_batches`**, **`write_ndjson_batches`**. **Parquet**, **IPC**, and **JSON-array** batch paths need **`pydantable[arrow]`** (PyArrow). **CSV** / **NDJSON** use the stdlib (plus **`json`**).
- **IPC file vs stream:** **`iter_ipc`** / **`write_ipc_batches`** take **`as_stream=`**. Defaults differ (**reader** assumes on-disk **file** format; **writer** defaults to **stream** format). For a round-trip, pass the **same** flag on read and write (see {doc}`IO_IPC`).
- **Async:** **`aiter_parquet`**, **`aiter_ipc`**, **`aiter_csv`**, **`aiter_ndjson`**, **`aiter_json_lines`**, **`aiter_json_array`** mirror the sync iterators (thread offload). **`aiter_sqlmodel`** / **`aiter_sql_raw`** (deprecated: **`aiter_sql`**) stream SQL batches similarly ({doc}`IO_SQL`).
- **Extras:** **`iter_excel`**, **`iter_delta`**, **`iter_avro`**, **`iter_orc`**, **`iter_bigquery`**, **`iter_snowflake`**, **`iter_kafka_json`** — same column-dict batch shape where the underlying library allows streaming; see {doc}`IO_EXTRAS`.
- **Imports:** use **`from pydantable import iter_parquet, …`** (see root **`__init__.py`** for the full list).

### Multi-file paths, globs, and memory

**Single path per call:** **`iter_parquet`**, **`iter_csv`**, **`iter_ndjson`**, **`aiter_*`**, etc. each take **one** file path (or an open handle)—they do **not** accept a directory or glob string. To walk several files, expand paths yourself (**`pathlib.Path.glob`**, **`sorted(glob.glob(...))`**, or an explicit list), then call **`iter_*`** per file, or use **`iter_chain_batches`** ( **`pydantable.io.batches`**, contributor-only) to chain iterators — prefer lazy **`read_*`** with **`glob`** / directory when possible ({doc}`IO_DECISION_TREE`).

**Bounded memory:** Prefer yielding **per-file** batches in a loop. **`iter_concat_batches`** concatenates **all** batches into **one** column dict—fine for tests or small data; for many large files it can allocate a huge dict—often better to use lazy **`read_*`** (directory / **`glob=True`**) and **`to_dict()`**, **`stream`**, or **`write_*`**.

**When to use lazy `read_*`:** Multi-file or hive-style datasets are usually clearer with **`MyModel.read_*`** + **`scan_kwargs`** (Polars-backed scan)—see {doc}`IO_DECISION_TREE` (**Multi-file, directories, and globs**) and the runnable example **`docs/examples/io/iter_glob_parquet_batches.py`** (per-file **`iter_parquet`** vs lazy **`read_parquet`**).

**Async:** **`aiter_*`** mirror the same **single-path** contract as sync **`iter_*`** (thread offload); compose multiple paths the same way as in synchronous code.

This layer is **orthogonal** to **lazy **`read_*`** / **`write_*`** on **`DataFrame`**: use **`read_*`** when you want the Rust engine and Polars planning; use **`iter_*`** when you already have a **pull**-style batch loop in Python or need a format PyArrow reads without building a **`ScanFileRoot`**.

**Multi-file Parquet output:** **`write_parquet_batches`** always targets **one** output file. For a **hive-style partitioned** dataset (directory tree **`col=value/...`**), use **`DataFrame.write_parquet(..., partition_by=[...])`** ({doc}`IO_PARQUET`).

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

From the repository root, with **`pydantable-native`** built (**`maturin develop`** in `pydantable-native`, or a wheel):

```bash
python docs/examples/io/overview_roundtrip.py
```

From a source tree without installing the package, set **`PYTHONPATH=python`** (path to the **`python/`** directory that contains **`pydantable`**).

```{literalinclude} examples/io/overview_roundtrip.py
:language: python
```

## Lazy **`scan_kwargs`** and sink **`write_kwargs`**

Optional Polars scan/write options are accepted as **`**scan_kwargs`** on lazy file reads and **`write_kwargs={...}`** on lazy file writes (same on **`DataFrame`** / **`DataFrameModel`**). Allowed keys are validated in Rust; unknown keys raise **`ValueError`**. The full matrix lives in {doc}`DATA_IO_SOURCES` (**Lazy read `**scan_kwargs` and write `write_kwargs`**).
