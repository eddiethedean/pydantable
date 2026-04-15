# Data sources for read/write (planning reference)

**Per-format API reference:** {doc}`IO_OVERVIEW` — **default:** **`DataFrame` / `DataFrameModel`** classmethods and instance methods; eager **`materialize_*`**, **`iter_*`**, SQL helpers (**`from pydantable import …`**); untyped **`ScanFileRoot`** and other extension hooks live in **`pydantable.io`** (internal / advanced).

This document lists **common and useful** places applications read and write tabular data. It is intended to guide **0.23.x+** I/O work in pydantable: which formats and transports to support first, and which **async** stacks pair well with **FastAPI** and typed frames.

**Today (0.23.0+)**, the stack has three layers (implementations live under **`pydantable.io`**; **application code** uses **`DataFrame` / `DataFrameModel`** or **`from pydantable import …`**):

1. **`read_*` / `aread_*`** — lazy **local file** roots (**`ScanFileRoot`**) so **`DataFrame` / `DataFrameModel`** can plan on Polars **`LazyFrame`** without loading full columns into Python (**Parquet**, **CSV**, **NDJSON** / **`read_json`** alias, **IPC file**). A JSON **array** of objects in one file is **not** read lazily—use **`materialize_json`** / **`iter_json_array`** (see {doc}`IO_JSON`).
2. **`materialize_*` / `amaterialize_*`** — eager **`dict[str, list]`** reads (**Rust** on local paths where possible; **PyArrow** for bytes, HTTP bodies, column subsets, streaming IPC).
3. **`DataFrame.write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** (and **`DataFrameModel`** mirrors) — write the lazy pipeline from Rust without a giant Python dict.

4. **Batched `dict[str, list]` I/O (1.5.0+)** — **`iter_*` / `aiter_*`** (**`from pydantable import …`**) and **`write_*_batches`** for pull-style, chunk-sized reads and writes (PyArrow-backed for Parquet/IPC where noted). Each **`iter_*`** call targets **one** path; multi-file batching is composed in Python (see {doc}`IO_OVERVIEW` **Multi-file paths, globs, and memory**). **`write_csv_batches`** / **`write_ndjson_batches`** support **`mode="w"`** (truncate) vs **`"a"`** (append). **`write_parquet_batches`**, **`write_ipc_batches`**, **`write_csv_batches`**, and **`write_ndjson_batches`** require a **single file** path (or stream)—an **existing directory** path raises **`ValueError`**. **`DataFrameModel.write_*_batches`** accepts iterators of column dicts (or row objects with **`to_dict()`**) for the same shapes.

## Lazy read **`**scan_kwargs`** and write **`write_kwargs`**

**Reads** (**`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_json`** (alias of **`read_ndjson`**), **`read_ipc`**, and async **`aread_*`**) accept Polars scan options as keyword arguments; they are forwarded to the Rust layer as **`scan_kwargs`**. **Writes** accept an optional **`write_kwargs=dict`** (same idea for the Polars sink). Unknown keys raise **`ValueError`** with an **allowed:** list.

| Format | Read kwargs (examples) | Write kwargs |
|--------|------------------------|--------------|
| **Parquet** | **`n_rows`**, **`low_memory`**, **`rechunk`**, **`use_statistics`**, **`cache`**, **`glob`**, **`allow_missing_columns`** (multi-file: union missing columns as null when **`True`**), **`parallel`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`** | **`write_kwargs`:** **`compression`**, **`row_group_size`**, **`data_page_size`**, **`statistics`**, **`parallel`** (per shard). **`DataFrame.write_parquet`:** **`partition_by`** (list of column names for hive-style output), **`mkdir`** (create dataset root)—not part of **`write_kwargs`**. |
| **CSV** | **`has_header`**, **`separator`**, **`skip_rows`**, **`skip_lines`**, **`n_rows`**, **`infer_schema_length`**, **`ignore_errors`**, **`low_memory`**, **`rechunk`**, **`glob`**, **`cache`**, **`quote_char`**, **`eol_char`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**, **`raise_if_empty`**, **`truncate_ragged_lines`**, **`decimal_comma`**, **`try_parse_dates`** | **`include_header`**, **`include_bom`** (plus top-level **`separator`**, **`compression`** where applicable) |
| **NDJSON** | **`low_memory`**, **`rechunk`**, **`ignore_errors`**, **`n_rows`**, **`infer_schema_length`**, **`glob`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`** | **`json_format`** (**`"lines"`** / **`"json"`**) |
| **IPC** | **`record_batch_statistics`**, **`glob`**, **`cache`**, **`rechunk`**, **`n_rows`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`** | Use top-level **`compression=`** only; extra **`write_kwargs`** are rejected. |

For **when to tune** NDJSON kwargs (large files, dirty logs, sampling), **presets**, and how **`read_json`** relates to **`read_ndjson`**, see {doc}`IO_JSON` (**Large files**, **NDJSON scan kwargs**).

(local-io-audit)=
### Audit: Polars 0.53.x vs pydantable (1.11.0 Phase A)

**Scope:** Polars Rust **0.53.0** (the version pinned by **`pydantable-core`**) compared to the kwargs pydantable forwards from **`pydantable-core/src/plan/execute_polars/scan_kw.rs`** (`dispatch_file_scan`). The matrix and summary table below reflect **1.11.0** behavior: **Parquet** hive / lineage / row index; **CSV** directory/glob and **`LazyCsvReader`** options; **NDJSON** **`glob`** / **`include_file_paths`** / **`row_index_*`**; **IPC** **`IpcScanOptions`** + **`UnifiedScanArgs`**; **`read_json`** as **`read_ndjson`** alias; **partitioned Parquet writes** (**`partition_by`**) and **`write_*_batches`** path semantics; multi-file Parquet **`allow_missing_columns`**. Remaining gaps (e.g. **`ScanArgsParquet.schema`**, **`HiveOptions.schema`**) are tracked in {doc}`ROADMAP`.

**Directory and glob semantics**

- Lazy reads pass the path string to Polars **`PlRefPath`** and use **`LazyFrame::scan_*`** (or equivalent) inside the native extension (**`pydantable_native._core`**). **Glob expansion, directory listing, and hive-style path parsing** follow **Polars** for the options pydantable sets on the Rust side.
- **`ScanArgsParquet::default()`** in Polars uses **`glob: true`** (so a directory or glob pattern is expanded unless **`glob=False`** is passed through **`scan_kwargs`** where supported).
- **Typed validation** (`trusted_mode`, per-cell checks, etc.) runs at **materialization** (**`collect()`**, **`to_dict()`**, …), not when constructing the lazy root—see {doc}`DATAFRAMEMODEL` and {doc}`INTERFACE_CONTRACT` (**Local lazy file scans**).

**Parquet — `ScanArgsParquet` (Polars) vs pydantable `scan_kwargs`**

| Polars `ScanArgsParquet` field | pydantable `scan_kwargs` | Notes |
|--------------------------------|---------------------------|--------|
| `n_rows` | **mapped** | |
| `parallel` | **mapped** | string: `none`, `columns`, `row_groups`, `prefiltered`, `auto` |
| `low_memory` | **mapped** | |
| `rechunk` | **mapped** | |
| `use_statistics` | **mapped** | |
| `cache` | **mapped** | |
| `glob` | **mapped** | default in Polars is **`true`** |
| `allow_missing_columns` | **mapped** | |
| `hive_options` | **partially mapped** | **`hive_partitioning`** → `enabled` (pass **`None`** to clear to Polars automatic mode); **`hive_start_idx`**; **`try_parse_hive_dates`** → `try_parse_dates`. **`HiveOptions.schema`** (partition dtype overrides) is **not** exposed. |
| `row_index` | **mapped** | **`row_index_name`** (non-empty **`str`**; use explicit **`None`** to clear); **`row_index_offset`** (default **`0`**). |
| `schema` | **not exposed** | |
| `cloud_options` | **not exposed** | |
| `include_file_paths` | **mapped** | column name **`str`**; explicit **`None`** clears. |

**CSV — `LazyCsvReader` (Polars) vs pydantable `scan_kwargs`**

| Polars / reader concern | pydantable `scan_kwargs` | Notes |
|-------------------------|---------------------------|--------|
| CSV parse / skip / infer options (`has_header`, `separator`, `skip_rows`, …) | **mapped** | see summary table above; matches `lazy_csv_with_kwargs` allowlist in **`scan_kw.rs`** |
| `glob` | **mapped** | default **`true`** on **`LazyCsvReader::new`** in Polars |
| **`UnifiedScanArgs.hive_options`** inside Polars CSV scan | **Polars sets `HiveOptions::new_disabled()`** | Hive-style partition columns from **directory paths are not** applied for **lazy CSV** in Polars 0.53 (even if pydantable forwards **`glob`**). |
| `include_file_paths`, `row_index` | **mapped** | same kwargs as Parquet-style lineage / row index (**`include_file_paths`** column name; **`row_index_name`** / **`row_index_offset`**) |
| `raise_if_empty`, `truncate_ragged_lines`, `decimal_comma`, `try_parse_dates` | **mapped** | boolean options on **`LazyCsvReader`** |
| `schema`, `cloud_options`, … | **not exposed** | builder methods exist on **`LazyCsvReader`**; not in pydantable allowlist yet |

**Tests:** multi-file directory and **`*.csv`** glob behavior are covered by **`tests/test_csv_scan_directory_b2.py`** (Phase B2).

**NDJSON — `LazyJsonLineReader` (Polars) vs pydantable `scan_kwargs`**

| Polars / reader concern | pydantable `scan_kwargs` | Notes |
|-------------------------|---------------------------|--------|
| `low_memory`, `rechunk`, `ignore_errors`, `n_rows`, `infer_schema_length` | **mapped** | |
| **`glob` (toggle)** | **mapped** | **`glob=True`** or omitted: accepted (Polars **`LazyJsonLineReader::finish`** uses **`UnifiedScanArgs { glob: true, … }`**). **`glob=False`**: **`ValueError`** (cannot disable expansion in Polars 0.53). |
| **`HiveOptions`** for NDJSON | **`HiveOptions::new_disabled()`** in Polars | Hive-style partition columns from paths are **disabled** for NDJSON scans in Polars 0.53. |
| `include_file_paths`, `row_index` | **mapped** | **`include_file_paths`** column name; **`row_index_name`** / **`row_index_offset`** (same pattern as Parquet/CSV). |
| `schema`, `cloud_options`, … | **not exposed** | |

**Tests:** multi-file directory, **`*.jsonl`** glob, hive path behavior, unknown kw, **`glob=False`**, row index / include paths, mixed-extension glob—**`tests/test_ndjson_scan_directory_b3.py`** (Phase B3).

**IPC — `IpcScanOptions` + `UnifiedScanArgs` (Polars) vs pydantable `scan_kwargs`**

| Polars type / field | pydantable `scan_kwargs` | Notes |
|---------------------|---------------------------|--------|
| `IpcScanOptions.record_batch_statistics` | **mapped** | |
| `IpcScanOptions.checked` | **not exposed** | |
| **`UnifiedScanArgs.glob`**, **`cache`**, **`rechunk`** | **mapped** | default **`glob: true`** in Polars **`Default`** |
| **`UnifiedScanArgs.pre_slice`** | **mapped** via **`n_rows`** | positive slice from row 0 |
| **`UnifiedScanArgs.hive_options`** | **mapped** | **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`** (same pattern as Parquet) |
| **`UnifiedScanArgs.include_file_paths`**, **`row_index`** | **mapped** | **`include_file_paths`** column name; **`row_index_name`** / **`row_index_offset`** |

**Tests:** multi-file directory, **`*.arrow`** glob, hive-style path layout, unknown kw, row index / include paths, **`glob=False`** on a single file—**`tests/test_ipc_scan_directory_b4.py`** (Phase B4).

**`read_parquet_url`**: URL fetch still uses **`**kwargs`** for the HTTP path; avoid mixing fetch and scan options in one dict unless you split them in application code.

## `read_parquet_url` / `aread_parquet_url` temp-file lifecycle

These helpers **`fetch_bytes`** from HTTP(S), write a **named temp** **`.parquet`** file, and return **`ScanFileRoot(path, "parquet", columns)`**. The file is **not** deleted when the **`ScanFileRoot`** or **`DataFrame`** is garbage-collected.

- **Delete after use:** when your pipeline finishes (after **`write_*`**, **`collect()`**, etc.), remove the path (e.g. **`os.unlink(root.path)`** if you keep a reference to the native root, or track the temp path you created).
- **Context managers:** **`DataFrameModel.read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** (same behavior as the lower-level helpers in **`pydantable.io`**) unlink the temp file when the block exits ({doc}`IO_HTTP`).
- **Async:** **`aread_parquet_url`** runs the same work in a thread pool like other **`aread_*`** helpers.

There is **no** true streaming HTTP Parquet scan without a local file or deeper Polars/object-store integration.

**`export_*` / `aexport_*`** persist column dicts to files (via **`DataFrameModel`** classmethods or **`from pydantable import export_parquet`, …** where re-exported). **SQL:** **`fetch_sqlmodel` / `afetch_sqlmodel`**, **`fetch_sql_raw` / `afetch_sql_raw`**, **`write_sqlmodel` / `awrite_sqlmodel`**, **`write_sql_raw` / `awrite_sql_raw`** (deprecated unprefixed **`fetch_sql`** / **`write_sql`**: {doc}`IO_SQL`) — **SQLAlchemy**; **`[sql]`** extra + your DB driver. **HTTP(S)** uses **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`**, **`fetch_bytes`** (experimental flag). **Object store:** **`read_from_object_store`**. Optional **Excel**, **Delta**, **Kafka**, **stdin/stdout**: see {doc}`IO_EXTRAS`. **Interchange:** **`to_arrow()`** / **`to_polars()`** and constructors (with **`[arrow]`**, **PyArrow** **`Table`** / **`RecordBatch`**).

---

## How pydantable fits in

- **Ingest target:** columnar **`dict[str, list]`**, row **`list[dict]`**, or **Arrow** / **Polars** when optional deps are installed—then **`DataFrameModel` / `DataFrame[Schema]`** applies schema and **`trusted_mode`** rules.
- **Egress target:** **`to_dict()`**, **`collect()`**, **`to_arrow()`**, **`to_polars()`**—then your writer (PyArrow, Polars, pandas, DB driver) persists bytes or rows.
- **Async rule of thumb:** blocking file/DB/network I/O should run in **`asyncio.to_thread`**, a **`ThreadPoolExecutor`**, or a **native async** API (see below). This matches existing **`acollect` / `ato_dict` / `ato_arrow`** semantics (engine work is still thread-offloaded; **file** I/O is separate).

---

## Rust-first I/O (fits the existing `pydantable-core` model)

Pydantable already executes typed plans in **Rust** (Polars-backed). Putting **format and transport I/O** in the same native extension is a strong default:

- **One wheel** carries both execution and readers/writers; fewer Python-only heavy deps for core paths.
- **GIL:** file parse and decode can run **without holding the GIL** (PyO3 `Python::allow_threads`), same idea as today’s engine work.
- **Reuse:** **Polars** / **Apache Arrow** Rust stacks already implement **Parquet**, **IPC**, **CSV**, **JSON** (Polars), and more; aligning I/O with the same physical types reduces copies and surprises vs “Python PyArrow read → dict → Rust plan.”

### What Rust handles well

| Area | Rust direction | Notes |
|------|----------------|--------|
| **Parquet / IPC / CSV / NDJSON** | **`polars` I/O**, **`arrow` / `parquet` crates** | Natural extension of the current crate dependency graph (already Polars for execution). |
| **Local & `std::fs` paths** | Sync Rust + **`allow_threads`**; optional **`tokio::fs`** inside an async story | Simple and fast for server workloads. |
| **Object storage (S3, GCS, Azure)** | **`object_store`** (Arrow ecosystem), vendor SDKs | Async-capable; pairs with Parquet/CSV readers over **`AsyncRead`**. |
| **HTTP(S) downloads** | **`reqwest`** (blocking or async) or **`ureq`** for minimal surface | Good for “URL → bytes → parse” ingestion. |
| **SQLite** | **`rusqlite`** or **`sqlx`** (with **`runtime-tokio`**) | Single-process, embedded; excellent for tests and edge nodes. |
| **Postgres / MySQL / …** | **`sqlx`** or **`tokio-postgres`** + manual row→column buffers | True async without Python DB-API; **driver matrix is narrower** than SQLAlchemy. |

### Where Python (SQLAlchemy / SQLModel) still wins

- **Broad driver coverage:** pydantable’s SQL helpers use **SQLAlchemy 2.x** sync **Engine** / **Connection** (or URL strings). Prefer **`fetch_sqlmodel`** / **`write_sqlmodel`** for mapped tables, **`fetch_sql_raw`** / **`write_sql_raw`** for string SQL, or legacy **`fetch_sql`** / **`write_sql`** (deprecated). Any dialect SQLAlchemy documents works as long as the **driver** is installed; pydantable does not bundle DB drivers.
- **ORM-heavy** apps can pair **SQLModel** / **`Session`** with the same engines for CRUD, and use **`fetch_sqlmodel`** (or **`fetch_sql_raw`**) for bulk **`SELECT …` → `dict[str, list]`** for **`DataFrameModel`**.

### Async Rust ↔ FastAPI

- **Option A (simplest):** Rust exposes **sync** `read_*` / `write_*` that release the GIL; Python **`aread_*`** = **`asyncio.to_thread`** (same as **`acollect`** today).
- **Option B (deeper):** **`pyo3-async-runtimes`** + Tokio inside **`pydantable_native._core`** for **`async_execute_plan`** / **`async_collect_plan_batches`** (see {doc}`EXECUTION`). File and SQL **`aread_*`** / **`afetch_*`** still use thread offload by default.

### Tradeoffs to accept

- **Compile time and binary size** grow with every optional I/O feature; use **Cargo features** mirroring Python extras (`polars_io`, `sqlx-postgres`, …).
- **Security / advisories** apply to the **Rust** dependency tree too (`cargo audit` / `cargo deny` already run in CI).
- **Excel, BigQuery, Snowflake:** often remain **Python**-first (official SDKs) or **thin** Rust wrappers over HTTP until a clear ROI appears.

### Suggested split for 0.22+

1. **Rust:** Parquet/IPC/CSV/JSON read+write, optional object-store paths, **sqlx** (or similar) for a **small** set of databases.
2. **Python:** thin façade in **`pydantable.io`** (implementation) calling **`_core`**, plus **SQLAlchemy/SQLModel** helpers as an **extra** for full driver flexibility—**application code** uses **`from pydantable import …`** or **`DataFrameModel`**.
3. **Document** which entrypoints are **Rust-native** vs **Python-shim** in the public docs and changelog.

---

## Tier 1 — Highest impact (support first)

| Source / sink | Read | Write | Notes |
|---------------|------|-------|--------|
| **Parquet** | Yes (PyArrow today) | **`export_parquet`** / lazy **`DataFrame.write_parquet`** | Columnar, typed, compression; de facto analytics interchange. |
| **Arrow IPC / Feather** | Yes (IPC today) | **`export_ipc`** / lazy **`write_ipc`** | Zero-copy friendly within Arrow ecosystem. |
| **CSV** | **`materialize_csv`** / **`read_csv`** | **`export_csv`** / lazy **`write_csv`** | Universal but weak typing; needs explicit dtypes / nullable handling. |
| **JSON** | Add **`read_json`** (array of objects **and** line-delimited) | Add **`write_json`** | APIs and logs; align with Pydantic-friendly row shapes. |
| **SQL (relational)** | **`fetch_sqlmodel`** / **`fetch_sql_raw`** | **`write_sqlmodel`** / **`write_sql_raw`** (append/replace) | **SQLAlchemy 2.x** + your DB’s driver (**`pydantable[sql]`** ships SQLAlchemy + SQLModel). |

### SQL: pydantable helpers vs async engines

**In pydantable** (SQLModel-first and **`*_raw`** helpers; deprecated unprefixed **`fetch_sql`** / **`write_sql`**: {doc}`IO_SQL`)

- **Any SQLAlchemy URL or engine:** e.g. **`postgresql+psycopg://…`**, **`mysql+pymysql://…`**, **`sqlite:///…`**, **`mssql+pyodbc://…`**, etc.
- **Sync execution** inside SQLAlchemy; from **`async def`** routes use **`afetch_sqlmodel`** / **`afetch_sql_raw`** / **`awrite_sqlmodel`** / **`awrite_sql_raw`** (**`asyncio.to_thread`** or **`executor=`**) so the event loop is not blocked.
- For **large result sets**, prefer **streaming batches** with **`iter_sqlmodel`** / **`iter_sql_raw`** / **`aiter_*`**, or rely on **`fetch_sql_raw`** / **`afetch_sql_raw`** which may return lazily built **`StreamingColumns`** (see {doc}`IO_SQL`) instead of one giant **`dict[str, list]`**.
- **`write_sql_raw` … `if_exists="replace"`** uses generic **DDL** (**`DropTable` / `CreateTable`**); **`write_sqlmodel`** **`replace`** uses **SQLModel** DDL. Exotic dialects or production schemas may still prefer migrations instead.

**Native async SQLAlchemy (asyncpg, …)**

- Pydantable’s **`afetch_*`** SQL helpers are still **thread-offloaded sync** SQLAlchemy. If you already use **`AsyncSession`** / **`AsyncConnection`**, you can **`await conn.stream(text(sql))`** yourself, build **`dict[str, list]`**, and pass that to constructors—or call **`fetch_sql_raw`** from a thread pool for simplicity.

### MongoDB (optional)

- **Install:** **`pip install "pydantable[mongo]"`** (**PyMongo**, **Beanie**, Mongo plan stack). **Guide:** {doc}`MONGO_ENGINE` (lazy **`MongoDataFrame`** / **`MongoDataFrameModel`**, eager **`dict[str, list]`** I/O, **PyMongo surface area** — **`skip`**, **`session`**, **`max_time_ms`**, sync vs **`AsyncCollection`**).
- **Lazy typed transforms:** **`from_beanie`** / **`from_beanie_async`**, **`from_collection`**, **`sync_pymongo_collection`**.
- **Eager column dicts:** **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** and **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`** (native async when the handle is **`pymongo.asynchronous.AsyncCollection`**).
- **Beanie ODM reads/writes (hooks, validation-on-save):** {doc}`BEANIE` (**`afetch_beanie`**, **`awrite_beanie`**, …). **Which helper?** {doc}`IO_DECISION_TREE` → Mongo rows.

---

## Tier 2 — Very common in enterprises

| Source / sink | Read | Write | Notes |
|---------------|------|-------|--------|
| **Excel (.xlsx)** | Optional extra | Optional extra | **`openpyxl`** / **`xlsxwriter`**; large files need streaming discipline. |
| **SQLite** | Via SQLAlchemy URL `sqlite:///...` | Same | Single-file DB; great for tests and edge deployments. |
| **Delta Lake** | Polars / PyArrow Delta | Same | Often **`deltalake`** + object storage; versioned tables. |
| **Avro / ORC** | PyArrow | PyArrow | Common in data lakes; schema evolution story matters. |
| **Google BigQuery** | Official / Arrow path | Load jobs | Often async-friendly client patterns; may map to Arrow then pydantable. |
| **Snowflake** | Connector + Arrow / pandas | Connector | Enterprise analytics; credential and session patterns differ. |

---

## Tier 3 — Cloud, HTTP, and streams

| Source / sink | Read | Write | Notes |
|---------------|------|-------|--------|
| **S3 / GCS / Azure** (`s3://`, `gs://`, …) | **`fsspec`**, **`s3fs`**, **`gcsfs`** | Same | Pair with Parquet/CSV readers; credentials via env / IAM. |
| **HTTP(S)** | **`httpx`** (sync/async), **`aiohttp`** | POST/PUT bodies | JSON APIs, presigned URLs, CSV/Parquet downloads. |
| **Kafka / messaging** | Consumer → batches | Producer ← batches | Not “files”; still a **data source** for microservices. |
| **stdin / stdout** | Line reader | Line writer | CLI and Unix pipes. |

---

## Async I/O options (by category)

### Files (local disk)

- **`asyncio.to_thread(open(...).read)`** or **`aiofiles`** for read/write when the underlying operations are **synchronous** at the OS level. **`aiofiles`** is widely used but ultimately bridges to blocking file APIs (typically via a thread pool)—fine for many apps, but not the same as **streaming I/O that stays off the event loop without thread offload**.
- **Memory maps** (`mmap`) for very large read-only scans—still usually wrapped in **`to_thread`**.

### True-async Python packages to evaluate (`rap*`)

For **async-first** Python services that want **streaming** file, CSV, or SQLite access **without** the “async wrapper around blocking work” pattern, consider these PyPI packages (summaries from their metadata: emphasis on **no fake async** / **no GIL stalls**):

| Package | Role | PyPI |
|---------|------|------|
| **`rapfiles`** | Async filesystem I/O | [rapfiles](https://pypi.org/project/rapfiles/) |
| **`rapcsv`** | Streaming async CSV | [rapcsv](https://pypi.org/project/rapcsv/) |
| **`rapsqlite`** | Async SQLite | [rapsqlite](https://pypi.org/project/rapsqlite/) |

Contrast with common stacks:

- **`aiofiles`** — convenient **`async with`** around files; implementation relies on **thread-pool offload** for actual reads/writes, which is not “fake” but is **not** in-process non-blocking syscalls end-to-end.
- **`aiosqlite`** — **`async`/`await`** API over SQLite; under the hood still coordinates with **blocking** SQLite work (often thread-backed). Good ergonomics; different tradeoff than libraries that target **true async** I/O paths.

**For pydantable:** these are candidates for **`aread_*` / `awrite_*` Python shims** *alongside* Rust-backed I/O: use **`rapcsv`/`rapfiles`/`rapsqlite`** when the deployment wants **pure-Python async** pipelines; use **`_core` + `allow_threads`** or **Rust async** when everything should live in the extension. Treat maturity, Windows support, and dependency weight as **release-blocker checks** before promoting any of them as default.

### SQL

- **Preferred long-term:** **SQLAlchemy 2.0 async** engine + **`AsyncConnection`** streaming cursors to build column buffers without loading ORM rows.
- **Interim:** sync **`fetch_sql_raw`** / **`fetch_sqlmodel`** in **`asyncio.to_thread`** (simple, portable, good enough for many FastAPI apps with a small pool).
- **SQLite-only async:** **`rapsqlite`** (above) as an alternative to **`aiosqlite`** when the project standard is **streaming / non-threaded async** semantics; validate against your SQLite usage (WAL, concurrent readers, etc.).

### HTTP

- **`httpx.AsyncClient`** for JSON/CSV/Parquet bytes from URLs; then parse in thread if the parser releases the GIL poorly.

### Object storage

- Many SDKs are sync: **`to_thread`** around **`fsspec`** file open + PyArrow / **`materialize_parquet`** is a common pattern.

---

## Implemented API shape (0.23.0+)

**Public surface:** **`DataFrame` / `DataFrameModel`** classmethods, plus **`from pydantable import …`** for eager helpers—the **`pydantable.io`** module implements the same paths for the native extension and **SQLAlchemy** (optional extra):

- **Lazy file roots:** **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`** (+ async **`aread_*`**).
- **Eager columns:** **`materialize_*` / `amaterialize_*`** → **`dict[str, list]`** for constructors and tests.
- **Pipeline write:** **`DataFrame.write_parquet`** / **`DataFrameModel.write_parquet`** (and **`write_csv`**, **`write_ipc`**, **`write_ndjson`**) — Rust; no Python dict round-trip on the hot path.
- **Export from dicts:** **`export_*` / `aexport_*`**, **`write_sqlmodel`** / **`write_sql_raw`** (or deprecated **`write_sql`**) fed from **`to_dict()`** or ad hoc buffers.

**Dependencies:** mirror **Cargo features** and Python extras (`sql` = Rust `sqlx` subset **or** Python SQLAlchemy, document which); **`pydantable[excel]`** may stay Python-only for a long time.

---

## Out of scope (unless explicitly promoted)

- **Distributed Spark** cluster I/O (separate from the in-process PySpark façade).
- **Arbitrary ODBC** everywhere without documented driver matrix.
- **Proprietary SaaS APIs** without a small stable subset.

---

## Related documentation

- {doc}`EXECUTION` — materialization costs and interchange.
- {doc}`FASTAPI` — async routes, executors, trust boundaries.
- {doc}`SUPPORTED_TYPES` — what schemas can express after ingest.
