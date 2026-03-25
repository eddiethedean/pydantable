# Data sources for read/write (planning reference)

This document lists **common and useful** places applications read and write tabular data. It is intended to guide **0.22.x+** I/O work in pydantable: which formats and transports to support first, and which **async** stacks pair well with **FastAPI** and typed frames.

**Today (0.22.0+)**, pydantable ships **`pydantable.io`**: **Rust-first** **`read_*` / `write_*`** for **Parquet**, **Arrow IPC**, **CSV**, and **NDJSON** into **`dict[str, list]`** (plus **PyArrow** for buffers, **column projection**, and **streaming IPC**); **async** **`aread_*` / `awrite_*`**; **SQL** via **SQLAlchemy** **`read_sql` / `write_sql`** (**`[sql]`** extra) for **any URL/dialect** SQLAlchemy supports—install the matching **DBAPI driver** in your environment ( **`psycopg`**, **`pymysql`**, **`pyodbc`**, …); **experimental** HTTP / **`fsspec`** object-store helpers; and optional **Excel**, **Delta/Avro/ORC**, **BigQuery**, **Snowflake**, **Kafka**, **stdin/stdout** entrypoints documented below. **Export** remains **`to_arrow()`** / **`to_polars()`** and constructors that accept columnar Python data and (with **`[arrow]`**) **PyArrow** **`Table`** / **`RecordBatch`**.

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

- **Broad driver coverage:** **`pydantable.io.read_sql` / `write_sql`** use **SQLAlchemy 2.x** sync **Engine** / **Connection** (or URL strings). Any dialect SQLAlchemy documents works as long as the **driver** is installed; pydantable does not bundle DB drivers.
- **ORM-heavy** apps can pair **SQLModel** / **`Session`** with the same engines for CRUD, and use **`read_sql`** for bulk **`SELECT …` → `dict[str, list]`** for **`DataFrameModel`**.

### Async Rust ↔ FastAPI

- **Option A (simplest):** Rust exposes **sync** `read_*` / `write_*` that release the GIL; Python **`aread_*`** = **`asyncio.to_thread`** (same as **`acollect`** today).
- **Option B (deeper):** **`tokio`** runtime inside the extension + **`pyo3-asyncio`** (or equivalent) to await Rust futures from **`async def`** routes—higher integration cost; use when profiling shows thread-pool overhead dominates.

### Tradeoffs to accept

- **Compile time and binary size** grow with every optional I/O feature; use **Cargo features** mirroring Python extras (`polars_io`, `sqlx-postgres`, …).
- **Security / advisories** apply to the **Rust** dependency tree too (`cargo audit` / `cargo deny` already run in CI).
- **Excel, BigQuery, Snowflake:** often remain **Python**-first (official SDKs) or **thin** Rust wrappers over HTTP until a clear ROI appears.

### Suggested split for 0.22+

1. **Rust:** Parquet/IPC/CSV/JSON read+write, optional object-store paths, **sqlx** (or similar) for a **small** set of databases.
2. **Python:** thin **`pydantable.io`** façade calling **`_core`**, plus **SQLAlchemy/SQLModel** helpers as an **extra** for full driver flexibility.
3. **Document** which entrypoints are **Rust-native** vs **Python-shim** in the public docs and changelog.

---

## Tier 1 — Highest impact (support first)

| Source / sink | Read | Write | Notes |
|---------------|------|-------|--------|
| **Parquet** | Yes (PyArrow today) | Add **`write_parquet`** (PyArrow or Polars) | Columnar, typed, compression; de facto analytics interchange. |
| **Arrow IPC / Feather** | Yes (IPC today) | Add **IPC / Feather** writers | Zero-copy friendly within Arrow ecosystem. |
| **CSV** | Add **`read_csv`** | Add **`write_csv`** | Universal but weak typing; needs explicit dtypes / nullable handling. |
| **JSON** | Add **`read_json`** (array of objects **and** line-delimited) | Add **`write_json`** | APIs and logs; align with Pydantic-friendly row shapes. |
| **SQL (relational)** | **`read_sql`** | **`write_sql`** (append/replace) | **SQLAlchemy 2.x** + your DB’s driver (**`pydantable[sql]`** ships SQLAlchemy only). |

### SQL: pydantable helpers vs async engines

**In pydantable (`read_sql` / `write_sql`, `aread_sql` / `awrite_sql`)**

- **Any SQLAlchemy URL or engine:** e.g. **`postgresql+psycopg://…`**, **`mysql+pymysql://…`**, **`sqlite:///…`**, **`mssql+pyodbc://…`**, etc.
- **Sync execution** inside SQLAlchemy; from **`async def`** routes use **`aread_sql`** / **`awrite_sql`** (**`asyncio.to_thread`** or **`executor=`**) so the event loop is not blocked.
- **`write_sql` … `if_exists="replace"`** uses generic **DDL** (**`DropTable` / `CreateTable`**); exotic dialects or production schemas may still prefer migrations instead.

**Native async SQLAlchemy (asyncpg, …)**

- Pydantable’s **`aread_sql`** is still **thread-offloaded sync** SQLAlchemy. If you already use **`AsyncSession`** / **`AsyncConnection`**, you can **`await conn.stream(text(sql))`** yourself, build **`dict[str, list]`**, and pass that to constructors—or call **`read_sql`** from a thread pool for simplicity.

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
- **Interim:** sync **`read_sql`** in **`asyncio.to_thread`** (simple, portable, good enough for many FastAPI apps with a small pool).
- **SQLite-only async:** **`rapsqlite`** (above) as an alternative to **`aiosqlite`** when the project standard is **streaming / non-threaded async** semantics; validate against your SQLite usage (WAL, concurrent readers, etc.).

### HTTP

- **`httpx.AsyncClient`** for JSON/CSV/Parquet bytes from URLs; then parse in thread if the parser releases the GIL poorly.

### Object storage

- Many SDKs are sync: **`to_thread`** around **`fsspec`** file open + PyArrow **`read_parquet`** is a common pattern.

---

## Suggested pydantable API shape (0.22+ direction)

Keep **one clear Python module** (e.g. `pydantable.io`) that mostly **delegates to `pydantable._core`** for format/transport work, with **SQLAlchemy** helpers as an optional Python-only extra:

- **Sync (Rust-backed where possible):** `read_csv`, `read_json`, `read_parquet`, `read_ipc`, `read_sql`, … returning **`dict[str, list]`** (or optional **lazy** in-memory frame later).
- **Async:** `aread_*` with **`executor=`** mirroring **`acollect`** (thread pool around sync Rust that releases the GIL), or native async later if justified.
- **Write:** `write_csv`, `write_json`, `write_parquet`, `write_ipc`, `write_sql`, … fed from **`to_dict()`** or zero-copy-ish paths from the engine when available.

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
