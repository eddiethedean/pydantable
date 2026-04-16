# Data I/O by format (overview)

**Default (application code):** use **`DataFrame[Schema]`** and **`DataFrameModel`** for **lazy** **`read_*`** / **`aread_*`**, **`export_*`**, SQL (**`write_sqlmodel`** / **`awrite_sqlmodel`**, or deprecated **`write_sql`** / **`awrite_sql`**), and lazy **`write_*`** (Rust-backed where documented). For **eager** column dicts, import **`materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`iter_sqlmodel`** / **`iter_sql_raw`**, **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** and **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`** (MongoDB via **PyMongo** — sync **`Collection`** or **`pymongo.asynchronous.AsyncCollection`**; app models: **Beanie** **`Document`** with **`from_beanie`** / **`from_beanie_async`**, **`sync_pymongo_collection`**, ODM **`afetch_beanie`** / … — [MONGO_ENGINE](/integrations/engines/mongo.md), [BEANIE](/integrations/engines/beanie.md)), … **from `pydantable`** and pass **`dict[str, list]`** into **`MyModel(...)`** / **`DataFrame[Schema](...)`** (or load from / write to Mongo — [MONGO_ENGINE](/integrations/engines/mongo.md)). (These names are implemented in **`pydantable.io`** but **you should not import `pydantable.io` in application code** — use the package root.) SQL naming and deprecations: [IO_SQL](/io/sql.md).

**Same functions** (**`materialize_*`**, **`fetch_sqlmodel`**, URL helpers, iterators, …) are defined in **`pydantable.io`** for the library’s own layering; end users rely on **`from pydantable import …`** or **`DataFrame` / `DataFrameModel`** methods.

For **execution semantics** (lazy vs collect, Rust engine), see [EXECUTION](/user-guide/execution.md). For **roadmap-style** “what to support next,” see [DATA_IO_SOURCES](/io/data-io-sources.md). **Polars 0.53 scan kwargs vs pydantable** (paths, globs, hive): {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`. **Which API should I call?** See [IO_DECISION_TREE](/io/decision-tree.md).

(full-api-in-pydantableio)=
## Internal layout: `pydantable.io`

The **`pydantable.io`** package holds the concrete implementations; **application code** imports from **`pydantable`** (see the root **`__init__.py`**) or calls **`DataFrame` / `DataFrameModel`** classmethods. Only **extension authors** or **contributors** should import **`pydantable.io`** directly (e.g. **`ScanFileRoot`**, **`pydantable.io.extras`**, batch helpers in **`pydantable.io.batches`** when not re-exported).

## Primary API: `DataFrame` and `DataFrameModel`

| What | `DataFrame[Schema]` | `DataFrameModel` subclass |
|------|------------------------|----------------------------|
| **Lazy local file** | **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** | **`MyModel.read_*`** — classmethods; same **`**scan_kwargs`** |
| **Lazy Parquet URL** | **`read_parquet_url`** | **`MyModel.read_parquet_url`** — **`**kwargs`** for **`fetch_bytes`** only |
| **Temp Parquet URL cleanup** | Build frame inside **`read_parquet_url_ctx`** ( **`io`** ) or use **`DataFrameModel.read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** | Same — context managers unlink the download when the block exits ([IO_HTTP](/io/http.md)) |
| **Async lazy reads** | **`DataFrame[Schema].aread_*`** (mirrors **`MyModel.aread_*`**) | **`await MyModel.aread_parquet(...)`**, **`aread_csv`**, …, optional **`executor=`**; URL without ctx: **`aread_parquet_url`** (prefer **`aread_parquet_url_ctx`**) |
| **Eager reads** | Constructor **`DataFrame[Schema](cols)`** from **`dict[str, list]`** | **`from pydantable import materialize_*`, `fetch_sqlmodel`, …** then **`MyModel(cols)`** |
| **Lazy writes** | **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** | Same **instance** methods on **`model`** — **`streaming`**, **`write_kwargs`**, etc. |

**Ingest validation options:** `trusted_mode`, `fill_missing_optional`, `ignore_errors`, `on_validation_errors` apply on **constructors** and on **typed lazy reads** (`DataFrame[Schema].read_*` / `aread_*`, `DataFrameModel.read_*` / `aread_*`) at **materialization time** (`to_dict()` / `collect()` / `to_arrow()` / `to_polars()`).

- `trusted_mode=None` / `"off"`: full per-cell validation (default).
- `ignore_errors=True` (only meaningful when `trusted_mode` is `"off"`): invalid rows are skipped and `on_validation_errors` receives one batch payload.
- `trusted_mode="shape_only"` / `"strict"`: skip per-cell validation (still enforces shape + nullability; `"strict"` adds dtype-compat checks). `ignore_errors` does not skip rows in these modes.
- `fill_missing_optional=True` (default): missing optional columns are filled with `None` during ingest/materialization.
- `fill_missing_optional=False`: missing optional columns/fields raise an error unless the schema field has an explicit default; explicit defaults are used in that case.

See [DATAFRAMEMODEL](/user-guide/dataframemodel.md) for the detailed ingest contract.

**`DataFrameModel`** classmethods call the same implementations as **`pydantable.io`** internally. **`pydantable.io.extras`** (Excel, …) has no **`DataFrameModel`** wrapper — prefer **`materialize_*`** / **`iter_*`** from **`pydantable`** where re-exported, or see [IO_EXTRAS](/io/extras.md) for advanced cases.

## Engine matrix (`materialize_*`)

**`PYDANTABLE_IO_ENGINE`:** **`auto`** (default), **`rust`**, or **`pyarrow`** where supported.

!!! note
    Some eager **Rust** I/O paths (especially **`export_*`** / column-dict writes) require the optional **`polars`** Python package at runtime. If you force `engine="rust"` without that extra installed, you may get an `ImportError`. Using `engine="auto"` will fall back where a pure-Python / PyArrow path exists.


!!! important
    **`engine="auto"` (default):** implementations **try the Rust fast path first** for formats that support it (local file, right shape of arguments). If the Rust reader **raises**, pydantable **catches the failure** and continues with **PyArrow** or **stdlib** parsing where a fallback exists. You get working data, but you **do not** get an error that says “Rust failed.” To **surface** Rust-only failures (debugging or strict native-only pipelines), set **`engine="rust"`** (or **`PYDANTABLE_IO_ENGINE=rust`**) so the exception propagates. See also [IO_DECISION_TREE](/io/decision-tree.md) (**Engine selection**).


| Function | Rust path (typical) | PyArrow / fallback |
|----------|---------------------|------------------------|
| **`materialize_parquet`** | Local file path, **`columns is None`** | **`columns`** set, or **`bytes`** / **`BinaryIO`** source, or `auto` fallback |
| **`materialize_csv`** | Local path | stdlib **`csv`** if Rust fails under **`auto`** |
| **`materialize_ndjson`** | Local path | Python JSON lines if Rust fails under **`auto`** |
| **`materialize_ipc`** | Local IPC file, **`as_stream=False`** | Streams, **`as_stream=True`**, buffers |

Details: [IO_DECISION_TREE](/io/decision-tree.md) (**Engine selection**).

## Public imports (`from pydantable import …`)

Use **`from pydantable import …`** for eager **`materialize_*`**, SQL **`fetch_sqlmodel`** / **`fetch_sql_raw`**, Mongo **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** (and async **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**), **`iter_*`**, and the same names documented in [IO_SQL](/io/sql.md). **Lazy files:** **`MyModel.read_*`** / **`aread_*`**. Only import **`pydantable.io`** directly if you need **`ScanFileRoot`**, **`pydantable.io.extras`**, or symbols not on the root package.

| Layer | Role |
|-------|------|
| **`read_*` / `aread_*`** | Lazy **local file** scan → **`ScanFileRoot`** → Polars **`LazyFrame`** in the Rust plan (no full column lists in Python). |
| **`read_parquet_url` / `aread_parquet_url`** | HTTP(S) download to a **temp Parquet file**, then same lazy root — prefer **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** for automatic cleanup ([IO_HTTP](/io/http.md)). |
| **`materialize_*` / `amaterialize_*`** | Eager **`dict[str, list]`** (Rust and/or PyArrow / stdlib, depending on path). |
| **`fetch_*_url`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`fetch_mongo`** / **`iter_mongo`**, **`afetch_mongo`** / **`aiter_mongo`**, **`read_from_object_store`**, **`pydantable.io.extras`** | Other sources that return **`dict[str, list]`** — import **`fetch_*`** / **`iter_mongo`** from **`pydantable`** where re-exported; **`object_store`** / **`extras`** may still require **`pydantable.io`** (see [IO_EXTRAS](/io/extras.md)). Mongo helpers accept **`skip`**, **`session`**, **`max_time_ms`** on **`find`**-backed reads ([MONGO_ENGINE](/integrations/engines/mongo.md)). |
| **`export_*` / `aexport_*`**, **`write_sqlmodel`** / **`write_sql_raw`** (deprecated: **`write_sql`**), **`write_mongo`** | Eager writes from an in-memory column dict or your own pipeline (Mongo **`insert_many`** for **`write_mongo`**). |

## Batched column dict I/O (`iter_*`, `write_*_batches`, `aiter_*`)

For **bounded memory** pipelines in plain Python (outside the Rust **`LazyFrame`** plan), import **`iter_parquet`**, **`iter_csv`**, … **from `pydantable`** (chunked **`dict[str, list]`**) plus batch writers re-exported from the root package.

- **Contract:** each yielded batch is **rectangular**. Helpers **`ensure_rectangular`** and **`iter_concat_batches`** live in **`pydantable.io.batches`** (import that path only if you need those helpers; otherwise prefer lazy **`read_*`**).
- **Core formats:** **`iter_parquet`**, **`iter_ipc`**, **`iter_csv`**, **`iter_ndjson`** (**`iter_json_lines`** is an alias), **`iter_json_array`** — and **`write_parquet_batches`**, **`write_ipc_batches`**, **`write_csv_batches`**, **`write_ndjson_batches`**. **Parquet**, **IPC**, and **JSON-array** batch paths need **`pydantable[arrow]`** (PyArrow). **CSV** / **NDJSON** use the stdlib (plus **`json`**).
- **IPC file vs stream:** **`iter_ipc`** / **`write_ipc_batches`** take **`as_stream=`**. Defaults differ (**reader** assumes on-disk **file** format; **writer** defaults to **stream** format). For a round-trip, pass the **same** flag on read and write (see [IO_IPC](/io/ipc.md)).
- **Async:** **`aiter_parquet`**, **`aiter_ipc`**, **`aiter_csv`**, **`aiter_ndjson`**, **`aiter_json_lines`**, **`aiter_json_array`** mirror the sync iterators (thread offload). **`aiter_sqlmodel`** / **`aiter_sql_raw`** (deprecated: **`aiter_sql`**) stream SQL batches similarly ([IO_SQL](/io/sql.md)). **`aiter_mongo`** streams Mongo batches ([MONGO_ENGINE](/integrations/engines/mongo.md)).
- **Extras:** **`iter_excel`**, **`iter_delta`**, **`iter_avro`**, **`iter_orc`**, **`iter_bigquery`**, **`iter_snowflake`**, **`iter_kafka_json`** — same column-dict batch shape where the underlying library allows streaming; see [IO_EXTRAS](/io/extras.md).
- **Imports:** use **`from pydantable import iter_parquet, …`** (see root **`__init__.py`** for the full list).

### Multi-file paths, globs, and memory

**Single path per call:** **`iter_parquet`**, **`iter_csv`**, **`iter_ndjson`**, **`aiter_*`**, etc. each take **one** file path (or an open handle)—they do **not** accept a directory or glob string. To walk several files, expand paths yourself (**`pathlib.Path.glob`**, **`sorted(glob.glob(...))`**, or an explicit list), then call **`iter_*`** per file, or use **`iter_chain_batches`** ( **`pydantable.io.batches`**, contributor-only) to chain iterators — prefer lazy **`read_*`** with **`glob`** / directory when possible ([IO_DECISION_TREE](/io/decision-tree.md)).

**Bounded memory:** Prefer yielding **per-file** batches in a loop. **`iter_concat_batches`** concatenates **all** batches into **one** column dict—fine for tests or small data; for many large files it can allocate a huge dict—often better to use lazy **`read_*`** (directory / **`glob=True`**) and **`to_dict()`**, **`stream`**, or **`write_*`**.

**When to use lazy `read_*`:** Multi-file or hive-style datasets are usually clearer with **`MyModel.read_*`** + **`scan_kwargs`** (Polars-backed scan)—see [IO_DECISION_TREE](/io/decision-tree.md) (**Multi-file, directories, and globs**) and the runnable example **`docs/examples/io/iter_glob_parquet_batches.py`** (per-file **`iter_parquet`** vs lazy **`read_parquet`**).

**Async:** **`aiter_*`** mirror the same **single-path** contract as sync **`iter_*`** (thread offload); compose multiple paths the same way as in synchronous code.

This layer is **orthogonal** to **lazy **`read_*`** / **`write_*`** on **`DataFrame`**: use **`read_*`** when you want the Rust engine and Polars planning; use **`iter_*`** when you already have a **pull**-style batch loop in Python or need a format PyArrow reads without building a **`ScanFileRoot`**.

**Multi-file Parquet output:** **`write_parquet_batches`** always targets **one** output file. For a **hive-style partitioned** dataset (directory tree **`col=value/...`**), use **`DataFrame.write_parquet(..., partition_by=[...])`** ([IO_PARQUET](/io/parquet.md)).

## One page per source or target family

| Topic | Guide |
|-------|--------|
| **Parquet** (files, URLs, lazy write) | [IO_PARQUET](/io/parquet.md) |
| **CSV** | [IO_CSV](/io/csv.md) |
| **NDJSON** (newline-delimited JSON) | [IO_NDJSON](/io/ndjson.md) |
| **JSON** (array of objects; lazy + materialize) | [IO_JSON](/io/json.md) |
| **Arrow IPC / Feather file** | [IO_IPC](/io/ipc.md) |
| **HTTP(S), object stores** | [IO_HTTP](/io/http.md) |
| **SQL** (SQLAlchemy) | [IO_SQL](/io/sql.md) |
| **MongoDB** (Beanie, lazy Mongo `DataFrame`, PyMongo column dicts) | [MONGO_ENGINE](/integrations/engines/mongo.md), [BEANIE](/integrations/engines/beanie.md) |
| **Excel, Delta, Avro, ORC, cloud warehouses, Kafka, stdin/stdout** | [IO_EXTRAS](/io/extras.md) |

## Runnable example

From the repository root, with **`pydantable-native`** built (**`maturin develop`** in `pydantable-native`, or a wheel):

```bash
python docs/examples/io/overview_roundtrip.py
```

From a source tree without installing the package, set **`PYTHONPATH=python`** (path to the **`python/`** directory that contains **`pydantable`**).


--8<-- "examples/io/overview_roundtrip.py"

## Lazy **`scan_kwargs`** and sink **`write_kwargs`**

Optional Polars scan/write options are accepted as **`**scan_kwargs`** on lazy file reads and **`write_kwargs={...}`** on lazy file writes (same on **`DataFrame`** / **`DataFrameModel`**). Allowed keys are validated in Rust; unknown keys raise **`ValueError`**. The full matrix lives in [DATA_IO_SOURCES](/io/data-io-sources.md) (**Lazy read `**scan_kwargs` and write `write_kwargs`**).
