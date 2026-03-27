# Changelog

All notable changes to this project are documented here. The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

<!-- Add notes here for the next 1.x release. -->

## [1.1.0] — 2026-03-27

### Added

- **Typing:** `DataFrameModel` transform methods return derived model types so mypy/pyright
  can verify the schema after `select`, `drop`, `rename`, `with_columns`, `join`, and
  `group_by(...).agg(...)` without materializing between steps (see GitHub issue #1).
- **Tests:** expanded coverage (I/O fallbacks, PySpark/expr edges, schema helpers) and mypy
  regression updates in `tests/test_mypy_dataframe_model_return_types.py`.

## [1.0.0] — 2026-03-26

### Scope

- Production-ready major release focused on API stability and semver contract clarity.
- No large new execution-engine features are required for the tag.

### Added

- Ingest/docs consistency for missing optional behavior:
  - `fill_missing_optional` documented consistently across constructor and typed lazy-read materialization paths.
  - Explicit schema defaults on optional fields (for example `note: str | None = "n/a"` or `= None`) now take precedence when `fill_missing_optional=False` instead of raising.
- **1.0.0** readiness documentation:
  - explicit 1.x semver policy in {doc}`VERSIONING`,
  - release gate checklist and security-advisory handling in {doc}`DEVELOPER`,
  - roadmap, README, and docs index updates for 1.0 communication and support matrix policy.
- **`[docs]`** extra includes **SQLAlchemy** so Sphinx (**`-W`**) and `sphinx-autodoc-typehints` resolve `DataFrameModel` **`Engine`** / **`Connection`** annotations in CI (matches Read the Docs).

### Changed

- Documentation includes migration guidance from earlier `missing_optional` string-style wording (`"fill_none"` / `"error"`) to boolean `fill_missing_optional=True/False`.

### Stability commitments

- 1.x patch/minor/major policy is defined in {doc}`VERSIONING`.
- Behavioral semantics continue to be defined in {doc}`INTERFACE_CONTRACT`.

### Upgrade guidance

- Canonical upgrade path from 0.20.x/0.23.x is documented in `README.md` and linked from the docs index; I/O renames from 0.22.x/0.23.x are summarized under **0.23.0** below.

## [0.23.0] — 2026-03-25

### Highlights

- **Out-of-core file workflows:** **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** (and **`aread_*`**) return a **`ScanFileRoot`** so **`DataFrame` / `DataFrameModel`** can run transforms on a Polars **`LazyFrame`** without loading the full file into Python lists first.
- **`DataFrame.write_parquet`** (and **`write_csv`**, **`write_ipc`**, **`write_ndjson`**): write the lazy pipeline from the Rust engine without building a giant **`dict[str, list]`** for the result.
- **Breaking — public I/O renames:** sync/async eager file reads into columns are **`materialize_*` / `amaterialize_*`**. Lazy local files use **`read_*` / `aread_*`**. Eager **`dict[str, list]` → file** uses **`export_*` / `aexport_*`**. SQL **`read_sql` / `aread_sql`** → **`fetch_sql` / `afetch_sql`**. Eager HTTP(S) column readers (0.22 **`read_*_url`**) → **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`**. **Lazy** HTTP Parquet (temp file on disk) stays **`read_parquet_url` / `aread_parquet_url`** — use **`read_parquet_url_ctx` / `aread_parquet_url_ctx`** to delete the temp file when done ({doc}`IO_HTTP`). Top-level **`pydantable`** exports and **`DataFrameModel`** classmethods follow the same vocabulary.
- **Pre-release / internal names:** development builds that still exposed **`scan_*` / `ascan_*`** or **`sink_*`** for lazy I/O now align with the public **`read_*` / `write_*`** names; **`pydantable`** re-exports **`read_parquet`**, **`read_parquet_url`**, **`aread_parquet`**, **`aread_parquet_url`**, **`export_parquet`** (replacing **`scan_*` / `write_parquet`** on the package root).

### Added

- **JSON (array of objects):** **`read_json`**, **`materialize_json`**, **`export_json`**, **`aread_json`**, **`amaterialize_json`**, **`aexport_json`** — local lazy scan and eager column dicts (see {doc}`IO_JSON`).
- **`read_parquet_url_ctx` / `aread_parquet_url_ctx`:** context managers that delete the temporary Parquet file when the block exits (see {doc}`IO_HTTP`).
- **`DataFrameModel`:** classmethods **`export_*`**, **`write_sql`** / **`awrite_sql`**, **`from_sql`** / **`afrom_sql`** delegating to **`pydantable.io`**.
- **`MissingRustExtensionError`:** subclass of **`NotImplementedError`** when **`pydantable._core`** is missing or incomplete on lazy scan/sink paths and **`execute_plan`** (still catchable as **`NotImplementedError`**).
- **HTTP / object store safety:** **`max_bytes`** on **`fetch_bytes`** and **`read_from_object_store`**; chunked reads with **`ValueError`** when exceeded.
- **Docs:** {doc}`IO_DECISION_TREE`, {doc}`IO_JSON`, {doc}`IO_HTTP` updates, engine matrix in {doc}`IO_OVERVIEW`, FASTAPI executor guidance; README and manual pages refreshed for **0.23.x** I/O.

### Details

- **Rust:** **`ScanFileRoot`**, **`plan_to_lazyframe`**, internal sink exports for lazy writes; join/groupby/reshape entrypoints work with lazy file roots where implemented (see {doc}`EXECUTION` matrix).
- **Python:** **`read_csv_stdin`** uses **`materialize_csv`** internally.
- **Docs:** {doc}`EXECUTION` memory model and streaming/collect compatibility matrix (**`PYDANTABLE_ENGINE_STREAMING`** reserved); {doc}`DATA_IO_SOURCES`, {doc}`FASTAPI`, {doc}`INTERFACE_CONTRACT`, {doc}`ROADMAP`, {doc}`README`.

### Migration (from 0.22.x)

| Old (0.22.x) | Use instead (0.23.0) |
|--------------|----------------------|
| Eager file → **`dict[str, list]`** via **`read_parquet`**, **`aread_parquet`**, … | **`materialize_parquet`**, **`amaterialize_parquet`**, … |
| **`read_sql`**, **`aread_sql`** | **`fetch_sql`**, **`afetch_sql`** |
| Eager URL → columns via **`read_parquet_url`** / **`read_csv_url`** / **`read_ndjson_url`** | **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`** |
| Lazy HTTP Parquet (unchanged name, new cleanup helpers) | Still **`read_parquet_url`** / **`aread_parquet_url`**; prefer **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** for automatic temp-file removal |
| Large local file, filter → Parquet out | **`read_parquet`** + transforms + **`DataFrame.write_parquet`** |

## [0.22.0] — 2026-03-25

### Highlights

- **Comprehensive I/O:** the **`pydantable.io`** package adds Rust-backed (Polars) **`read_*` / `write_*`** for **Parquet**, **Arrow IPC**, **CSV**, and **NDJSON** into **`dict[str, list]`**, with **`Python::allow_threads`** on read hot paths; **PyArrow** remains the default for **buffers**, **column projection**, and **streaming IPC**. Async mirrors **`aread_*` / `awrite_*`** use **`asyncio.to_thread`** (optional **`executor=`**), matching **`acollect`** / **`ato_arrow`**.
- **SQLAlchemy bridge:** **`read_sql`** / **`write_sql`** ( **`pip install 'pydantable[sql]'`** + your DB driver) for URL/engine **`SELECT`** → column dict and append/replace inserts across **SQLAlchemy-supported** databases.
- **Transports (experimental):** HTTP(S) **`fetch_bytes`**, **`read_parquet_url`**, **`read_csv_url`**, **`read_ndjson_url`**, and **`fsspec`**-based **`read_from_object_store`** — opt in with **`experimental=True`** or **`PYDANTABLE_IO_EXPERIMENTAL=1`**.
- **Tier-2/3 extras (best-effort):** **`[excel]`**, **`[kafka]`**, **`[bq]`**, **`[snowflake]`**, **`[cloud]`**; helpers such as **`read_excel`**, **`read_delta`**, **`read_kafka_json_batch`**, **`read_csv_stdin` / `write_csv_stdout`** (see **`docs/DATA_IO_SOURCES.md`**).
- **Optional `[rap]`:** true-async CSV via **`aread_csv_rap`** when **`rapcsv`** + **`rapfiles`** are installed.
- **Engine override:** set **`PYDANTABLE_IO_ENGINE=rust`** or **`pyarrow`** to force file readers/writers.
- **Release quality bar:** the **`v0.22.0`** tag is cut from a commit that passes **`make check-full`**, full **`pytest`**, and Rust checks including **`--no-default-features`**.
- **Supply chain:** the release workflow publishes **CycloneDX SBOMs** (Python + Rust) alongside wheels/sdist.
- **Support matrix:** Python **3.10–3.13**.

### Details

- **Rust:** new **`pydantable._core`** exports **`io_read_*_path`** / **`io_write_*_path`**; column-dict writes round-trip through Python **`polars.DataFrame`** → IPC → Rust writers (install **`pydantable[polars]`** for writes).
- **Tests:** **`tests/test_io_comprehensive.py`** (round-trips, SQLite SQL, local HTTP server for URL Parquet).
- **CI:** Python test job installs **`sqlalchemy`** with other dev deps.

## [0.21.0] — 2026-03-25

### Highlights

- **Streamlit:** `DataFrame` and `DataFrameModel` implement the **Python DataFrame Interchange Protocol** (`__dataframe__`) via PyArrow so `st.dataframe(df)` can render a typed `pydantable` frame directly when `pyarrow` is installed (`pip install 'pydantable[arrow]'`). For editing, use `st.data_editor(df.to_arrow())` (or `to_polars()`). See {doc}`STREAMLIT` and {doc}`EXECUTION` (**interchange**).

## [0.20.0] — 2026-03-25

**Supersedes:** 0.19.0.

### Highlights

- **UX / discovery:** Core **`DataFrame`** and **`DataFrameModel`** expose **`columns`**, **`shape`**, **`empty`**, **`dtypes`**, **`info()`**, and **`describe()`** for **int**, **float**, **bool**, and **str** columns (one **`to_dict()`** materialization). **`shape[0]`** follows **root-buffer** semantics—see {doc}`INTERFACE_CONTRACT` **Introspection**, {doc}`EXECUTION`.
- **Docs:** {doc}`QUICKSTART` (five-minute tour), repository **`notebooks/five_minute_tour.ipynb`**, {doc}`EXECUTION` sections on **materialization costs**, **import styles**, **copy-as / interchange**; **naming map** in {doc}`PANDAS_UI` / {doc}`PYSPARK_UI`.
- **Display:** **`pydantable.display`** — **`get_repr_html_limits`**, **`set_display_options`**, **`reset_display_options`**; env **`PYDANTABLE_REPR_HTML_*`** for Jupyter HTML preview bounds.
- **`DataFrame.value_counts`** / **`DataFrameModel.value_counts`** (group-by path); **`_repr_mimebundle_`** on **`DataFrame`** and **`DataFrameModel`** (`text/plain` + `text/html`).
- **Debugging:** **`PYDANTABLE_VERBOSE_ERRORS=1`** appends schema context to **`ValueError`** from **`execute_plan`**.
- **Expressions:** **`Expr`**, **`ColumnRef`**, **`WhenChain`**, and pending window helpers implement readable **`__repr__`**. Tests: **`tests/test_expr_repr.py`**.
- **PySpark façade:** **`DataFrame.show()`** and **`summary()`** (alias of **`describe()`**). See {doc}`PYSPARK_UI`, {doc}`PYSPARK_PARITY`.
- **Documentation:** {doc}`README`, {doc}`index`, {doc}`ROADMAP`, {doc}`PARITY_SCORECARD`, {doc}`PANDAS_UI`, {doc}`DEVELOPER`.

### Details

- **Repr / HTML:** Multi-line **`DataFrame.__repr__`** and **`_repr_html_`** (card-style HTML; grouped/model banners). See {doc}`EXECUTION`, **`tests/test_dataframe_repr.py`**.
- **Tests:** **`tests/test_display_options.py`**, **`tests/test_dataframe_discovery.py`**, **`tests/test_rust_engine_verbose_errors.py`**. See {doc}`EXECUTION`, {doc}`INTERFACE_CONTRACT`.
- **Release hygiene:** **`make check-full`**, full **pytest**, **`cargo test --all-features`** per {doc}`DEVELOPER`.

## [0.19.0] — 2026-03-24

### Highlights

- **Pre-1.0 consolidation:** {doc}`VERSIONING` documents **0.x** patch vs minor expectations; {doc}`INTERFACE_CONTRACT` links there for semver scope while staying the behavioral source of truth.
- **Roadmap to 1.0:** {doc}`ROADMAP` **Shipped in 0.19.0** replaces the planned checklist; **Planned v1.0.0** items that belong on the **1.0.0** tag (full **1.x** semver policy, SBOM, comms) remain explicitly deferred there with rationale below.
- **Parity docs:** {doc}`POLARS_TRANSFORMATIONS_ROADMAP`, {doc}`PARITY_SCORECARD`, {doc}`PYSPARK_PARITY`, {doc}`README`, and {doc}`index` updated for **current release** and **0.19 → 1.0** clarity—no new table methods or PySpark `functions` rows.
- **Performance:** {doc}`PERFORMANCE` adds an **0.19.0 validation** note (key scripts spot-checked; no headline number refresh vs **0.18.x** paths).
- **CI / tests:** Grouped output comparisons in **`tests/test_v018_features.py`** sort by group key where row order is not API-guaranteed (stable **`pytest-xdist`** on Linux).

### Details

See {doc}`ROADMAP` **Shipped in 0.19.0**. Release hygiene: **`make check-full`**, **`cargo test --all-features`**, **`cargo check --no-default-features`**, full **pytest** before tag; GitHub Actions install deps aligned with {doc}`DEVELOPER` / **`pyproject.toml`** **`[dev]`**.

**Deferred to v1.0.0 tag (not blocking 0.19.0):** formal **1.x** semver publication, PyPI packaging dry-run narrative, SBOM/supply-chain notes, support matrix as a **1.0.x** commitment, and README/index “1.0 leads” copy—see {doc}`ROADMAP` **Planned v1.0.0**.

## [0.18.0] — 2026-03-22

### Highlights

- **Grouped execution errors:** Polars **`collect()`** failures during **`group_by().agg()`** may include **`(group_by().agg())`** in the **`ValueError`** text (via **`polars_err_ctx`**) so they are identifiable as grouped aggregation runtime errors. See {doc}`EXECUTION`.
- **Maps:** **Non-string** map keys (**`dict[int, T]`**, non-UTF-8 Arrow map keys) remain **unsupported** and are **explicitly deferred** for this release ({doc}`SUPPORTED_TYPES`, {doc}`ROADMAP` **Later**).
- **Documentation:** Post–**P7** note in {doc}`POLARS_TRANSFORMATIONS_ROADMAP` (phases complete; further parity is additive). {doc}`PARITY_SCORECARD`, {doc}`PYSPARK_PARITY`, {doc}`DEVELOPER`, {doc}`ROADMAP` updated. **No** new PySpark **`sql.functions`** wrappers or table API changes.
- **Tests:** Hypothesis + integration coverage for **`group_by`** / **`join`** (`tests/test_hypothesis_properties.py`, **`tests/test_v018_features.py`**); Rust **`polars_err_ctx`** message format (`execute_polars/common.rs`, **`polars_err_format_tests`**).

### Details

See {doc}`ROADMAP` **Shipped in 0.18.0**. {doc}`INTERFACE_CONTRACT` aggregation rules are unchanged; the doc notes optional **`group_by().agg()`** error-message context.

## [0.17.0] — 2026-03-18

### Highlights

- **Maps (string keys):** Documented and tested **Expr** behavior for **`map_get`** / **`map_contains_key`** on columns ingested from PyArrow **`map<utf8, …>`** (missing key → null). **Non-string** Python **`dict[int, T]`** map keys remain **unsupported** (deferred); see {doc}`ROADMAP` **Later**.
- **PySpark façade:** [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md) — new thin **`pydantable.pyspark.sql.functions`** wrappers: **`str_replace`**, **`regexp_replace`** (alias, literal replace), **`strip_prefix`**, **`strip_suffix`**, **`strip_chars`**, **`strptime`**, **`binary_len`**, **`list_len`**, **`list_get`**, **`list_contains`**, **`list_min`**, **`list_max`**, **`list_sum`** (core **`Expr`** / Rust lowering unchanged). Tests: **`tests/test_pyspark_sql.py`**.
- **Docs:** Refreshed [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) (map + Arrow ingest note).

### Details

See {doc}`ROADMAP` **Shipped in 0.17.0**.

## [0.16.1] — 2026-03-27

### Fixed

- **Expression typing:** Binary arithmetic on **`dict[str, T]`** map columns (for example `df.m + 1` or `df.m + df.m`) now raises **`TypeError`** in Rust (`infer_arith_dtype`) instead of panicking on an internal unwrap. Regression test: **`tests/test_expr_070_surfaces.py`**.
- **Constructors:** **`validate_columns_strict`** (and therefore **`DataFrame[Schema](pa.Table)`** / **`RecordBatch`**) imported Arrow conversion helpers from the wrong submodule (`pydantable.schema.io`, which does not exist). Imports now use **`pydantable.io`**, matching **`DataFrameModel`**. Regression: **`tests/test_arrow_interchange.py`** (`test_dataframe_generic_accepts_pa_table`).

## [0.16.0] — 2026-03-26

### Highlights

- **Arrow interchange:** **`read_parquet`** and **`read_ipc`** (optional **`as_stream`** for streaming IPC) return **`dict[str, list]`** for **`DataFrame` / `DataFrameModel`**. **`to_arrow`** / **`ato_arrow`** materialize a PyArrow **`Table`** after the same engine path as **`to_dict`** (not zero-copy). Optional extra **`pydantable[arrow]`** (**`pyarrow>=14`**). Constructors accept **`pa.Table`** / **`RecordBatch`** when **`pyarrow`** is installed.
- **FastAPI:** [`FASTAPI.md`](FASTAPI.md) — multipart Parquet upload, **`Depends`** executor pattern, background-task notes, **422** vs application error guidance. **`python-multipart`** in **`[dev]`** and CI workflows. Tests: **`tests/test_fastapi_recipes.py`** (multipart + invalid body **422**), **`tests/test_arrow_interchange.py`**; **`scripts/verify_doc_examples.py`** extended.
- **Docs:** [`EXECUTION.md`](EXECUTION.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md), [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md), [`ROADMAP.md`](ROADMAP.md), [`README.md`](README.md), [`index.md`](index.md).

### Details

See {doc}`ROADMAP` **Shipped in 0.16.0**. Sync **`read_parquet` / `read_ipc`** are blocking; use **`asyncio.to_thread`** or an executor from **`async def`** routes for large files if loop latency matters.

- **CI / release:** **`actions/cache@v5`** in **`ci.yml`** and **`release.yml`** (clears GitHub Actions Node 20 deprecation warnings for **`actions/cache@v4`**). **`release.yml`** uses **`maturin build`** + **`twine upload --skip-existing`** per platform instead of deprecated **`maturin publish`** (see [PyO3/maturin#2334](https://github.com/PyO3/maturin/issues/2334)); **`TWINE_USERNAME=__token__`** and **`PYPI_API_TOKEN`** unchanged.

## [0.15.0] — 2026-03-25

### Highlights

- **Async materialization:** **`acollect`**, **`ato_dict`**, **`ato_polars`** on **`DataFrame`**; **`DataFrameModel`** adds the same plus **`arows`** and **`ato_dicts`**. Work runs in **`asyncio.to_thread`** or an optional **`executor=`**. See {doc}`EXECUTION`, {doc}`FASTAPI`.
- **FastAPI:** **`async def`** route examples, **`lifespan`** + **`ThreadPoolExecutor`**, and **`StreamingResponse`** guidance (manual chunking; no built-in row iterator yet). **`tests/test_fastapi_recipes.py`** and **`scripts/verify_doc_examples.py`** extended.
- **Arrow-native maps:** PyArrow **`map<utf8, …>`** arrays (and chunked) ingest for **`dict[str, T]`** columns; convert to Python **`dict`** cells. String keys only; **`strict`** checks scalar value types (nested map values: best-effort). Tests: **`tests/test_pyarrow_map_ingest.py`**. {doc}`SUPPORTED_TYPES` updated.
- **PySpark façade:** **`trim`**, **`abs`**, **`round`**, **`floor`**, **`ceil`** in **`pydantable.pyspark.sql.functions`** (and package **`__all__`**). {doc}`PYSPARK_PARITY` updated.
- **Constructor cleanup:** **`validate_data`** removed from **`DataFrame`** and **`DataFrameModel`**. Ingest depth uses **`trusted_mode`** only (`off` / `shape_only` / `strict`; omit for full per-element validation). Passing **`validate_data=...`** raises **`TypeError`**. Removed internal schema helpers **`_VALIDATE_DATA_KW_UNSET`**, **`_warn_validate_data_kw_deprecated`**, and **`_coerce_validate_data_kw`**. Direct callers of **`validate_columns_strict`** may still use **`validate_elements`** as a legacy bridge. Docs (**`DATAFRAMEMODEL`**, **`FASTAPI`**, **`SUPPORTED_TYPES`**, **`PERFORMANCE`**, etc.) describe **`trusted_mode`** only on constructors.
- **Dev:** **`pytest-asyncio`** in **`[dev]`**; **`asyncio_mode = auto`** in **`pyproject.toml`**.

### Tests

- **`tests/test_async_materialization.py`**, **`tests/test_pyarrow_map_ingest.py`**, **`tests/test_v015_features.py`**, **`tests/test_v015_constructor_api.py`**; extended **`tests/test_fastapi_recipes.py`**. **`tests/test_v014_features.py`**, **`tests/test_dataframe_model.py`**, **`tests/test_dataframe_ops.py`**: **`TypeError`** when **`validate_data`** is passed; trusted paths use **`trusted_mode`** only.

### Details

See {doc}`ROADMAP` **Shipped in 0.15.0**. Sync **`collect` / `to_dict` / `to_polars`** are unchanged aside from constructor kwargs (drop **`validate_data`**; use **`trusted_mode`**). You may replace manual **`asyncio.to_thread`** wrappers with **`acollect`** / **`ato_*`**.

**`rust_version()`** in the extension reports **`env!("CARGO_PKG_VERSION")`** so it matches **`pyproject.toml`** / **`Cargo.toml`**.

## [0.14.0] — 2026-03-23

### Highlights

- **Window `orderBy` null placement:** **`nulls_last`** on **`Window.partitionBy(...).orderBy(...)`** (per-column list or bool); framed windows use all keys; unframed Polars **`.over`** uses the first key for **`SortOptions`**. Docs: {doc}`WINDOW_SQL_SEMANTICS`, {doc}`INTERFACE_CONTRACT`.
- **Trusted `shape_only`:** **`pydantable.DtypeDriftWarning`** when data would fail **`strict`**; env **`PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS=1`** to silence. See {doc}`SUPPORTED_TYPES`.
- **`validate_data` deprecation:** explicit **`validate_data=`** without **`trusted_mode`** raises **`DeprecationWarning`** (removal shipped in **0.15.0**). See {doc}`DATAFRAMEMODEL`, {doc}`FASTAPI`.
- **PySpark façade:** **`dayofmonth`**, **`lower`**, **`upper`** in **`pydantable.pyspark.sql.functions`**. {doc}`PYSPARK_PARITY` updated.
- **FastAPI DX:** **`TestClient`** recipes and OpenAPI notes in {doc}`FASTAPI`; **`tests/test_fastapi_recipes.py`**; **`fastapi`** / **`httpx`** in **`[dev]`** and CI.
- **Hypothesis:** extra property test for **`with_columns`** identity; {doc}`DEVELOPER` documents running property tests.
- **Tests:** **`tests/test_v014_features.py`** covers **`DtypeDriftWarning`** (including multi-column drift), window **`lag`** / **`row_number`** with null sort order, FastAPI **422** / OpenAPI **`requestBody`**, and PySpark **`dayofmonth` / `lower` / `upper`**. (Constructor **`validate_data`** was deprecated here and removed in **0.15.0** — see changelog **\[0.15.0\]**.)

### Details

See {doc}`ROADMAP` **Shipped in 0.14.0**.

## [0.13.0] — 2026-03-23

### Highlights

- **Stabilization + combined scope:** {doc}`FASTAPI` — **`trusted_mode`** / **`validate_data`**, column-shaped **`dict[str, list]`** bodies, **sync** materialization and pointers forward to async work (shipped in **0.15.0**); trust-boundary guidance for large / pre-validated tables and **Polars** / **Arrow**; install notes for PyPI wheels vs git builds.
- **Sync-only I/O (at time of 0.13.0):** {doc}`EXECUTION` and {doc}`PERFORMANCE` described **blocking** materialization; **async** APIs arrived in **0.15.0** ({doc}`EXECUTION`, {doc}`FASTAPI`). Tuning text prefers **`trusted_mode`** alongside **`validate_data`**.
- **Window semantics (docs):** null ordering and **`CURRENT ROW`** / peer framing in {doc}`WINDOW_SQL_SEMANTICS` and {doc}`INTERFACE_CONTRACT`; `Window` docstring in `window_spec.py`. (**User-facing `NULLS FIRST` / `LAST`** shipped in **0.14.0**.)
- **Trusted `strict` + PyArrow:** `isinstance(..., pa.Array | pa.ChunkedArray)` in trusted buffers (concrete array types such as `Int64Array`); stricter scalars for **int**, **float**, **decimal**, **enum**, **uuid**, and temporal Arrow types. Tests in `tests/test_trusted_strict_pyarrow.py`; **`pyarrow>=14`** in **`[dev]`** and CI. (**`shape_only` drift warnings** shipped in **0.14.0**.)
- **Performance:** `benchmarks/framed_window_bench.py`, `benchmarks/trusted_polars_ingest_bench.py`; {doc}`PERFORMANCE` table and cross-links.
- **Discoverability:** {doc}`index`, {doc}`README` roadmap table, and cross-links across {doc}`INTERFACE_CONTRACT`, {doc}`WINDOW_SQL_SEMANTICS`, {doc}`POLARS_TRANSFORMATIONS_ROADMAP`.
- **CI:** **`RUSTSEC-2025-0141`** audit-step comment; GitHub Actions versions reviewed (**`actions/cache@v4`**, etc.).
- **Examples:** `scripts/verify_doc_examples.py` covers new FastAPI patterns (trusted ingest + columnar body).

### Details

Release audit: `make check-full` and full **pytest** green with a **release** `maturin` build. No regressions to **`rangeBetween`**, trusted **`strict`**, or **`map_from_entries`** beyond documentation and **PyArrow** **`strict`** hardening above.

**Roadmap (editorial):** **0.13.0** ships the documentation-first stabilization track together with the scope formerly planned as **Remaining in 0.13.x** / **0.14.0**. Async materialization shipped in **0.15.0** (see changelog **\[0.15.0\]**).

See {doc}`FASTAPI`, {doc}`EXECUTION`, {doc}`PERFORMANCE`, {doc}`ROADMAP`, and {doc}`INTERFACE_CONTRACT`.

## [0.12.0] — 2026-03-22

### Highlights

- **Multi-key `rangeBetween`:** aggregate window frames may use multiple `orderBy` columns; sort is lexicographic and **range bounds apply to the first** sort key (PostgreSQL-style). Documented in {doc}`WINDOW_SQL_SEMANTICS`.
- **Trusted `strict` ingest:** Polars columns are matched structurally to nested annotations (`list` / `dict[str, T]` / nested `Schema` structs); columnar Python paths get the same nested shape checks.
- **Contracts and parity docs:** refresh `INTERFACE_CONTRACT`, PySpark UI/scorecard, roadmap; add duplicate-key policy for `map_from_entries` ({doc}`SUPPORTED_TYPES`).
- **Regression tests:** broader coverage for multi-key `rangeBetween` (desc/mixed `orderBy`, partitions, `date`/`datetime` axis, `window_mean`/`window_min`), PySpark window mirrors, trusted `strict` nested paths (Python + Polars), `map_from_entries` duplicate keys, and `DataFrame` / `DataFrameModel` strict parity.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`PARITY_SCORECARD`, {doc}`WINDOW_SQL_SEMANTICS`, and {doc}`ROADMAP`.

## [0.11.0] — 2026-03-23

### Highlights

- **Window range semantics v2:** `rangeBetween` supports numeric, `date`, `datetime`, and `duration` order keys (single `orderBy` key), with deterministic boundary-inclusive behavior.
- **Map ergonomics expanded:** add `map_from_entries()` and PySpark-compatible `element_at()` alias; map entry roundtrip coverage expanded.
- **Trusted ingest modes:** add explicit trusted modes (`shape_only`, `strict`) alongside compatibility with `validate_data`, including stricter nullability and dtype checks for trusted columnar paths.
- **Parity coverage expansion:** add dedicated DataFrame/DataFrameModel parity tests and additional PySpark map parity contracts.
- **Release hardening:** update docs/contracts and version metadata for the 0.11.0 line.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`SUPPORTED_TYPES`, and {doc}`ROADMAP`.

## [0.10.0] — 2026-03-23

### Highlights

- **Framed windows expanded:** framed execution now covers `window_mean`, `window_min`, `window_max`, `lag`, `lead`, `rank`, and `dense_rank` in addition to `row_number` / `window_sum`.
- **Map utilities:** add `map_keys()` and `map_values()` to complement `map_len`, `map_get`, and `map_contains_key`.
- **Parity and interop hardening:** PySpark parity tests for framed windows/map utilities plus trusted constructor coverage for Polars DataFrame input (`validate_data=False`).
- **Window range contracts tightened:** `rangeBetween` now enforces exactly one `orderBy` key for supported aggregate frames, with explicit typed errors.
- **Map v2 parity expanded:** add `map_entries()` and PySpark wrappers for `map_len`, `map_get`, and `map_contains_key`.
- **Trusted ingest hardening:** Polars trusted constructor path rejects nulls in non-nullable schema fields when `validate_data=False`.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`SUPPORTED_TYPES`, and {doc}`ROADMAP`.

## [0.9.0] — 2026-03-23

### Highlights

- **Bad-input ingest controls:** `ignore_errors=True` with `on_validation_errors=...` (`row_index`, `row`, `errors`) across `DataFrameModel` and `DataFrame` constructor paths.
- **Framed windows:** `rowsBetween` / `rangeBetween` frame metadata is wired through Python/PySpark/Rust; framed execution is supported for `row_number` / `window_sum` (`rangeBetween` on integer order keys, range offset computed from first `orderBy` key).
- **Map v2 values:** `dict[str, T]` map columns now support nested JSON-like value dtypes (lists/maps/structs and nullable unions), with `map_len`, `map_get`, and `map_contains_key` behavior preserved.
- **Release hardening:** expanded 0.9.0 edge-case tests and full quality-gate coverage (`make check-full`, docs example validation, Sphinx warnings-as-errors build).

### Details

See {doc}`DATAFRAMEMODEL`, {doc}`INTERFACE_CONTRACT`, {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.8.0] — 2026-03-23

### Highlights

- **Global row count:** `global_row_count()` and PySpark `functions.count()` with no column (`count(*)`-style) for `DataFrame.select`.
- **Casts:** `str` → `date` / `datetime` via `Expr.cast(...)` (Polars parsing); use `strptime` for fixed formats.
- **Maps:** `Expr.map_get(key)` / `map_contains_key(key)` on `dict[str, T]` columns (list-of-struct encoding).
- **Windows:** `window_min` / `window_max`; IR carries optional `WindowFrame::Rows` for future Spark-style `rowsBetween` (lowering not yet implemented).
- **Docs:** `INTERFACE_CONTRACT`, `PYSPARK_PARITY`, `SUPPORTED_TYPES` updates.

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, {doc}`ROADMAP`, and {doc}`INTERFACE_CONTRACT`.

### Testing

- Broader integration tests for 0.7.0 / 0.8.0 surfaces (`test_v070_features`, `test_v080_features`), including PySpark `F.count()` with no column.

### Documentation

- README feature bullets; {doc}`INTERFACE_CONTRACT` (global `select`); {doc}`POLARS_WORKFLOWS` (single-row globals example); {doc}`index`, {doc}`EXECUTION`, {doc}`PYSPARK_UI`, {doc}`PYSPARK_PARITY`, {doc}`PYSPARK_INTERFACE`.

## [0.7.0] — 2026-03-23

### Highlights

- **Global aggregates:** `global_count`, `global_min`, `global_max` for `DataFrame.select` (non-null `count`; Polars `min`/`max`); PySpark `functions.count` / `min` / `max` on typed columns.
- **Windows:** `lag` / `lead` (Polars `shift` + `.over(...)`); still no SQL-style `rowsBetween` / `rangeBetween` in the IR (see `INTERFACE_CONTRACT.md`).
- **Temporal:** `Expr.strptime` / `Expr.unix_timestamp`, PySpark `to_date(..., format=...)` and `unix_timestamp`; `dt_nanosecond` for `datetime` and `time`.
- **Maps / binary:** `Expr.map_len()`, `Expr.binary_len()` (byte length of `bytes` columns).

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.6.0] — 2026-03-22

### Highlights

- **Scalar types:** `datetime.time`, `bytes`, and homogeneous `dict[str, T]` map columns (Polars-backed I/O; map execution surface is intentionally small).
- **Windows:** `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean` with `Window.partitionBy(...).orderBy(...)` / `.spec()`; Rust `ExprNode::Window` + Polars `.over(...)`.
- **Global aggregates (Phase D):** `DataFrame.select(...)` with `global_sum` / `global_mean` or PySpark `functions.sum` / `avg` / `mean` on typed columns — single-row results.
- **PySpark façade:** `dropDuplicates(subset=...)`, date helpers (`year`, `month`, …, `to_date`), documentation parity fixes (`union`, types).

### Development and testing

- **`[dev]` optional dependencies** include `numpy` (for `collect(as_numpy=...)` tests), `pytest-cov`, `coverage`, `pytest-xdist`, and `polars`.
- **CI** runs parallel `pytest` on Linux, optional **coverage** XML on Ubuntu + Python 3.11, installs **Polars** on that leg so `to_polars()` tests run, runs **`scripts/verify_doc_examples.py`**, and uses **`GITHUB_ACTIONS`-scaled** performance guardrails.

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.5.0] — 2026

### Highlights

- **PydanTable** naming and docs alignment with the {doc}`ROADMAP` (0.5.x line).
- **Typed `DataFrameModel`** and **`DataFrame[Schema]`** with a Rust execution core (Polars-backed in the native extension).
- **Materialization:** `collect()` returns Pydantic row models; `to_dict()` / `collect(as_lists=True)` for columnar data; optional `to_polars()` with the `[polars]` extra.
- **Rich column types:** nested Pydantic models (structs), homogeneous `list[T]`, `uuid.UUID`, `decimal.Decimal`, `enum.Enum`, plus `explode`, `unnest`, and extended `Expr` helpers (see {doc}`SUPPORTED_TYPES`).

### Details

For phase history and future direction, see {doc}`ROADMAP` and {doc}`POLARS_TRANSFORMATIONS_ROADMAP`.
