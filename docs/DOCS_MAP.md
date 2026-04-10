# Docs map

Use this page when you know **what you need to do**, but not **where it is documented**.

## Start here (by goal)

- **Model a table schema and do typed transforms**: {doc}`DATAFRAMEMODEL` (primary user guide).
- **Build an API with FastAPI**: {doc}`GOLDEN_PATH_FASTAPI` (shortest runnable path), then {doc}`FASTAPI` (common patterns + reference tables), and {doc}`FASTAPI_ADVANCED` (less-common async/I/O patterns). Roadmap → {doc}`FASTAPI_ENHANCEMENTS`; observability → {doc}`/cookbook/fastapi_observability`; background **`submit`** → {doc}`/cookbook/fastapi_background_tasks`; lazy async scans → {doc}`/cookbook/async_lazy_pipeline`. **Example layout:** `docs/examples/fastapi/service_layout/`. **Integration tests:** **`pydantable.testing.fastapi`** (**`fastapi_test_client`**, **`fastapi_app_with_executor`**).
- **Understand execution/materialization costs**: {doc}`EXECUTION`; **four plan materialization modes** (blocking / async / deferred / chunked): {doc}`MATERIALIZATION`.
- **Choose an I/O entrypoint**: {doc}`IO_DECISION_TREE`, then the per-format pages under {doc}`IO_OVERVIEW`.
- **Know what behavior is guaranteed (joins/nulls/windows/order)**: {doc}`INTERFACE_CONTRACT`.
- **Understand versioning/semver expectations**: {doc}`VERSIONING`.
- **Learn supported dtypes and edge cases**: {doc}`SUPPORTED_TYPES`.
- **Debug plans / observe runtime / discover extension points**: {doc}`PLAN_AND_PLUGINS`.
- **PlanFrame–first `DataFrameModel` API** (and `_df`-only ops): {doc}`PLANFRAME_FALLBACKS`; adapter coverage, backlog, and surveys: {doc}`PLANFRAME_ADAPTER_ROADMAP`.

## Reference paths (by topic)

### Data I/O

- **Overview**: {doc}`IO_OVERVIEW`
- **Decision tree**: {doc}`IO_DECISION_TREE`
- **Formats**: {doc}`IO_PARQUET`, {doc}`IO_CSV`, {doc}`IO_NDJSON`, {doc}`IO_JSON`, {doc}`IO_IPC`
- **Transports**: {doc}`IO_SQL` (**SQLModel:** **`fetch_sqlmodel`** / **`write_sqlmodel`**, runnable **`docs/examples/io/sql_sqlite_sqlmodel_*.py`**; **`sqlmodel_columns`**, **`DataFrameModel.assert_sqlmodel_compatible`**; **string SQL:** **`fetch_sql_raw`** / **`write_sql_raw`**; deprecated **`fetch_sql`** / **`write_sql`**; **`DataFrameModel`** SQLModel helpers), {doc}`IO_HTTP`, {doc}`IO_EXTRAS`
- **SQLModel-first SQL I/O roadmap (phases 0–6; shipped in v1.13.0)**: {doc}`SQLMODEL_SQL_ROADMAP`
- **Planning transports and async stacks**: {doc}`DATA_IO_SOURCES`

### Semantics and contracts

- **Behavior contract**: {doc}`INTERFACE_CONTRACT`
- **Windows**: {doc}`WINDOW_SQL_SEMANTICS`
- **Why pydantable vs native Polars**: {doc}`WHY_NOT_POLARS`

### Alternate import surfaces

- **Pandas-shaped names**: {doc}`PANDAS_UI` (includes **`assign`/`merge`**, duplicate masks, **`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm().mean()`**, typed **`pivot`**). Integration tests: **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`**.
- **PySpark-shaped names**: {doc}`PYSPARK_UI`, {doc}`PYSPARK_INTERFACE`, {doc}`PYSPARK_PARITY`

### Project & contribution

- **Roadmap**: {doc}`ROADMAP`
- **SQLModel-first SQL I/O roadmap**: {doc}`SQLMODEL_SQL_ROADMAP`
- **Custom dtypes**: {doc}`CUSTOM_DTYPES` — semantic scalar types via Pydantic v2 CoreSchema + `pydantable.dtypes.register_scalar`.
- **Strictness**: {doc}`STRICTNESS` — per-column and nested validation strictness (Phase 4).
- **Service ergonomics**: {doc}`SERVICE_ERGONOMICS` — OpenAPI enrichments, alias ingestion, and redaction defaults (Phase 5).
- **JSON & structs (1.10.0) and local I/O (1.11.0):** release narratives in {doc}`CHANGELOG` **1.10.0** / **1.11.0**; ongoing work in {doc}`ROADMAP`; NDJSON cookbook {doc}`/cookbook/json_logs_unnest_export`; lazy scan audit {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`; entrypoints {doc}`IO_OVERVIEW`, {doc}`IO_DECISION_TREE`, {doc}`DATA_IO_SOURCES`.
- **Developer guide**: {doc}`DEVELOPER`
- **Execution engine abstraction (ADR)**: {doc}`ADR-engines`
- **Custom engine package (third-party backends)**: {doc}`CUSTOM_ENGINE_PACKAGE`
- **Moltres SQL engine (`SqlDataFrame` / `SqlDataFrameModel`, optional `pydantable[moltres]`)**: {doc}`MOLTRES_SQL`
- **Performance notes**: {doc}`PERFORMANCE`
- **Changelog**: {doc}`CHANGELOG`
- **Troubleshooting / FAQ**: {doc}`TROUBLESHOOTING`

### Errors and HTTP mapping (services)

- **`pydantable.errors`**: **`ColumnLengthMismatchError`**, **`PydantableUserError`** — used by strict column validation and mapped by **`register_exception_handlers`** in {doc}`FASTAPI` (see {ref}`fastapi-errors` there).

