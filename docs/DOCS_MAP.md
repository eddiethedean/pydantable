# Docs map

Use this page when you know **what you need to do**, but not **where it is documented**.

## Start here (by goal)

- **Model a table schema and do typed transforms**: [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) (primary user guide).
- **Build an API with FastAPI**: [GOLDEN_PATH_FASTAPI](/GOLDEN_PATH_FASTAPI.md) (shortest runnable path), then [FASTAPI](/FASTAPI.md) (common patterns + reference tables), and [FASTAPI_ADVANCED](/FASTAPI_ADVANCED.md) (less-common async/I/O patterns). Roadmap → [FASTAPI_ENHANCEMENTS](/FASTAPI_ENHANCEMENTS.md); observability → [fastapi_observability](/cookbook/fastapi_observability.md); background **`submit`** → [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md); lazy async scans → [async_lazy_pipeline](/cookbook/async_lazy_pipeline.md). **Example layout:** `docs/examples/fastapi/service_layout/`. **Integration tests:** **`pydantable.testing.fastapi`** (**`fastapi_test_client`**, **`fastapi_app_with_executor`**).
- **Understand execution/materialization costs**: [EXECUTION](/EXECUTION.md); **four plan materialization modes** (blocking / async / deferred / chunked): [MATERIALIZATION](/MATERIALIZATION.md).
- **Choose an I/O entrypoint**: [IO_DECISION_TREE](/IO_DECISION_TREE.md), then the per-format pages under [IO_OVERVIEW](/IO_OVERVIEW.md).
- **Optional swap-in engines (keep the `DataFrame` API; different backends)**: [MOLTRES_SQL](/MOLTRES_SQL.md) (**`pydantable[sql]`**, SQL), [MONGO_ENGINE](/MONGO_ENGINE.md) (**`pydantable[mongo]`** — **PyMongo**, **Beanie**, Mongo plan stack; lazy **`MongoDataFrame`** via **`from_beanie`** / **`from_beanie_async`** / **`from_collection`**; eager **`dict[str, list]`** via **`fetch_mongo`** / **`write_mongo`** and async mirrors — see **PyMongo surface area** there), [SPARK_ENGINE](/SPARK_ENGINE.md) (**`pydantable[spark]`** — **raikou-core** `SparkDataFrame` / `SparkDataFrameModel`, **SparkDantic** schema helpers under **`pydantable.pyspark.sparkdantic`**, troubleshooting vs [PYSPARK_UI](/PYSPARK_UI.md) façade), [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md) (ship your own **`ExecutionEngine`**).
- **Beanie-first Mongo** (ODM queries, projections, links, hooks, **`afetch_beanie`** / **`awrite_beanie`**): [BEANIE](/BEANIE.md).
- **Know what behavior is guaranteed (joins/nulls/windows/order)**: [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md).
- **Understand versioning/semver expectations**: [VERSIONING](/VERSIONING.md).
- **Learn supported dtypes and edge cases**: [SUPPORTED_TYPES](/SUPPORTED_TYPES.md).
- **Debug plans / observe runtime / discover extension points**: [PLAN_AND_PLUGINS](/PLAN_AND_PLUGINS.md).

## Reference paths (by topic)

### Data I/O

- **Overview**: [IO_OVERVIEW](/IO_OVERVIEW.md)
- **Decision tree**: [IO_DECISION_TREE](/IO_DECISION_TREE.md)
- **Formats**: [IO_PARQUET](/IO_PARQUET.md), [IO_CSV](/IO_CSV.md), [IO_NDJSON](/IO_NDJSON.md), [IO_JSON](/IO_JSON.md), [IO_IPC](/IO_IPC.md)
- **Transports**: [IO_SQL](/IO_SQL.md) (**SQLModel:** **`fetch_sqlmodel`** / **`write_sqlmodel`**, runnable **`docs/examples/io/sql_sqlite_sqlmodel_*.py`**; **`sqlmodel_columns`**, **`DataFrameModel.assert_sqlmodel_compatible`**; **string SQL:** **`fetch_sql_raw`** / **`write_sql_raw`**; deprecated **`fetch_sql`** / **`write_sql`**; **`DataFrameModel`** SQLModel helpers), [IO_HTTP](/IO_HTTP.md), [IO_EXTRAS](/IO_EXTRAS.md)
- **SQLModel-first SQL I/O roadmap (phases 0–6; shipped in v1.13.0)**: [SQLMODEL_SQL_ROADMAP](/SQLMODEL_SQL_ROADMAP.md)
- **Planning transports and async stacks**: [DATA_IO_SOURCES](/DATA_IO_SOURCES.md)

### Semantics and contracts

- **Behavior contract**: [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md)
- **Windows**: [WINDOW_SQL_SEMANTICS](/WINDOW_SQL_SEMANTICS.md)
- **Why pydantable vs native Polars**: [WHY_NOT_POLARS](/WHY_NOT_POLARS.md)

### Alternate import surfaces

- **Pandas-shaped names**: [PANDAS_UI](/PANDAS_UI.md) (includes **`assign`/`merge`**, duplicate masks, **`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm().mean()`**, typed **`pivot`**). Integration tests: **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`**.
- **PySpark-shaped names**: [PYSPARK_UI](/PYSPARK_UI.md), [PYSPARK_INTERFACE](/PYSPARK_INTERFACE.md), [PYSPARK_PARITY](/PYSPARK_PARITY.md)

### Project & contribution

- **Roadmap**: [ROADMAP](/ROADMAP.md)
- **SQLModel-first SQL I/O roadmap**: [SQLMODEL_SQL_ROADMAP](/SQLMODEL_SQL_ROADMAP.md)
- **Custom dtypes**: [CUSTOM_DTYPES](/CUSTOM_DTYPES.md) — semantic scalar types via Pydantic v2 CoreSchema + `pydantable.dtypes.register_scalar`.
- **Strictness**: [STRICTNESS](/STRICTNESS.md) — per-column and nested validation strictness (Phase 4).
- **Service ergonomics**: [SERVICE_ERGONOMICS](/SERVICE_ERGONOMICS.md) — OpenAPI enrichments, alias ingestion, and redaction defaults (Phase 5).
- **JSON & structs (1.10.0) and local I/O (1.11.0):** release narratives in [CHANGELOG](/CHANGELOG.md) **1.10.0** / **1.11.0**; ongoing work in [ROADMAP](/ROADMAP.md); NDJSON cookbook [json_logs_unnest_export](/cookbook/json_logs_unnest_export.md); lazy scan audit {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`; entrypoints [IO_OVERVIEW](/IO_OVERVIEW.md), [IO_DECISION_TREE](/IO_DECISION_TREE.md), [DATA_IO_SOURCES](/DATA_IO_SOURCES.md).
- **Developer guide**: [DEVELOPER](/DEVELOPER.md)
- **Execution engine abstraction (ADR)**: [ADR-engines](/ADR-engines.md)
- **Custom engine package (third-party backends)**: [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md)
- **Lazy SQL DataFrame (`SqlDataFrame` / `SqlDataFrameModel`, optional `pydantable[sql]`)**: [MOLTRES_SQL](/MOLTRES_SQL.md)
- **Mongo engine (`pydantable[mongo]` + Beanie `Document` preferred)**: [MONGO_ENGINE](/MONGO_ENGINE.md) (**`MongoPydantableEngine`**, **`MongoRoot`** from the Mongo plan stack; **`from_beanie`** / **`from_beanie_async`** / **`sync_pymongo_collection`**; eager **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`**, **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**, optional **`is_async_mongo_collection`** / **`*_mongo_async`**).
- **Performance notes**: [PERFORMANCE](/PERFORMANCE.md)
- **Changelog**: [CHANGELOG](/CHANGELOG.md)
- **Troubleshooting / FAQ**: [TROUBLESHOOTING](/TROUBLESHOOTING.md)

### Errors and HTTP mapping (services)

- **`pydantable.errors`**: **`ColumnLengthMismatchError`**, **`PydantableUserError`** — used by strict column validation and mapped by **`register_exception_handlers`** in [FASTAPI](/FASTAPI.md) (see {ref}`fastapi-errors` there).

