# Docs map

Use this page when you know **what you need to do**, but not **where it is documented**.

## Start here (by goal)

- **Model a table schema and do typed transforms**: [DATAFRAMEMODEL](/user-guide/dataframemodel/) (primary user guide).
- **Build an API with FastAPI**: [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path/) (shortest runnable path), then [FASTAPI](/integrations/fastapi/fastapi/) (common patterns + reference tables), and [FASTAPI_ADVANCED](/integrations/fastapi/advanced/) (less-common async/I/O patterns). Roadmap → [FASTAPI_ENHANCEMENTS](/integrations/fastapi/enhancements/); observability → [fastapi_observability](/cookbook/fastapi_observability/); background **`submit`** → [fastapi_background_tasks](/cookbook/fastapi_background_tasks/); lazy async scans → [async_lazy_pipeline](/cookbook/async_lazy_pipeline/). **Example layout:** `docs/examples/fastapi/service_layout/`. **Integration tests:** **`pydantable.testing.fastapi`** (**`fastapi_test_client`**, **`fastapi_app_with_executor`**).
- **Understand execution/materialization costs**: [EXECUTION](/user-guide/execution/); **four plan materialization modes** (blocking / async / deferred / chunked): [MATERIALIZATION](/user-guide/materialization/).
- **Choose an I/O entrypoint**: [IO_DECISION_TREE](/io/decision-tree/), then the per-format pages under [IO_OVERVIEW](/io/overview/).
- **Optional swap-in engines (keep the `DataFrame` API; different backends)**: [SQL_ENGINE](/integrations/engines/sql/) (**`pydantable[sql]`**, SQL), [MONGO_ENGINE](/integrations/engines/mongo/) (**`pydantable[mongo]`** — **PyMongo**, **Beanie**, Mongo plan stack; lazy **`MongoDataFrame`** via **`from_beanie`** / **`from_beanie_async`** / **`from_collection`**; eager **`dict[str, list]`** via **`fetch_mongo`** / **`write_mongo`** and async mirrors — see **PyMongo surface area** there), [SPARK_ENGINE](/integrations/engines/spark/) (**`pydantable[spark]`** — **raikou-core** `SparkDataFrame` / `SparkDataFrameModel`, **SparkDantic** schema helpers under **`pydantable.pyspark.sparkdantic`**, troubleshooting vs [PYSPARK_UI](/integrations/alternate-surfaces/pyspark-ui/) façade), [CUSTOM_ENGINE_PACKAGE](/integrations/engines/custom-engine-package/) (ship your own **`ExecutionEngine`**).
- **Beanie-first Mongo** (ODM queries, projections, links, hooks, **`afetch_beanie`** / **`awrite_beanie`**): [BEANIE](/integrations/engines/beanie/).
- **Know what behavior is guaranteed (joins/nulls/windows/order)**: [INTERFACE_CONTRACT](/semantics/interface-contract/).
- **Understand versioning/semver expectations**: [VERSIONING](/semantics/versioning/).
- **Learn supported dtypes and edge cases**: [SUPPORTED_TYPES](/user-guide/supported-types/).
- **Debug plans / observe runtime / discover extension points**: [PLAN_AND_PLUGINS](/user-guide/plan-and-plugins/).

## Reference paths (by topic)

### Data I/O

- **Overview**: [IO_OVERVIEW](/io/overview/)
- **Decision tree**: [IO_DECISION_TREE](/io/decision-tree/)
- **Formats**: [IO_PARQUET](/io/parquet/), [IO_CSV](/io/csv/), [IO_NDJSON](/io/ndjson/), [IO_JSON](/io/json/), [IO_IPC](/io/ipc/)
- **Transports**: [IO_SQL](/io/sql/) (**SQLModel:** **`fetch_sqlmodel`** / **`write_sqlmodel`**, runnable **`docs/examples/io/sql_sqlite_sqlmodel_*.py`**; **`sqlmodel_columns`**, **`DataFrameModel.assert_sqlmodel_compatible`**; **string SQL:** **`fetch_sql_raw`** / **`write_sql_raw`**; deprecated **`fetch_sql`** / **`write_sql`**; **`DataFrameModel`** SQLModel helpers), [IO_HTTP](/io/http/), [IO_EXTRAS](/io/extras/)
- **SQLModel-first SQL I/O roadmap (phases 0–6; shipped in v1.13.0)**: [SQLMODEL_SQL_ROADMAP](/project/sqlmodel-sql-roadmap/)
- **Planning transports and async stacks**: [DATA_IO_SOURCES](/io/data-io-sources/)

### Semantics and contracts

- **Behavior contract**: [INTERFACE_CONTRACT](/semantics/interface-contract/)
- **Windows**: [WINDOW_SQL_SEMANTICS](/semantics/window-sql-semantics/)
- **Why pydantable vs native Polars**: [WHY_NOT_POLARS](/semantics/why-not-polars/)

### Alternate import surfaces

- **Pandas-shaped names**: [PANDAS_UI](/integrations/alternate-surfaces/pandas-ui/) (includes **`assign`/`merge`**, duplicate masks, **`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm().mean()`**, typed **`pivot`**). Integration tests: **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`**.
- **PySpark-shaped names**: [PYSPARK_UI](/integrations/alternate-surfaces/pyspark-ui/), [PYSPARK_INTERFACE](/integrations/alternate-surfaces/pyspark-interface/), [PYSPARK_PARITY](/integrations/alternate-surfaces/pyspark-parity/)

### Project & contribution

- **Roadmap**: [ROADMAP](/project/roadmap/)
- **SQLModel-first SQL I/O roadmap**: [SQLMODEL_SQL_ROADMAP](/project/sqlmodel-sql-roadmap/)
- **Custom dtypes**: [CUSTOM_DTYPES](/user-guide/custom-dtypes/) — semantic scalar types via Pydantic v2 CoreSchema + `pydantable.dtypes.register_scalar`.
- **Strictness**: [STRICTNESS](/user-guide/strictness/) — per-column and nested validation strictness (Phase 4).
- **Service ergonomics**: [SERVICE_ERGONOMICS](/user-guide/service-ergonomics/) — OpenAPI enrichments, alias ingestion, and redaction defaults (Phase 5).
- **JSON & structs (1.10.0) and local I/O (1.11.0):** release narratives in [CHANGELOG](/project/changelog/) **1.10.0** / **1.11.0**; ongoing work in [ROADMAP](/project/roadmap/); NDJSON cookbook [json_logs_unnest_export](/cookbook/json_logs_unnest_export/); lazy scan audit {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`; entrypoints [IO_OVERVIEW](/io/overview/), [IO_DECISION_TREE](/io/decision-tree/), [DATA_IO_SOURCES](/io/data-io-sources/).
- **Developer guide**: [DEVELOPER](/project/developer/)
- **Execution engine abstraction (ADR)**: [ADR-engines](/project/adrs/engines/)
- **Custom engine package (third-party backends)**: [CUSTOM_ENGINE_PACKAGE](/integrations/engines/custom-engine-package/)
- **Lazy SQL DataFrame (`SqlDataFrame` / `SqlDataFrameModel`, optional `pydantable[sql]`)**: [SQL_ENGINE](/integrations/engines/sql/)
- **Mongo engine (`pydantable[mongo]` + Beanie `Document` preferred)**: [MONGO_ENGINE](/integrations/engines/mongo/) (**`MongoPydantableEngine`**, **`MongoRoot`** from the Mongo plan stack; **`from_beanie`** / **`from_beanie_async`** / **`sync_pymongo_collection`**; eager **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`**, **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**, optional **`is_async_mongo_collection`** / **`*_mongo_async`**).
- **Performance notes**: [PERFORMANCE](/project/performance/)
- **Changelog**: [CHANGELOG](/project/changelog/)
- **Troubleshooting / FAQ**: [TROUBLESHOOTING](/getting-started/troubleshooting/)

### Errors and HTTP mapping (services)

- **`pydantable.errors`**: **`ColumnLengthMismatchError`**, **`PydantableUserError`** — used by strict column validation and mapped by **`register_exception_handlers`** in [FASTAPI](/integrations/fastapi/fastapi/) (see {ref}`fastapi-errors` there).

