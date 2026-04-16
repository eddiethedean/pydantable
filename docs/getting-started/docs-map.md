# Docs map

Use this page when you know **what you need to do**, but not **where it is documented**.

## Start here (by goal)

- **Model a table schema and do typed transforms**: [DATAFRAMEMODEL](../user-guide/dataframemodel.md) (primary user guide).
- **Build an API with FastAPI**: [GOLDEN_PATH_FASTAPI](../integrations/fastapi/golden-path.md) (shortest runnable path), then [FASTAPI](../integrations/fastapi/fastapi.md) (common patterns + reference tables), and [FASTAPI_ADVANCED](../integrations/fastapi/advanced.md) (less-common async/I/O patterns). Roadmap → [FASTAPI_ENHANCEMENTS](../integrations/fastapi/enhancements.md); observability → [fastapi_observability](../cookbook/fastapi_observability.md); background **`submit`** → [fastapi_background_tasks](../cookbook/fastapi_background_tasks.md); lazy async scans → [async_lazy_pipeline](../cookbook/async_lazy_pipeline.md). **Example layout:** `docs/examples/fastapi/service_layout/`. **Integration tests:** **`pydantable.testing.fastapi`** (**`fastapi_test_client`**, **`fastapi_app_with_executor`**).
- **Understand execution/materialization costs**: [EXECUTION](../user-guide/execution.md); **four plan materialization modes** (blocking / async / deferred / chunked): [MATERIALIZATION](../user-guide/materialization.md).
- **Choose an I/O entrypoint**: [IO_DECISION_TREE](../io/decision-tree.md), then the per-format pages under [IO_OVERVIEW](../io/overview.md).
- **Optional swap-in engines (keep the `DataFrame` API; different backends)**: [SQL_ENGINE](../integrations/engines/sql.md) (**`pydantable[sql]`**, SQL), [MONGO_ENGINE](../integrations/engines/mongo.md) (**`pydantable[mongo]`** — **PyMongo**, **Beanie**, Mongo plan stack; lazy **`MongoDataFrame`** via **`from_beanie`** / **`from_beanie_async`** / **`from_collection`**; eager **`dict[str, list]`** via **`fetch_mongo`** / **`write_mongo`** and async mirrors — see **PyMongo surface area** there), [SPARK_ENGINE](../integrations/engines/spark.md) (**`pydantable[spark]`** — **raikou-core** `SparkDataFrame` / `SparkDataFrameModel`, **SparkDantic** schema helpers under **`pydantable.pyspark.sparkdantic`**, troubleshooting vs [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md) façade), [CUSTOM_ENGINE_PACKAGE](../integrations/engines/custom-engine-package.md) (ship your own **`ExecutionEngine`**).
- **Beanie-first Mongo** (ODM queries, projections, links, hooks, **`afetch_beanie`** / **`awrite_beanie`**): [BEANIE](../integrations/engines/beanie.md).
- **Know what behavior is guaranteed (joins/nulls/windows/order)**: [INTERFACE_CONTRACT](../semantics/interface-contract.md).
- **Understand versioning/semver expectations**: [VERSIONING](../semantics/versioning.md).
- **Learn supported dtypes and edge cases**: [SUPPORTED_TYPES](../user-guide/supported-types.md).
- **Debug plans / observe runtime / discover extension points**: [PLAN_AND_PLUGINS](../user-guide/plan-and-plugins.md).

## Reference paths (by topic)

### Data I/O

- **Overview**: [IO_OVERVIEW](../io/overview.md)
- **Decision tree**: [IO_DECISION_TREE](../io/decision-tree.md)
- **Formats**: [IO_PARQUET](../io/parquet.md), [IO_CSV](../io/csv.md), [IO_NDJSON](../io/ndjson.md), [IO_JSON](../io/json.md), [IO_IPC](../io/ipc.md)
- **Transports**: [IO_SQL](../io/sql.md) (**SQLModel:** **`fetch_sqlmodel`** / **`write_sqlmodel`**, runnable **`docs/examples/io/sql_sqlite_sqlmodel_*.py`**; **`sqlmodel_columns`**, **`DataFrameModel.assert_sqlmodel_compatible`**; **string SQL:** **`fetch_sql_raw`** / **`write_sql_raw`**; deprecated **`fetch_sql`** / **`write_sql`**; **`DataFrameModel`** SQLModel helpers), [IO_HTTP](../io/http.md), [IO_EXTRAS](../io/extras.md)
- **SQLModel-first SQL I/O roadmap (phases 0–6; shipped in v1.13.0)**: [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md)
- **Planning transports and async stacks**: [DATA_IO_SOURCES](../io/data-io-sources.md)

### Semantics and contracts

- **Behavior contract**: [INTERFACE_CONTRACT](../semantics/interface-contract.md)
- **Windows**: [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md)
- **Why pydantable vs native Polars**: [WHY_NOT_POLARS](../semantics/why-not-polars.md)

### Alternate import surfaces

- **Pandas-shaped names**: [PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md) (includes **`assign`/`merge`**, duplicate masks, **`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm().mean()`**, typed **`pivot`**). Integration tests: **`tests/test_pandas_ui.py`**, **`tests/test_pandas_ui_popular_features.py`**.
- **PySpark-shaped names**: [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md), [PYSPARK_INTERFACE](../integrations/alternate-surfaces/pyspark-interface.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md)

### Project & contribution

- **Roadmap**: [ROADMAP](../project/roadmap.md)
- **SQLModel-first SQL I/O roadmap**: [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md)
- **Custom dtypes**: [CUSTOM_DTYPES](../user-guide/custom-dtypes.md) — semantic scalar types via Pydantic v2 CoreSchema + `pydantable.dtypes.register_scalar`.
- **Strictness**: [STRICTNESS](../user-guide/strictness.md) — per-column and nested validation strictness (Phase 4).
- **Service ergonomics**: [SERVICE_ERGONOMICS](../user-guide/service-ergonomics.md) — OpenAPI enrichments, alias ingestion, and redaction defaults (Phase 5).
- **JSON & structs (1.10.0) and local I/O (1.11.0):** release narratives in [CHANGELOG](../project/changelog.md) **1.10.0** / **1.11.0**; ongoing work in [ROADMAP](../project/roadmap.md); NDJSON cookbook [json_logs_unnest_export](../cookbook/json_logs_unnest_export.md); lazy scan audit {ref}`Polars 0.53 vs pydantable scan audit <local-io-audit>`; entrypoints [IO_OVERVIEW](../io/overview.md), [IO_DECISION_TREE](../io/decision-tree.md), [DATA_IO_SOURCES](../io/data-io-sources.md).
- **Developer guide**: [DEVELOPER](../project/developer.md)
- **Execution engine abstraction (ADR)**: [ADR-engines](../project/adrs/engines.md)
- **Custom engine package (third-party backends)**: [CUSTOM_ENGINE_PACKAGE](../integrations/engines/custom-engine-package.md)
- **Lazy SQL DataFrame (`SqlDataFrame` / `SqlDataFrameModel`, optional `pydantable[sql]`)**: [SQL_ENGINE](../integrations/engines/sql.md)
- **Mongo engine (`pydantable[mongo]` + Beanie `Document` preferred)**: [MONGO_ENGINE](../integrations/engines/mongo.md) (**`MongoPydantableEngine`**, **`MongoRoot`** from the Mongo plan stack; **`from_beanie`** / **`from_beanie_async`** / **`sync_pymongo_collection`**; eager **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`**, **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**, optional **`is_async_mongo_collection`** / **`*_mongo_async`**).
- **Performance notes**: [PERFORMANCE](../project/performance.md)
- **Changelog**: [CHANGELOG](../project/changelog.md)
- **Troubleshooting / FAQ**: [TROUBLESHOOTING](../getting-started/troubleshooting.md)

### Errors and HTTP mapping (services)

- **`pydantable.errors`**: **`ColumnLengthMismatchError`**, **`PydantableUserError`** — used by strict column validation and mapped by **`register_exception_handlers`** in [FASTAPI](../integrations/fastapi/fastapi.md) (see {ref}`fastapi-errors` there).

