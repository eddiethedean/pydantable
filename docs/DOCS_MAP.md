# Docs map

Use this page when you know **what you need to do**, but not **where it is documented**.

## Start here (by goal)

- **Model a table schema and do typed transforms**: {doc}`DATAFRAMEMODEL` (primary user guide).
- **Build an API with FastAPI**: {doc}`FASTAPI` (request bodies, responses, async patterns).
- **Understand execution/materialization costs**: {doc}`EXECUTION`.
- **Choose an I/O entrypoint**: {doc}`IO_DECISION_TREE`, then the per-format pages under {doc}`IO_OVERVIEW`.
- **Know what behavior is guaranteed (joins/nulls/windows/order)**: {doc}`INTERFACE_CONTRACT`.
- **Understand versioning/semver expectations**: {doc}`VERSIONING`.
- **Learn supported dtypes and edge cases**: {doc}`SUPPORTED_TYPES`.
- **Debug plans / observe runtime / discover extension points**: {doc}`PLAN_AND_PLUGINS`.

## Reference paths (by topic)

### Data I/O

- **Overview**: {doc}`IO_OVERVIEW`
- **Decision tree**: {doc}`IO_DECISION_TREE`
- **Formats**: {doc}`IO_PARQUET`, {doc}`IO_CSV`, {doc}`IO_NDJSON`, {doc}`IO_JSON`, {doc}`IO_IPC`
- **Transports**: {doc}`IO_HTTP`, {doc}`IO_SQL`, {doc}`IO_EXTRAS`
- **Planning transports and async stacks**: {doc}`DATA_IO_SOURCES`

### Semantics and contracts

- **Behavior contract**: {doc}`INTERFACE_CONTRACT`
- **Windows**: {doc}`WINDOW_SQL_SEMANTICS`
- **Why pydantable vs native Polars**: {doc}`WHY_NOT_POLARS`

### Alternate import surfaces

- **Pandas-shaped names**: {doc}`PANDAS_UI`
- **PySpark-shaped names**: {doc}`PYSPARK_UI`, {doc}`PYSPARK_INTERFACE`, {doc}`PYSPARK_PARITY`

### Project & contribution

- **Roadmap**: {doc}`ROADMAP`
- **Developer guide**: {doc}`DEVELOPER`
- **Performance notes**: {doc}`PERFORMANCE`
- **Changelog**: {doc}`changelog`

