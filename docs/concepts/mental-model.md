# Mental model

This page is the “map” of pydantable: the core concepts and how they relate.

If you’re new, read this once, then jump to the [Five-minute tour](../getting-started/quickstart.md) or [DataFrameModel](../user-guide/dataframemodel.md).

## The four core nouns

### `Schema` (row shape)

`Schema` is a Pydantic-style row model used to describe the **shape and types** of a table.

- Used with `DataFrame[RowSchema]`
- Used as an output type for materialized rows (e.g. `collect()`)

See: [DataFrameModel](../user-guide/dataframemodel.md) (it covers `Schema` as part of the typed table story).

### `DataFrame[T]` (typed table)

`DataFrame[T]` is a typed table whose columns match the schema `T`.

You can create a `DataFrame` from columnar data, from I/O helpers, or from optional engine-specific sources.

Start here:

- [Five-minute tour](../getting-started/quickstart.md)
- [Execution](../user-guide/execution.md)

### `Expr` (typed expressions)

`Expr` is how you refer to and compute columns (e.g., `df.score > 8.0`).

Expressions are designed to remain type-aware so transforms can stay typed and composable.

Start here:

- [Typing](../user-guide/typing.md)
- [Selectors](../user-guide/selectors.md)

### `DataFrameModel` (a “table model”)

`DataFrameModel` is the higher-level “SQLModel-style” concept: a reusable typed table definition with methods for:

- ingest/validation rules
- typed transforms
- structured I/O entrypoints

If you’re building real pipelines (especially in services), this is usually the best place to start.

Start here: [DataFrameModel](../user-guide/dataframemodel.md)

## Execution: plans and materialization

pydantable is designed so that many operations can remain **lazy** until you explicitly choose to materialize.

Two key pages:

- [Execution](../user-guide/execution.md): what runs when, and what costs what
- [Materialization](../user-guide/materialization.md): the “how” of turning a plan into output

### Materialization outputs (what you get at the end)

Common end states include:

- **Pydantic rows** (a list of row models)
- **Columnar dicts** (`dict[str, list]`)
- **Engine-native objects** (e.g. Polars/Arrow) when the relevant extras are installed

See: [Execution](../user-guide/execution.md) (copy/interchange + display) and [Materialization](../user-guide/materialization.md) (modes).

### A common early gotcha: `shape` is not always “executed rows”

After lazy transforms, `df.shape` follows **root-buffer semantics** and may not reflect the number of rows that will materialize after execution.

This is documented as part of the compatibility contract:

- [Interface contract](../semantics/interface-contract.md)
- [Troubleshooting](../getting-started/troubleshooting.md)

## Engines and backends (what “engine” means here)

There are two related ideas:

1) **Execution backend**: where the plan runs (the default native engine is Polars-backed inside the Rust extension).
2) **Data sources / sinks**: how you read/write data (files, HTTP, SQL, etc.).

### Default execution

Out of the box, pydantable executes via the native extension.

If you want to understand the runtime and cost model:

- [Execution](../user-guide/execution.md)

### Optional swap-in engines

pydantable also supports optional engines that keep the DataFrame API but use different backends:

- [SQL engine](../integrations/engines/sql.md)
- [Mongo engine](../integrations/engines/mongo.md)
- [Spark engine](../integrations/engines/spark.md)
- [Engine parity](../integrations/engines/engine-parity.md)

### v2 engine selection + handoff (reader-matched engines)

In pydantable v2, there are two related “engine” concepts:

- **Engine selection**: when you create a frame from an engine-specific source (SQL table, Mongo collection, Spark DataFrame), the frame will **auto-match** to a source-appropriate engine when available.\n  Each integration exposes **`engine_mode="auto"|"default"`** so shared code can force the process-wide default engine.\n  **Explicit `engine=` always wins.**
- **Engine handoff**: switching engines is an **explicit boundary**.\n  Plans are engine-defined, so to move between backends you **materialize** and then **re-root** under a different engine using **`to_native()`** / **`to_engine(...)`** (and convenience helpers like **`to_sql_engine()`**, **`to_mongo_engine()`**, **`to_spark_engine()`**).

### I/O is a separate story (choose an entrypoint)

Even if you stay on the default execution engine, you still need to choose I/O entrypoints:

- [I/O decision tree](../io/decision-tree.md)
- [I/O overview](../io/overview.md)

## Where pydantable fits in the ecosystem

If you’re deciding between tools, start here:

- [Why pydantable?](../positioning/why-pydantable.md)
- [Comparisons](../positioning/comparisons/index.md)

