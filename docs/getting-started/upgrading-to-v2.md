# Upgrading to v2 (2.0.0)

This page is the **migration guide** for the engines-focused **v2.0.0** release.

## What v2 is about

v2’s headline is **multi-engine workflows**:

- **Engine selection**: frames created from an engine-backed source (SQL/Mongo/Spark) can default to a matching engine.\n  Escape hatch: `engine_mode="default"`. Explicit `engine=` always wins.
- **Engine handoff**: switching engines is an explicit **materialize + re-root** boundary.\n  Use `to_native()` / `to_engine(...)` (and `to_sql_engine()` / `to_mongo_engine()` / `to_spark_engine()`).

See also: [Execution](../user-guide/execution.md) and engine guides:\n[SQL engine](../integrations/engines/sql.md), [Mongo engine](../integrations/engines/mongo.md), [Spark engine](../integrations/engines/spark.md).

## Engine selection (`engine_mode`) migration

If you previously relied on always using the process-wide default engine, v2 adds an explicit switch:

```python
# v2: force the default engine, even when reading from an engine-backed source
df = SqlDataFrame[Row].from_sql_table(table, sql_config=cfg, engine_mode="default")
```

Precedence (v2):

1. `engine=` wins.
2. Else `engine_mode="default"` forces `get_default_engine()`.
3. Else `engine_mode="auto"` uses the source-matching engine (SQL/Mongo/Spark).

## Engine handoff migration

In v1, it was easy to accidentally mix engine-specific expectations. In v2, the supported way to “flow” between engines is explicit:

### SQL → native transforms

```python
sql_df = SqlDataFrame[Row].from_sql_table(table, sql_engine=eng).sort("id")
native_df = sql_df.to_native()
out = native_df.with_columns(id2=native_df.id * 2).select("id2").to_dict()
```

### native → SQL engine

```python
sql_df = df.to_sql_engine(sql_config=cfg)
out = sql_df.sort("id").to_dict()
```

### Mongo → native transforms

```python
df = MongoDataFrame[Row].from_collection(coll).sort("x")
native_df = df.to_native()
out = native_df.with_columns(x2=native_df.x * 2).select("x2").to_dict()
```

### Spark → native transforms

```python
df = SparkDataFrame[Row].from_spark_dataframe(spark_df)
native_df = df.to_native()
out = native_df.select("x").to_dict()
```

## Execution policy and observability (v2)

- Terminals such as **`collect()`** / **`to_dict()`** accept **`execution_policy=`** (`"fallback_to_native"` default, `"pushdown"` / `"error_on_fallback"` for strict behavior). See [Engine policy](../user-guide/engine-policy.md).
- **`engine_report()`** and **`explain_execution()`** on **`DataFrame`** / **`DataFrameModel`** summarize the active engine and capabilities for logging.

## 2.0.0 removals (breaking)

The following removals are documented in [Versioning](../semantics/versioning.md) under **Removed in 2.0.0**:

- `as_polars=` on `collect()` / `acollect()` (use `to_polars()` / `ato_polars()` instead).
- Legacy string-SQL I/O aliases (`fetch_sql`, `iter_sql`, `write_sql`, async variants, batch variants).\n  Use `*_sql_raw` / `*_sqlmodel` instead.
- Deprecated lazy SQL kwarg `moltres_engine=` (use `sql_engine=`).
- Deprecated Mongo `Entei*` names (use `Mongo*`).

## Pre-upgrade checklist

- Run your tests with `PYTHONWARNINGS=error::DeprecationWarning` on v1 to find remaining deprecated API usage.\n- Upgrade and ensure `mkdocs build --strict`, `make check-full`, and the full test suite are green.

