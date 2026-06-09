# Migration and upgrading

Guidance for moving between pydantable versions and from pandas/Polars mental models.

## Upgrading within 1.x

**1.x** follows [semantic versioning](../semantics/versioning.md):

- **Patch** (`1.19.1` → `1.19.2`) — bug fixes, docs, no API breaks
- **Minor** (`1.18.x` → `1.19.0`) — additive APIs, backward-compatible defaults
- **Major** (`2.0.0`) — breaking removals (see below)

Always read [Changelog](../project/changelog.md) for the target release. Pin all three
packages together:

```bash
pip install "pydantable==1.19.2"
```

## Deprecated names (remove in 2.0.0)

| Deprecated | Replacement |
|------------|-------------|
| **`EnteiDataFrame`**, **`EnteiDataFrameModel`**, **`EnteiPydantableEngine`** | **`MongoDataFrame`**, **`MongoDataFrameModel`**, **`MongoPydantableEngine`** |
| **`moltres_engine=`**, **`sql_engine_from_config`** (old names) | **`sql_engine=`**, current SQL engine helpers |
| **`fetch_sql`**, **`iter_sql`**, **`write_sql`**, async mirrors | **`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**, or SQLModel-first **`fetch_sqlmodel`** / **`write_sqlmodel`** |
| **`as_polars=`** on **`collect`** / **`acollect`** | **`to_polars()`** / **`ato_polars()`** |

Runtime **`DeprecationWarning`** is emitted for these aliases in current 1.x.

## Planned 2.0.0 removals

Consolidated from [Versioning](../semantics/versioning.md) **Planned removals**:

- **`as_polars=`** keyword on collection methods — use interchange helpers instead
- Legacy string-SQL I/O names listed above
- Lazy SQL/Mongo façade rename table ( **`Entei*`** → **`Mongo*`** )

No committed date for **2.0.0**; track [Roadmap](../project/roadmap.md) and changelog.

## From pandas

pydantable is **not** a drop-in pandas replacement. Typical migration path:

1. Define a **`DataFrameModel`** (or **`DataFrame[Schema]`**) for your table shape
2. Replace **`df[col]`** filtering with **`df.filter(df.col > …)`** and typed **`Expr`**
3. Use **`collect()`** for row lists or **`to_dict()`** for columnar dicts
4. Prefer lazy **`read_*`** → transforms → **`write_*`** for large files

Optional pandas-shaped names: [Pandas UI](../integrations/alternate-surfaces/pandas-ui.md).
Comparison: [pydantable vs pandas](../positioning/comparisons/pandas.md).

## From Polars

pydantable uses Polars **inside** the native extension but exposes a **Pydantic-first**
contract layer:

- Schemas drive validation and service I/O, not Polars dtypes alone
- Materialization defaults to Pydantic rows or **`dict[str, list]`**, not a Polars frame
- Row order and join semantics are documented in [Interface contract](../semantics/interface-contract.md)

When you need Polars interchange: **`pip install "pydantable[polars]"`** then **`to_polars()`**.
Parity gaps and migration notes: [Polars alignment](../project/polars-alignment/workflows.md).

## From pre-1.0 (0.x) installs

If you still reference **`materialize_*`** renames or old I/O vocabulary:

- Eager file reads: **`materialize_parquet`** (not legacy **`read_parquet`** returning dicts)
- Lazy scans: **`DataFrameModel.read_parquet`** / **`read_*`**
- Full 0.x history: [Roadmap history](../project/history.md) and [Changelog](../project/changelog.md)

## Semantics that surprise migrators

| Topic | Where documented |
|-------|------------------|
| **`df.shape`** after lazy ops | [Interface contract](../semantics/interface-contract.md), [Troubleshooting](troubleshooting.md) |
| Row order not stable | Sort by keys in tests |
| Async cancellation | Does not stop in-flight Rust work |
| **`trusted_mode`** | [DataFrameModel](../user-guide/dataframemodel.md), [Best practices](../user-guide/best-practices.md) |

## Getting help

- [Troubleshooting / FAQ](troubleshooting.md)
- [Docs map](docs-map.md) — find a guide by task
- [GitHub issues](https://github.com/eddiethedean/pydantable/issues)
