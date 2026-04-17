# Engine API parity (typed surface)

This page tracks **API parity across execution engines** for the *typed* `pydantable`
surface (`DataFrame[Schema]` / `DataFrameModel`), independent of alternate naming
surfaces (`pydantable.pandas`, `pydantable.pyspark`).

It is intentionally **smaller** than the full [Interface contract](../../semantics/interface-contract.md):
this is a checklist-style matrix used to prevent drift between engines.

## Contract: typed-first vs engine-native expressions

PydanTable supports two kinds of expression inputs:

- **Typed expressions**: `pydantable.expressions.Expr` (built by the Rust expression runtime).
- **Engine-native expressions**: backend-specific expression objects (e.g. `pyspark.sql.Column`).

**Policy (current):**

- **Core typed API** (`pydantable.DataFrame`): typed expressions (`Expr`) everywhere.
- **Engine-backed DataFrames** may accept **engine-native** expressions when their engine
  executes outside the native Rust expression runtime.
- **Alternate surfaces** (`pydantable.pandas`, `pydantable.pyspark`) provide ergonomic
  wrappers and may accept typed expressions even when the underlying engine-backed class
  uses engine-native expressions.

In other words: **typed-first for the core**, **engine-native where required**, with
explicit wrappers/adapters for consistency.

## Transform surface matrix (high-signal core ops)

Legend:

- **Y**: supported with typed expressions (`Expr`)
- **N**: not supported
- **Native**: supported but expects engine-native expressions

| Transform | Core (native) | SQL | Mongo | Spark |
|---|---:|---:|---:|---:|
| `select` | Y | Y | Y | Y |
| `with_columns` | Y | Y | Y | **Native** |
| `filter` | Y | Y | Y | **Native** |
| `join` | Y | Y | Y | Y |
| `group_by` | Y | Y | Y | Y |
| `sort` | Y | Y | Y | Y |
| `limit` | Y | Y | Y | Y |
| `distinct` / `unique` | Y | Y | Y | Y |
| `pivot` | Y | Y | Y | Y |
| `explode` | Y | Y | Y | Y |
| `concat` | Y | Y | Y | Y |

Notes:

- **Spark** (`SparkDataFrame`) is backed by `raikou-core` and expects Spark-native expressions
  for `filter` / `with_columns`. Use the PySpark-shaped wrapper export to keep the typed `Expr`
  experience (see below).

## Recommended wrappers for consistency

When you want a **consistent UI** across engines, prefer these wrappers:

- **Pandas-shaped**: `pydantable.pandas.{SqlDataFrame,SparkDataFrame,MongoDataFrame}`
- **PySpark-shaped**: `pydantable.pyspark.{SqlDataFrame,SparkDataFrame,MongoDataFrame}`

These wrappers exist to keep method names and (where feasible) expression types consistent.

## Engine-specific typed methods (additive)

Each engine may also expose **additive** methods that preserve PydanTable’s typing
by validating against the schema and keeping raw-string escape hatches explicit.

- **SQL** (`SqlDataFrame`):
  - `from_sql(selectable, ...)`: build a lazy root from an existing SQLAlchemy selectable.
  - `where(whereclause)`: push down a SQLAlchemy `WHERE` clause with schema validation.
- **Mongo** (`MongoDataFrame`):
  - `match(filter: dict[str, Any])`: `$match`-style filter pushdown with schema key validation.
  - `project(fields: Sequence[str] | dict[str, int])`: typed projection (schema update).
- **Spark** (`SparkDataFrame`):
  - `where_native(condition)`: explicit engine-native filter (PySpark `Column`).
  - `select_native(*cols)`: explicit engine-native projection for simple column refs.

