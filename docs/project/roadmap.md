# Roadmap

**Current release: [1.19.2](changelog.md).** PydanTable is **stable 1.x** under [semantic versioning](../semantics/versioning.md).

This page lists **forward-looking** work. Shipped history and phase checklists live in [Roadmap history](history.md).

## Product direction

The public API stays **SQLModel-like**:

- **`DataFrameModel`** as the primary service-oriented table type
- Typed **`Expr`** transforms with schema propagation
- Default **Rust + Polars** execution; optional **SQL**, **Mongo**, and **Spark** engines
- Explicit **materialization** (rows, column dicts, Arrow/Polars when extras installed)

Guides: [DataFrameModel](../user-guide/dataframemodel.md), [Architecture](architecture.md).

## Near term (maintainer backlog)

| Area | Direction |
|------|-----------|
| **Engine parity** | Keep SQL/Mongo/Spark capability matrix and error messages aligned ([Engine parity](../integrations/engines/engine-parity.md)) |
| **Docs & DX** | Continue adoption-focused docs, API reference coverage, and verified examples |
| **Polars alignment** | Close high-value Expr/table gaps where schema story stays clear ([Polars alignment](polars-alignment/workflows.md)) |
| **FastAPI** | Optional ecosystem helpers per demand ([Enhancements](../integrations/fastapi/enhancements.md)) |

## Later (not scheduled)

- **Non-string map keys** (`dict[int, T]`, heterogeneous Arrow maps) — deferred; see [Supported types](../user-guide/supported-types.md)
- **JSON-native incremental async iterators** without full dict materialization first
- **Map transforms** (`map_filter`, `map_entries_sorted`, …) where Polars ops are stable
- **Window extras**: `percent_rank`, `ntile`, `cume_dist`, `first_value`/`last_value` with ignore-nulls
- **Table analytics helpers**: multi-column `quantile`/`median`, `corr`/`cov` (with materialization cost documented)

Full candidate list: [Future method candidates](history.md#future-expr-and-dataframe-method-candidates-not-scheduled) in history.

## After 1.x (major surfaces)

Not committed to a release train:

- **Distributed Spark execution** — compile plans to JVM Spark (separate from today's PySpark façade)
- **SQL-backed plan execution** — lower logical plans to database SQL beyond current lazy-SQL bridge scope

## 2.0.0 preview

Breaking removals planned for **2.0.0** (no fixed date): see [Migration guide](../getting-started/migration.md) and [Versioning](../semantics/versioning.md).

## How to influence the roadmap

Open a [GitHub issue](https://github.com/eddiethedean/pydantable/issues) with your use case, or contribute via [CONTRIBUTING.md](https://github.com/eddiethedean/pydantable/blob/main/CONTRIBUTING.md).
