# Why pydantable?

pydantable is a **typed DataFrame layer** designed for **Python services** (especially FastAPI + Pydantic).

It combines:

- **Pydantic-first schemas** (`Schema` / `DataFrameModel`) for column typing, validation, and stable I/O contracts
- A DataFrame API built around **typed expressions** (`Expr`)
- A default native execution core (Rust + Polars) with an execution model oriented around **plans** and **materialization**

If you’re already productive with Polars or pandas, you can think of pydantable as the “typed contract + service ergonomics + predictable semantics” layer that sits **above** a DataFrame engine.

## Use pydantable when…

- **You’re building a service** (FastAPI or similar) and want request/response bodies that are **columnar, typed, and validated**.
  - Start here: [Golden path (FastAPI)](../integrations/fastapi/golden-path.md)
- **Your team wants a single schema source of truth** for:
  - ingest validation
  - transform typing
  - materialized outputs (Pydantic models, dict-of-lists, Arrow/Polars)
  - Start here: [DataFrameModel](../user-guide/dataframemodel.md)
- **You care about explicit semantics** (nullability, joins, windows, ordering guarantees, interface contracts).
  - Start here: [Interface contract](../semantics/interface-contract.md)
- **You want typed transforms that stay typed**, even across chains.
  - Start here: [Typing](../user-guide/typing.md)

## Don’t start with pydantable when…

- **You only need a local analytics DataFrame** and aren’t shipping a typed boundary (service contracts, stable schema).
  - Start with Polars: [Polars user guide](https://docs.pola.rs/user-guide/)
- **You need a fully interactive notebook-first workflow** and the main value is ad-hoc exploration.
  - Start with Polars or pandas; consider pydantable later when you want to “productize” a pipeline into a service contract.
- **You need distributed execution** as the primary execution model (cluster-first Spark workloads).
  - See integrations/alternates: [Spark engine](../integrations/engines/spark.md) and [PySpark UI façade](../integrations/alternate-surfaces/pyspark-ui.md)

## A useful mental model

If you only read one conceptual page, read this:

- [Mental model](../concepts/mental-model.md)

It ties together `Schema`, `DataFrameModel`, typed `Expr`, lazy execution, and the “materialization modes” you can choose from.

## Where pydantable fits (ecosystem map)

- **Polars**: execution engine + expressions; pydantable builds typed service contracts above it.
  - Comparison: [pydantable vs Polars](comparisons/polars.md)
  - Deeper rationale: [Why not Polars (end-to-end)?](../semantics/why-not-polars.md)
- **pandas**: ubiquitous API + ecosystem; pydantable offers a typed service-oriented layer and also provides a pandas-shaped façade.
  - Comparison: [pydantable vs pandas](comparisons/pandas.md)
  - Façade: [pandas UI](../integrations/alternate-surfaces/pandas-ui.md)
- **Pydantic**: schema/type system; pydantable uses it to define and validate tabular rows/columns.
  - Guide: [DataFrameModel](../user-guide/dataframemodel.md)
- **SQLModel / SQLAlchemy**: database modeling + SQL I/O; pydantable can read/write via SQL helpers and has an optional SQL execution engine.
  - Start: [SQL I/O](../io/sql.md), [SQL engine](../integrations/engines/sql.md)

## Next steps

- **Want the shortest runnable path?** [Five-minute tour](../getting-started/quickstart.md)
- **Want the core user guide?** [DataFrameModel](../user-guide/dataframemodel.md)
- **Not sure where something is documented?** [Docs map](../getting-started/docs-map.md)

