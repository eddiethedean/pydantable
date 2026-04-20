# pydantable vs Polars

Polars is a DataFrame engine with a rich expression system and excellent performance.

pydantable is a **typed DataFrame layer** that (by default) executes using a native Rust core that is **Polars-backed**, but adds:

- **Pydantic-first schemas** for tabular models (`Schema`, `DataFrameModel`)
- A focus on **service boundaries** (FastAPI request/response patterns, error mapping, stable contracts)
- A plan/materialization model that makes execution cost and output shape explicit

If you’re already using Polars, the most useful framing is:

> Use **Polars** for pure data work. Use **pydantable** when that data work becomes a **typed contract** in a service (validation + semantics + stable output types).

## Choose Polars when…

- You want maximal expressiveness and performance for local analytics or pipelines.
- You don’t need a typed boundary (e.g., you’re not enforcing a schema contract at ingest/output).
- You want to stay inside the Polars ecosystem end-to-end (LazyFrame/DataFrame API, plugin ecosystem).

## Choose pydantable when…

- You want schema types that are native to your service stack (Pydantic models) and reusable across I/O and transforms.
- You want stable, documented semantics and interface guarantees.
- You want ergonomics designed around FastAPI (typed columnar bodies, error mapping, testing helpers).
  - Start: [Golden path (FastAPI)](../../integrations/fastapi/golden-path.md)

## “When should I drop down to Polars?”

pydantable is intentionally not a 1:1 replacement for the full Polars surface area.

Drop down to Polars for:

- **A missing transformation** you need today.
- **A performance hotspot** where you need the exact Polars primitive.
- **A tight inner loop** where you’re already working with Arrow/Polars objects.

Where to look in the docs:

- **Copy/interchange APIs and display costs**: [Execution](../../user-guide/execution.md)
- **Materialization modes (blocking/async/deferred/chunked)**: [Materialization](../../user-guide/materialization.md)
- **What pydantable guarantees about semantics**: [Interface contract](../../semantics/interface-contract.md)
- **Why this project exists beyond “just use Polars”**: [Why not Polars (end-to-end)?](../../semantics/why-not-polars.md)

## A quick “what’s different?” checklist

- **Schemas and typing**: pydantable attaches a schema (`Schema` / `DataFrameModel`) and tries to keep types meaningful across transforms.
  - Start: [DataFrameModel](../../user-guide/dataframemodel.md), [Typing](../../user-guide/typing.md)
- **Execution model**: many operations can remain lazy; you choose when and how to materialize.
  - Start: [Execution](../../user-guide/execution.md), [Materialization](../../user-guide/materialization.md)
- **Contracts**: pydantable documents interface guarantees explicitly (and treats them as a compatibility promise).
  - Start: [Interface contract](../../semantics/interface-contract.md)

