# Why Not Just Use Polars?

Short answer: you probably should use Polars for many workloads.

`pydantable` is not trying to replace Polars as a general-purpose dataframe
engine. It is trying to solve a different product problem:

- **FastAPI/Pydantic-native contracts**
- **typed schema evolution through transformations**
- **service-layer safety by default**

Polars remains a great execution engine choice, and `pydantable` can sit above
it conceptually as an API/backend contract layer.

**Python package:** installing `pydantable` does **not** require the PyPI **`polars`**
package. Execution uses a **Rust** Polars engine inside `pydantable_native._core` (shipped by `pydantable-native`). If you want a
**Polars `DataFrame` in Python**, install the extra: `pip install 'pydantable[polars]'`
and use **`DataFrame.to_polars()`**.

## The real question

For a backend team, the practical question is usually:

> "Do we need just fast dataframe execution, or do we need execution + API
> contract safety + schema governance in one workflow?"

If execution alone is the goal, Polars is often enough.
If your pain is API boundary correctness and schema drift in FastAPI services,
that is where `pydantable` is intentionally opinionated.

## What Polars is excellent at

Polars is excellent when you want:

- high-performance analytical transformations
- broad dataframe feature coverage
- mature lazy execution patterns
- direct control over expression and query APIs

For notebook analysis, ETL-heavy jobs, and pure dataframe compute tasks, Polars
is usually the most direct tool.

## Where pydantable adds value

`pydantable` focuses on the backend integration gap:

- **Pydantic schema as source of truth**
  - the same type contract can drive validation and dataframe shape
- **Typed transformation surface**
  - expression/type errors fail early during AST build
- **Nullability-aware schema migration**
  - derived schemas preserve optionality and result dtypes
- **FastAPI-oriented workflow**
  - route models + transformation logic align instead of drifting

In other words, it optimizes for "safe backend data flows," not "maximum
standalone dataframe surface area."

## Trade-offs (be explicit)

Compared to using Polars directly, `pydantable` currently trades:

- **Breadth for safety**
  - fewer operations in the skeleton today
- **Control for convention**
  - stronger rules around dtype/null semantics
- **Direct engine use for layered abstraction**
  - one more API layer between app code and execution engine

Those trade-offs are intentional if your team values API contract reliability.

## Async and services

The **Python** `polars` package exposes a small async-oriented surface (for example
`LazyFrame.collect_async` and `collect_all_async`): they schedule **`collect`** work on a
**thread pool** so asyncio can keep running — similar in spirit to wrapping
`collect()` in **`asyncio.to_thread`**. Polars does not replace **Pydantic-typed** API
contracts or **`DataFrameModel`**-centric I/O.

`pydantable` focuses on **service-shaped** workflows: **typed** scan roots (**`aread_*`**),
schema evolution through transforms, **`acollect` / `ato_dict`**, and docs aimed at
**FastAPI** ([GOLDEN_PATH_FASTAPI](/GOLDEN_PATH_FASTAPI.md), [FASTAPI](/FASTAPI.md)). You can use Polars as an engine
under the hood without exposing Polars APIs at your HTTP boundary.

## Decision guide

Use **Polars directly** when:

- you need the widest dataframe feature set now
- your bottleneck is mostly analytical/compute throughput
- API contract typing is handled elsewhere and already stable

Use **pydantable** when:

- your FastAPI service logic repeatedly breaks on schema drift
- you want typed transformations tied to Pydantic models
- you want runtime and type-level expectations to align by default

Use **both together** when:

- you want `pydantable` for contracts and typed pipeline ergonomics
- and an underlying Rust/Polars-style engine for execution performance

## A concrete backend example

If your endpoint receives data, transforms it, and returns a typed response:

- with Polars alone, you still need to design/maintain validation + response
  model synchronization manually
- with `pydantable`, the schema contract and transformation typing are designed
  to move together

That is the product wedge.

## Positioning statement

`pydantable` is:

> "The typed dataframe contract layer for FastAPI services."

It is not:

> "A replacement for Polars in every dataframe workload."

Both can be true: Polars can be the engine, while `pydantable` is the safety and
integration layer your API team works against.

