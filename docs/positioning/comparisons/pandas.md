# pydantable vs pandas

pandas is the default DataFrame library for Python and has a huge ecosystem.

pydantable is not trying to replace pandas for all analytics use cases. Its primary value is when your DataFrame work becomes part of a **typed service boundary** (Pydantic schemas + predictable semantics + execution controls).

## Choose pandas when…

- Your workflow is notebook-first and you rely on the pandas ecosystem directly.
- You want maximum compatibility with third-party libraries that expect pandas DataFrames.
- You don’t need strong typing and schema contracts across a transformation pipeline.

## Choose pydantable when…

- You want typed schemas and validation integrated with your service stack.
  - Start: [DataFrameModel](../../user-guide/dataframemodel.md)
- You want a single story for:
  - ingest validation
  - typed transforms
  - materialized typed outputs (Pydantic rows / dict-of-lists / Arrow / Polars)
  - Start: [Execution](../../user-guide/execution.md), [Materialization](../../user-guide/materialization.md)
- You want explicit semantics guarantees and a compatibility promise.
  - Start: [Interface contract](../../semantics/interface-contract.md)

## “Will it feel familiar?”

If you’re coming from pandas, you have two common onramps:

- **Learn the pydantable surface directly** (recommended for service code).
  - Start: [Five-minute tour](../../getting-started/quickstart.md)
- **Use pandas-shaped names** for common operations.
  - See: [pandas UI façade](../../integrations/alternate-surfaces/pandas-ui.md)

The façade is meant to ease migration and reduce context switching, but it is not a complete reimplementation of pandas.

## Interop guidance

- If a downstream library strictly requires a pandas DataFrame, it’s usually better to keep that portion of the pipeline in pandas.
- If the boundary is an API, database read/write, or a stable dataset contract, pydantable’s `DataFrameModel` tends to be a better fit.

## Related docs

- [Why pydantable?](../why-pydantable.md)
- [pydantable vs Polars](polars.md)
- [Typing](../../user-guide/typing.md)

