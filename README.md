# Pydantable

**Strongly-typed DataFrames for Python, powered by Rust.**

Pydantable combines Pydantic schemas with a query engine so DataFrame operations
can be validated and typed end-to-end.

## Status (v0.4.0 skeleton)

This release provides:
- Typed DataFrames core (`DataFrame[Schema]`) with runtime schema enforcement (low-level API)
- A typed expression AST with operator overloads (`df.age * 2`, `df.age > 10`, ...)
- `select()`, `with_columns()`, `filter()`, and `collect()` execution

Public API direction (next step): `DataFrameModel` as a SQLModel-like wrapper that:
- represents the whole DataFrame
- generates a per-row Pydantic model for FastAPI
- supports both input formats (columns and rows)
- returns new model types for every transformation (schema migration)

`collect()` executes in the Rust core for the currently supported skeleton
operations.

## Installation

From this repo:

```bash
pip install .
```

`pip install .` builds the Rust extension via `maturin` when toolchains are
available. The current skeleton requires the Rust extension for expression
typing and `collect()`.

## Quick start

```python
from pydantable import DataFrame, Schema

class User(Schema):
    id: int
    age: int

df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})

df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)
result = df4.collect()
print(result)  # {"id": [2], "age2": [60]}
```

## Supported Expression Dtypes (skeleton)
Rust enforces expression typing (at AST-build time) and executes expressions
with the following supported dtypes:
- `int`, `float`, `bool`, `str`

Null semantics are SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the result is `NULL` (typed as `Optional[bool]`)
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`; drops rows where the condition is `False` or `NULL`

These rules are enforced by the Rust core so that derived schemas and runtime
values stay aligned.

`Optional[T]` fields in your schema are supported:
- DataFrame input accepts `None` values for `Optional[T]`
- derived schemas produced by `select()` / `with_columns()` / `filter()` propagate nullability through expression result types

See the `DataFrameModel` design spec: `docs/DATAFRAMEMODEL.md`.

## Project Roadmap

See `docs/ROADMAP.md`.

## License

MIT

