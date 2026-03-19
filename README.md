# Pydantable

**The dataframe layer built for FastAPI + Pydantic services.**

Pydantable gives you typed dataframe transformations with Rust execution, while
keeping your Pydantic models as the source of truth for API contracts and
validation.

## Why Pydantable

- **FastAPI-native contracts**: Use Pydantic schemas to define request/response types and dataframe shape in one place.
- **Safer transformations**: Column typing, nullability, and expression errors are checked early (at AST build time).
- **Performance-ready core**: Execute plans in Rust without giving up Python ergonomics.

## Best Fit

Pydantable is ideal for teams building FastAPI backends that need:

- typed data pipelines between API boundaries and business logic
- schema-safe transformations that evolve cleanly over time
- high performance execution with a Python-first developer experience

## Status (v0.4.0 skeleton)

This release provides:
- `DataFrameModel` as the primary FastAPI-facing API
- Typed DataFrames core (`DataFrame[Schema]`) as the lower-level API
- A typed expression AST with operator overloads (`df.age * 2`, `df.age > 10`, ...)
- `select()`, `with_columns()`, `filter()`, and `collect()` execution

`DataFrameModel` currently:
- represents the whole DataFrame
- generates a per-row Pydantic model for FastAPI
- supports both input formats (columns and rows)
- wraps `select()`, `with_columns()`, and `filter()`
- has Phase 2 expression parity with `DataFrame[Schema]` (including reflected arithmetic like `2 + df.age`)
- has Phase 3 transformation guarantees (schema migration, collision replacement, and input-format parity)
- has Phase 4 logical-plan boundary guarantees (Rust-owned plan validation + schema metadata contract)

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
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int

df = User({"id": [1, 2], "age": [20, 30]})

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

Phase 2 expression system status:
- parity verified across `DataFrameModel` and lower-level `DataFrame[Schema]`
- invalid combinations fail at AST-build time with typed errors
- derived schema nullability/dtypes are validated through chained transforms

Phase 3 transformation status:
- `select()`, `with_columns()`, and `filter()` transformation contract is locked
- `with_columns()` collision replacement semantics are verified
- row-input and column-input transformation parity is validated

Phase 4 logical-plan status:
- Rust is the source of truth for remaining transformation-time validation (`select` projection arity included)
- Python derived schema migration consumes Rust metadata descriptors (`base`, `nullable`)
- plan/schema contract is validated with Rust-side and Python integration tests

Phase 5 execution-engine status:
- `collect()` now executes through Rust Polars LazyFrame lowering for `select`, `with_columns`, and `filter`
- `DataFrameModel` and `DataFrame[Schema]` API behavior is preserved at the Python boundary
- integration parity tests and a baseline benchmark harness are included

Null semantics are SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the result is `NULL` (typed as `Optional[bool]`)
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`; drops rows where the condition is `False` or `NULL`

These rules are enforced by the Rust core so that derived schemas and runtime
values stay aligned.

`Optional[T]` fields in your schema are supported:
- DataFrame input accepts `None` values for `Optional[T]`
- derived schemas produced by `select()` / `with_columns()` / `filter()` propagate nullability through expression result types

See:
- `docs/FASTAPI.md` for full FastAPI integration examples
- `docs/DATAFRAMEMODEL.md` for the `DataFrameModel` design spec
- `docs/WHY_NOT_POLARS.md` for positioning and trade-offs vs Polars
- `docs/DEVELOPER.md` for local setup and contribution workflow

## Project Roadmap

See `docs/ROADMAP.md`.

## License

MIT

