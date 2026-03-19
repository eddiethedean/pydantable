# DataFrameModel (SQLModel-like)

This doc describes the `DataFrameModel` public API for `pydantable`: a container
that represents the **whole DataFrame** while exposing a **per-row Pydantic model**
for FastAPI integration and row-level validation.

The goal is to keep the query-building/typing story (`select`, `with_columns`,
`filter`) while making DataFrames feel native in typical Pydantic/FastAPI
workflows.

## Terms

- **Row model**: A normal Pydantic `BaseModel` describing a single row (e.g. `UserRow`).
- **DataFrameModel**: The user-facing DataFrame container type (e.g. `UserDF`).
  - `UserDF` holds column data for many rows.
  - `UserDF` can validate input and (later) materialize rows as `UserRow`.
- **Schema**: Internally, `DataFrameModel` is derived from the annotated schema fields.

## Defining a DataFrameModel

Users define a DataFrame schema once, similarly to SQLModel:

```python
from pydantable import DataFrameModel

class UserDF(DataFrameModel):
    id: int
    age: int
```

From this definition, `DataFrameModel` generates:
- `UserDF.RowModel`: a Pydantic model for a single row
- a schema-backed typed dataframe wrapper used for query building and execution

## Input formats (both supported)

`UserDF(...)` must accept **both** representations:

### Column format

```python
df1 = UserDF({"id": [1, 2], "age": [20, 30]})
```

This format is ideal for analytic/calc workflows because it matches Rust-side
columnar execution (`Rust Polars`).

### Row format

```python
df2 = UserDF([
    {"id": 1, "age": 20},
    {"id": 2, "age": 30},
])
```

This format is ideal for REST/JSON APIs and works naturally with FastAPI.

### Current implementation note

Internally, row-format inputs are transposed into a column dictionary before
building the logical plan (and before storing the current schema).

Row validation uses the generated `RowModel` so errors point to concrete row
fields.

## Transformations always return new models

Unlike “view” style APIs, this design treats transformations as **schema migration**:

- every call to `select`, `with_columns`, `filter`, etc returns a **new DataFrameModel type**
- the new type encodes the migrated schema (and therefore the migrated row model)

This enables strong typing all the way into FastAPI responses:

- `UserDF` -> `UserDF_WithColumns` (derived schema)
- `UserDF_WithColumns` -> `UserDF_SelectProjection` (derived schema)

## Collision handling (replacement semantics)

For `with_columns(...)`, column name collisions must use **replacement** semantics:

- if the derived column name already exists, the new expression definition replaces it
- other columns remain unchanged

Example:

```python
df2 = df1.with_columns(age2=df1.age * 2)
# age2 is added if missing; if age2 already exists, it is replaced.
```

## Query-building and typed expressions

Transformations rely on a typed expression AST built from column references:

```python
df2 = df1.with_columns(age2=df1.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)
```

The expression system must:
- validate that referenced columns exist in the *current* schema
- infer result dtypes (for `with_columns`)
- propagate the new schema into the *returned* DataFrameModel type

## Typed Dtypes + Null Semantics (skeleton contract)

Supported expression dtypes (for the current skeleton): `int`, `float`, `bool`,
and `str`.

Null semantics are SQL-like (`propagate_nulls`):

- arithmetic: if either operand is `NULL`, the result is `NULL`
- comparisons: if either operand is `NULL`, the result is `NULL` (typed as
  `Optional[bool]`)
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`;
  drops rows where the condition is `False` or `NULL`

`Optional[T]` handling:

- schema fields annotated as `Optional[T]` accept `None` values at DataFrame
  construction time
- derived schemas produced by `select()` / `with_columns()` / `filter()`
  propagate nullability through expression result types

Error timing expectations:

- invalid expressions fail early when the expression AST is built (during
  operator overloads / literal coercion)
- `filter()` validates that the condition expression is typed as `bool` or
  `Optional[bool]` before execution

In the current Rust-first skeleton, these checks are enforced in the Rust
core (PyO3) during AST construction, before any execution happens.

In practice, a `DataFrameModel` instance (and/or its generated `RowModel`)
exposes typed column references while still avoiding the "row vs dataframe
attribute" confusion.

## FastAPI integration

The primary reason for this design:

- `DataFrameModel` is a Pydantic model type, so it can be used directly as request/response
  types in FastAPI.
- every transformation returns a *new* Pydantic-validated model type.

### Typical request flow

```python
from fastapi import FastAPI

app = FastAPI()

@app.post("/users")
def create_users(payload: UserDF):
    # payload is validated (supports both input formats)
    df: UserDF = payload
    return df.select("id", "age")  # returns a new model type
```

### Typical response flow

Because transformations migrate the model type, response types can become
as precise as the query’s projected schema.

## Materializing row models

When you need row-wise output (e.g. for response serialization), the DataFrameModel
should be able to produce:

- `df.rows()` -> `list[UserDF.RowModel]`
- `df.to_dicts()` -> list of dicts (JSON-friendly)

This is the “bridge” between columnar execution and Pydantic row semantics.

## Current skeleton status

In the current repository skeleton:

- `DataFrameModel` is available as the primary FastAPI-facing API
- `DataFrame[SchemaType]` remains available as the lower-level API
- `DataFrameModel` subclasses define schema annotations
- a per-row `RowModel` is generated
- transformation methods return derived `DataFrameModel` subclasses with
  migrated schema

## Roadmap implications

This interface should survive the Rust planner migration:
- Python remains responsible for building the typed AST
- Rust remains responsible for validating and executing logical plans

The “schema migration produces new model types” rule is especially important for
keeping type information available to both Python and FastAPI users.

