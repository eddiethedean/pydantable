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

Defining the class does not print anything; it registers `UserDF.RowModel` and the schema model used by the internal `DataFrame`.

### Field annotations (supported dtypes)

Column fields must use **scalar** types from **`SUPPORTED_TYPES.md`** (`int`, `float`, `bool`, `str`, `datetime`, `date`, `timedelta`, and nullable forms such as `Optional[T]` / `T | None`). If you declare an unsupported annotation (for example `list[int]` or `int | str` on one field), pydantable raises **`TypeError` while the class body is executing**—before instances can be constructed—so bad schemas fail at import/definition time. Plain **`Schema`** subclasses used with **`DataFrame[Schema]`** do not get this early check; see **`SUPPORTED_TYPES.md`** (“When unsupported field types fail”).

From this definition, `DataFrameModel` generates:
- `UserDF.RowModel`: a Pydantic model for a single row
- a schema-backed typed dataframe wrapper used for query building and execution

## Input formats (all supported)

`UserDF(...)` accepts **columnar data**, **row dicts**, or **sequences of Pydantic models** (including `UserDF.RowModel` instances).

### Column format

```python
df1 = UserDF({"id": [1, 2], "age": [20, 30]})
print(df1.to_dict())
```

Output (one run):

```text
{'id': [1, 2], 'age': [20, 30]}
```

This format is ideal for analytic/calc workflows because it matches Rust-side
columnar execution (`Rust Polars`).

### Row format

```python
df2 = UserDF([
    {"id": 1, "age": 20},
    {"id": 2, "age": 30},
])
print(df2.to_dict())
```

Output (one run):

```text
{'age': [20, 30], 'id': [1, 2]}
```

This format is ideal for REST/JSON APIs and works naturally with FastAPI.

### Row models (Pydantic instances)

```python
RM = UserDF.row_model()
df3 = UserDF([RM(id=1, age=20), RM(id=2, age=30)])
print(df3.to_dict())
```

Output (one run):

```text
{'age': [20, 30], 'id': [1, 2]}
```

Use this when you already have validated row objects—for example **`list[UserDF.RowModel]`** from a FastAPI request body (see `docs/FASTAPI.md`).

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
df1 = UserDF({"id": [1, 2], "age": [20, 40]})
df2 = df1.with_columns(age2=df1.age * 2)
print(df2.to_dict())
```

`age2` is added if missing; if `age2` already exists, it is replaced.

Output (one run):

```text
{'id': [1, 2], 'age2': [40, 80], 'age': [20, 40]}
```

## Query-building and typed expressions

Transformations rely on a typed expression AST built from column references:

```python
df1 = UserDF({"id": [1, 2, 3], "age": [10, 50, 60]})
df2 = df1.with_columns(age2=df1.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 40)
print(df4.to_dict())
```

Output (one run):

```text
{'id': [2, 3], 'age2': [100, 120]}
```

The expression system must:
- validate that referenced columns exist in the *current* schema
- infer result dtypes (for `with_columns`)
- propagate the new schema into the *returned* DataFrameModel type
- keep parity with the lower-level `DataFrame[Schema]` expression behavior
  (including reflected arithmetic such as `2 + df.age`)

### Global aggregates in `select` (0.7–0.8)

`select` can collapse the frame to **one row** using globals such as `global_sum`,
`global_row_count()`, or PySpark `F.count()` with no argument. Rules (mixing projections
vs globals, row count vs non-null count) are documented in **`INTERFACE_CONTRACT.md`**
under *Global aggregates in `select`*.

## Typed Dtypes + Null Semantics

Supported **scalar** dtypes for schema fields and expressions:

- `int`, `float`, `bool`, `str`
- `datetime`, `date`, `timedelta` (from the `datetime` module)

Use `Optional[T]` / `T | None` for nullable columns. The full contract (descriptor names, unsupported cases, bulk ingest): **`SUPPORTED_TYPES.md`**.

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

- **unsupported `DataFrameModel` field types** fail when the **subclass is defined**
  (see **`SUPPORTED_TYPES.md`**)
- invalid expressions fail early when the expression AST is built (during
  operator overloads / literal coercion)
- `filter()` validates that the condition expression is typed as `bool` or
  `Optional[bool]` before execution

In the current Rust-first skeleton, these checks are enforced in the Rust
core (PyO3) during AST construction, before any execution happens.

Phase 4 contract note:

- logical-plan validation ownership remains on Rust for transformation-time checks
- schema migration metadata crossing Python/Rust uses an explicit descriptor
  contract (`{"base": "...", "nullable": ...}`) before Python rebuilds
  annotations for derived `DataFrameModel` types

In practice, a `DataFrameModel` instance (and/or its generated `RowModel`)
exposes typed column references while still avoiding the "row vs dataframe
attribute" confusion.

## FastAPI integration

The primary reason for this design:

- `DataFrameModel` is a Pydantic model type, so it can be used directly as request/response
  types in FastAPI.
- every transformation returns a *new* Pydantic-validated model type.

### Typical request flow (JSON array of row objects)

For endpoints that receive `[{"id": 1, "age": 20}, ...]`, type the body as
`list[UserDF.RowModel]` and pass it straight into `UserDF`:

```python
from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int


class UserRow(BaseModel):
    """Response row; matches the selected projection."""

    id: int
    age: int


app = FastAPI()


@app.post("/users", response_model=list[UserRow])
def create_users(rows: list[UserDF.RowModel]):
    df = UserDF(rows)
    projected = df.select("id", "age")
    return projected.collect()
```

The handler mirrors `UserDF(rows).select(...).collect()` on validated row models;
registering routes does not run the handler until you serve the app (for example with Uvicorn).

### Typical response flow

Because transformations migrate the model type, response types can become
as precise as the query’s projected schema. For a **JSON array of objects**, return
**`collect()`** and declare **`response_model=list[YourRow]`**; FastAPI validates
and filters the response to that schema (see `docs/FASTAPI.md`).

## Materializing row models

When you need row-wise output (e.g. for response serialization), the DataFrameModel
produces:

- `df.collect()` -> `list` of Pydantic models validated against the **current** inner schema type
- `df.rows()` -> same as `collect()` (default arguments)
- `df.to_dict()` -> columnar `dict[str, list]` (use for column-shaped API responses)
- `df.to_dicts()` -> list of dicts (JSON-friendly), derived from row models

This is the “bridge” between columnar execution and Pydantic row semantics.

## Current skeleton status

In the current repository skeleton:

- `DataFrameModel` is available as the primary FastAPI-facing API
- `DataFrame[SchemaType]` remains available as the lower-level API
- `DataFrameModel` subclasses define schema annotations
- a per-row `RowModel` is generated
- transformation methods return derived `DataFrameModel` subclasses with
  migrated schema
- basic transformation guarantees are locked for MVP (`select`, `with_columns`,
  `filter`, collision replacement, and input-format parity)
- schema migration boundary now consumes Rust schema descriptors for derived
  type reconstruction (Phase 4)

## Roadmap implications

This interface should survive the Rust planner migration:
- Python remains responsible for building the typed AST
- Rust remains responsible for validating and executing logical plans

The “schema migration produces new model types” rule is especially important for
keeping type information available to both Python and FastAPI users.

