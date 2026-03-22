# PydanTable Roadmap (to v1.0.0)

This document tracks implementation phases for `pydantable` from the `0.5.x` era
through the current **`0.6.x`** line, targeting `v1.0.0`.

For detailed method-by-method Polars parity planning, see:
`docs/POLARS_TRANSFORMATIONS_ROADMAP.md`.

## Key interface direction: `DataFrameModel`

The public API is intended to be SQLModel-like:

- `DataFrameModel` represents the whole DataFrame and is used as the FastAPI
  request/response type.
- From the annotated schema, `DataFrameModel` generates a per-row Pydantic
  `RowModel` for row-level validation and serialization.
- `DataFrameModel` instances support both input formats:
  - column dict: `{"id": [1, 2], "age": [20, 30]}`
  - row list: `[{"id": 1, "age": 20}, ...]`
- Transformations always produce new model types (schema migration).
- `with_columns(...)` uses replacement semantics for name collisions.

Spec: `docs/DATAFRAMEMODEL.md`.

## Milestones

- MVP: end of Phase 3
- Beta: end of Phase 6
- v1.0.0: end of Phase 7

## 0.6.x API note (materialization)

- **Python `polars` is optional**; core installs use Pydantic + `typing-extensions` only.
- Default **`collect()`** returns **`list[BaseModel]`** (validated against the current projected schema). Columnar data: **`to_dict()`**. Polars **`DataFrame`**: **`to_polars()`** with the `[polars]` extra.
- Eager Rust paths from Python use **dict-of-lists** handoff (no Python `import polars` required for normal execution).

## Progress Snapshot (current)

- Phase 0: completed
- Phase 1: completed
  - `DataFrameModel` base class added
  - generated per-row `RowModel` added
  - both column-input and row-input formats supported (row input normalized to columns)
  - `DataFrameModel` wrappers for `select()`, `with_columns()`, `filter()` added
  - tests added for input formats, normalization, and schema behavior
- Phase 2: completed
  - expression behavior parity verified between `DataFrameModel` and `DataFrame[Schema]`
  - reflected arithmetic operator support added (`literal <op> column`)
  - AST-build-time error timing verified for invalid expression combinations
  - derived schema dtype/nullability propagation validated in chained transformations
  - comprehensive expression and `DataFrameModel` parity tests expanded
- Phase 3: completed
  - transformation contract locked for `select()`, `with_columns()`, `filter()`
  - `with_columns()` replacement semantics verified for name collisions
  - derived schema migration guarantees validated through chained transforms
  - row-input vs column-input transformation parity coverage added
  - MVP test matrix completed for Phase 3 transformation guarantees
- Phase 4: completed
  - Rust schema metadata contract locked with explicit descriptor format (`base`, `nullable`)
  - Python derived-schema construction now consumes Rust descriptors at the boundary
  - remaining `select()` arity validation moved into Rust logical-plan validation
  - Rust-originated transformation errors standardized and preserved through thin Python wrappers
  - Rust-side plan contract tests and Python metadata-flow integration tests added
- Phase 5: completed (initial execution engine milestone)
  - Rust Polars-backed `collect()` execution path wired for `select`, `with_columns`, and `filter`
  - Python API boundary preserved for `DataFrameModel` and `DataFrame[Schema]`
  - integration coverage added for input-format parity and derived-schema correctness
  - baseline benchmark harness added for execution pipeline timing
- Phase 6: completed
  - Rust Polars-backed `join()` and `group_by(...).agg(...)` added
  - deterministic join collision handling implemented via suffix for right-side non-key columns
  - derived schema descriptors preserved through advanced ops and surfaced in `DataFrameModel`
  - Python integration tests added for join/group_by/aggregations parity + correctness
- Phase 7: completed
  - `DataFrameModel.rows()` / `DataFrameModel.to_dicts()` added for row-wise serialization
  - Derived model types now inherit from the originating subclass for better DX/autocomplete
  - Rust-side validation errors enriched with additional context (without breaking error substrings)
  - MkDocs docs site scaffold added (`mkdocs.yml`, `docs/index.md`, and contributor build instructions)

## Phase 0: Repo Setup

Goals:
- Initialize project structure
- Set up Python + Rust integration

Deliverables:
- Python package + Rust crate scaffolding
- CI pipeline (lint, test, build)

## Phase 1: Core Schema System (Python-first)

Goals:
- Define schema types
- Validate DataFrame creation (both input formats)
- Provide the `DataFrameModel` container abstraction

Deliverables:
- [x] `Schema` base class and strict runtime schema validation logic
- [x] `DataFrameModel`:
  - [x] generates a per-row Pydantic `RowModel`
  - [x] accepts both row-input and column-input formats
  - [x] normalizes input internally into column form
- [x] `DataFrameModel` wrapper supports `select()`, `with_columns()`, `filter()`
- [x] `DataFrameModel` is the primary user entrypoint (FastAPI-friendly)

## Phase 2: Expression System

Goals:
- Typed expressions
- Operator overloading
- Expression typing drives derived schema migration

Deliverables:
- [x] `Expr` class / AST for typed expression building
- [x] Column references and literals for query building
- [x] Arithmetic and comparison operators
- [x] Type inference rules (and nullability propagation)
- [x] Validation errors when expressions reference non-existent columns

## Phase 3: Basic Transformations

Goals:
- Schema-aware transformations
- Transformations migrate the model type (new derived `DataFrameModel`s)

Deliverables:
- [x] `select()`
- [x] `with_columns()`
- [x] `filter()`
- [x] Schema migration rules:
  - [x] `select()` produces a projection-derived schema
  - [x] `with_columns()` produces a schema-derived type with **collision replacement**
  - [x] `filter()` preserves schema, changes row values
- [x] Unit tests for:
  - [x] both input formats
  - [x] schema propagation through transformations
  - [x] correct replacement semantics for `with_columns`

## Phase 4: Logical Plan (Rust)

Goals:
- Move validation and logical plan validation into Rust
- Keep schema migration metadata aligned with Python-visible `DataFrameModel` types

Deliverables:
- [x] Rust `Schema` / `Expr` / `LogicalPlan` representation
- [x] Python -> Rust plan conversion with schema/migration metadata
- [x] Rust unit tests

## Phase 5: Execution Engine

Goals:
- Execute queries via Rust Polars

Deliverables:
- [x] LogicalPlan -> Rust Polars LazyFrame conversion
- [x] `collect()`
- [x] Integration tests and performance benchmarks
- [x] Correctness for both input formats and derived schema results

## Phase 6: Advanced Operations

Goals:
- Feature completeness

Deliverables:
- [x] `join()`
- [x] `group_by()`
- [x] Aggregations (count, mean, sum)
- [x] Column collision handling rules extended consistently to advanced ops (join right-side non-keys via `suffix`)
- [x] Unit + integration tests

## Phase 7: Polishing & DX (v1.0.0 target)

Goals:
- Developer experience
- FastAPI integration quality

Deliverables:
- [x] Improved error messages (schema + expression validation)
- [x] Better type hints across `DataFrameModel` and derived model types
- [x] Autocomplete support
- [x] Documentation site, examples, and tutorials
- [x] Row-wise materialization helpers (e.g. `rows()` returning `RowModel`s) for
  response serialization workflows

## Post-7 follow-ups (in flight / planned)

- [x] **Nested Pydantic model columns** (struct dtypes, Polars struct I/O, conservative expression typing, derived-schema identity merge where descriptors match prior annotations). See [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).
- [x] **Struct field projection** via `Expr.struct_field(...)` (Rust + Polars).
- [x] **PySpark UI**: `annotation_to_data_type` builds nested `StructType` for nested `BaseModel` columns.
- [x] **Homogeneous list columns** (`list[T]`) with list dtypes, descriptors, Polars list I/O, and **`explode()`** for list-typed columns.
- [x] **`unnest()` for struct columns** — promote struct fields to top-level columns (Polars `unnest` with `{parent}_{field}` names); list `explode` remains separate.
- [x] **Additional scalars** — `uuid.UUID`, `decimal.Decimal` (fixed scale 9), and concrete `enum.Enum` subclasses in `BaseType`, descriptors, and Polars I/O (see [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md)).
- [x] **Extended `Expr` surface** — string helpers (`str_replace`, `strip_prefix` / `strip_suffix` / `strip_chars`), `dt_date()`, `datetime`/`date` ± `timedelta`, and list helpers (`list_get`, `list_contains`, `list_min` / `list_max` / `list_sum`).

## Later (not started)

- [ ] **Map / dict-like cells** with a fixed value type, or a dedicated Arrow map dtype.
- [ ] **`time` of day** as a distinct column type (separate from `datetime` / `timedelta`).
- [ ] **`bytes`** / binary blobs with a minimal execution surface.

