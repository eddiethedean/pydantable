# Pydantable Roadmap (to v1.0.0)

This document tracks implementation phases for `pydantable` starting at `0.4.0`
and targeting `v1.0.0`.

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

## Progress Snapshot (current)

- Phase 0: completed
- Phase 1: completed
  - `DataFrameModel` base class added
  - generated per-row `RowModel` added
  - both column-input and row-input formats supported (row input normalized to columns)
  - `DataFrameModel` wrappers for `select()`, `with_columns()`, `filter()` added
  - tests added for input formats, normalization, and schema behavior
- Phase 2+: pending

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
- `Expr` class / AST for typed expression building
- Column references and literals for query building
- Arithmetic and comparison operators
- Type inference rules (and nullability propagation)
- Validation errors when expressions reference non-existent columns

## Phase 3: Basic Transformations

Goals:
- Schema-aware transformations
- Transformations migrate the model type (new derived `DataFrameModel`s)

Deliverables:
- `select()`
- `with_columns()`
- `filter()`
- Schema migration rules:
  - `select()` produces a projection-derived schema
  - `with_columns()` produces a schema-derived type with **collision replacement**
  - `filter()` preserves schema, changes row values
- Unit tests for:
  - both input formats
  - schema propagation through transformations
  - correct replacement semantics for `with_columns`

## Phase 4: Logical Plan (Rust)

Goals:
- Move validation and logical plan validation into Rust
- Keep schema migration metadata aligned with Python-visible `DataFrameModel` types

Deliverables:
- Rust `Schema` / `Expr` / `LogicalPlan` representation
- Python -> Rust plan conversion with schema/migration metadata
- Rust unit tests

## Phase 5: Execution Engine

Goals:
- Execute queries via Rust Polars

Deliverables:
- LogicalPlan -> Rust Polars LazyFrame conversion
- `collect()`
- Integration tests and performance benchmarks
- Correctness for both input formats and derived schema results

## Phase 6: Advanced Operations

Goals:
- Feature completeness

Deliverables:
- `join()`
- `groupby()`
- Aggregations (count, mean, sum)
- Column collision handling rules extended consistently to all advanced ops
- Unit + integration tests

## Phase 7: Polishing & DX (v1.0.0 target)

Goals:
- Developer experience
- FastAPI integration quality

Deliverables:
- Improved error messages (schema + expression validation)
- Better type hints across `DataFrameModel` and derived model types
- Autocomplete support
- Documentation site, examples, and tutorials
- Row-wise materialization helpers (e.g. `rows()` returning `RowModel`s) for
  response serialization workflows

