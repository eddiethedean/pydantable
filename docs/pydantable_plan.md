# Pydantable -- Plan Document

## Vision

Pydantable is a strongly-typed DataFrame system for Python, combining
Pydantic schemas with a Rust execution engine.

## Goals

-   Enforce schemas at runtime
-   Provide type-safe transformations
-   Integrate with Rust Polars for performance
-   Track schema through all operations

## Architecture

Python API → Typed AST → Rust Planner → Rust Polars Engine

## Phases

### Phase 1 (completed)

-   `DataFrameModel` base abstraction
-   per-row `RowModel` generation
-   dual input formats (column dict + row list) with normalization
-   wrappers for `select`, `filter`, `with_columns`
-   `DataFrame[Schema]` retained as lower-level API

### Phase 2 (completed)

-   Rust-backed expression typing/AST build-time validation
-   Nullability tracking + `propagate_nulls` semantics

### Phase 3 (completed)

-   Rust integration (PyO3)
-   Transformation contract lock (`select`, `with_columns`, `filter`)

### Phase 4 (completed)

-   Rust-owned logical-plan validation contract hardening
-   Python<->Rust schema metadata boundary standardized via descriptors (`base`, `nullable`)
-   Rust unit tests + Python integration tests for metadata flow

### Phase 5 (next)

-   Advanced ops (join, groupby)
-   Performance optimization

## Key Components

-   Schema system
-   Expression system
-   Planner
-   Execution engine

## Risks

-   Python typing limitations
-   Performance overhead
-   API complexity

## Future Ideas

-   Static type checker plugin
-   SQL interface
-   Arrow-native engine
