# Pydantable -- Plan Document

## Vision

Pydantable is a strongly-typed DataFrame system for Python, combining
Pydantic schemas with a Rust execution engine.

## Goals

-   Enforce schemas at runtime
-   Provide type-safe transformations
-   Integrate with Polars for performance
-   Track schema through all operations

## Architecture

Python API → Typed AST → Rust Planner → Polars Engine

## Phases

### Phase 1 (MVP)

-   Schema definition (Pydantic + native)
-   DataFrame\[Schema\]
-   select, filter, with_columns

### Phase 2

-   Expression typing system
-   Nullability tracking

### Phase 3

-   Rust integration (PyO3)
-   Logical plan validation

### Phase 4

-   Polars backend execution
-   Lazy execution

### Phase 5

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
