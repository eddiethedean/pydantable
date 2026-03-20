# Polars Transformation Parity Roadmap

This document tracks the plan to reach broad transformation-method parity with
Polars while preserving `pydantable`'s typed schema contracts.

## Current baseline (implemented)

- `select`
- `with_columns`
- `filter`
- `join` (with suffix handling)
- `group_by(...).agg(...)` with `count`, `sum`, `mean`

Reference:
- `python/pydantable/dataframe.py`
- `python/pydantable/dataframe_model.py`

## Roadmap principles

- Keep typed schema/nullability guarantees as first-class behavior.
- Land features in Rust + Python boundary together (no orphan APIs).
- Add contract tests for each transformation family before calling parity.
- Prioritize high-frequency analytics workflows first.

## Phase P1: Core table-shape transformations

Goal: cover most daily transform workflows beyond the current MVP.

Deliverables:
- [x] `sort` (single/multi-key, asc/desc, null placement behavior documented)
- [x] `unique` / `distinct` (subset + keep policy)
- [x] `drop` / `rename`
- [x] `head` / `tail` / `slice`
- [x] `concat` (vertical first, then horizontal)

Validation:
- [x] Contract tests for ordering + determinism expectations
- [x] Schema propagation tests (renames/drops/slices preserve expected types)

Progress note:
- Implemented on branch `v0.5.0`:
  - Rust plan/execution support for P1 unary methods and concat
  - Python `DataFrame` / `DataFrameModel` API parity for P1 methods
  - backend protocol extended for concat execution
  - tests expanded for API contracts and backend equivalence smoke coverage

## Phase P2: Null/type conversion operations

Goal: make null and dtype workflows Polars-like.

Deliverables:
- [x] `fill_null` (scalar + strategy variants)
- [x] `drop_nulls` (all columns + subset)
- [x] `cast` (column expression casting)
- [x] `is_null` / `is_not_null` expression helpers

Validation:
- [x] Null-semantics tests aligned with existing SQL-like contract
- [x] Type error contracts for unsupported casts

Progress note:
- Implemented on branch `v0.5.0`:
  - Rust expression AST adds `cast`, `is_null`, `is_not_null`
  - Rust planner/executor adds `fill_null` and `drop_nulls` (row-wise + Polars paths)
  - Python `Expr`, `DataFrame`, and `DataFrameModel` expose P2 methods
  - contract and backend-equivalence tests added for fill/drop/cast/null-predicate behavior

## Phase P3: Join parity expansion

Goal: complete mainstream join variants.

Deliverables:
- [ ] join types: `left`, `inner`, `full`, `right`, `semi`, `anti`, `cross`
- [ ] expression-based join keys
- [ ] stricter duplicate-column + suffix policy docs

Validation:
- [ ] Join contract suite for each join type
- [ ] Schema descriptor tests for nullable behavior by join type

## Phase P4: Aggregation and group-by parity

Goal: extend aggregations beyond count/sum/mean.

Deliverables:
- [ ] Aggregations: `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`
- [ ] Multi-aggregation expression API parity improvements
- [ ] Group-by behavior docs for empty/all-null groups

Validation:
- [ ] Aggregation contract tests for each op
- [ ] Numeric/non-numeric dtype rejection tests where required

## Phase P5: Reshaping transformations

Goal: cover pivot/unpivot/explode workflows.

Deliverables:
- [ ] `explode`
- [ ] `melt`/`unpivot`
- [ ] `pivot` (initially non-streaming constraints acceptable)
- [ ] `unnest` (if list/struct roadmap supports it)

Validation:
- [ ] Shape-change schema migration tests
- [ ] Collision/column naming contract tests

## Phase P6: Window and time-series transformations

Goal: advanced analytics parity.

Deliverables:
- [ ] Window expressions (`over`)
- [ ] Rolling aggregations
- [ ] Dynamic group-by/time-window groupings
- [ ] Time-based expression helpers needed by above

Validation:
- [ ] Time-window correctness tests
- [ ] Window null/ordering behavior tests

## Phase P7: Stabilization and parity scorecard

Goal: declare practical parity target and freeze contracts.

Deliverables:
- [ ] Parity scorecard table (Implemented / Partial / Missing)
- [ ] End-to-end examples ported from common Polars workflows
- [ ] Performance guardrails for major transforms
- [ ] Final documentation pass with migration notes

## Suggested execution order

1. P1 + P2 (highest user impact, lowest conceptual risk)
2. P3 + P4 (join/groupby breadth)
3. P5 (reshape)
4. P6 (window/time-series)
5. P7 (hardening + scorecard)

## Definition of done for each method

- Rust planner/executor support implemented
- Python API exposed and typed
- Schema descriptor flow validated end-to-end
- User-facing docs added with at least one example
- Contract tests added (happy path + error path)
