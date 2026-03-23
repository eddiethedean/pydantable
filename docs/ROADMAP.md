# PydanTable roadmap (0.7.0 → v1.0.0)

**Current release: `0.7.0`.** This document summarizes what recent releases include, how they relate to the original phase plan, and what is still open before calling **`v1.0.0`**.

For Polars-style API parity at the method level, see
[`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md).

---

## Product direction: `DataFrameModel`

The public API stays **SQLModel-like**:

- `DataFrameModel` is the whole-table type for FastAPI and similar stacks.
- Annotations drive a generated per-row **`RowModel`** for validation and serialization.
- Inputs: **column dict** (`{"id": [1, 2]}`) or **row list** (`[{"id": 1}, …]`).
- Every transform returns a **new** model type (schema migration).
- `with_columns(...)` uses **replacement** semantics when names collide.

Details: [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md).

---

## What 0.5.0 includes

### Execution and materialization

- **`polars` on the Python side is optional.** Core installs need Pydantic and `typing-extensions` only.
- **`collect()`** returns **`list[BaseModel]`** for the current projected schema (validated rows).
- **`to_dict()`** / **`collect(as_lists=True)`** for columnar **`dict[str, list]`**.
- **`to_polars()`** when the **`[polars]`** extra is installed.
- The Rust engine talks to Python via **dict-of-lists**; normal execution does not require `import polars` in user code.

More context: [`EXECUTION.md`](EXECUTION.md).

### Core platform (original phases 0–7)

These milestones are **done** and are what 0.5.0 is built on:

| Phase | Theme | Status |
|------|--------|--------|
| 0 | Repo, Python + Rust scaffolding, CI | Done |
| 1 | `Schema`, `DataFrameModel`, row/column inputs | Done |
| 2 | Typed `Expr`, operators, schema propagation | Done |
| 3 | `select` / `with_columns` / `filter`, MVP guarantees | Done |
| 4 | Logical plan + descriptors in Rust | Done |
| 5 | Polars-backed execution, `collect`, benchmarks | Done |
| 6 | `join`, `group_by`, aggregations, suffix collisions | Done |
| 7 | DX polish, docs site, `rows()` / `to_dicts()`, better errors | Done |

Originally: **MVP** end of Phase 3, **beta** end of Phase 6, **v1.0.0** targeted end of Phase 7. **As of 0.5.0, all of those phases are complete**; the **`v1.0.0` tag** is reserved for a final stability / packaging / comms cut when the project is ready to call 1.0 (not a large missing feature tranche).

### Richer schema and expressions (shipped in 0.5.0)

Beyond the original Phase 7 checklist, **0.5.0** also ships:

- **Nested `BaseModel` columns** (struct dtypes, Polars struct I/O, conservative struct typing in `Expr`). See [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).
- **`Expr.struct_field(...)`** for field projection.
- **PySpark UI:** nested `StructType` from annotations for nested models.
- **Homogeneous `list[T]`** columns, descriptors, Polars list I/O, **`explode()`**.
- **`unnest()`** on struct columns (Polars `unnest`, `{parent}_{field}` names).
- **Scalars:** `uuid.UUID`, `decimal.Decimal` (fixed scale), concrete **`enum.Enum`** — `BaseType`, descriptors, Polars I/O.
- **More `Expr` APIs:** e.g. `str_replace`, `strip_prefix` / `strip_suffix` / `strip_chars`, `dt_date()`, `datetime`/`date` ± `timedelta`, `list_get`, `list_contains`, `list_min` / `list_max` / `list_sum`.

---

## Toward v1.0.0

No single “Phase 8” gate is defined here. **v1.0.0** is mainly a **stability and commitment** milestone when maintainers are comfortable locking semver semantics and publishing “1.0” messaging. Practical inputs:

- Close or explicitly defer remaining gaps in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) (and related parity docs).
- Keep CI green across supported Python versions and platforms.
- Optional: release notes, migration guide if numbering ever jumps in a breaking way.

---

## Shipped in 0.6.0 (schema additions)

- [x] **`time`** (`datetime.time`) — Polars **Time**; clock time distinct from `datetime` / `timedelta`.
- [x] **`bytes`** — Polars **Binary**; small execution surface (I/O, equality, `len` where supported).
- [x] **Homogeneous `dict[str, T]`** map-like cells — logical map dtype (Polars list-of-struct physical encoding); limited `Expr` surface vs full Polars map ops.

---

## Shipped in 0.7.0 (expressions + globals)

- [x] **Global `select` aggregates:** `count` / `min` / `max` on a typed column (plus existing `sum` / `mean`).
- [x] **Window shifts:** `lag` / `lead` via Polars `shift` + window `over` (requires `order_by`).
- [x] **Temporal helpers:** `strptime`, `unix_timestamp` (seconds or milliseconds), `dt_nanosecond`, string `to_date` with format in PySpark façade.
- [x] **Map / binary:** `map_len`, `binary_len`.

---

## Later (not started)

Directions beyond **0.7.0** (non-exhaustive):

- [ ] **Arrow-native map dtype** or heterogeneous map keys (beyond `dict[str, T]` v1).
- [ ] SQL-style **window frames** (`rowsBetween` / `rangeBetween`) in the IR and Polars lowering.

---

## Reference: phase checklists (completed)

The sections below record the original deliverable lists for **phases 0–7**. They are **historical checklists**; status is **[x] complete**.

### Phase 0: repo setup

Goals: project layout, Python + Rust integration.  
Deliverables: package + crate scaffolding, CI (lint, test, build).

### Phase 1: core schema (Python-first)

Goals: schema types, strict validation, `DataFrameModel` container.

Deliverables:

- [x] `Schema` and strict runtime validation
- [x] `DataFrameModel` with `RowModel`, row + column input, internal column normalization
- [x] `select`, `with_columns`, `filter` on `DataFrameModel`
- [x] `DataFrameModel` as primary entrypoint (FastAPI-oriented)

### Phase 2: expression system

Goals: typed expressions, operators, schema migration driven by expression types.

Deliverables:

- [x] `Expr` / AST, columns and literals, arithmetic and comparisons
- [x] Inference and nullability rules, errors for bad references

### Phase 3: basic transformations

Goals: schema-aware transforms and migration rules.

Deliverables:

- [x] `select`, `with_columns`, `filter`
- [x] Projection / replacement / filter semantics as documented
- [x] Tests for both input formats and schema propagation

### Phase 4: logical plan (Rust)

Goals: validation and plan in Rust; Python types stay aligned with descriptors.

Deliverables:

- [x] Rust `Schema` / `Expr` / `LogicalPlan`, Python → Rust conversion, Rust tests

### Phase 5: execution engine

Goals: run plans via Rust Polars.

Deliverables:

- [x] Plan → LazyFrame, `collect`, integration tests, benchmarks

### Phase 6: advanced operations

Goals: joins and grouped aggregation.

Deliverables:

- [x] `join`, `group_by`, aggregations, suffix rules for join collisions, tests

### Phase 7: polishing and DX (original v1.0 target)

Goals: errors, typing, docs, row helpers.

Deliverables:

- [x] Clearer validation errors
- [x] Better hints and autocomplete on derived models
- [x] Docs site and examples
- [x] `rows()` / `to_dicts()` (and related row-wise helpers)
