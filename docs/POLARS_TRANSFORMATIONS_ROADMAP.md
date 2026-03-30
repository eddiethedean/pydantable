# Polars Transformation Parity Roadmap

This document tracks the plan to reach broad transformation-method parity with
Polars while preserving `pydantable`'s typed schema contracts.

Notes that say **Implemented on branch `v0.5.0`** refer to historical work landed in the 0.5.x line.
The current release is **1.6.1** (see `docs/changelog.md`). Older bullets below still record the 0.5.x baseline plus later additions.

**Post–P7 (0.18.0+):** Phases **P1–P7** below are **complete**. Further Polars-style parity is **additive** (new `Expr` methods, transforms, or façade wrappers) and is tracked in [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md), and [`ROADMAP.md`](ROADMAP.md)—not as a new “P8” checklist unless maintainers add one explicitly.
For execution details (Pydantic-first `collect()`, optional Python `polars`), see `docs/EXECUTION.md`.

## Current baseline (implemented)

- `select`
- `with_columns`
- `filter`
- `join` (with suffix handling)
- `group_by(...).agg(...)` with `count`, `sum`, `mean`

Reference:
- `python/pydantable/dataframe/_impl.py` (``DataFrame`` implementation)
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
  - Rust `execute_concat` wired through `python/pydantable/rust_engine.py`
  - tests expanded for API contracts and UI equivalence smoke coverage (`tests/test_ui_equivalence_smoke.py`)

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
  - contract and UI-equivalence tests added for fill/drop/cast/null-predicate behavior

## Phase P3: Join parity expansion

Goal: complete mainstream join variants.

Deliverables:
- [x] join types: `left`, `inner`, `full`, `right`, `semi`, `anti`, `cross`
- [x] expression-based join keys
- [x] stricter duplicate-column + suffix policy docs

Validation:
- [x] Join contract suite for each join type
- [x] Schema descriptor tests for nullable behavior by join type

Progress note:
- Implemented on branch `v0.5.0`:
  - Rust join execution now supports `right`, `semi`, `anti`, and `cross`
  - expression-based join keys supported through Python -> PyO3 -> Rust path
  - join collision/suffix policy documented and enforced with expanded contract tests

## Phase P4: Aggregation and group-by parity

Goal: extend aggregations beyond count/sum/mean.

Deliverables:
- [x] Aggregations: `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`
- [x] Multi-aggregation expression API parity improvements
- [x] Group-by behavior docs for empty/all-null groups

Validation:
- [x] Aggregation contract tests for each op
- [x] Numeric/non-numeric dtype rejection tests where required

Progress note:
- Implemented on branch `v0.5.0`:
  - Rust group-by execution now supports `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`
  - SQL-like all-null group semantics are preserved across nullable aggregate outputs
  - Python aggregation API/docs updated and parity tests expanded for new operators

## Phase P5: Reshaping transformations

Goal: cover pivot/unpivot/explode workflows.

Deliverables:
- [x] `explode`
- [x] `melt`/`unpivot`
- [x] `pivot` (initially non-streaming constraints acceptable)
- [x] `unnest` (if list/struct roadmap supports it)

Validation:
- [x] Shape-change schema migration tests
- [x] Collision/column naming contract tests

Progress note:
- Implemented on branch `v0.5.0`:
  - Added reshape APIs for `melt`/`unpivot` and `pivot` with deterministic schema descriptors and naming contracts.
  - Added `explode`/`unnest` API entrypoints with explicit typed-contract errors until list/struct schema support is introduced.
  - Expanded reshape contract tests, DataFrameModel schema checks, and UI equivalence smoke coverage.

## Phase P6: Window and time-series transformations

Goal: advanced analytics parity.

Deliverables:
- [x] Window expressions (`over`)
- [x] Rolling aggregations
- [x] Dynamic group-by/time-window groupings
- [x] Time-based expression helpers needed by above

Validation:
- [x] Time-window correctness tests
- [x] Window null/ordering behavior tests

Progress note:
- Implemented on branch `v0.5.0`:
  - Added temporal schema descriptor support (`datetime`, `date`, `duration`) across Rust/Python schema boundaries.
  - Added window/time-series API surface for `over`, `rolling_agg`, and `group_by_dynamic(...).agg(...)`.
  - Expanded contracts, model/schema checks, and UI equivalence smoke coverage for P6 workflows.
  - Completed temporal execution/materialization + literal support across existing core/join/groupby/reshape/window feature paths.

## Phase P7: Stabilization and parity scorecard

Goal: declare practical parity target and freeze contracts.

Deliverables:
- [x] Parity scorecard table (Implemented / Partial / Missing)
- [x] End-to-end examples ported from common Polars workflows
- [x] Performance guardrails for major transforms
- [x] Final documentation pass with migration notes

Validation:
- [x] Scorecard published and linked in docs navigation
- [x] At least 3 end-to-end workflow examples added
- [x] Performance guardrail suite added for major transform families
- [x] Full docs pass completed with migration guidance

Progress note:
- **0.18.0:** Maintainability and docs—contextual Polars errors for **`group_by().agg()`**, explicit deferral of non-string map keys, post–P7 roadmap note — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.18.0**.
- **0.19.0:** Pre-1.0 consolidation—[`VERSIONING.md`](VERSIONING.md) for **0.x** semver, parity/README/index pass, **`PERFORMANCE.md`** benchmark spot-check note, release-hygiene alignment — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.19.0**.
- **0.20.0:** UX / discovery / docs—core **`columns`** / **`shape`** / **`info`** / **`describe`** (numeric, bool, str), **`Expr`** **`repr`**, PySpark **`show`** / **`summary`**, quickstart + execution guides, **`set_display_options`**, **`value_counts`**, **`_repr_mimebundle_`**, **`PYDANTABLE_VERBOSE_ERRORS`**; no new Polars transform matrix rows—see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.20.0**.
- **0.17.0:** String-keyed **map** contract tests + docs after PyArrow map ingest; PySpark **`functions`** wrappers for string replace/strip, **`strptime`**, **`binary_len`**, list **`Expr`** helpers — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.17.0**.
- **0.16.1:** Map-column arithmetic typing fix — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.16.1**.
- **0.16.0:** eager Parquet/IPC **`dict[str, list]`** readers, **`to_arrow` / `ato_arrow`**, **`Table` / `RecordBatch`** constructors, **FastAPI** multipart / **`Depends`** / HTTP-status docs — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.16.0**. (**0.23.0** renames eager reads to **`materialize_*`**, lazy file entry to **`read_*` / `aread_*`**, lazy output to **`DataFrame.write_*`**, and eager dict→file to **`export_*` / `aexport_*`**.)
- **0.15.0:** **async** **`acollect` / `ato_dict` / `ato_polars`**, **FastAPI `async` + `lifespan`**, **PyArrow `map`** ingest for **`dict[str, T]`**, PySpark **`trim` / `abs` / `round` / `floor` / `ceil`** — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.15.0**.
- **0.14.0:** window **`orderBy` `nulls_last`** (SQL **NULLS FIRST/LAST**), **`DtypeDriftWarning`** for **`shape_only`**, **`validate_data`** deprecation, **Hypothesis** + **FastAPI `TestClient`** coverage, PySpark **`dayofmonth` / `lower` / `upper`** helpers — see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.14.0**.
- Implemented on branch `v0.5.0`:
  - Added docs parity scorecard (`Implemented`/`Partial`/`Missing`) and linked it in docs nav.
  - Added end-to-end Polars-style workflow examples for join/groupby, reshape, and time-series operations.
  - Added lightweight performance guardrail tests for join, groupby, reshape, and window transforms.
  - Completed final docs pass with migration notes and consolidated contract references.
  - Added PySpark select-transformation parity wrappers (`withColumn`, `withColumns`, `withColumnRenamed`, `withColumnsRenamed`, `toDF`, `transform`) and typed projection helper (`select_typed`), while explicitly keeping SQL-string `selectExpr` out of scope.

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
