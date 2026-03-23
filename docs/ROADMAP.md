# PydanTable roadmap (0.13.x → 0.14.0 → 0.15.x → v1.0.0)

**Current release: `0.13.1`.** This document summarizes what recent releases include, how they relate to the original phase plan, and what is still open before calling **`v1.0.0`**.

Release history (high level): [`changelog.md`](changelog.md).

For Polars-style API parity at the method level, see
[`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md). Window **RANGE** rules for multi-column `orderBy` are documented in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) (PostgreSQL-style first-key axis; not universal SQL parity).

---

## Product direction: `DataFrameModel`

The public API stays **SQLModel-like**:

- `DataFrameModel` is the whole-table type for FastAPI and similar stacks.
- Annotations drive a generated per-row **`RowModel`** for validation and serialization.
- Inputs: **column dict** (`{"id": [1, 2]}`) or **row list** (`[{"id": 1}, …]`).
- Every transform returns a **new** model type (schema migration).
- `with_columns(...)` uses **replacement** semantics when names collide.

Details: [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md).

**FastAPI / ASGI:** [`FASTAPI.md`](FASTAPI.md) covers `response_model`, row-list and **column-shaped** bodies, **`trusted_mode`** / **`validate_data`**, joins/aggregations, and **sync** materialization (with **0.15.0** **async I/O** called out on the roadmap). Still **planned** for later minors: multipart/file ingestion, **`Depends`** / **lifespan** recipes, **`TestClient`** helpers, and richer error → HTTP status mapping (see **0.14.0** and **Toward v1.0.0** below).

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

- Close or explicitly defer remaining gaps in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) (and related parity docs: [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md)).
- Keep CI green across supported Python versions and platforms; keep extension + optional **`[polars]`** matrices exercised in CI.
- Decide whether **`validate_data`** should emit **`DeprecationWarning`** with a documented removal timeline (today it remains a compatibility alias for **`trusted_mode`**).
- Optional: consolidated **migration guide** if semver ever jumps in a breaking way; keep [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) the semantics source of truth.
- **Async I/O:** ship **full `async` read/write** coverage for stable materialization and interchange (see **Planned 0.15.0**) so ASGI stacks can avoid blocking the event loop without ad-hoc wrappers.
- **FastAPI integration maturity:** treat [`FASTAPI.md`](FASTAPI.md) as the **canonical service guide**—expand it with **multipart / file** ingestion, **`Depends`** and **lifespan** patterns, **background tasks**, **error → HTTP status** mapping for validation vs engine errors, and **OpenAPI** notes for **row-list vs columnar** JSON bodies. Add **recipe tests** or **docs-only CI checks** so examples stay in sync with releases.

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

## Shipped in 0.8.0 (maps, windows, casts, globals)

- [x] **Global row count** without a column (`global_row_count`, PySpark `count()` with no arg).
- [x] **`str` → `date` / `datetime`** `cast` (Polars); `strptime` remains the fixed-format path.
- [x] **Map ops:** `map_get`, `map_contains_key` (string keys; physical list-of-struct).
- [x] **Window min/max** over partitions; **`WindowFrame::Rows`** in Rust IR (serialization-ready). **Framed execution** (`rowsBetween` / `rangeBetween`) shipped in **0.9.0+**; see [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md).

---

## Shipped in 0.9.0 (framing and map v2 baseline)

- [x] **Input quality controls:** `ignore_errors` + `on_validation_errors` callback payload contract.
- [x] **Framed execution baseline:** `rowsBetween` / `rangeBetween` execution support for initial operators, with explicit typed constraints.
- [x] **Map v2 values:** nested JSON-like map value dtypes with string keys.

---

## Shipped in 0.10.0 (framing completion + map ergonomics)

- [x] **Framed windows expanded:** `window_mean`, `window_min`, `window_max`, `lag`, `lead`, `rank`, and `dense_rank`.
- [x] **Map utilities:** `map_keys()`, `map_values()`, and `map_entries()`.
- [x] **Range frame guardrails:** `rangeBetween` aggregate windows require at least one `orderBy` key; multi-key ordering is allowed starting in **0.12.0** (see [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)).
- [x] **Parity and interop hardening:** expanded PySpark parity wrappers/tests and trusted constructor coverage.

---

## Shipped in 0.11.0 (range v2, map completion, trusted modes)

- [x] **Window range semantics v2:** `rangeBetween` on numeric, `date`, `datetime`, and `duration` **first** `orderBy` keys (multi-column `orderBy` shipped in **0.12.0**).
- [x] **Map ergonomics:** `map_from_entries()`, `Expr.element_at()` / `functions.element_at()` (map lookup alias).
- [x] **Trusted ingest:** `trusted_mode` (`off` / `shape_only` / `strict`) on `DataFrame` / `DataFrameModel`, with `validate_data` as the compatibility bridge.
- [x] **CI:** newer GitHub Actions (`actions/checkout@v5`, `actions/setup-python@v6`) to align with Node 24 runner defaults.

---

## Shipped in 0.12.0 (multi-key range + contract cleanup)

- [x] **Multi-key `rangeBetween`:** lexicographic `orderBy`; range offsets on the **first** sort column (PostgreSQL-style); see [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md).
- [x] **Trusted `strict` nested dtypes:** stricter Polars dtype checks for list / struct / map columns in trusted ingest; columnar Python paths get nested shape checks for list / dict / struct cells.
- [x] **Docs:** `INTERFACE_CONTRACT`, PySpark UI / parity scorecard, `map_from_entries` duplicate-key policy (`SUPPORTED_TYPES`), `validate_data` → `trusted_mode` migration notes (`DATAFRAMEMODEL`, `SUPPORTED_TYPES`).
- [x] **Regression tests:** multi-key range (asc/desc/mixed order, partitions, `date`/`datetime` axis, `window_mean`/`window_min`); PySpark mirror tests; strict nested + map duplicate-key cases; `DataFrame` / `DataFrameModel` strict parity.

---

## 0.13.x — stabilization + windows / trusted ingest (combined track)

The **0.13.x** line combined documentation-first stabilization (**0.13.0**) with follow-up items formerly scoped as **0.14.0**; **0.13.1** closes that remainder (see **Shipped in 0.13.1**). Optional **`NULLS FIRST` / `LAST`** window API and **`shape_only`** dtype-drift **warnings** remain for **Planned 0.14.0** if prioritized.

### Shipped in 0.13.0 (stabilization baseline)

**Themes:** absorb **0.12.0** feedback, tighten docs and CI, and clarify sync-only I/O and FastAPI patterns.

- [x] **Hardening / audit:** `make check-full` and full **pytest** on a **release** extension build; no regressions requiring code changes in that cycle (follow-up patches use **0.13.2+** as needed).
- [x] **Docs:** cross-links and “related documentation” sections in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) and [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md); [`README.md`](README.md) and doc site [`index.md`](index.md) aligned with current behavior.
- [x] **FastAPI guide refresh:** [`FASTAPI.md`](FASTAPI.md) — **`trusted_mode` / `validate_data`**, column-shaped JSON bodies, links to [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md) / [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md), **sync** handlers and **0.15.0** async pointer.
- [x] **CI and tooling:** reviewed **GitHub Actions** (`actions/checkout@v5`, `actions/setup-python@v6`, `actions/cache@v4`); documented **`cargo audit`** ignore for **RUSTSEC-2025-0141** in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
- [x] **Tests / examples:** extended [`scripts/verify_doc_examples.py`](../scripts/verify_doc_examples.py) for new FastAPI patterns; no trivial remaining **PySpark**/**pandas** one-line façade gaps identified in that release.
- [x] **I/O documentation:** [`EXECUTION.md`](EXECUTION.md) and [`PERFORMANCE.md`](PERFORMANCE.md) label **sync-only** materialization/interchange and point to **0.15.0** async work.

### Shipped in 0.13.1 (windows / trusted / benchmarks / FastAPI bulk)

**Themes:** execution semantics documentation, safer **PyArrow** **`strict`** paths, benchmarks, and service trust-boundary docs.

- [x] **Window polish (docs):** null ordering and **`CURRENT ROW`** / peer framing called out in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) and [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md); module docstring on `Window` in [`window_spec.py`](../python/pydantable/window_spec.py). **No** new **`NULLS FIRST` / `LAST`** API yet (defer to **0.14.0** if needed).
- [x] **Trusted ingest:** **`strict`** dtype checks for **PyArrow** `Array` / `ChunkedArray` columns (including **decimal** and **enum**-compatible Arrow types); accept all concrete Arrow array classes in trusted column buffers (`isinstance(..., pa.Array)`). Tests in `tests/test_trusted_strict_pyarrow.py`; **`pyarrow`** added to **`[dev]`** and CI install. Optional **`shape_only`** drift **warnings** still **deferred**.
- [x] **Performance:** [`framed_window_bench.py`](../benchmarks/framed_window_bench.py) and [`trusted_polars_ingest_bench.py`](../benchmarks/trusted_polars_ingest_bench.py); [`PERFORMANCE.md`](PERFORMANCE.md) table updated.
- [x] **FastAPI + trusted bulk:** “Large tables, Polars, Arrow, and trust boundaries” in [`FASTAPI.md`](FASTAPI.md); [`PERFORMANCE.md`](PERFORMANCE.md) cross-link.

---

## Planned 0.14.0 (parity + API breadth)

**Themes:** close documented gaps in alternate APIs and strengthen regression depth.

- [ ] **Window API (optional):** user-facing **`NULLS FIRST` / `NULLS LAST`** (or equivalent) on window `orderBy`, if demand is clear after **0.13.1** docs.
- [ ] **Trusted ingest:** optional **warnings** when **`shape_only`** would hide scalar dtype drift; any further **PyArrow** / **Polars** **`strict`** edge cases discovered in the field.
- [ ] **Polars parity:** burn down high-value items in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) and reflect outcomes in [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md).
- [ ] **PySpark façade:** additional **`functions`** / **`Column`** helpers that map cleanly onto existing `ExprNode` (see [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md)).
- [ ] **Property-based tests:** selective **Hypothesis** (or similar) coverage for expression typing and small plan round-trips where behavior is deterministic.
- [ ] **`validate_data` policy:** decide on **`DeprecationWarning`** and a documented removal timeline in favor of **`trusted_mode`**.
- [ ] **FastAPI testing & DX:** add **copy-paste recipes** (and optional small **test helpers** if worthwhile) for **`TestClient`** / **`httpx`** against `DataFrameModel` routes; document stable **`openapi.json`** patterns for **`RowModel`** and nested columns; optional **`Depends()`** patterns for injecting schema-typed dataframe builders or shared **`APIRouter`** factories.

---

## Planned 0.15.0 (schema / I/O depth)

**Themes:** richer dtypes, **async I/O**, and semantic parity without committing to post-1.0 engines.

- [ ] **Maps and keys:** spike **Arrow-native map** dtype and/or **heterogeneous map keys** (beyond `dict[str, T]` v1); document I/O and expression limits.
- [ ] **Async I/O (reads and writes, full coverage):** first-class **`async`** APIs for **every stable I/O path** the library documents for user-facing **reads** and **writes**—including **materialization** (`collect`, `to_dict` / `collect(as_lists=True)`, row models) and **interchange** (e.g. **`to_polars()`** when the **`[polars]`** extra is installed, plus any **Arrow / Parquet / IPC / file** helpers added in earlier releases). Document how **blocking Rust + Polars** work is isolated (**`asyncio.to_thread`**, dedicated executors, or native async only where upstream supports it). Update [`FASTAPI.md`](FASTAPI.md) and [`EXECUTION.md`](EXECUTION.md) with **non-blocking** patterns and limits (GIL, copy costs, cancellation).
- [ ] **FastAPI `async` routes:** first-class **`async def`** handler examples using the new **async materialization** APIs; cover **`StreamingResponse`** / **chunked** JSON (row or column) **if** stable APIs exist; **lifespan** hooks for warm-up or shared resources used by dataframe pipelines.
- [ ] **Spark façade depth:** broader **semantic parity** where the Polars-backed core can match Spark names and behavior; stay clear this is **not** a distributed Spark engine (see **After v1.0.0**).
- [ ] **Docs and migration:** optional consolidated **0.13–0.15 migration** notes if any release introduces user-visible contract changes (including **FastAPI** handler **sync → async** migration if APIs change).

---

## Later (not started)

Directions beyond **0.15.x** and still before (or orthogonal to) calling **v1.0.0**:

- [ ] Items deferred from **0.13.x–0.15.0** above when scope slips or priorities change.
- [ ] Longer-horizon experimental work that does not fit a minor release train.
- [ ] **FastAPI ecosystem (optional):** thin **`pydantable[fastapi]`** extra with **pinned** compatible **`fastapi` / `starlette`** ranges, reusable **middleware**, or **router** kits—**only** if demand and maintenance bandwidth are clear.

---

## After v1.0.0 (future engines)

These are **explicitly not** part of the path to **v1.0.0**; they would be major new execution or language surfaces on top of the existing Rust logical plan and typed schema.

- [ ] **Spark engine:** compile pydantable logical plans to a real **Apache Spark** `DataFrame` (JVM / `pyspark` driver), for distributed execution. This is separate from the current **PySpark-shaped façade** (`pydantable.pyspark`), which reuses the Polars-backed core with no Spark runtime.
- [ ] **SQL-backed execution engine:** lower pydantable **logical plans** to **SQL** (e.g. PostgreSQL dialect) and run them against a live database, instead of (or alongside) the in-process Polars path. Integration could follow **SQLAlchemy / SQLModel**-style sessions and connection management; scope, supported plan ops, and escape hatches for unsupported expressions would need an explicit contract. This is **not** “parse arbitrary user SQL into `ExprNode`” as the primary story—it is **execute our plan via SQL**. **Python Moltres** is very similar to this SQL engine concept: same idea of keeping the typed logical plan and targeting a SQL-capable runtime rather than embedded Polars.

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
