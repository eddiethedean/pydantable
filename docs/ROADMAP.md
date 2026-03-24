# PydanTable roadmap (0.19.x → v1.0.0)

**Current release: `0.19.0`.** This document summarizes what recent releases include, how they relate to the original phase plan, and the **Planned v1.0.0** phase for the **production-ready** major release.

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

**FastAPI / ASGI:** [`FASTAPI.md`](FASTAPI.md) covers `response_model`, row-list and **column-shaped** bodies, **`trusted_mode`**, **`TestClient`** recipes, joins/aggregations, **sync** and **`async` materialization** (`acollect` / `ato_dict` / `ato_arrow` / …), **`lifespan`** + executor patterns, **multipart Parquet** uploads, **`Depends`**-injected pools, **background tasks**, **HTTP status** guidance, and streaming notes (**0.16.0** for file/interchange docs—see **Shipped in 0.16.0**).

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

No single “Phase 8” gate is defined here. **v1.0.0** is the **production-ready** major: a **stability and commitment** cut when maintainers lock **semver** expectations, ship **PyPI** artifacts with aligned Rust/Python versions, and publish clear **1.0** messaging. Detailed checklist: **Planned v1.0.0** (below). **0.19.0** is the last planned **0.x** consolidation before that gate.

Practical inputs that feed that phase:

- Close or explicitly defer remaining gaps in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) (and related parity docs: [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md)).
- Keep CI green across supported Python versions and platforms; keep extension + optional **`[polars]`** matrices exercised in CI.
- **Constructor ingest:** **`validate_data`** was removed in **0.15.0**; use **`trusted_mode`** only ([`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md)).
- Optional: consolidated **migration guide** if semver ever jumps in a breaking way; keep [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) the semantics source of truth.
- **Async I/O:** **0.15.0** ships **`acollect` / `ato_dict` / `ato_polars`** (and **`DataFrameModel`** **`arows` / `ato_dicts`**) using **`asyncio.to_thread`** or a custom executor; **0.16.0** adds **`ato_arrow`** and synchronous **`read_parquet` / `read_ipc`** returning **`dict[str, list]`** (see [`EXECUTION.md`](EXECUTION.md), [`FASTAPI.md`](FASTAPI.md)).
- **FastAPI integration maturity:** treat [`FASTAPI.md`](FASTAPI.md) as the **canonical service guide**. **0.14.0** added **`TestClient`** / OpenAPI notes; **0.15.0** added **`async`** route examples and **`lifespan`**; **0.16.0** documents **multipart** Parquet/IPC, **`Depends`** executors, **background tasks**, and **422 vs application errors**.
- **Release train:** **0.19.0** → **Planned v1.0.0** (below); dates are not committed here. The **1.0.0** tag waits until the **Planned v1.0.0** checklist is satisfied, unless scope slips.

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

## Shipped in 0.13.0 (stabilization + windows / trusted / benchmarks / FastAPI)

**0.13.0** combines documentation-first stabilization with items formerly scoped as **Remaining in 0.13.x** / early **0.14.0** planning. User-facing **`NULLS FIRST` / `LAST`** (`orderBy(..., nulls_last=...)`) and **`shape_only`** **`DtypeDriftWarning`** shipped in **0.14.0** (see **Shipped in 0.14.0** below).

**Themes:** absorb **0.12.0** feedback, tighten docs and CI, clarify sync-only I/O and FastAPI patterns (including bulk / Polars / Arrow trust boundaries), document window null / peer semantics, and harden **PyArrow** **`strict`** ingest.

- [x] **Hardening / audit:** `make check-full` and full **pytest** on a **release** extension build; no regressions requiring code changes in that cycle (follow-up patches ship in later **0.13.x** releases as needed).
- [x] **Docs:** cross-links and “related documentation” sections in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) and [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md); [`README.md`](README.md) and doc site [`index.md`](index.md) aligned with current behavior.
- [x] **FastAPI guide:** [`FASTAPI.md`](FASTAPI.md) — **`trusted_mode`**, column-shaped JSON bodies, large-table / Polars / Arrow trust boundaries, links to [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md) / [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md), **sync** handlers and **0.15.0** async pointer.
- [x] **CI and tooling:** reviewed **GitHub Actions** (`actions/checkout@v5`, `actions/setup-python@v6`, `actions/cache@v5`); documented **`cargo audit`** ignore for **RUSTSEC-2025-0141** in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
- [x] **Tests / examples:** extended [`scripts/verify_doc_examples.py`](../scripts/verify_doc_examples.py) for new FastAPI patterns; no trivial remaining **PySpark**/**pandas** one-line façade gaps identified in that release.
- [x] **I/O documentation:** [`EXECUTION.md`](EXECUTION.md) and [`PERFORMANCE.md`](PERFORMANCE.md) label **sync-only** materialization/interchange and point to **0.15.0** async work; [`PERFORMANCE.md`](PERFORMANCE.md) cross-links **FastAPI** bulk guidance.
- [x] **Window polish (docs):** null ordering and **`CURRENT ROW`** / peer framing in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) and [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md); module docstring on `Window` in [`window_spec.py`](../python/pydantable/window_spec.py). User-facing **`NULLS FIRST` / `LAST`** via **`orderBy(..., nulls_last=...)`** shipped in **0.14.0**.
- [x] **Trusted ingest:** **`strict`** dtype checks for **PyArrow** `Array` / `ChunkedArray` columns (including **decimal** and **enum**-compatible Arrow types); accept all concrete Arrow array classes in trusted column buffers (`isinstance(..., pa.Array)`). Tests in `tests/test_trusted_strict_pyarrow.py`; **`pyarrow`** added to **`[dev]`** and CI install. **`shape_only`** dtype-drift **`DtypeDriftWarning`** shipped in **0.14.0**.
- [x] **Performance:** [`framed_window_bench.py`](../benchmarks/framed_window_bench.py) and [`trusted_polars_ingest_bench.py`](../benchmarks/trusted_polars_ingest_bench.py); [`PERFORMANCE.md`](PERFORMANCE.md) table updated.

---

## Shipped in 0.14.0 (parity + API breadth)

**Themes:** window null ordering, trusted-ingest warnings, PySpark façade helpers, **`validate_data`** deprecation, FastAPI testing docs/tests, and selective **Hypothesis** expansion.

- [x] **Window API:** **`Window.orderBy(..., nulls_last=...)`** (**NULLS FIRST** / **LAST** per key; framed windows honor all keys; unframed Polars `.over` uses the first key’s flag for **`SortOptions`** — see [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)).
- [x] **Trusted ingest:** **`DtypeDriftWarning`** when **`trusted_mode='shape_only'`** would accept data **`strict`** would reject; opt-out env **`PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS=1`**.
- [x] **Polars parity docs:** scorecard and [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) updated for **0.14.0** (transformation phases P1–P7 remain complete; this release is façade / contract polish).
- [x] **PySpark façade:** **`dayofmonth`**, **`lower`**, **`upper`** in **`pydantable.pyspark.sql.functions`** (thin wrappers over core **`Expr`**).
- [x] **Hypothesis:** additional pipeline property (`with_columns` identity) in `tests/test_hypothesis_properties.py`; documented in [`DEVELOPER.md`](DEVELOPER.md).
- [x] **`validate_data` policy:** **`DeprecationWarning`** when **`validate_data=`** is passed without **`trusted_mode`** in **0.14.x**; **`validate_data`** removed in **0.15.0**.
- [x] **FastAPI testing & DX:** **`TestClient`** / columnar body examples in [`FASTAPI.md`](FASTAPI.md); **`tests/test_fastapi_recipes.py`**; **`fastapi`** / **`httpx`** in **`[dev]`** and CI pytest install.
- [x] **Regression tests:** **`tests/test_v014_features.py`** (deprecation, drift, windows, PySpark, FastAPI); extra Hypothesis coverage for **`shape_only`** without drift on int columns.

---

## Shipped in 0.15.0 (async I/O, Arrow maps, PySpark breadth, constructor cleanup)

**Themes:** non-blocking materialization, Arrow **`map<utf8, …>`** ingest for **`dict[str, T]`**, more PySpark-named helpers, and removal of the legacy **`validate_data`** constructor argument (**`trusted_mode`** only).

- [x] **Maps and keys:** **Arrow-native `map`** columns (PyArrow **`MapType`** with **string keys**) ingest on constructors (**including `trusted_mode='off'`** after conversion); cells become Python **`dict`**. **`strict`** checks scalar map **value** types against Arrow (nested value dtypes: best-effort / documented limits). **Heterogeneous map keys** (e.g. **`dict[int, T]`**) remain **out of scope** for this release—see **Later** below.
- [x] **Async materialization:** **`acollect`**, **`ato_dict`**, **`ato_polars`** on **`DataFrame`**; **`DataFrameModel`** adds the same plus **`arows`** and **`ato_dicts`**. Blocking Rust/Polars work runs in **`asyncio.to_thread`** or **`executor=`**. Documented limits: cancellation does not stop in-flight engine work; **`ato_polars`** still materializes a Python dict first. [`EXECUTION.md`](EXECUTION.md), [`FASTAPI.md`](FASTAPI.md).
- [x] **FastAPI `async` routes:** **`async def`** examples, **`lifespan`** + **`ThreadPoolExecutor`**, **`StreamingResponse`** guidance (manual chunking; no built-in async row iterator). Tests: **`tests/test_fastapi_recipes.py`**, **`scripts/verify_doc_examples.py`**.
- [x] **Spark façade depth:** **`trim`**, **`abs`**, **`round`**, **`floor`**, **`ceil`** in **`pydantable.pyspark.sql.functions`** (still **not** a distributed Spark engine).
- [x] **Constructor API:** **`validate_data`** removed from **`DataFrame.__init__`** and **`DataFrameModel.__init__`**; passing it raises **`TypeError`**. Removed schema helpers **`_VALIDATE_DATA_KW_UNSET`**, **`_warn_validate_data_kw_deprecated`**, **`_coerce_validate_data_kw`**, and internal **`_skip_validate_data_deprecation`** / bridge kwargs; trimmed **`validate_columns_strict`** docstring. Source: `python/pydantable/dataframe.py`, `python/pydantable/dataframe_model.py`, `python/pydantable/schema.py`.
- [x] **Docs:** [`changelog.md`](changelog.md) **0.15.0**; **`DATAFRAMEMODEL`**, **`FASTAPI`**, **`SUPPORTED_TYPES`**, **`PERFORMANCE`**, **`INTERFACE_CONTRACT`**, **`index`**, **`README`**.
- [x] **Regression tests:** **`tests/test_async_materialization.py`**, **`tests/test_pyarrow_map_ingest.py`**, **`tests/test_v015_features.py`**, **`tests/test_v015_constructor_api.py`**; **`tests/test_v014_features.py`**, **`tests/test_dataframe_model.py`**, **`tests/test_dataframe_ops.py`** (constructor **`TypeError`** coverage).

---

## Shipped in 0.16.0 (interchange & service hardening)

**Themes:** PyArrow **Parquet / IPC** read helpers, **`to_arrow` / `ato_arrow`**, **`Table` / `RecordBatch`** constructor ingest, and **FastAPI** deployment patterns.

- [x] **Interchange:** **`pydantable.read_parquet`** and **`read_ipc`** ( **`as_stream`** for streaming IPC) return **`dict[str, list]`**; **`DataFrame.to_arrow`** / **`ato_arrow`** and **`DataFrameModel`** mirrors materialize a PyArrow **`Table`** after the same path as **`to_dict`** (documented copies; not zero-copy). **`pyproject`** **`[arrow]`** extra (**`pyarrow>=14`**). [`EXECUTION.md`](EXECUTION.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md), [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md).
- [x] **Constructors:** **`validate_columns_strict`** and **`DataFrameModel`** accept **`pa.Table`** / **`RecordBatch`** when **`pyarrow`** is installed (convert to Python lists, then existing validation).
- [x] **Async:** **`ato_arrow`** uses the same thread-offload model as **`ato_polars`**. **`read_parquet` / `read_ipc`** remain synchronous (document **`asyncio.to_thread`** / executor for large files from async routes).
- [x] **FastAPI:** [`FASTAPI.md`](FASTAPI.md) — **multipart** Parquet upload example, **`Depends`** executor injection, **background tasks** caveats, **422 vs `HTTPException` / uncaught constructor errors**; **`python-multipart`** in **`[dev]`** and CI. **`tests/test_fastapi_recipes.py`**: multipart + **422** on bad row types; **`scripts/verify_doc_examples.py`**: **`read_parquet`** + **`to_arrow`** smoke.
- [x] **Tests:** **`tests/test_arrow_interchange.py`** (read/write helpers, **`to_arrow` / `ato_arrow`**, **`Table` / `RecordBatch` constructors); **`tests/test_fastapi_recipes.py`** (multipart Parquet, **422** where applicable); **`scripts/verify_doc_examples.py`** (**`read_parquet`** + **`to_arrow`** smoke).

---

## Shipped in 0.16.1 (patch)

- [x] **Expression typing:** **`infer_arith_dtype`** rejects **`dict[str, T]`** map operands for binary arithmetic ( **`TypeError`** instead of a Rust panic). Test: **`tests/test_expr_070_surfaces.py`**.
- [x] **Constructors:** **`validate_columns_strict`** loads **`pydantable.io`** helpers for **`pa.Table`** / **`RecordBatch`** (fixes **`DataFrame[Schema](...)`** with Arrow inputs). Test: **`tests/test_arrow_interchange.py`** (`test_dataframe_generic_accepts_pa_table`).

---

## Shipped in 0.17.0 (maps contract + PySpark façade + parity docs)

**Themes:** **String-keyed** maps only—deepen **Arrow map** ingest + **`Expr`** contracts; thin **PySpark** wrappers over existing core **`Expr`**; refresh **parity** docs.

- [x] **Maps / keys (string-keyed):** Documented **`map_get`** / **`map_contains_key`** after PyArrow **`map<string, …>`** ingest (missing key → null). Regression: **`tests/test_pyarrow_map_ingest.py`**. **Non-string** Python **`dict[int, T]`** / non-string Arrow map keys **deferred** (see **Later**).
- [x] **PySpark façade:** New **`pydantable.pyspark.sql.functions`** wrappers where core **`Expr`** already implements the op ([`PYSPARK_PARITY.md`](PYSPARK_PARITY.md)); execution remains the **Polars-backed** core (**facade only**).
- [x] **Parity documentation:** [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) updated for **0.17.0**; no **`PERFORMANCE.md`** number refresh in this release.

---

## Shipped in 0.18.0 (maintainability, execution seams, parity docs)

**Themes:** stable user-facing API; clearer **Rust / Polars** error context for **grouped** execution; explicit **deferral** of non-string map keys; documentation and light **Hypothesis** smoke tests—no new **`Expr`** or PySpark façade methods.

- [x] **Rust plan / Python boundary:** [`polars_err_ctx`](../pydantable-core/src/plan/execute_polars/common.rs) prefixes Polars **`collect()`** failures during **`group_by().agg()`** with **`(group_by().agg())`** in the **`ValueError`** message. [`DEVELOPER.md`](DEVELOPER.md) updated.
- [x] **Polars transformations:** Phases **P1–P7** in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) remain complete; **post–P7** note—future parity is **additive** (`Expr` / transforms), not a new phase backlog. [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md) and [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md) refreshed for **0.18.0** (no new façade rows).
- [x] **Maps:** **Non-string map keys** explicitly **not** in **0.18.0**; [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) and **Later** (below) updated.
- [x] **CI and quality:** Hypothesis + integration tests for **`group_by().agg()`** and **`join`** (`tests/test_hypothesis_properties.py`, `tests/test_v018_features.py`); Rust **`polars_err_ctx`** format tests in `execute_polars/common.rs`.
- [x] **Docs:** [`changelog.md`](changelog.md); [`EXECUTION.md`](EXECUTION.md) and [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) note on grouped **`group_by().agg()`** Polars error wording. Aggregation **semantics** unchanged.

---

## Shipped in 0.19.0 (pre-1.0 consolidation)

**Themes:** documentation and process gate before **v1.0.0**—no large new **`Expr`** or PySpark façade surface; align parity docs, semver story, and release hygiene.

- [x] **v1.0 readiness review:** Re-read **Toward v1.0.0** and **Planned v1.0.0**; items that belong on the **1.0.0** tag itself (**full** semver policy for 1.x, SBOM, PyPI dry-run comms, support matrix as a **1.0.x** commitment) remain under **Planned v1.0.0** below—**explicitly deferred** to the major release with rationale in [`changelog.md`](changelog.md) **0.19.0**. **0.19.0** delivers the **0.x** policy doc and doc-site clarity so the path to 1.0 is obvious.
- [x] **Contract and semver:** [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) audited against **windows**, **trusted ingest**, **async** materialization, and **interchange**; [`VERSIONING.md`](VERSIONING.md) documents **0.x** patch vs minor expectations and points here for behavior.
- [x] **Parity and roadmap docs:** Pass on [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md), [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md), [`README.md`](README.md), and doc site [`index.md`](index.md)—**current release** and **0.19 → 1.0** narrative updated.
- [x] **Performance and ops:** [`PERFORMANCE.md`](PERFORMANCE.md) — key benchmark scripts spot-checked under a **release** build on supported Polars; narrative note for **0.19.0** (no material numeric refresh required vs **0.18.x** execution paths).
- [x] **Release hygiene:** **`make check-full`**, **`cargo test --all-features`**, **`cargo check --no-default-features`**, and **`pytest`** (with **`-n auto`** where CI uses it) on a **release** extension build before tagging; [`.github/workflows/_shared-ci.yml`](../.github/workflows/_shared-ci.yml) install list checked against [`DEVELOPER.md`](DEVELOPER.md) / **`pyproject.toml`** **`[dev]`** (no drift found in this cycle).
- [x] **Tests:** **`group_by`** integration tests sort grouped output before assert where row order is not API-guaranteed (CI **`pytest-xdist`** stability); see **`tests/test_v018_features.py`**.

---

## Planned v1.0.0 (production-ready major release)

**Goal:** tag **`v1.0.0`** when the project is ready to tell production users and library authors: **stable public API** under **semver**, **documented** semantics, and **repeatable** release quality—not a large new feature dump. **1.0.0** follows **0.19.0**; anything that does not block that bar belongs in **Later** or **After v1.0.0**.

- [x] **Precondition:** **Shipped in 0.19.0** (above) is complete or any remaining gap is noted in this file or [`changelog.md`](changelog.md).
- [ ] **Semver contract:** publish a **1.0** policy (expand [`VERSIONING.md`](VERSIONING.md) and/or [`README.md`](README.md)): what counts as **patch** vs **minor** vs **major** for **1.x** for `DataFrame` / `DataFrameModel` / `Expr` / Rust extension boundaries; confirm [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) is the behavioral source of truth. (**0.x** expectations already live in [`VERSIONING.md`](VERSIONING.md).)
- [ ] **Packaging and versions:** **`pyproject.toml`** / **`Cargo.toml`** / extension **`rust_version()`** alignment; **Maturin** release workflow (e.g. [`.github/workflows/release.yml`](../.github/workflows/release.yml)) exercised or dry-run validated; **PyPI** **sdist + wheels** for declared platforms; optional **SBOM** or supply-chain notes if policy requires them.
- [ ] **Quality bar:** full **`make check-full`**, **`cargo test --all-features`**, **`cargo check --no-default-features`**, and **pytest** (including optional-deps legs that match **CI**) on the **exact** commit tagged **`v1.0.0`**; **no known P0/P1** regressions against **INTERFACE_CONTRACT**.
- [ ] **Security tooling:** **`cargo audit`** / **`cargo deny`** (or documented exceptions) current; policy for how **1.x** will handle **RUSTSEC** / advisory bumps.
- [ ] **Documentation and comms:** **README** + doc site **`index`** lead with **1.0** positioning; **changelog** **`1.0.0`** section highlights stability scope; **upgrade path** from **0.19.x** in one place (even if “no breaking changes from last 0.19”).
- [ ] **Support matrix:** state supported **Python** versions and **Polars** optional-extra expectations for **1.0.x**; link [`DEVELOPER.md`](DEVELOPER.md) for contributors.

**Out of scope for the 1.0.0 tag itself:** new execution engines (**Spark**, SQL backend, etc.)—those stay under **After v1.0.0** unless a maintainer explicitly promotes an exception.

---

## Later (not started)

Work **not** scheduled in the **0.17.0–0.19.0** shipped sections or **Planned v1.0.0** above, or explicitly deferred when scope slips:

- [ ] **Non-string map keys** (**`dict[int, T]`** and Arrow maps whose keys are not UTF-8 strings): still **not shipped** after **0.19.0** (explicitly deferred; see **Shipped in 0.18.0** / **0.19.0** and [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md)). **Heterogeneous** keys / full **Arrow + expression** parity may be revisited after **v1.0.0** unless promoted earlier.
- [ ] Items deferred from earlier releases when priorities change.
- [ ] **Chunked / streaming** async iterators for JSON or row batches (no minimal contract yet).
- [ ] Longer-horizon experiments that do not fit the **pre-1.0** train (**0.17–0.19**) or the **v1.0.0** production gate.
- [ ] **FastAPI ecosystem (optional):** thin **`pydantable[fastapi]`** extra with **pinned** **`fastapi` / `starlette`**, **middleware**, or **router** kits—**only** if demand and maintenance bandwidth are clear.

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
