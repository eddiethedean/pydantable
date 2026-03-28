# PydanTable roadmap (shipped → v1.0.0)

**Latest release: `1.3.0`.** The **Planned v1.0.0** checklist (below) is **complete** for that tag. **Shipped in 0.23.0** adds **lazy `read_*` / `aread_*` file roots** (Parquet, CSV, NDJSON, IPC, **JSON**), **`DataFrame.write_*`** lazy pipeline output, **`export_*`** for eager dict→file, **`DataFrameModel`** glue (**`from_sql`**, **`write_sql`**, **`read_parquet_url_ctx`**), **HTTP/object-store `max_bytes`**, **`MissingRustExtensionError`**, and **breaking renames** (**`materialize_*`**, **`fetch_sql`**, **`fetch_*_url`**)—see **Shipped in 0.23.0** below and {doc}`IO_OVERVIEW`. **0.22.0** introduced the **`pydantable.io`** package (vocabulary evolved in **0.23.0**; see {doc}`changelog`). Earlier **0.20.0** / **0.21.0** items (**UX** docs, **Streamlit** interchange, …) remain in their sections. **ipywidgets** / interactive explorers remain **Later** unless promoted. This document also summarizes shipped history and **Later** / **After v1.0.0** backlogs.

Release history (high level): [`changelog.md`](changelog.md).

For Polars-style API parity at the method level, see
[`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md). **Future Expr /
table methods** we may add next are under [**Future method candidates**](future-expr-and-dataframe-method-candidates-not-scheduled) (below). Window **RANGE** rules for multi-column `orderBy` are documented in [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md) (PostgreSQL-style first-key axis; not universal SQL parity).

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

No single “Phase 8” gate is defined here. **v1.0.0** is the **production-ready** major: a **stability and commitment** cut when maintainers lock **semver** expectations, ship **PyPI** artifacts with aligned Rust/Python versions, and publish clear **1.0** messaging. Detailed checklist: **Planned v1.0.0** (below). **0.19.0** was the pre-1.0 **documentation consolidation**; **0.20.0** shipped **UX / discovery** (repr, **`info`** / **`describe`**, PySpark **`show`**) on the same Rust core.

Practical inputs that feed that phase:

- Close or explicitly defer remaining gaps in [`POLARS_TRANSFORMATIONS_ROADMAP.md`](POLARS_TRANSFORMATIONS_ROADMAP.md) (and related parity docs: [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md)).
- Keep CI green across supported Python versions and platforms; keep extension + optional **`[polars]`** matrices exercised in CI.
- **Constructor ingest:** **`validate_data`** was removed in **0.15.0**; use **`trusted_mode`** only ([`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md)).
- Optional: consolidated **migration guide** if semver ever jumps in a breaking way; keep [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) the semantics source of truth.
- **Async I/O:** **0.15.0** ships **`acollect` / `ato_dict` / `ato_polars`** (and **`DataFrameModel`** **`arows` / `ato_dicts`**) using **`asyncio.to_thread`** or a custom executor; **0.16.0** adds **`ato_arrow`** and synchronous Parquet/IPC readers into **`dict[str, list]`** ( **`materialize_*`** since **0.23.0**); **0.23.0** adds **`read_*`** + **`DataFrame.write_*`** for out-of-core paths (see [`EXECUTION.md`](EXECUTION.md), [`FASTAPI.md`](FASTAPI.md)).
- **FastAPI integration maturity:** treat [`FASTAPI.md`](FASTAPI.md) as the **canonical service guide**. **0.14.0** added **`TestClient`** / OpenAPI notes; **0.15.0** added **`async`** route examples and **`lifespan`**; **0.16.0** documents **multipart** Parquet/IPC, **`Depends`** executors, **background tasks**, and **422 vs application errors**.
- **Release train:** **0.20.0** → **Planned v1.0.0** (below); dates are not committed here. The **1.0.0** tag waits until the **Planned v1.0.0** checklist is satisfied, unless scope slips.

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

- [x] **Interchange:** **`pydantable.read_parquet`** and **`read_ipc`** ( **`as_stream`** for streaming IPC) return **`dict[str, list]`** (**renamed** **`materialize_*`** in **0.23.0**); **`DataFrame.to_arrow`** / **`ato_arrow`** and **`DataFrameModel`** mirrors materialize a PyArrow **`Table`** after the same path as **`to_dict`** (documented copies; not zero-copy). **`pyproject`** **`[arrow]`** extra (**`pyarrow>=14`**). [`EXECUTION.md`](EXECUTION.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md), [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md).
- [x] **Constructors:** **`validate_columns_strict`** and **`DataFrameModel`** accept **`pa.Table`** / **`RecordBatch`** when **`pyarrow`** is installed (convert to Python lists, then existing validation).
- [x] **Async:** **`ato_arrow`** uses the same thread-offload model as **`ato_polars`**. Parquet/IPC readers remain synchronous (document **`asyncio.to_thread`** / executor for large files from async routes).
- [x] **FastAPI:** [`FASTAPI.md`](FASTAPI.md) — **multipart** Parquet upload example, **`Depends`** executor injection, **background tasks** caveats, **422 vs `HTTPException` / uncaught constructor errors**; **`python-multipart`** in **`[dev]`** and CI. **`tests/test_fastapi_recipes.py`**: multipart + **422** on bad row types; **`scripts/verify_doc_examples.py`**: Parquet + **`to_arrow`** smoke ( **`materialize_parquet`** after **0.23.0**).
- [x] **Tests:** **`tests/test_arrow_interchange.py`** (read/write helpers, **`to_arrow` / `ato_arrow`**, **`Table` / `RecordBatch` constructors); **`tests/test_fastapi_recipes.py`** (multipart Parquet, **422** where applicable); **`scripts/verify_doc_examples.py`** (Parquet + **`to_arrow`** smoke).

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

## Shipped in 0.20.0 (UX, discovery, PySpark previews)

**Themes:** **REPL / notebook** ergonomics, **lightweight discovery** on the default **`DataFrame`**, readable **`Expr`** **`repr`**, and **PySpark**-named **`show`** / **`summary`**—all on the existing **Rust + Polars** path.

### String representation (`repr`)

- [x] **`DataFrame` / `DataFrameModel` / grouped handles:** multi-line **`repr`** and **`_repr_html_`** (schema, column dtypes, wide-schema truncation; no row count in **`repr`**). See [`EXECUTION.md`](EXECUTION.md), [`changelog.md`](changelog.md) **0.20.0**, **`tests/test_dataframe_repr.py`**.
- [x] **`Expr` and related:** **`__repr__`** for **`Expr`**, **`ColumnRef`**, literals, **`WhenChain`**, and pending window builders (AST snippet + dtype / referenced columns). **`tests/test_expr_repr.py`**.
- [x] **Docs / tests:** [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) **Introspection**; discovery tests in **`tests/test_dataframe_discovery.py`**.

### Discovery and convenience (core + façades)

- [x] **Core API:** **`columns`**, **`shape`**, **`empty`**, **`dtypes`** on **`DataFrame`** / **`DataFrameModel`** (root-buffer semantics for **`shape[0]`**—see **Introspection** in [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md)).
- [x] **`info()`** — multi-line **str** with schema and column list (row count when consistent with **`shape`** policy).
- [x] **`describe()`** — **numeric** **`int` / `float`**, **bool**, and **str** summaries; materializes via **`to_dict()`** once; see [`EXECUTION.md`](EXECUTION.md).
- [x] **PySpark façade:** **`DataFrame.show()`** (text table; **`head`**-like), **`summary()`** → same string contract as **`describe()`**. See [`PYSPARK_UI.md`](PYSPARK_UI.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md).

### Notebook utilities (Jupyter, VS Code, Colab)

- [x] **`_repr_html_`** — bounded **HTML** preview (**stdlib** escape only).
- [x] **Notebook note** — short subsection in [`DEVELOPER.md`](DEVELOPER.md) (**Notebooks**).
- [x] **Display options** — **`set_display_options`**, env **`PYDANTABLE_REPR_HTML_*`** (see [`pydantable.display`](../python/pydantable/display.py)).
- [ ] **Later:** **ipywidgets** explorers; optional **CI** smoke for **IPython** display hooks.

### Documentation and extended UX

- [x] **Quickstart:** [`QUICKSTART.md`](QUICKSTART.md), [`notebooks/five_minute_tour.ipynb`](../notebooks/five_minute_tour.ipynb), links from [`README.md`](README.md), [`index.md`](index.md), [`DEVELOPER.md`](DEVELOPER.md).
- [x] **Execution guide:** materialization cost table, import-style table, copy-as / interchange in [`EXECUTION.md`](EXECUTION.md).
- [x] **Naming map:** core ↔ pandas ↔ PySpark in [`PANDAS_UI.md`](PANDAS_UI.md) and [`PYSPARK_UI.md`](PYSPARK_UI.md).
- [x] **`value_counts(column)`** on **`DataFrame`** / **`DataFrameModel`**; **`_repr_mimebundle_`** for Jupyter; **`PYDANTABLE_VERBOSE_ERRORS`** for **`execute_plan`** **`ValueError`** context.
- [x] **Tests:** [`tests/test_display_options.py`](../tests/test_display_options.py), [`tests/test_rust_engine_verbose_errors.py`](../tests/test_rust_engine_verbose_errors.py).

### Quality and release

- [x] **Tests:** **`tests/test_dataframe_discovery.py`**, **`tests/test_expr_repr.py`**, **`tests/test_dataframe_repr.py`**.
- [x] **Docs:** [`README.md`](README.md), [`index.md`](index.md), [`PARITY_SCORECARD.md`](PARITY_SCORECARD.md), [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md), [`PANDAS_UI.md`](PANDAS_UI.md).
- [x] **Release hygiene:** **`make check-full`**, **`pytest`**, **changelog** **0.20.0**, version bump **`pyproject.toml`** / **`__init__.py`** / **`pydantable-core/Cargo.toml`**.

**Explicitly not in 0.20.0:** new **Expr** analytics transforms beyond **`describe`** / **`value_counts`**; **non-string map keys**; **distributed** Spark; **ipywidgets**-heavy UIs.

---

## Shipped in 0.21.0 (Streamlit: `st.write`, `st.dataframe`, `st.data_editor`)

**Today:** [Streamlit](https://streamlit.io/) **`st.dataframe`** and **`st.data_editor`** accept **pandas**, **PyArrow**, **Polars**, and objects that expose the **Python DataFrame Interchange Protocol** (`__dataframe__` / [SPEC 21](https://data-apis.org/dataframe-protocol/latest/purpose.html)). A **pydantable** **`DataFrame`** is none of these unless you convert (e.g. **`to_polars()`**, **`to_arrow()`**, or columnar **`to_dict()`** wrapped for display). **`st.write`** may render **`_repr_html_`** / plain **`repr`**, not a native interactive table.

**Goal (shipped): first-class Streamlit ergonomics**

- [x] **Interchange protocol (preferred path):** implemented **`__dataframe__`** on **`DataFrame`** (and **`DataFrameModel`** via delegation) so **`st.dataframe(df)`** works without manual conversion where Streamlit’s stack supports the exported Arrow-backed interchange (documented dtype / nullability limits and materialization costs).
- [x] **Fallback documentation:** documented **`st.dataframe(df.to_polars())`**, **`st.dataframe(df.to_arrow())`**, **`st.data_editor(df.to_arrow())`** (editing fallback), and **`st.write`** behavior in **`STREAMLIT.md`** and an {doc}`EXECUTION` interchange subsection.
- [x] **Tests:** CI smoke coverage using Streamlit’s built-in app testing harness (`streamlit.testing.v1.AppTest`) for **`st.write`** / **`st.dataframe`** and the supported **`st.data_editor(df.to_arrow())`** fallback.
- [x] **Packaging:** documented **`pip install streamlit`** alongside **`pydantable[arrow]`** / **`pydantable[polars]`**; CI pins a supported Streamlit range.
- [x] **Changelog + README:** Streamlit integration called out in **changelog** and **README**.

**Non-goals for 0.21.0:** custom Streamlit **components** beyond what **`st.dataframe`** / **`st.data_editor`** provide; **hosted** Streamlit Cloud–specific packaging.

---

## Shipped in 0.22.0 (comprehensive `pydantable.io`)

- [x] **Rust readers/writers:** **`pydantable._core`** **`io_read_*_path`** / **`io_write_*_path`** for **Parquet**, **IPC**, **CSV**, **NDJSON** ( **`Python::allow_threads`** on reads).
- [x] **Python façade:** **`pydantable.io`** sync/async API, **`PYDANTABLE_IO_ENGINE`**, PyArrow fallbacks, **`[io]`** / **`[sql]`** / **`[cloud]`** / **`[excel]`** / **`[kafka]`** / **`[bq]`** / **`[snowflake]`** / **`[rap]`** extras in **`pyproject.toml`**.
- [x] **SQLAlchemy:** **`read_sql`** / **`write_sql`** (any SQLAlchemy URL/dialect; parameterized SQL; drivers installed separately). **Renamed** to **`fetch_sql`** in **0.23.0**.
- [x] **Experimental transports:** **`fetch_bytes`**, URL readers, **`fsspec`** object-store helper behind **`PYDANTABLE_IO_EXPERIMENTAL`**.
- [x] **Docs:** **`DATA_IO_SOURCES.md`**, **`EXECUTION.md`**, **`FASTAPI.md`**, **`changelog.md`**, this section.
- [x] **Tests:** **`tests/test_io_comprehensive.py`**.

**Deferred / not in-tree:** Rust **`sqlx`** drivers (documented SQLAlchemy-first); **`pyo3-asyncio`** + Tokio for I/O (thread offload remains default).

---

## Shipped in 0.23.0 (out-of-core scan roots + I/O renames)

- [x] **Lazy file entry:** **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`** (+ **`aread_*`**) return **`ScanFileRoot`**; engine builds Polars **`LazyFrame`** from the path without a full Python **`dict[str, list]`** for the scanned table.
- [x] **Lazy write:** **`DataFrame.write_parquet`** / **`DataFrameModel.write_parquet`** (and **`write_csv`**, **`write_ipc`**, **`write_ndjson`**) — Rust pipeline output (internal **`sink_*`** symbols).
- [x] **Breaking renames:** file **`read_*` / `aread_*`** → **`materialize_*` / `amaterialize_*`**; **`read_sql` / `aread_sql`** → **`fetch_sql` / `afetch_sql`**; HTTP **`read_*_url`** → **`fetch_*_url`**. **`DataFrameModel`** classmethods follow the same names.
- [x] **Limitations:** **join**, **concat**, **group_by**, **melt**, **pivot**, **explode**, **unnest**, **dynamic group** on lazy file roots: see [`EXECUTION.md`](EXECUTION.md) matrix (evolves with Polars).
- [x] **Docs + tests:** [`EXECUTION.md`](EXECUTION.md), [`DATA_IO_SOURCES.md`](DATA_IO_SOURCES.md), [`FASTAPI.md`](FASTAPI.md), [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md); **`tests/test_io_comprehensive.py`** (**`test_read_parquet_filter_write_roundtrip`**, HTTP **`fetch_*`**, SQL **`Connection`**); **`tests/test_io_improvements.py`** (JSON, **`max_bytes`**, URL context managers, **`MissingRustExtensionError`** subprocess, async I/O + **`DataFrameModel`** SQL shims); **`tests/test_hypothesis_properties.py`** (bounded lazy Parquet **`read_*` + filter**).

**Later:** Polars **`streaming`** / **`PYDANTABLE_ENGINE_STREAMING`** knob; **`collect_batches`**; scan-backed joins.

---

## Planned v1.0.0 (production-ready major release) — completed for `1.0.0`

**Goal (achieved for `v1.0.0`):** a **stable public API** under **semver**, **documented** semantics, and **repeatable** release quality—not a large new feature dump.

- [x] **Precondition:** **Shipped in 0.19.0** and **0.20.0** (above) are complete or any remaining gap is noted in this file or [`changelog.md`](changelog.md).
- [x] **Semver contract:** publish a **1.0** policy (expand [`VERSIONING.md`](VERSIONING.md) and/or [`README.md`](README.md)): what counts as **patch** vs **minor** vs **major** for **1.x** for `DataFrame` / `DataFrameModel` / `Expr` / Rust extension boundaries; confirm [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) is the behavioral source of truth. (**0.x** expectations already live in [`VERSIONING.md`](VERSIONING.md).)
- [x] **Packaging and versions:** **`pyproject.toml`** / **`Cargo.toml`** / extension **`rust_version()`** alignment; **Maturin** release workflow (e.g. [`.github/workflows/release.yml`](../.github/workflows/release.yml)) exercised or dry-run validated; **PyPI** **sdist + wheels** for declared platforms; optional **SBOM** or supply-chain notes if policy requires them.
- [x] **Quality bar:** full **`make check-full`**, **`cargo test --all-features`**, **`cargo check --no-default-features`**, and **pytest** (including optional-deps legs that match **CI**) on the **exact** commit tagged **`v1.0.0`**; **no known P0/P1** regressions against **INTERFACE_CONTRACT**.
- [x] **Security tooling:** **`cargo audit`** / **`cargo deny`** (or documented exceptions) current; policy for how **1.x** will handle **RUSTSEC** / advisory bumps.
- [x] **Documentation and comms:** **README** + doc site **`index`** lead with **1.0** positioning; **changelog** **`1.0.0`** section highlights stability scope; **upgrade path** from **0.20.x** in one place (even if “no breaking changes from last 0.20”).
- [x] **Support matrix:** state supported **Python** versions and **Polars** optional-extra expectations for **1.0.x**; link [`DEVELOPER.md`](DEVELOPER.md) for contributors.

**Out of scope for the 1.0.0 tag itself:** new execution engines (**Spark**, SQL backend, etc.)—those stay under **After v1.0.0** unless a maintainer explicitly promotes an exception.

---

## Later (not started)

Work **not** scheduled in the **0.17.0–0.20.0** shipped sections or **Planned v1.0.0** above, or explicitly deferred when scope slips:

- [ ] **Non-string map keys** (**`dict[int, T]`** and Arrow maps whose keys are not UTF-8 strings): still **not shipped** after **0.20.0** (explicitly deferred; see **Shipped in 0.18.0** / **0.19.0** and [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md)). **Heterogeneous** keys / full **Arrow + expression** parity may be revisited after **v1.0.0** unless promoted earlier.
- [ ] Items deferred from earlier releases when priorities change.
- [ ] **Chunked / streaming** async iterators for JSON or row batches (no minimal contract yet).
- [ ] Longer-horizon experiments that do not fit the **pre-1.0** train (**0.17–0.19**) or the **v1.0.0** production gate.
- [ ] **FastAPI ecosystem (optional):** thin **`pydantable[fastapi]`** extra with **pinned** **`fastapi` / `starlette`**, **middleware**, or **router** kits—**only** if demand and maintenance bandwidth are clear.

---

(future-expr-and-dataframe-method-candidates-not-scheduled)=
## Future Expr and DataFrame method candidates (not scheduled)

**Additive** APIs aligned with Polars / PySpark ergonomics. Each needs Rust IR + typing +
`INTERFACE_CONTRACT` / `SUPPORTED_TYPES` updates, façade mirrors where applicable, and
contract tests. Order is **not** priority order.

### String and text

- [ ] **`str_replace_all`** / **regex replace-all** distinct from single **`str_replace`**
  (Rust-regex dialect; document vs Polars naming).
- [ ] **`str_extract_all`** → **`list[str]`** (all non-overlapping matches; dtype story for
  empty matches).
- [ ] **`str_count_matches`** (regex or literal; consistent with existing predicate dialect
  split).
- [ ] **`str_find`** / **`str_rfind`** (substring index or null; Unicode scalar index rules).
- [ ] **`str_pad_start` / `str_pad_end` variants:** width from another column (expression
  width) if we extend the IR beyond scalar **`length`**.
- [ ] **Unicode normalize** (`NFC` / `NFD` / …) as an opt-in string unary (policy: which
  forms are supported on the Polars path).
- [ ] **Parsing helpers on `str`:** **`parse_int`**, **`parse_float`**, **`parse_bool`**
  (strict vs loose; null on failure).
- [ ] **`base64_encode` / `base64_decode`** (Binary ↔ str contracts).
- [ ] **Title / case variants:** e.g. Polars **`to_titlecase`** if distinct from
  **`upper`/`lower`** for user locales.

### Numeric and boolean

- [ ] **`clip(lower, upper)`** on **`int` / `float`** (inclusive bounds; null propagation).
- [ ] **`sign`**; **`is_nan`**, **`is_finite`**, **`is_infinite`** on float (and **Decimal**
  policy).
- [ ] **Element-wise math:** **`pow`**, **`sqrt`**, **`log`**, **`log10`**, **`exp`**
  (separate from **`round`/`floor`/`ceil`**).
- [ ] **Typed `between(low, high)`** as **`Expr`** (inclusive/exclusive flags; three-valued
  logic with nulls).
- [ ] **Bitwise ops** on integers where dtypes are unambiguous (`&`, `|`, `^`, `~` or named
  methods).

### Temporal

- [ ] **ISO week-year pairing:** **`dt_iso_year`** (or **`dt_week_year`**) alongside
  **`dt_week`** where users expect ISO year boundaries.
- [ ] **Offset / truncate / round:** **`dt_offset_by`**, **`dt_truncate`**, **`dt_round`**
  (calendar buckets; timezone-aware semantics must match Polars and docs).
- [ ] **`dt_combine`** ( **`date` + `time` → `datetime`** ) and related constructors from
  parts.

### Lists

- [ ] **`list_slice`**, **`list_head`**, **`list_tail`** (count / index from end; OOB rules
  like **`list_get`**).
- [ ] **`list_concat`** (per-row concat of two **`list[T]`** columns with compatible **`T`**).
- [ ] **`list_drop_nulls`** / **`list_compact`** (null elements inside list cells).
- [ ] **`list_arg_min`**, **`list_arg_max`** (index of min/max; tie-break policy).
- [ ] **`list_std`**, **`list_var`** (population vs sample; mirror Polars).
- [ ] **`list_reverse`**, **`list_shuffle`** (deterministic seed story if we expose RNG).
- [ ] **`list_eval`** / element-wise lambda (very large scope; likely last—needs a typed
  closure or limited sub-language).

### Structs and maps

- [ ] **`struct_rename_fields`**, **`struct_with_fields`** (add/replace nested fields by
  name).
- [ ] **`struct_json_encode`** / **`struct_json_path_match`** symmetry with string JSON
  helpers.
- [ ] **Map transforms:** **`map_filter`**, **`map_entries_sorted`**, **`map_zip`** where
  Polars exposes stable operations and our schema story stays **`dict[str, T]`**.

### Windows and ranking

- [ ] **`percent_rank`**, **`ntile`**, **`cume_dist`** (frame and null ordering spelled out
  per **`WINDOW_SQL_SEMANTICS`**; **`row_number`**, **`rank`**, and **`dense_rank`** already
  exist).
- [ ] **`first_value` / `last_value`** with **ignore nulls** flags (Polars parity).
- [ ] **`lag` / `lead`** extensions: optional **default** value when the shift falls outside
  the partition (Spark-style **`default`** parameter).

### Table-level and analytics helpers

- [ ] **`DataFrame` / `DataFrameModel`:** **`quantile`**, **`median`** (multi-column),
  **`corr`** / **`cov`** matrix helpers (materialization cost documented).
- [ ] **`approx_n_unique`** / HyperLogLog-style sketch (if we add global or grouped
  approx aggregates).

### Interop and literals

- [ ] **`Expr.hash`** / row fingerprint (algorithm choice; stable across sessions or not).
- [ ] **Arrow / Polars scalar bridging** in expressions (only if we define a strict
  embedding contract).

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
