# Developer Guide

This guide is for contributors working on `pydantable` internals.

## Prerequisites

- Python `3.10+` (project metadata targets `>=3.10`; CI exercises 3.10–3.13)
- Rust toolchain (`rustup`, `cargo`) — **`rust-toolchain.toml`** at repo root pins **`1.94.1`** (rustup uses it automatically in this directory)
- `maturin` for building the PyO3 extension

## Local Environment Setup

From repo root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
# Editable protocol (same repo; satisfies the ``pydantable-protocol`` pin for local work):
.venv/bin/python -m pip install -e ./pydantable-protocol
# Editable install + test/lint/docs extras (matches CI-ish dev env):
.venv/bin/python -m pip install -e ".[dev,docs]"
# Build the extension (required for lazy I/O tests and accurate doc version):
(cd pydantable-native && maturin develop --manifest-path ../pydantable-core/Cargo.toml)
```

Minimal alternative (narrower deps): **`pip install maturin pytest pytest-asyncio ruff`** then **`pip install -e .`** — you still need **`maturin develop`** from `pydantable-native` (or to install a published `pydantable-native` wheel) for native execution.

**`make check-full`** (from repo root) runs Ruff on the whole tree, Astral **`ty`** on the three first-party trees (see **`[tool.ty]`** in **`pyproject.toml`**), a minimal **`ty`** venv (no NumPy/PyArrow), Pyright, typing-artifact checks, Sphinx, and Rust **`cargo fmt` / clippy / test**. **`make rust-test`** prepends **`python/`**, **`pydantable-protocol/python`**, and **`pydantable-native/python`** to **`PYTHONPATH`** (then the venv’s **`site-packages`**) so PyO3 tests resolve **`pydantable`**, **`pydantable_protocol`**, and **`polars`** consistently without relying on editable installs alone. **`make native-develop`** runs **`pip install -e ./pydantable-protocol`** before **`maturin develop`**. **`mypy`** remains in **`[dev]`** for the optional schema-evolving plugin and **`tests/test_mypy_*.py`** subprocess checks.

For **which checker runs where**, **`ty`** vs **mypy** vs **Pyright**, phased strictness, and public-vs-internal **`Any`**, see {doc}`TYPING` (contributor section).

### Architecture notes (SOLID-oriented)

Contributors should keep boundaries clear so the codebase stays testable and third-party engines remain possible (see {doc}`CUSTOM_ENGINE_PACKAGE` and {doc}`ADR-engines`).

| Principle | In this repo |
|-----------|----------------|
| **S** (single responsibility) | Prefer focused modules: plan execution vs expr typing vs I/O. Avoid growing “god” files when a submodule or helper will do. |
| **O** (open/closed) | Extend via new plan steps, `ExprNode` variants, or `EngineCapabilities` flags rather than editing unrelated code paths. |
| **L** (Liskov substitution) | Any `ExecutionEngine` implementation (native, stub, SQL/Moltres) must honor the protocol: same error types for unsupported ops, capabilities that match behavior. |
| **I** (interface segregation) | Depend on the smallest protocol that suffices: `PlanExecutor` for execute-only call sites; full `ExecutionEngine` only when building plans or using sinks. |
| **D** (dependency inversion) | Application code depends on **`pydantable_protocol`** protocols and `get_default_engine()` / injected `engine=`, not on `pydantable_native` or `pydantable._core` directly. |

**Engine coupling gate:** `make check-python` runs **`scripts/check_engine_bypass.py`**, which fails if `pydantable._core` is imported outside the allowlist (`python/pydantable/engine/`, `rust_engine.py`, `_extension.py`). Do not bypass this without an ADR update.

**Imports:** Prefer `from pydantable.engine import get_default_engine` (and protocols from `pydantable.engine` or `pydantable_protocol`) over `pydantable_native` in non-engine packages.

**Expected native touchpoints (audit):** `pydantable_native` and `pydantable_native.io_core` are allowed where lazy I/O or scan roots must talk to Rust (`python/pydantable/io/`, `python/pydantable/io/http.py`). `pydantable._core` stays behind `scripts/check_engine_bypass.py`. `python/pydantable/plan.py` documents native plan types; `rust_engine.py` and `python/pydantable/engine/` load the extension. Everything else should go through protocols (`ExecutionEngine`, `PlanExecutor`) or documented facades.

Activate when working interactively:

```bash
source .venv/bin/activate
```

## Optional CycloneDX SBOMs (local)

CI and the tag **Release** workflow do **not** generate SBOMs. If you need **CycloneDX**
files for compliance, install **`cyclonedx-bom`** and **`cargo-cyclonedx`** locally and run
**`cyclonedx-py environment`** / **`cargo cyclonedx`** against your checkout (see historical
**1.2.0** changelog notes for the former automation shape).

## Rust feature flags (`pydantable-core`)

The crate’s `[features]` block in `pydantable-core/Cargo.toml` gates the Polars engine (`polars_engine`, on by default). Splitting optional codecs or transports into finer-grained features would need coordinated **maturin** / **pyproject.toml** / CI changes; treat that as a release-engineering follow-up rather than everyday contributor work.

## Repository Layout

- `pydantable-protocol/python/pydantable_protocol/`: **`ExecutionEngine`**, shared exceptions (**`MissingRustExtensionError`**, **`UnsupportedEngineOperationError`**), and related types with **no runtime dependencies** (for third-party backends that should not install **`pydantable`**).
- **`entei-core`** (separate PyPI package) — **`EnteiPydantableEngine`** + **`MongoRoot`**. **`EnteiDataFrame`** / **`EnteiDataFrameModel`** live in **`pydantable.mongo_entei`**, like **`SqlDataFrame`** in **`pydantable.sql_moltres`**. Install **`pip install entei-core`** or **`pip install "pydantable[mongo]"`**. Run **`make test-mongo`** for **`tests/mongo/`** (**mongomock**). **entei-core**’s own tests live in that project’s repository.
- `python/pydantable/`: thin Python API layer + Pydantic integration (`dataframe/`, `schema/` packages with `_impl.py` bodies)
- `python/pydantable/engine/`: execution engine abstraction; the native backend ships in **`pydantable-native`**, which depends **only** on **`pydantable-protocol`** and wraps **`pydantable_native._core`**. **`pydantable`** requires **`pydantable-native`** on PyPI, so **`get_default_engine()`** works after **`pip install pydantable`**. **`rust_engine.py`** remains a thin compatibility shim delegating to the default engine.
- `pydantable-core/src/`: Rust core (`dtype`, `expr`, `plan`, PyO3 exports)
- `tests/`: Python integration/unit tests for behavior contracts
- `docs/`: product docs + roadmap/spec docs (built with **Sphinx** + MyST; see below)

### Adding another execution engine

Authoritative guide for **separate PyPI packages**: {doc}`CUSTOM_ENGINE_PACKAGE`.

Implement **`ExecutionEngine`** from **`pydantable_protocol`** (also re-exported as **`pydantable.engine.protocols`**): plan transforms, **`execute_plan`** / async variants, sinks, **`capabilities`**. Raise **`pydantable_protocol.UnsupportedEngineOperationError`** (or **`pydantable.errors.UnsupportedEngineOperationError`**, which inherits from it) when the backend cannot support a call. Set **`EngineCapabilities.backend`** to **`"custom"`** (or **`"stub"`** for test doubles) and populate feature flags honestly.

**Dependency shape:** a separate PyPI project can **`pip install pydantable-protocol`** only — no **`pydantable`** dependency — and still type-check against **`ExecutionEngine`**. End users combine your engine with **`pydantable`** at the application layer (for example **`DataFrameModel(..., engine=...)`**).

**Checklist**

1. **`make_plan`** / **`plan_*`** — logical plan transforms used by **`DataFrame`** (mirror **`NativePolarsEngine`**).
2. **Execution** — **`execute_plan`**, **`async_execute_plan`**, **`collect_batches`**, **`async_collect_plan_batches`**, and **`execute_*`** helpers as needed.
3. **Sinks** — **`write_parquet`**, **`write_csv`**, **`write_ipc`**, **`write_ndjson`** or raise **`UnsupportedEngineOperationError`**.
4. **Async flags** — **`has_async_execute_plan`**, **`has_async_collect_plan_batches`** must reflect real support.
5. **Expressions (Track A):** **`get_expression_runtime()`** only works automatically for **`NativePolarsEngine`**; other defaults need **`set_expression_runtime(...)`** or a parallel API until a portable expression IR exists.

**Tests:** Prefer patching **`NativePolarsEngine`** on the class when replacing engine behavior for frames that use the default engine. See **`StubExecutionEngine`** and **`tests/test_engine_stub.py`**.

**Guardrail:** Run **`python scripts/check_engine_bypass.py`** (included in **`make check-python`** as **`engine-bypass-check`**) after changes under **`python/pydantable/`**. It rejects direct native-extension imports and **`get_default_engine().rust_core`** outside the allowlist documented in {doc}`ADR-engines`.

- **Protocols:** `PlanExecutor`, `SinkWriter`, and **`ExecutionEngine`** in **`pydantable_protocol`** (thin re-export in `python/pydantable/engine/protocols.py`). **`EngineCapabilities`** includes **`backend`** and optional-feature flags.
- **ADR:** See {doc}`ADR-engines` for design notes and Track B (portable IR).

### Engine injection (dependency inversion)

Prefer **explicit** engines over relying on the process-wide default when you need predictable tests or multiple backends:

- **Per frame:** pass **`engine=your_engine`** to **`DataFrame[Schema](..., engine=...)`** (and **`DataFrameModel`** / **`SqlDataFrameModel`** where supported). This is the clearest contract for libraries and unit tests.
- **Process default:** **`get_default_engine()`** / **`set_default_engine(...)`** in **`pydantable.engine`** (used when **`engine=`** is omitted). Use **`set_default_engine(None)`** in test teardown to restore lazy construction of the native engine.
- **Future option:** a **`contextvars`-scoped override** could be added without removing the global API; it is not implemented today—if you need thread-local engines, pass **`engine=`** per frame or open a design discussion.

For third-party **`ExecutionEngine`** packages, see {doc}`CUSTOM_ENGINE_PACKAGE`.

(docs-sphinx-build)=

## Documentation builds (Sphinx)

**Read the Docs** (configured via `.readthedocs.yaml` at the repository root) uses **Sphinx only**, not MkDocs. The published site is the single source of truth for navigation and cross-links. RTD installs **Sphinx + theme deps only** (not `pip install .[docs]`), because installing the native extension typically **exceeds** RTD memory/time limits. `docs/conf.py` puts `python/` on `sys.path`, so autodoc and `import pydantable` work without `pydantable-native`.

Local HTML build (from repo root, after `pip install -e ".[docs]"`):

```bash
sphinx-build -b html docs docs/_build/html
```

Strict mode (warnings as errors), matching a tightened RTD config:

```bash
sphinx-build -W -b html docs docs/_build/html
```

Check external links (optional):

```bash
sphinx-build -b linkcheck docs docs/_build/linkcheck
```

User-facing doc changes should keep the repository `README.md` and `docs/` aligned; run `scripts/verify_doc_examples.py` before merging (see below).

## Notebooks (Jupyter / VS Code)

Canonical walkthrough: {doc}`QUICKSTART` and **`notebooks/five_minute_tour.ipynb`** at the repo root.

Open the repo’s notebook or a scratch **`.ipynb`** with the venv that has **`pip install -e ".[dev]"`** and `pydantable-native` installed/built. In a cell:

```python
from pydantable import DataFrame
from pydantable import Schema

class Row(Schema):
    x: int

df = DataFrame[Row]({"x": [1, 2, 3]})
df  # last expression → HTML table in Jupyter / VS Code
```

- **`display(df)`** or the last expression in a cell uses **`_repr_html_()`** on **`DataFrame`** / **`DataFrameModel`** (bounded rows/columns; see {doc}`EXECUTION` **Jupyter / HTML**).
- **`repr(df)`** is the plain-text path (no **`collect()`** for row counts—see {doc}`EXECUTION` **repr**).
- **`df.shape`**, **`df.info()`**, **`df.describe()`** follow {doc}`INTERFACE_CONTRACT` **Introspection** ( **`describe`** covers **int** / **float** / **bool** / **str** / **`date`** / **`datetime`** per that page).

No extra **ipywidgets** dependency is required for the default HTML table.

**Interactive widgets (optional / later):** richer **sliders** or **page-size** controls for HTML previews would require **`ipywidgets`** (or similar) and a maintenance commitment—they are **not** bundled. Use **`set_display_options`** / env vars for bounded previews, or wrap **`display(HTML(df._repr_html_()))`** in your own widget code.

## Architecture (Rust-first)

Current contract direction:

- Python is the ergonomic API boundary (`DataFrameModel`, stubs, docs, Pydantic)
- Rust is the source of truth for expression typing, logical-plan validation, and execution for supported operations
- Python wrappers should avoid duplicating validation that Rust already enforces

### SOLID-oriented layout (internals)

- **Rust — `plan/execute_polars/`:** Polars execution is split into `common`, `runner` (`PolarsPlanRunner`), `materialize`, `join_exec`, `groupby_exec`, `concat_exec`, `literal_agg`, and `reshape_exec`, re-exported from `execute_polars/mod.rs`. **`common::polars_err_ctx`** (0.18.0+) labels Polars **`collect()`** failures in **`groupby_exec`** as **`group_by().agg()`** in the Python **`ValueError`** text—see `execute_polars/common.rs` and {doc}`EXECUTION`.
- **Rust — `python_api/`:** PyO3 bindings are split into `types` (`PyExpr`, `PyPlan`), `expr_fns`, `plan_fns`, and `exec_fns`; `mod.rs` only registers symbols on `_core`.
- **Rust — `plan/executor.rs`:** `PhysicalPlanExecutor` dispatches full-plan `execute_plan`. With `polars_engine`, inherent methods on `PolarsExecutor` (`join`, `groupby_agg`, `concat`, `melt`, `pivot`, `explode`, `unnest`, `groupby_dynamic_agg`) forward to `execute_polars::*` so call sites depend on the executor type rather than raw free functions.
- **Python — `dataframe/` and `schema/`:** Public API stays `pydantable.dataframe` and `pydantable.schema`; implementations live in `_impl.py` inside each package for a single-responsibility split without changing imports.

**`unwrap()` / `expect()` in non-test Rust:** Prefer `?` and structured errors in new code. A scan of `pydantable-core/src` excluding test-only modules shows production uses are minimal (e.g. `plan/execute_polars/literal_agg.rs` uses `expect` only after an explicit non-empty guard). CI runs **`cargo clippy`** with **`-D warnings`** on `pydantable-core`.

### Extension checklist (new surface area)

When adding a feature, touch the minimal set in order:

1. **Expr / dtype (Rust):** `expr/ir.rs`, `expr/typing.rs`, `expr/lower_polars.rs` (and PyO3 builders in `python_api/expr_fns.rs` if exposed to Python).
2. **Plan step (Rust):** `plan/ir.rs` (`PlanStep`), `plan/build.rs`, `plan/serialize.rs`, `plan/execute_polars/runner.rs` (or the relevant `execute_polars/*` module), plus tests under `pydantable-core`.
3. **Python `DataFrame` API:** `python/pydantable/dataframe/_impl.py` (and `rust_engine.py` if a new `_core` entry point is required).
4. **Docs / changelog:** user-visible behavior belongs in the docs site and `docs/CHANGELOG.md`.

### Optional: narrow `Protocol` for tests (Python)

Integration tests can depend on a small structural protocol instead of mocking all of `_core`:

```python
from typing import Any, Protocol

class RustCorePlan(Protocol):
    def make_plan(self, schema_fields: dict[str, Any]) -> Any: ...
    def execute_plan(self, plan: Any, data: Any, *, as_python_lists: bool = False) -> Any: ...
```

Use this only where it reduces brittle monkeypatching; production code reaches the extension via `pydantable-native` and `get_default_engine()`.

Phase 4 boundary contract:

- Rust plan schema metadata is exported as descriptors:
  - `{"base": "int|float|bool|str", "nullable": bool}`
- Python reconstructs type annotations for derived schemas from those descriptors

## Build and Test Commands

### Which tests are authoritative

- **`cargo test` in `pydantable-core/`** exercises Rust plan/expr contracts and PyO3 wiring. From the **repo root**, prefer **`make rust-test`** (or set `PYO3_PYTHON` to `.venv/bin/python` and `PYTHONPATH` to the venv’s `site-packages`, as in the `Makefile`): PyO3’s embedded interpreter does not always load `site-packages`, and some Polars-backed plan tests **import Python `polars`** — without that env, two tests can fail with `ModuleNotFoundError: polars`.
- **`pytest` on `tests/`** is the **CI-facing** check for end-to-end behavior (`collect`, joins, UIs). Prefer adding user-visible regressions here when behavior crosses the Python boundary.
- **Pandas UI:** `tests/test_pandas_ui.py` (broad façade) and `tests/test_pandas_ui_popular_features.py` (duplicates, **`get_dummies`**, **`cut`/`qcut`**, **`factorize_column`**, **`ewm`**, **`pivot`**).
- **Async tests:** **`pytest-asyncio`** is in **`[dev]`**; `pyproject.toml` sets **`asyncio_mode = auto`** so `async def` tests run without extra markers unless you prefer explicit `@pytest.mark.asyncio`.
- **Markers:** **`slow`** — timing guardrails (`pytest -m "not slow"`). **`network`** — loopback HTTP servers (`pytest -m "not network"` for a faster local loop). **`optional_cloud`** — mocked/heavy cloud SDK tests (see `tests/test_io_extras_and_transports.py`). Registered in **`pyproject.toml`** **`[tool.pytest.ini_options]`** **`markers`**.

### Release ↔ tests map (0.15.0–0.20.0)

Use this table to locate **Python tests and doc-example smoke** that back shipped minors (see `docs/CHANGELOG.md` for narrative highlights). It is not a substitute for line coverage.

| Release | Highlights (summary) | Primary test modules |
| --- | --- | --- |
| **0.15.0** | Async `acollect` / `ato_*`, Arrow `map` ingest, PySpark `trim` / `abs` / …, `validate_data` removal | `tests/test_async_materialization.py`, `tests/test_pyarrow_map_ingest.py`, `tests/test_v015_features.py`, `tests/test_v015_constructor_api.py`; `tests/test_fastapi_recipes.py` (sync `TestClient` / OpenAPI from **0.14+** plus async materialization routes) |
| **0.16.0** | Parquet/IPC **`dict[str, list]`** readers (later **`materialize_*`**), `to_arrow` / `ato_arrow`, `Table` / `RecordBatch` constructors, FastAPI multipart | `tests/test_arrow_interchange.py`; `tests/test_fastapi_recipes.py` (multipart Parquet); `scripts/verify_doc_examples.py` (Parquet + `to_arrow` smoke; see comment near `materialize_parquet`) |
| **0.23.0** | **`read_*` / `aread_*`** lazy roots, **`DataFrame.write_*`**, **`export_*`**, JSON/HTTP ctx, **`MissingRustExtensionError`**, I/O renames (**`materialize_*`**, **`fetch_sql`**, **`fetch_*_url`**) | `tests/test_io_comprehensive.py` (round-trips, HTTP **`fetch_*`**, SQL **`Connection`**); `tests/test_io_improvements.py` (JSON, **`max_bytes`**, URL ctx, subprocess stub-`_core` error, async I/O shims); `docs/EXECUTION.md` streaming matrix |
| **0.16.1** | Map-column arithmetic `TypeError` (not panic); `validate_columns_strict` Arrow `pydantable.io` import fix | `tests/test_expr_070_surfaces.py`; `tests/test_arrow_interchange.py` (`test_dataframe_generic_accepts_pa_table`) |
| **0.17.0** | Map `Expr` contracts after Arrow ingest; PySpark `functions` string/list/bytes wrappers | `tests/test_pyarrow_map_ingest.py` (`test_arrow_map_ingest_then_map_get_and_contains`); `tests/test_pyspark_sql.py` (new façade tests) |
| **0.18.0** | Grouped Polars error context (`polars_err_ctx`); map-key deferral (docs); Hypothesis + integration `join` / `group_by` smoke | `tests/test_v018_features.py`; `tests/test_hypothesis_properties.py` (`test_group_by_sum_matches_manual`, `test_inner_join_unique_ids_row_count`, …); Rust: `execute_polars/common.rs` (`polars_err_format_tests`), `groupby_exec.rs` |
| **0.19.0** | Pre-1.0 doc consolidation (`VERSIONING`, parity/README/index, `PERFORMANCE` note); CI-stable grouped test ordering; bug-hunt hardening (see below) | `tests/test_v018_features.py` (`_sort_group_output`); `tests/test_schema_type_hints_narrowing.py`; `tests/test_fastapi_recipes.py` (`StreamingResponse` smoke); join assertions via `assert_table_eq_sorted` in `tests/test_advanced_ops_phase6.py`, `tests/test_dataframe_ops.py`; `scripts/verify_doc_examples.py` (`os._exit` teardown); docs: `docs/VERSIONING.md`, `docs/ROADMAP.md` **Shipped in 0.19.0** |
| **0.20.0** | Discovery (`columns`, `shape`, `info`, `describe` bool/str, `value_counts`); `pydantable.display` / `PYDANTABLE_VERBOSE_ERRORS` / `_repr_mimebundle_`; `Expr` `repr`; PySpark `show` / `summary`; `DataFrame` `repr` / `_repr_html_` | `tests/test_dataframe_discovery.py`, `tests/test_display_options.py`, `tests/test_rust_engine_verbose_errors.py`, `tests/test_expr_repr.py`, `tests/test_dataframe_repr.py`; docs: `docs/ROADMAP.md` **Shipped in 0.20.0** |

#### Changelog-driven audit (0.15.0–0.20.0)

Cross-check each bullet in `docs/CHANGELOG.md` for recent minors against tests or `verify_doc_examples.py`.

**0.15.0**

- **`executor=`** on async APIs: covered in `tests/test_async_materialization.py` (`acollect`, `ato_dict`, `ato_polars` with a custom executor).
- **`ato_polars`**, **`arows`**, **`ato_dicts`**: same module.
- **Arrow-native `map` ingest:** `tests/test_pyarrow_map_ingest.py`.
- **PySpark façade breadth (`trim`, …):** `tests/test_v015_features.py` (and related parity tests as listed in the changelog).
- **`validate_data` removal / `trusted_mode`:** `tests/test_v015_constructor_api.py` plus constructor tests named in the changelog (**`test_v014_features.py`**, **`test_dataframe_model.py`**, **`test_dataframe_ops.py`**).
- **FastAPI async routes (`acollect` / `ato_dict`):** `tests/test_fastapi_recipes.py`. **`StreamingResponse`** smoke: `test_streaming_response_after_ato_dict`. **`lifespan`** and **`ThreadPoolExecutor`** are described in `docs/FASTAPI.md`; there is **no pytest** that wires a full **`lifespan`** + executor stack, and `scripts/verify_doc_examples.py` does not reference those symbols by name.

**0.16.0**

- **`read_ipc(..., as_stream=True)`:** `tests/test_arrow_interchange.py` (`test_read_ipc_stream_format_bytes`).
- **`RecordBatch` constructor path:** same file (`test_constructor_accepts_record_batch`).
- **`to_arrow` parity with `to_dict`:** `test_to_arrow_and_from_pydict_matches_to_dict`; **`ato_arrow`:** `test_ato_arrow`. Async **`ato_arrow`** is intentionally **not** repeated in `tests/test_async_materialization.py` (cross-link in that module’s docstring).
- **Multipart Parquet upload:** `tests/test_fastapi_recipes.py` (`test_multipart_parquet_upload`); **422** on bad row types: `test_row_list_invalid_type_is_422`.

**0.16.1**

- **Map arithmetic typing:** `tests/test_expr_070_surfaces.py` (`test_map_column_arithmetic_raises_typeerror_not_panic`).
- **`DataFrame[Schema](pa.Table)`:** `tests/test_arrow_interchange.py` (`test_dataframe_generic_accepts_pa_table`).

**0.17.0**

- **Map `map_get` / `map_contains_key` after PyArrow map ingest:** `tests/test_pyarrow_map_ingest.py` (`test_arrow_map_ingest_then_map_get_and_contains`).
- **PySpark `sql.functions` wrappers:** `tests/test_pyspark_sql.py` (`test_sql_functions_string_and_list_wrappers`, `test_sql_functions_strptime_and_binary_len`).

**0.18.0**

- **Grouped execution error context:** Rust `polars_err_ctx` on **`group_by().agg()`** `collect()` in `execute_polars/common.rs` / `groupby_exec.rs`; notes in `docs/EXECUTION.md` and `docs/INTERFACE_CONTRACT.md`. Unit tests: `polars_err_format_tests` in `execute_polars/common.rs`.
- **Non-string map keys:** deferred—`docs/SUPPORTED_TYPES.md`, `docs/ROADMAP.md` **Later** (no new ingest code).
- **Hypothesis / integration:** `tests/test_hypothesis_properties.py` (group_by sum/count, inner/left join); `tests/test_v018_features.py` (empty group_by, multi-agg, semi/anti/left join, count vs group size).

**0.19.0**

- **Docs-only release** for API surface; no new Rust `Expr` or PySpark façade rows.
- **`VERSIONING.md`:** 0.x semver; links from `INTERFACE_CONTRACT.md`.
- **`PERFORMANCE.md`:** validation subsection (re-run scripts; no new headline timings vs 0.18.x).
- **Grouped tests:** `tests/test_v018_features.py` — `_sort_group_output` before comparing `group_by` columnar output to reference (order not API-guaranteed; `pytest -n auto` on Ubuntu CI).
- **Schema introspection:** `tests/test_schema_type_hints_narrowing.py` — narrowed `get_type_hints` exception tuples in `schema/_impl.py`.
- **FastAPI:** `tests/test_fastapi_recipes.py` — `test_streaming_response_after_ato_dict`.
- **Join ordering:** `assert_table_eq_sorted(..., keys=[...])` for inner-join dict equality in `tests/test_advanced_ops_phase6.py`, `tests/test_dataframe_ops.py`.
- **Doc-example script:** `scripts/verify_doc_examples.py` ends with `os._exit(0)` when `__name__ == "__main__"` to avoid teardown SIGABRT; CI no longer masks exit **134**.

**0.20.0**

- **Discovery / `describe` / `value_counts` / PySpark `show`:** `tests/test_dataframe_discovery.py`, `tests/test_display_options.py`.
- **Display / mimebundle / verbose errors:** `tests/test_display_options.py`, `tests/test_rust_engine_verbose_errors.py`.
- **`Expr` `repr`:** `tests/test_expr_repr.py`.
- **`DataFrame` `repr` / HTML:** `tests/test_dataframe_repr.py`.

### Optional follow-ups (non-blocking)

- **`lifespan`** + **`ThreadPoolExecutor`:** covered by **`tests/test_fastapi_recipes.py`** **`test_lifespan_thread_pool_executor_acollect`** (mirrors **`docs/FASTAPI.md`** async lifespan snippet).
- Split `tests/test_fastapi_recipes.py` by concern (sync vs async vs multipart) only if maintainers want stronger file-level separation; default is section comments in the single module.

### Run Python tests

```bash
.venv/bin/python -m pytest -q
```

Parallel (uses `pytest-xdist` from the `dev` extra):

```bash
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m pytest -q -n auto
```

**CI** (`.github/workflows/_shared-ci.yml` **python-tests**): every matrix leg runs **`pytest -q -n auto`** (Linux, Windows, macOS). **Ubuntu + Python 3.11** additionally runs **`--cov=pydantable --cov-report=xml --cov-report=term-missing:skip-covered --cov-fail-under=83`** and uploads **`coverage.xml`** as a workflow artifact (**`coverage-xml-py311-ubuntu`**).

**Hypothesis** property tests live in `tests/test_hypothesis_properties.py` (installed via **`[dev]`** / CI pip list). They run under the same `pytest` command; examples use bounded `max_examples` for CI speed.

Exclude timing-based guardrails (`tests/test_performance_guardrails.py`) for a quicker loop:

```bash
.venv/bin/python -m pytest -q -m "not slow"
```

Skip loopback HTTP tests locally if needed:

```bash
.venv/bin/python -m pytest -q -m "not network"
```

### Coverage (optional)

Install the `dev` extra (includes **`pytest-cov`**), then measure line/branch coverage of `python/pydantable/` (no local threshold enforced):

```bash
.venv/bin/python -m pytest -q --cov=pydantable --cov-report=term-missing
```

XML for tooling: **`--cov-report=xml`** (writes **`coverage.xml`**; gitignored). **CI** produces the same XML on **Ubuntu 3.11** (see above).

### Lint/format/type-check

```bash
.venv/bin/ruff format .
.venv/bin/ruff check .
.venv/bin/basedpyright
```

Typing-focused checks (generator drift + ty + typing snippet tests):

```bash
make check-typing
```

Typing contract tests live in:

- `tests/test_mypy_*typing*_contracts.py`
- `tests/test_pyright_*typing*_contracts.py`

When you add or change typing surface area:

- update `scripts/generate_typing_artifacts.py` (stub templates)
- re-run `python scripts/generate_typing_artifacts.py`
- extend snippet-based typing tests and re-run `make check-typing`

**Stub ownership:** Most `python/pydantable/**/*.pyi` files next to sources are **generated** by `generate_typing_artifacts.py` and mirrored under `typings/pydantable/`. **`dataframe_model.pyi`** and **`awaitable_dataframe_model.pyi`** are **hand-maintained** (the generator copies them verbatim); when you add or change `DataFrameModel` / awaitable I/O surface, update those two files and re-run the generator so `typings/` stays in sync.

### Build docs (Sphinx)

```bash
.venv/bin/python -m pip install -e ".[docs]"
.venv/bin/python -m sphinx -b html docs docs/_build/html
```

See [Documentation builds (Sphinx)](#docs-sphinx-build) for `sphinx-build -W` and `linkcheck`.

### Verify runnable doc snippets (README + `docs/`)

After a normal editable install (so `pydantable-native` is built), run from repo root:

```bash
PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py
```

This executes the same patterns as the Quick Start and several doc examples (no network). Fix failures before merging user-facing doc changes.

**`docs/examples/**/*.py`:** `tests/test_docs_example_scripts.py` subprocess-runs each example script (skips `docs/examples/fastapi/service_layout/`, which is meant to be run with `uvicorn` from that directory). Keeps runnable files under `docs/examples/` in sync with the API; requires a built `pydantable-native`.

**CI** runs `scripts/verify_doc_examples.py` on **Ubuntu + Python 3.11** after tests (see `.github/workflows/_shared-ci.yml` via `ci.yml`).

Optional Python **`polars`** for local `to_polars()` / benchmark comparisons: `pip install -e ".[polars]"` or `".[benchmark]"` (see benchmark sections below). CI installs Polars on the **Ubuntu 3.11** matrix leg so `import polars` / `to_polars()` tests are not skipped there.

### Build package wheel (mixed Python/Rust)

```bash
.venv/bin/maturin build --release
```

### Benchmarks (use a **release** Rust build)

Editable installs often compile the native extension in **debug** mode. That is fine for development but **not** comparable to production performance. Before running any benchmark that exercises the extension, build the optimized library:

```bash
(cd pydantable-native && .venv/bin/python -m maturin develop --release --manifest-path ../pydantable-core/Cargo.toml)
```

Then run the benchmark scripts (after `pip install -e ".[benchmark]"` for Polars/pandas).

**One-shot (release build + vs Polars + vs pandas):**

```bash
chmod +x benchmarks/run_release.sh   # once
./benchmarks/run_release.sh
./benchmarks/run_release.sh --rows 10000 50000 --rounds 7
```

### Run the Phase 5 execution baseline benchmark

Use a release extension (see above), then:

```bash
.venv/bin/python benchmarks/phase5_collect_baseline.py
```

### Compare PydanTable vs native Polars (Python)

Install the optional benchmark extra (pulls in Polars for the comparison side):

```bash
.venv/bin/python -m pip install -e ".[benchmark]"
(cd pydantable-native && .venv/bin/python -m maturin develop --release --manifest-path ../pydantable-core/Cargo.toml)
.venv/bin/python benchmarks/pydantable_vs_polars.py
```

The script reports mean wall time per scenario (filter/project, join, group-by) and the pydantable/polars ratio. Higher ratios mean more overhead from pydantable’s typed API and wrappers; both sides exercise similar Polars-backed work.

Default `--rows` includes **1,000,000** (along with 1k / 10k / 50k); a full default run can take **minutes**. For a quick check, pass smaller sets, e.g. `--rows 10000 50000`.

### Compare pydantable vs pandas

Uses the same optional `benchmark` extra (installs pandas alongside Polars):

```bash
.venv/bin/python -m pip install -e ".[benchmark]"
(cd pydantable-native && .venv/bin/python -m maturin develop --release --manifest-path ../pydantable-core/Cargo.toml)
.venv/bin/python benchmarks/pydantable_vs_pandas.py
```

Reports pydantable vs pandas wall time and ratio for the same three scenarios (eager pandas `DataFrame` / `merge` / `groupby`). Default `--rows` matches the Polars script (including **1,000,000**); use `--rows` to limit sizes for faster runs.

### Build/install extension in editable flow

Usually handled by `pip install -e .`. If you need a fresh wheel install:

```bash
.venv/bin/maturin build --release
.venv/bin/python -m pip install --force-reinstall pydantable-core/target/wheels/*.whl
```

## Contribution Guidelines

- Keep Python wrappers thin; prefer Rust ownership for planner/typing logic.
- Add tests with behavior changes:
  - Rust-side tests for internal plan contracts
  - Python tests for user-visible behavior and API compatibility
- Preserve existing error contracts unless intentionally changing behavior.
- Update docs (`README.md` + relevant docs in `docs/`) for any user-facing changes.
- **Local I/O kwargs:** New `scan_kwargs` / `write_kwargs` keys belong in `pydantable-core/src/plan/execute_polars/scan_kw.rs` (`ALLOWED` slices) and in `docs/DATA_IO_SOURCES.md` (and the format page) so unknown-key errors match what users can pass.

## Common Pitfalls

- Running tests outside `.venv` can pick up incompatible site-packages.
- PyO3/Rust test linking can vary by environment; Python integration tests are the authoritative CI-facing check for behavior parity.
- If local imports seem stale, rebuild/reinstall from `.venv` and rerun tests.

## Release Notes Checklist (for contributors)

- [ ] **Version matches everywhere (1.x.y):** root `pyproject.toml` (**`pydantable`**), `pydantable-protocol/pyproject.toml`, `pydantable-native/pyproject.toml`, `pydantable-core/Cargo.toml`, `python/pydantable/__init__.py` (**`__version__`**), `pydantable-protocol/python/pydantable_protocol/__init__.py` (**`__version__`**), and pinned deps (`pydantable-protocol==…`, `pydantable-native==…` on **`pydantable`**). **`rust_version()`** in `pydantable-core/src/python_api/expr_fns.rs` uses `env!("CARGO_PKG_VERSION")`. CI runs `tests/test_version_alignment.py` (`pydantable.__version__ == pydantable_native._core.rust_version()` and protocol matches core).
- [ ] `docs/CHANGELOG.md` has a section for the release with highlights
- [ ] `make check-full` passes (Ruff, ty, `cargo fmt --check`, `clippy -D warnings`, `cargo test --all-features`)
- [ ] Python tests pass in `.venv` (`pytest`)
- [ ] `scripts/verify_doc_examples.py` passes (requires native installed/built)
- [ ] Rust changes compile in package build path (`maturin build --release`)
- [ ] Docs updated for behavior/contract changes
- [ ] `sphinx-build -W -b html docs docs/_build/html` succeeds (matches Read the Docs `fail_on_warning`)
- [ ] Roadmap status updated when a phase milestone changes

## `v1.0.0` release gate checklist

Use this checklist on the exact commit intended for tag `v1.0.0` (and again for future **1.x.y** tags as needed):

- [ ] `make check-full` passes.
- [ ] `cargo test --manifest-path pydantable-core/Cargo.toml --all-features` passes.
- [ ] `cargo check --manifest-path pydantable-core/Cargo.toml --no-default-features` passes.
- [ ] full Python test suite passes in the CI-equivalent environment (`pytest -n auto` and required optional-deps legs).
- [ ] `tests/test_version_alignment.py` passes (`__version__ == _core.rust_version()`).
- [ ] release workflow dry-run evidence is captured (wheel/sdist expectations met).

### Security advisory policy (1.x)

For 1.x maintenance:

- `cargo audit` and `cargo deny` are expected to be current in CI.
- Any ignored advisory must be documented with:
  - advisory ID,
  - reason for temporary acceptance,
  - owner,
  - review/expiry target.
- Exceptions must be tracked in docs/CHANGELOG.md or workflow comments so they are visible during release review.

### Publishing (PyPI)

Pushing a git tag matching **`v*`** (for example **`v1.16.1`**) runs `.github/workflows/release.yml`: shared CI (same checks as **`ci.yml`**), then publishes **`pydantable-protocol`**, then **`pydantable-native`** **manylinux x86_64** (**sdist** + wheels) so **`pydantable-native`** is installable before **`pydantable`** (sdist + pure-Python wheel), while other **`pydantable-native`** **`maturin-action`** jobs run in parallel for additional platforms. The repository needs a **`PYPI_API_TOKEN`** secret (`TWINE_USERNAME` is **`__token__`** in the workflow). **`release.yml`** sets **`permissions: contents: read`** and **`actions: write`** so the reusable **`shared-ci`** job can use **`actions/cache`** and **`actions/upload-artifact`**. Versions come from each package’s **`pyproject.toml`** on the tagged commit. Keep the workflow’s **Python test install** (`.github/workflows/_shared-ci.yml`, **Install maturin and test deps**) aligned with **`pyproject.toml`** **`[project.optional-dependencies]`** **`dev`** + **`pandas`** + **`polars`** + **`pytest-cov`** + **`rapcsv`/`rapfiles`/`rapsqlite`** (plus the other CI-only pins: **`fastapi`**, **`httpx`**, **`sqlalchemy`**, **`streamlit`**, **`dataframe-api-compat`**, **`fsspec`**, **`openpyxl`**, **`kafka-python`**, **`google-cloud-bigquery`**, **`snowflake-connector-python`**, …) so optional tests are not skipped on matrix legs or on tag builds.

**GNU manylinux wheels** are built with **`PyO3/maturin-action`** inside the default **manylinux Docker** images (`manylinux: 2_17` / `2_28`). Avoid **`container: off`** plus **`--zig`** on the host for those targets: linker failures and **OOM** are common with a Polars-sized dependency tree. **musllinux** jobs still use **`--zig`** in `release.yml` as needed.

Tag the same commit for all distributions: **`v` + version** (for example **`v1.16.1`**). **`pip install pydantable==1.16.1`** resolves **`pydantable-protocol==1.16.1`** and **`pydantable-native==1.16.1`** from PyPI.
