# Roadmap: Local I/O ↔ Polars parity (1.11.0)

**Target release:** **1.11.0** (see {doc}`changelog`).

**Purpose:** Bring **filesystem-local** reads and writes closer to **Polars** capabilities—especially **multi-file and directory inputs**, **glob patterns**, **hive-style datasets**, and **partitioned outputs**—while keeping pydantable **schema-first** (typed `DataFrame` / `DataFrameModel`, Rust plans, `ScanFileRoot` → Polars `LazyFrame`).

**Implementation anchor:** Rust scan/write wiring in `pydantable-core` (`plan/execute_polars/scan_kw.rs` and related), Python surface in `pydantable.io` and `DataFrame` / `DataFrameModel` classmethods. **Polars version** is pinned by the crate (see `pydantable-core/Cargo.toml`); expose only options that exist on the **Rust** APIs in use.

**Related docs:** {doc}`IO_OVERVIEW`, {doc}`DATA_IO_SOURCES`, {doc}`IO_DECISION_TREE`, per-format pages ({doc}`IO_PARQUET`, {doc}`IO_CSV`, {doc}`IO_NDJSON`, {doc}`IO_JSON`, {doc}`IO_IPC`), {doc}`INTERFACE_CONTRACT`, {doc}`EXECUTION`, main {doc}`ROADMAP`.

---

## Goals

1. **Directory & glob reads:** Users can point **`read_*` / `aread_*`** at a **directory**, a **glob**, or a **recursive glob** and get the same *shape* of behavior they expect from **`pl.scan_parquet`**, **`scan_csv`**, **`scan_ndjson`**, **`scan_ipc`**—with explicit defaults and docs.
2. **NDJSON / IPC parity with Parquet & CSV:** Where Polars supports **file lists** / **glob** on JSON Lines and IPC, pydantable forwards **`scan_kwargs`** consistently (today **Parquet** and **CSV** already support **`glob`**; **NDJSON** does not yet in `scan_kw.rs`).
3. **Parquet dataset reads:** Support **hive-partitioned** and related **scan** options exposed by Polars Rust for **`scan_parquet`** (e.g. **`hive_partitioning`**, schema / missing-column behavior already partially covered by **`allow_missing_columns`**).
4. **Partitioned & multi-file writes:** Move beyond **single-file** sinks where Polars provides **partitioned** or **directory** lazy writes—starting with **Parquet** (highest demand), then evaluate **CSV** / **NDJSON** by cost and API fit.
5. **Eager & batched paths:** Clarify or extend **`materialize_*`** and **`iter_*` / `aiter_*`** for **multi-file** workflows (concatenated batches, per-file batches, or explicit “use lazy `read_*` instead”).
6. **Documentation & tests:** One **golden-path** narrative (“Polars-shaped local datasets”), **examples** under `docs/examples/io/`, and **contract tests** so semver and **allowed kwargs** lists stay truthful.

## Non-goals (for this roadmap unless promoted)

- **Object stores (S3, GCS, …)** and **HTTP** as primary deliverables—use existing **`read_from_object_store`**, **`fetch_*_url`**, and lazy URL Parquet; revisit in a **transport-focused** milestone if needed.
- **Full CSV parse parity** with every Polars Python option (**encoding**, **every** null sentinel, **dtypes** dict, …)—ship **high-value** kwargs first; track the rest under main {doc}`ROADMAP` **Later** or a future patch release.
- **Replacing Polars** or exposing raw **`LazyFrame`** as the default user API—pydantable remains **typed plans + materialization**.

---

## Phase A — Audit & semantics (docs-first)

**Outcome:** Agreed-upon behavior before large code changes; reduces churn in `INTERFACE_CONTRACT`.

| Item | Description | Status |
|------|-------------|--------|
| Polars Rust API survey | For **0.53.x** (or current crate pin): list **`ScanArgsParquet`**, **`LazyCsvReader`**, **`LazyJsonLineReader`**, **`scan_ipc`** options relevant to **paths / globs / hive**. | [ ] |
| Current pydantable matrix | Document what **`scan_kw.rs`** already maps vs what is **missing** (per format). | [ ] |
| Directory semantics | Specify: e.g. trailing **`/`** directory → implicit **`glob=True`** vs requiring explicit **`glob=True`** (match Polars; document in {doc}`IO_PARQUET` / {doc}`IO_CSV`). | [ ] |
| Lazy vs eager guidance | When to use **`read_*`** vs **`materialize_*`** vs **`iter_*`** for multi-file (table in {doc}`IO_DECISION_TREE` or {doc}`DATA_IO_SOURCES`). | [ ] |

**Checklist**

- [ ] Audit appendix landed (subsection in {doc}`DATA_IO_SOURCES` or {doc}`IO_OVERVIEW`, linked from here).
- [ ] {doc}`INTERFACE_CONTRACT` updated for any **new** defaults or guarantees.

---

## Phase B — Lazy reads (`ScanFileRoot`, `dispatch_file_scan`)

**Outcome:** Multi-file local scans match Polars for the supported matrix.

### B1 — Parquet

- [ ] Forward **hive-partitioning** (and related) **`scan_kwargs`** supported by **`LazyFrame::scan_parquet`** / **`ScanArgsParquet`** in the pinned Polars version; extend **`unknown_scan_keys`** allowlist and mapping in `scan_kw.rs`.
- [ ] Optional: **`include_file_paths`** / lineage-style columns if exposed on the same scan path without forking Polars internals.
- [ ] Tests: **hive-partitioned** fixture under `tests/` or `tests/fixtures/`; round-trip **`read_parquet`** → **`collect()`** / **`to_dict()`** with schema expectations.

### B2 — CSV

- [ ] Confirm **directory + `glob`** behavior matches Polars for representative cases; add tests for **`*.csv`** in a directory.
- [ ] Evaluate additional **high-traffic** `LazyCsvReader` options not yet in **`ALLOWED`** (only if needed for directory reads or common enterprise CSV).

### B3 — NDJSON / JSON Lines

- [ ] If **`LazyJsonLineReader`** / **`LazyFileListReader`** supports **`with_glob`** (or equivalent), add **`glob`** (and any required siblings) to **`lazy_ndjson_with_kwargs`** and document in {doc}`IO_NDJSON`.
- [ ] Tests: directory of **`.jsonl`** / **`.ndjson`** files; optional mixed extension policy (document if unsupported).

### B4 — IPC / Feather

- [ ] Inspect Polars **`scan_ipc`** + **`UnifiedScanArgs`** for **multi-file** / **glob**; thread supported options through **`dispatch_file_scan`** (today IPC scan kwargs are minimal).
- [ ] Tests: multi-file IPC or documented limitation.

### B5 — Lazy JSON array (`read_json`)

- [ ] Align **path** behavior with Phase A semantics (single file vs glob); document limits vs **NDJSON** in {doc}`IO_JSON`.

**Checklist**

- [ ] Python **`read_*` / `aread_*`** docstrings and **`pydantable.io` stubs** (`*.pyi`) list new kwargs.
- [ ] {doc}`DATA_IO_SOURCES` table (**Read kwargs**) updated.

---

## Phase C — Eager reads & iterators (`materialize_*`, `iter_*`, `aiter_*`)

**Outcome:** Clear multi-file story without forcing full materialization through undocumented paths.

| Item | Description | Status |
|------|-------------|--------|
| **`iter_*` over globs / dirs** | Helpers or documented patterns: yield **`dict[str, list]`** batches per file or concatenated, with bounded memory notes. | [ ] |
| **`materialize_*`** | Either implement **multi-file** eager reads where engine supports it, or **explicitly defer** with a single doc pointer to **`read_*`**. | [ ] |
| **Async** | **`aiter_*`** parity with any new sync capabilities (thread-pool behavior unchanged unless documented). | [ ] |

**Checklist**

- [ ] Examples in `docs/examples/io/` (e.g. `glob_iter_roundtrip.py` or extend existing).
- [ ] {doc}`IO_OVERVIEW` **Batched column dict I/O** subsection cross-links.

---

## Phase D — Writes (`write_*`, `write_*_batches`)

**Outcome:** Outputs can match **Polars-style partitioned datasets** where the engine allows.

### D1 — Lazy pipeline sinks (Rust plan → disk)

- [ ] **Partitioned Parquet:** directory output with **partition columns** and stable naming (hive-style layout), using Polars **sink** / write APIs available from the same execution stack as current **`write_parquet_file`**.
- [ ] **`write_kwargs`** extended for new sink options (compression, statistics, row group sizing—align with existing single-file keys where possible).
- [ ] Evaluate **CSV** / **NDJSON** partitioned or rolling multi-file writes (priority after Parquet).

### D2 — Batch writers

- [ ] **`write_*_batches`**: semantics for **target directory** vs **single file**, **append** vs **overwrite**, documented and tested.

**Checklist**

- [ ] {doc}`IO_PARQUET` (and related) describe **partitioned** usage; {doc}`INTERFACE_CONTRACT` notes guarantees (e.g. atomicity **not** promised unless stated).

---

## Phase E — Schema drift & observability

**Outcome:** Predictable behavior when files **disagree** on columns or dtypes.

- [ ] Document **`allow_missing_columns`** and **cast** patterns for multi-file Parquet (link from {doc}`SUPPORTED_TYPES` if needed).
- [ ] Optional: **warnings** (or structured hooks via {doc}`PLAN_AND_PLUGINS` / observe) when schema differs across files in a scan.
- [ ] Error messages: continue **allowed keys** lists in **`ValueError`** for new **`scan_kwargs` / `write_kwargs`**.

---

## Phase F — Quality bar

- [ ] **Pytest:** fixtures for **flat directory**, **hive-partitioned**, **glob**, **NDJSON directory** (post Phase B3), regression tests for **kwargs** allowlists.
- [ ] **Docs build:** Sphinx passes; new pages linked from {doc}`DOCS_MAP` and **`index`** toctree.
- [ ] **Changelog:** {doc}`changelog` **1.11.0** section summarizes user-visible I/O changes (link this roadmap).
- [ ] **Release hygiene:** version alignment (`pyproject.toml`, `Cargo.toml`, `__version__`, `rust_version()`), per {doc}`VERSIONING` and `tests/test_version_alignment.py`.

---

## Success criteria (1.11.0)

1. A user can **read a directory or glob** of **Parquet** / **CSV** with documented **`scan_kwargs`**, and—where implemented—**NDJSON** / **IPC** with the same mental model as Polars.
2. A user can **write a partitioned Parquet dataset** (or the doc explicitly states the minimum supported layout and follow-up work).
3. **Eager** and **iter** layers have **clear** multi-file guidance without contradictions across {doc}`IO_OVERVIEW`, {doc}`IO_DECISION_TREE`, and {doc}`DATA_IO_SOURCES`.
4. **Tests and changelog** back the advertised surface; no undocumented kwargs in the allowlists.

---

## References (Polars behavior)

Users compare against **[Polars Python](https://docs.pola.rs/)** **`scan_*`** / **`read_*`** and **lazy sink** docs; implementation in pydantable follows the **Rust** stack bundled in **`pydantable-core`**, so small naming or option differences are acceptable if documented in Phase A.
