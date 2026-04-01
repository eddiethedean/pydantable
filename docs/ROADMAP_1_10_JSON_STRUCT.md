# Roadmap: JSON processing & struct ergonomics (1.10.0)

**Target release:** **1.10.0** (see {doc}`changelog`).

**Purpose:** Track work to strengthen **JSON I/O**, **nested model (struct)** selection and transforms, and **user-facing helpers**—without turning pydantable into a schemaless JSON toolkit. The product remains **schema-first**: Pydantic-driven column types, Rust logical plans, Polars-backed execution.

**Related docs:** {doc}`IO_JSON`, {doc}`IO_NDJSON`, {doc}`SUPPORTED_TYPES`, {doc}`SELECTORS`, {doc}`INTERFACE_CONTRACT`, main {doc}`ROADMAP` (**Future Expr** / **Later** sections).

---

## Goals

1. **JSON type clarity:** Document and test how RFC-style JSON maps to `DataFrameModel` / `DataFrame[Schema]` fields (including nested objects, homogeneous arrays, maps, nulls, and numeric coercion).
2. **Struct parity:** Bring **struct-typed columns** closer to string-JSON workflows (encode, JSONPath, field renames / structural updates in-plan).
3. **I/O usability:** Reduce surprises around lazy vs eager JSON, inference, and large files; improve discoverability of the right entrypoint.
4. **Top-tier positioning (for our niche):** Best-in-class experience for **typed** tables loaded from or exported to JSON—not generic jq/dict processing.

## Non-goals (for this roadmap unless explicitly promoted)

- **Fully schemaless columns** (e.g. unrestricted `Any` / arbitrary JSON per cell) unless we add a deliberate dtype and execution story.
- **Non-string map keys** (`dict[int, T]`, non-UTF-8 Arrow map keys)—remains deferred per {doc}`ROADMAP` **Later** and {doc}`SUPPORTED_TYPES`.
- **Distributed Spark/SQL engines**—out of scope here.

---

## JSON ↔ schema coverage (audit deliverable)

Ship an authoritative **matrix** (in {doc}`SUPPORTED_TYPES` or a dedicated subsection, plus tests) covering at least:

| JSON | Intended modeling | Validation / ingest notes |
|------|-------------------|---------------------------|
| `null` | `Optional[...]` / `T \| None` | SQL-style null propagation |
| boolean | `bool` | |
| string | `str`, `Literal`, UUID, enum, IP, `Annotated[str, ...]`, etc. | See scalar table in {doc}`SUPPORTED_TYPES` |
| number | `int`, `float`, `Decimal` | Document JSON’s lack of int/float distinction and Pydantic coercion |
| array | `list[T]` (homogeneous) | Heterogeneous arrays: document **unsupported** or future `Json` cell type |
| object | Nested `BaseModel` → **struct**; `dict[str, T]` → **map** | String keys only for maps |

**Checklist**

- [ ] Matrix landed in docs and linked from {doc}`IO_JSON`.
- [ ] Integration tests: round-trip **export** / re-import for nested struct + list + map columns (where supported).
- [ ] Explicit statement on **heterogeneous JSON arrays** and **arbitrary nested JSON** (recommended pattern: `str` + expressions / Pydantic parse at materialization).

---

## Phase A — Foundation (tests + docs)

**Outcome:** No new user APIs required; risk reduction for later phases.

- [ ] Expand tests for **`materialize_json`** (array vs NDJSON), **`export_json`**, and lazy **`read_ndjson`** / **`read_json`** with nested columns (struct/list/map) aligned with Polars inference.
- [ ] Document **`export_json`** serialization for nested cells (`default=str`, datetimes, decimals—spell out behavior).
- [ ] Cross-link **FastAPI** columnar JSON patterns ({doc}`FASTAPI`) with struct/map examples.

---

## Phase B — Struct expressions (high leverage)

Mirror **string** JSON helpers for **struct** columns (see {doc}`ROADMAP` **Structs and maps** candidates).

| Capability | Intent | Notes |
|------------|--------|--------|
| `struct_json_encode` | Struct column → `str` (JSON text) per row | Symmetry with parsing story; wire/export APIs |
| `struct_json_path_match` | JSONPath on struct-backed JSON | Avoid encode + `str_json_path_match` when Polars supports struct path |
| `struct_rename_fields` | Rename inner field names (schema evolves) | May compose with existing `unnest` naming rules |
| `struct_with_fields` | Add/replace fields in a struct column | Typed field list; IR + nullability rules |

**Checklist**

- [ ] Rust `ExprNode` + typing in `pydantable-core` (+ serialization if needed).
- [ ] Python `Expr` methods and PySpark façade aliases where naming matches.
- [ ] `INTERFACE_CONTRACT` + {doc}`SUPPORTED_TYPES` updates (expression typing, struct nullability).
- [ ] Contract tests mirroring `tests/test_nested_model_dtype.py` / string JSON tests.

**Suggested priority order:** `struct_json_encode` → `struct_with_fields` / `struct_rename_fields` → `struct_json_path_match` (depends on Polars surface and typing).

---

## Phase C — String JSON → typed decode (optional for 1.10.0)

**Outcome:** Complement **`Expr.str_json_path_match`** with **structured** decode when the user supplies a target schema.

- [ ] Design: **`str` → struct** (and possibly **`str` → map**) as `Expr` methods with **known output dtype** at plan build time.
- [ ] Execution: Polars **`str.json_decode`** (or equivalent) with dtype alignment to `DTypeDesc::Struct` / map.
- [ ] Errors: invalid JSON per row—null vs raise; document policy.

If scope slips, move unfinished items to **1.11.0** and keep Phase B as the 1.10.0 centerpiece.

---

## Phase D — I/O and streaming

| Item | Description | Status |
|------|-------------|--------|
| Incremental NDJSON / array iteration | Avoid full **`dict[str, list]`** materialization up front where feasible | Deferred in main {doc}`ROADMAP` **Later**; promote if 1.10.0 bandwidth allows |
| Scan kwargs presets | Document “reasonable defaults” for **`infer_schema_length`**, **`ignore_errors`**, **`n_rows`** for JSON Lines | Docs-first acceptable |
| Naming clarity | **`read_json`** == lazy NDJSON: call out in {doc}`IO_JSON` and examples | Docs |

**Checklist**

- [ ] At least **docs + examples** for large-file JSON patterns (`read_ndjson` + `collect`, chunked `iter_ndjson`, when to use `materialize_json`).
- [ ] If implemented: async/sync iterators documented in {doc}`DATA_IO_SOURCES` and {doc}`EXECUTION`.

---

## Phase E — User experience polish

- [ ] **Selectors:** examples combining **`s.structs()`**, **`unnest`**, **`struct_field`** chains ({doc}`SELECTORS`).
- [ ] **Cookbook** entry: “JSON logs → typed frame → unnest → export”.
- [ ] **Changelog** entry for 1.10.0 listing JSON/struct items ({doc}`changelog`).

---

## Success criteria (1.10.0)

1. A reader can answer “**How do I model this JSON?**” using the official matrix and I/O pages alone.
2. Struct-heavy pipelines can **rename / extend / encode** struct columns without ad-hoc Python UDFs (Phase B shipped in whole or in part).
3. No regressions: `make check-full`, full **pytest**, `cargo test --all-features` on the release candidate.

---

## References in-repo

- Lazy NDJSON scan: `pydantable-core` `dispatch_file_scan` / `LazyJsonLineReader`
- Eager JSON: `python/pydantable/io/__init__.py` — `materialize_json`, `export_json`
- Struct unnest: `pydantable-core` `execute_unnest_polars`
- String JSONPath: `Expr.str_json_path_match` → `StringJsonPathMatch` in `pydantable-core`
