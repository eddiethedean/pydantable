# Changelog

All notable changes to this project are documented here. The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.12.0] — 2026-03-22

### Highlights

- **Multi-key `rangeBetween`:** aggregate window frames may use multiple `orderBy` columns; sort is lexicographic and **range bounds apply to the first** sort key (PostgreSQL-style). Documented in {doc}`WINDOW_SQL_SEMANTICS`.
- **Trusted `strict` ingest:** Polars columns are matched structurally to nested annotations (`list` / `dict[str, T]` / nested `Schema` structs); columnar Python paths get the same nested shape checks.
- **Contracts and parity docs:** refresh `INTERFACE_CONTRACT`, PySpark UI/scorecard, roadmap; add duplicate-key policy for `map_from_entries` ({doc}`SUPPORTED_TYPES`).
- **Regression tests:** broader coverage for multi-key `rangeBetween` (desc/mixed `orderBy`, partitions, `date`/`datetime` axis, `window_mean`/`window_min`), PySpark window mirrors, trusted `strict` nested paths (Python + Polars), `map_from_entries` duplicate keys, and `DataFrame` / `DataFrameModel` strict parity.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`PARITY_SCORECARD`, {doc}`WINDOW_SQL_SEMANTICS`, and {doc}`ROADMAP`.

## [0.11.0] — 2026-03-23

### Highlights

- **Window range semantics v2:** `rangeBetween` supports numeric, `date`, `datetime`, and `duration` order keys (single `orderBy` key), with deterministic boundary-inclusive behavior.
- **Map ergonomics expanded:** add `map_from_entries()` and PySpark-compatible `element_at()` alias; map entry roundtrip coverage expanded.
- **Trusted ingest modes:** add explicit trusted modes (`shape_only`, `strict`) alongside compatibility with `validate_data`, including stricter nullability and dtype checks for trusted columnar paths.
- **Parity coverage expansion:** add dedicated DataFrame/DataFrameModel parity tests and additional PySpark map parity contracts.
- **Release hardening:** update docs/contracts and version metadata for the 0.11.0 line.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`SUPPORTED_TYPES`, and {doc}`ROADMAP`.

## [0.10.0] — 2026-03-23

### Highlights

- **Framed windows expanded:** framed execution now covers `window_mean`, `window_min`, `window_max`, `lag`, `lead`, `rank`, and `dense_rank` in addition to `row_number` / `window_sum`.
- **Map utilities:** add `map_keys()` and `map_values()` to complement `map_len`, `map_get`, and `map_contains_key`.
- **Parity and interop hardening:** PySpark parity tests for framed windows/map utilities plus trusted constructor coverage for Polars DataFrame input (`validate_data=False`).
- **Window range contracts tightened:** `rangeBetween` now enforces exactly one `orderBy` key for supported aggregate frames, with explicit typed errors.
- **Map v2 parity expanded:** add `map_entries()` and PySpark wrappers for `map_len`, `map_get`, and `map_contains_key`.
- **Trusted ingest hardening:** Polars trusted constructor path rejects nulls in non-nullable schema fields when `validate_data=False`.

### Details

See {doc}`INTERFACE_CONTRACT`, {doc}`PYSPARK_PARITY`, {doc}`SUPPORTED_TYPES`, and {doc}`ROADMAP`.

## [0.9.0] — 2026-03-23

### Highlights

- **Bad-input ingest controls:** `ignore_errors=True` with `on_validation_errors=...` (`row_index`, `row`, `errors`) across `DataFrameModel` and `DataFrame` constructor paths.
- **Framed windows:** `rowsBetween` / `rangeBetween` frame metadata is wired through Python/PySpark/Rust; framed execution is supported for `row_number` / `window_sum` (`rangeBetween` on integer order keys, range offset computed from first `orderBy` key).
- **Map v2 values:** `dict[str, T]` map columns now support nested JSON-like value dtypes (lists/maps/structs and nullable unions), with `map_len`, `map_get`, and `map_contains_key` behavior preserved.
- **Release hardening:** expanded 0.9.0 edge-case tests and full quality-gate coverage (`make check-full`, docs example validation, Sphinx warnings-as-errors build).

### Details

See {doc}`DATAFRAMEMODEL`, {doc}`INTERFACE_CONTRACT`, {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.8.0] — 2026-03-23

### Highlights

- **Global row count:** `global_row_count()` and PySpark `functions.count()` with no column (`count(*)`-style) for `DataFrame.select`.
- **Casts:** `str` → `date` / `datetime` via `Expr.cast(...)` (Polars parsing); use `strptime` for fixed formats.
- **Maps:** `Expr.map_get(key)` / `map_contains_key(key)` on `dict[str, T]` columns (list-of-struct encoding).
- **Windows:** `window_min` / `window_max`; IR carries optional `WindowFrame::Rows` for future Spark-style `rowsBetween` (lowering not yet implemented).
- **Docs:** `INTERFACE_CONTRACT`, `PYSPARK_PARITY`, `SUPPORTED_TYPES` updates.

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, {doc}`ROADMAP`, and {doc}`INTERFACE_CONTRACT`.

### Testing

- Broader integration tests for 0.7.0 / 0.8.0 surfaces (`test_v070_features`, `test_v080_features`), including PySpark `F.count()` with no column.

### Documentation

- README feature bullets; {doc}`INTERFACE_CONTRACT` (global `select`); {doc}`POLARS_WORKFLOWS` (single-row globals example); {doc}`index`, {doc}`EXECUTION`, {doc}`PYSPARK_UI`, {doc}`PYSPARK_PARITY`, {doc}`PYSPARK_INTERFACE`.

## [0.7.0] — 2026-03-23

### Highlights

- **Global aggregates:** `global_count`, `global_min`, `global_max` for `DataFrame.select` (non-null `count`; Polars `min`/`max`); PySpark `functions.count` / `min` / `max` on typed columns.
- **Windows:** `lag` / `lead` (Polars `shift` + `.over(...)`); still no SQL-style `rowsBetween` / `rangeBetween` in the IR (see `INTERFACE_CONTRACT.md`).
- **Temporal:** `Expr.strptime` / `Expr.unix_timestamp`, PySpark `to_date(..., format=...)` and `unix_timestamp`; `dt_nanosecond` for `datetime` and `time`.
- **Maps / binary:** `Expr.map_len()`, `Expr.binary_len()` (byte length of `bytes` columns).

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.6.0] — 2026-03-22

### Highlights

- **Scalar types:** `datetime.time`, `bytes`, and homogeneous `dict[str, T]` map columns (Polars-backed I/O; map execution surface is intentionally small).
- **Windows:** `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean` with `Window.partitionBy(...).orderBy(...)` / `.spec()`; Rust `ExprNode::Window` + Polars `.over(...)`.
- **Global aggregates (Phase D):** `DataFrame.select(...)` with `global_sum` / `global_mean` or PySpark `functions.sum` / `avg` / `mean` on typed columns — single-row results.
- **PySpark façade:** `dropDuplicates(subset=...)`, date helpers (`year`, `month`, …, `to_date`), documentation parity fixes (`union`, types).

### Development and testing

- **`[dev]` optional dependencies** include `numpy` (for `collect(as_numpy=...)` tests), `pytest-cov`, `coverage`, `pytest-xdist`, and `polars`.
- **CI** runs parallel `pytest` on Linux, optional **coverage** XML on Ubuntu + Python 3.11, installs **Polars** on that leg so `to_polars()` tests run, runs **`scripts/verify_doc_examples.py`**, and uses **`GITHUB_ACTIONS`-scaled** performance guardrails.

### Details

See {doc}`SUPPORTED_TYPES`, {doc}`PYSPARK_PARITY`, and {doc}`ROADMAP`.

## [0.5.0] — 2026

### Highlights

- **PydanTable** naming and docs alignment with the {doc}`ROADMAP` (0.5.x line).
- **Typed `DataFrameModel`** and **`DataFrame[Schema]`** with a Rust execution core (Polars-backed in the native extension).
- **Materialization:** `collect()` returns Pydantic row models; `to_dict()` / `collect(as_lists=True)` for columnar data; optional `to_polars()` with the `[polars]` extra.
- **Rich column types:** nested Pydantic models (structs), homogeneous `list[T]`, `uuid.UUID`, `decimal.Decimal`, `enum.Enum`, plus `explode`, `unnest`, and extended `Expr` helpers (see {doc}`SUPPORTED_TYPES`).

### Details

For phase history and future direction, see {doc}`ROADMAP` and {doc}`POLARS_TRANSFORMATIONS_ROADMAP`.
