# Polars Parity Scorecard

This scorecard tracks practical parity for the currently implemented `pydantable`
surface.

Status definitions:
- `Implemented`: available and covered by contract/parity tests.
- `Partial`: available with explicit constraints or reduced semantics.
- `Missing`: not yet exposed as a stable API.

## 1.8.0 parity targets (planned)

This section tracks the **planned** parity work for **1.8.0** (see {doc}`POLARS_PARITY_1_8`).
Items listed here should move to the main table above once implemented and
contract-tested.

| Area | Target | Status | Notes |
|---|---|---|---|
| Core | `select_all()` / `select_prefix()` / `select_suffix()` | Implemented | Schema-driven selectors (no wildcard/regex DSL). |
| Core | `select` supports explicit expression aliasing | Implemented | `select((expr).alias(\"x\"))` for computed expressions; plain `Expr` requires `ColumnRef` or global agg. |
| Core | `with_columns` positional aliased expressions | Implemented | Keep kwargs; add `with_columns(expr.alias(\"x\"), ...)`. |
| Core | `sort(..., maintain_order=...)` | Implemented | `maintain_order=True` uses stable sort semantics in the Polars engine. |
| Core | `drop(..., strict=...)` | Implemented | `strict=False` ignores missing columns (no-op if all are missing). |
| Core | `rename(..., strict=...)` | Implemented | `strict=False` ignores missing rename keys. |
| Core | `unique/distinct(..., maintain_order=...)` | Implemented | `maintain_order=True` uses stable-unique semantics in the Polars engine. |
| GroupBy | Group-by convenience methods (`sum/mean/min/max/count/len`) | Implemented | Deterministic naming (`<col>_sum`, etc.) and `len` via synthetic constant column. |
| GroupBy | `group_by(..., maintain_order=..., drop_nulls=...)` | Implemented | `maintain_order=True` is stable; `drop_nulls=False` retains null-key groups. |
| Join | `join(..., coalesce=...)` | Partial | Implemented for `left_on`/`right_on` **name keys** (incl multi-key) on `inner`/`left`/`right`; not supported for `full`/`cross` or expression keys. |
| Join | `join(..., validate=...)` | Implemented | Cardinality checks supported for in-memory roots and scan roots (explicit cost). |
| Reshape | `pivot(..., sort_columns=..., separator=...)` | Implemented | `sort_columns=True` sorts pivot-value column generation; `separator` controls generated output names. |
| Utilities | `sample`, `shift`, `null_count`, `is_empty` | Implemented | Eager helpers (materialize via `to_dict()`), returning a new `DataFrame` (or `dict` for `null_count`). |

| Area | Method/Capability | Status | Notes |
|---|---|---|---|
| Core | `select`, `with_columns`, `filter` | Implemented | Typed expression validation and SQL-like null filter behavior. |
| Materialization | `collect()` (default), `to_dict()`, `to_polars()` | Implemented | Default `collect()` → `list[BaseModel]`; `to_dict()` columnar dict; `to_polars()` requires optional Python `polars` (`pydantable[polars]`). |
| Core | `sort`, `unique/distinct`, `drop`, `rename`, `slice/head/tail`, `concat` | Implemented | Contract-tested; deterministic schema propagation. |
| Null/type | `fill_null`, `drop_nulls`, `cast`, `is_null`, `is_not_null` | Implemented | Includes error contracts and nullable schema derivation. |
| Core | `limit/first/last/top_k/bottom_k` (schema-first helpers) | Implemented | Convenience wrappers over `slice`/`sort` with deterministic schemas. |
| Core | Selection/rename ergonomics (`select(exclude=...)`, reorder helpers, rename prefix/suffix/replace) | Implemented | Schema-driven column selection and naming helpers; collisions and empty selector matches raise explicit errors. |
| Join | `inner/left/right/full/semi/anti/cross` | Implemented | Includes expression key support and suffix collision policy. |
| GroupBy | `count/sum/mean/min/max/median/std/var/first/last/n_unique` | Implemented | SQL-like all-null-group behavior documented/tested. |
| Reshape | `melt/unpivot`, `pivot` | Implemented | Deterministic output naming and validation rules. |
| Pandas UI | `duplicated`, `drop_duplicates(keep=False)`, `get_dummies`, `cut`/`qcut`, `factorize_column`, `ewm().mean()`, façade `pivot` | Partial / Implemented | Duplicate mask + drop-duplicate-groups are plan steps on the Polars engine (`is_unique` / `is_first_distinct` features); encoding/binning/ewm paths are eager and may require **pandas** at runtime. Tests: `tests/test_pandas_ui.py`, `tests/test_pandas_ui_popular_features.py`. |
| Reshape | `explode`, `unnest` | Implemented | Polars-backed; multi-column explode, empty lists, struct `unnest` naming, and mismatch errors are contract-tested. Typed-schema rules (homogeneous lists, nested models as structs) are the intentional boundary vs raw Polars. |
| Window/time | `row_number`/`rank`/`dense_rank`/`window_sum`/`window_mean`/`window_min`/`window_max`/`lag`/`lead` + `WindowSpec`, `rolling_agg`, `group_by_dynamic(...).agg(...)` | Implemented | `Window.orderBy(..., nulls_last=...)` (**NULLS FIRST/LAST**); `row_number` requires `order_by`; `lag`/`lead` require `order_by`; generic `Expr.over(partition_by=..., order_by=...)` raises `TypeError` (use named window fns + `WindowSpec`). `rowsBetween` / `rangeBetween` framed windows use the Rust executor path; `rangeBetween` uses the first `orderBy` column as the range axis ([`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)). Unframed multi-key `.over`: Polars accepts one `SortOptions` for all order columns—**mixed** per-key `ascending` / `nulls_last` raises **`ValueError`**; use matching options on every key or a framed window. |
| Temporal typing | `datetime`, `date`, `duration`, `time` (+ nullable) | Implemented | End-to-end descriptor roundtrip and execution materialization paths. |
| Globals in `select` | `sum`/`mean`/`count`/`min`/`max` over a column, **`global_row_count`** / `count(*)` | Implemented | Single-row `DataFrame.select`; see `INTERFACE_CONTRACT`. |
| Expr helpers | `strptime`, `unix_timestamp`, `cast(str→date/datetime)`, `map_len`/`map_get`/`map_contains_key`, `binary_len`, `dt_nanosecond` | Implemented | Rust `ExprNode` + Polars lowering; contract tests. |
| Performance | Guardrails for major transforms | Implemented | Lightweight regression checks in test suite. |
| Ecosystem | Optional interfaces `pandas` and `pyspark` | Implemented | Alternate import/naming surfaces; execution is the same Rust core as default (not native pandas/Spark). **0.17.0:** PySpark `sql.functions` adds string/list/bytes helpers (`str_replace`, `strip_*`, `strptime`, `binary_len`, `list_*`) as thin wrappers over core `Expr`. **0.20.0:** PySpark UI `DataFrame.show()` / `summary()`; core + façades share `columns` / `shape` / `info` / `describe` (see `INTERFACE_CONTRACT` **Introspection**). |

## Remaining parity gaps

- Arbitrary Polars **nested/list dtypes** without a matching Pydantic `list[T]` / struct annotation are out of scope; the engine stays schema-first.
- Window frame semantics match the documented **PostgreSQL-style** `RANGE` rules for multi-key `orderBy`, not every SQL dialect; see [`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md).
- Additional advanced analytical APIs outside the current roadmap scope.

**0.18.0:** No new table methods or PySpark `functions` rows; this release focused on internals (clearer **group_by**/**Polars** error context), documentation, and deferred non-string **map** keys—see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.18.0**.

**0.19.0:** Scorecard matrix **unchanged**—pre-1.0 doc consolidation, [`VERSIONING.md`](VERSIONING.md), and CI-stable grouped tests; see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.19.0**.

**0.20.0:** One ecosystem row update (see table)—UX / discovery on core + PySpark **`show`** / **`summary`**, plus **`value_counts`**, **`pydantable.display`**, **`_repr_mimebundle_`**, optional verbose plan errors; see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.20.0**.
