# Polars Parity Scorecard

This scorecard tracks practical parity for the currently implemented `pydantable`
surface.

Status definitions:
- `Implemented`: available and covered by contract/parity tests.
- `Partial`: available with explicit constraints or reduced semantics.
- `Missing`: not yet exposed as a stable API.

| Area | Method/Capability | Status | Notes |
|---|---|---|---|
| Core | `select`, `with_columns`, `filter` | Implemented | Typed expression validation and SQL-like null filter behavior. |
| Materialization | `collect()` (default), `to_dict()`, `to_polars()` | Implemented | Default `collect()` → `list[BaseModel]`; `to_dict()` columnar dict; `to_polars()` requires optional Python `polars` (`pydantable[polars]`). |
| Core | `sort`, `unique/distinct`, `drop`, `rename`, `slice/head/tail`, `concat` | Implemented | Contract-tested; deterministic schema propagation. |
| Null/type | `fill_null`, `drop_nulls`, `cast`, `is_null`, `is_not_null` | Implemented | Includes error contracts and nullable schema derivation. |
| Join | `inner/left/right/full/semi/anti/cross` | Implemented | Includes expression key support and suffix collision policy. |
| GroupBy | `count/sum/mean/min/max/median/std/var/first/last/n_unique` | Implemented | SQL-like all-null-group behavior documented/tested. |
| Reshape | `melt/unpivot`, `pivot` | Implemented | Deterministic output naming and validation rules. |
| Reshape | `explode`, `unnest` | Implemented | Polars-backed; multi-column explode, empty lists, struct `unnest` naming, and mismatch errors are contract-tested. Typed-schema rules (homogeneous lists, nested models as structs) are the intentional boundary vs raw Polars. |
| Window/time | `row_number`/`rank`/`dense_rank`/`window_sum`/`window_mean`/`window_min`/`window_max`/`lag`/`lead` + `WindowSpec`, `rolling_agg`, `group_by_dynamic(...).agg(...)` | Implemented | `Window.orderBy(..., nulls_last=...)` (**NULLS FIRST/LAST**); `row_number` requires `order_by`; `lag`/`lead` require `order_by`; generic `Expr.over(partition_by=..., order_by=...)` raises `TypeError` (use named window fns + `WindowSpec`). `rowsBetween` / `rangeBetween` framed windows use the Rust executor path; `rangeBetween` uses the first `orderBy` column as the range axis ([`WINDOW_SQL_SEMANTICS.md`](WINDOW_SQL_SEMANTICS.md)). Unframed multi-key `.over`: only the first key’s `nulls_last` is passed to Polars `SortOptions`. |
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

**0.20.0:** One ecosystem row update (see table)—UX / discovery on core + PySpark **`show`** / **`summary`**; see [`ROADMAP.md`](ROADMAP.md) **Shipped in 0.20.0**.
