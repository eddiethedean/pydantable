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
| Window/time | `row_number`/`rank`/`dense_rank`/`window_sum`/`window_mean`/`window_min`/`window_max`/`lag`/`lead` + `WindowSpec`, `rolling_agg`, `group_by_dynamic(...).agg(...)` | Implemented | `row_number` requires `order_by`; `lag`/`lead` require `order_by`; `Expr.over(...)` with args removed (use window fns). |
| Temporal typing | `datetime`, `date`, `duration`, `time` (+ nullable) | Implemented | End-to-end descriptor roundtrip and execution materialization paths. |
| Globals in `select` | `sum`/`mean`/`count`/`min`/`max` over a column, **`global_row_count`** / `count(*)` | Implemented | Single-row `DataFrame.select`; see `INTERFACE_CONTRACT`. |
| Expr helpers | `strptime`, `unix_timestamp`, `cast(str→date/datetime)`, `map_len`/`map_get`/`map_contains_key`, `binary_len`, `dt_nanosecond` | Implemented | Rust `ExprNode` + Polars lowering; contract tests. |
| Performance | Guardrails for major transforms | Implemented | Lightweight regression checks in test suite. |
| Ecosystem | Optional interfaces `pandas` and `pyspark` | Implemented | Alternate import/naming surfaces; execution is the same Rust core as default (not native pandas/Spark). |

## Remaining parity gaps

- Arbitrary Polars **nested/list dtypes** without a matching Pydantic `list[T]` / struct annotation are out of scope; the engine stays schema-first.
- SQL-style **window frames** (`rowsBetween` / `rangeBetween`): **IR** field exists (`WindowFrame::Rows`); **Polars lowering** is not implemented yet (execution raises if set).
- Additional advanced analytical APIs outside the current roadmap scope.
