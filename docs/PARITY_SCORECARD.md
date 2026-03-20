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
| Core | `sort`, `unique/distinct`, `drop`, `rename`, `slice/head/tail`, `concat` | Implemented | Contract-tested; deterministic schema propagation. |
| Null/type | `fill_null`, `drop_nulls`, `cast`, `is_null`, `is_not_null` | Implemented | Includes error contracts and nullable schema derivation. |
| Join | `inner/left/right/full/semi/anti/cross` | Implemented | Includes expression key support and suffix collision policy. |
| GroupBy | `count/sum/mean/min/max/median/std/var/first/last/n_unique` | Implemented | SQL-like all-null-group behavior documented/tested. |
| Reshape | `melt/unpivot`, `pivot` | Implemented | Deterministic output naming and validation rules. |
| Reshape | `explode`, `unnest` | Partial | API exists; full list/struct-native behavior remains constrained by typed-schema model. |
| Window/time | `Expr.over`, `rolling_agg`, `group_by_dynamic(...).agg(...)` | Implemented | Includes time-like support and parity smoke coverage. |
| Temporal typing | `datetime`, `date`, `duration` (+ nullable) | Implemented | End-to-end descriptor roundtrip and execution materialization paths. |
| Performance | Guardrails for major transforms | Implemented | Lightweight regression checks in test suite. |
| Ecosystem | Optional interfaces `pandas` and `pyspark` | Implemented | Delegated through backend boundary and parity smoke tests. |

## Remaining parity gaps

- Full Polars-native list/struct expansion semantics beyond current typed constraints.
- Additional advanced analytical APIs outside the current roadmap scope.
