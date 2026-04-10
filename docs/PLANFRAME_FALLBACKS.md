# PlanFrame and `DataFrameModel`

`DataFrameModel` keeps a typed [PlanFrame](https://pypi.org/project/planframe/) `Frame` as `_pf` and executes it through `pydantable.planframe_adapter.PydantableAdapter` (Rust/native `DataFrame` backend).

**Requirement:** current pydantable releases depend on **PlanFrame `>=1.0.0,<2`** (see `pyproject.toml`).

## PlanFrame–first core API

For the methods below, **there is no silent legacy path**: the operation is expressed in PlanFrame and executed via the adapter.

| Method | Behavior |
|--------|----------|
| `select(*cols: str)` | Plain projection only; at least one name. |
| `select_schema(selector)` | PlanFrame `select_schema` with a pydantable `Selector` (`resolve`) or PlanFrame column selector (`select`). |
| `with_columns`, `filter` | Always PlanFrame `WithColumn` / `Filter`. |
| `with_columns_cast`, `with_columns_fill_null` | PlanFrame `cast_subset` / `cast_many`, `fill_null_subset` / `fill_null_many` via selector or mapping. |
| `drop(*columns: str, strict=…)` | PlanFrame `Drop` (no-op if no columns). |
| `sort(*by, …)` | PlanFrame `Sort`; each key is `str` or `planframe.expr.api` expression; supports `nulls_last` (per-key like PlanFrame). |
| `rename(..., strict=…)` | PlanFrame `Rename` supports `strict=True/False`. |
| `rename_upper`, `rename_lower`, `rename_title`, `rename_strip` | PlanFrame rename helpers; optional column subset via pydantable `Selector` or PlanFrame selector. |
| `join` | PlanFrame `Join`; `on` / `left_on` / `right_on` are `str`, Expr, or sequences of str/Expr; `how="cross"` with no keys; `JoinOptions` supported. `allow_parallel` / `force_parallel` raise `NotImplementedError` on `DataFrameModel` (use `to_dataframe()` if you need them on the core `DataFrame`). |
| `group_by(...).agg(...)` | PlanFrame `GroupBy` + `Agg`; keys must be `str` or `planframe.expr.api.col("name")` (other expressions raise `TypeError`; use `with_columns` to build a key column first). |
| `group_by_dynamic(...).agg(...)` | PlanFrame `DynamicGroupByAgg` via adapter; returns a dynamic grouped object whose `agg(...)` is PlanFrame-backed. |
| `rolling_agg(...)` | PlanFrame `RollingAgg` via adapter. |
| `unique`, `distinct`, `head`, `tail`, `slice` | PlanFrame nodes + `execute_frame`. |
| `with_row_count(name="row_nr", offset=0)` | User API name; PlanFrame `Frame` uses `with_row_index` internally. |
| `fill_null` | PlanFrame `FillNull` supports `value=` literals or expressions and `strategy=`. |
| `drop_nulls` | PlanFrame `DropNulls` supports `how="any"/"all"` and `threshold`. |
| `clip(lower=..., upper=..., subset=...)` | PlanFrame `clip` (note: `subset=None` clips **all numeric** columns). |
| `melt` | User API; PlanFrame `Frame` uses `unpivot` (narrowed: string column names only). |
| `unpivot` | PlanFrame `unpivot` (same reshape family as `melt`). |
| `pivot_longer`, `pivot_wider` | PlanFrame `pivot_longer` / `pivot_wider` (narrowed types; e.g. `pivot_wider` requires string `names_from`). |
| `pivot` | PlanFrame `Pivot` (narrowed: string column names only; no `streaming=`). |
| `explode` | PlanFrame `Explode` (narrowed: string column names only; no `streaming=`). Supports `outer=`. |
| `explode_all` | PlanFrame-backed: expands to `explode(*schema_fields)`. |
| `unnest` | PlanFrame `Unnest` (narrowed: string column names only; expands struct fields from schema; no `streaming=`). |
| `unnest_all` | PlanFrame-backed: expands to `unnest(*schema_fields)`. |
| `concat` | PlanFrame `concat(how="vertical"|"horizontal")` (narrowed: identical schemas for vertical; no overlaps for horizontal). |

Unsupported use cases (e.g. `select` with expressions only available via **`with_columns`**, parallel join flags, or **`group_by`** on arbitrary expressions without adding a column first) may still require the core **`DataFrame`**. Use **`DataFrameModel.to_dataframe()`** for engine-only APIs. See {doc}`PLANFRAME_ADAPTER_ROADMAP` Phase 3.

## `_pf` always defined and consistent

Every instance from `_from_dataframe`, `as_model`, or `concat` gets `_pf = Frame.source(inner_df, …)` (or an extended plan after `_dfm_sync_pf`). After each PlanFrame-backed transform, `_pf` stores the lazy plan and `_df` is kept in sync by executing that plan. Methods that delegate only to the inner `DataFrame` without updating `_pf` (for example `pipe`) do not extend the PlanFrame plan; use `to_dataframe()` when you need arbitrary engine operations, then wrap again if required.

## Explicit errors and remaining gaps (not silent legacy paths)

- **`join(..., allow_parallel=, force_parallel=)`** — `NotImplementedError` on `DataFrameModel`; use **`DataFrameModel.to_dataframe()`** for parallel join flags on the core **`DataFrame`** if needed.
- **`group_by`** — only `str` or `planframe.expr.api.col("x")`; other expression keys raise **`TypeError`** (see {doc}`PLANFRAME_ADAPTER_ROADMAP` Phase 3).
- **Expression coverage** — any `planframe.expr.api` node not lowered in `planframe_adapter/expr.py` still raises when executed through the adapter (see Phase 1 in {doc}`PLANFRAME_ADAPTER_ROADMAP`).

## Pydantable backlog (work still to do here)

| Area | What |
|------|------|
| **Improve reshape ergonomics** | Support richer `melt` / `pivot` kwargs (selectors, defaults) while keeping the PlanFrame-first typing ethos. |
| **Widen explode/unnest** | Multi-column explode/unnest, `outer=`, and schema-driven `*_all` variants need additional PlanFrame/pydantable surface design. |
| **Parity for `drop_nulls` / `fill_null`** | Ensure all `DataFrameModel` surface params are forwarded and covered by tests. |
| **Optional: computed `group_by` without `with_columns`** | Could be added via PlanFrame/compiler work; today users add a key column explicitly. |
| **`planframe_adapter/expr.py`** | Extend lowering for additional `planframe.expr.api` nodes and `AggExpr` / `Over` combinations as they are claimed supported (see {doc}`PLANFRAME_ADAPTER_ROADMAP`). |
| **Tests / typing artifacts** | Regenerate stubs and add tests when new PlanFrame-backed methods ship. |
| **`execute_frame`** | pydantable delegates to `planframe.execution.execute_plan`. |
| **`write_delta` / `write_avro` (adapter)** | `NotImplementedError` until the core `DataFrame` exposes matching sinks; `PydantableAdapter` mirrors that. |
| **`join` parallel flags** | Core `DataFrame.join` does not support `allow_parallel` / `force_parallel` in this build either; use `to_dataframe()` if a future engine adds them. |

## Upstream PlanFrame

PlanFrame **1.x** provides `ExecutionOptions` (e.g. `streaming` / `engine_streaming`) and `JoinOptions` for execution hints. **`DataFrameModel.to_dict`** and **`collect(as_lists=True)`** route columnar materialization through **`Frame.to_dict(options=…)`** so those hints follow PlanFrame’s boundary; other materialization paths may still use the inner `DataFrame` directly (see {doc}`PLANFRAME_ADAPTER_ROADMAP`).

### PlanFrame `Expr` lowering in the pydantable adapter

`planframe_adapter/expr.py` implements a **documented subset** of `planframe.expr.api`; unhandled nodes raise `NotImplementedError` when used inside a PlanFrame plan executed by this adapter. See Phase 1 in {doc}`PLANFRAME_ADAPTER_ROADMAP`.

### Async

PlanFrame is synchronous; pydantable `acollect` / `ato_dict` / … delegate to `DataFrame`. See [planframe#15](https://github.com/eddiethedean/planframe/issues/15).

## Recipe: computed group key without widening `group_by`

`DataFrameModel.group_by` accepts only column names or `planframe.expr.api.col("name")`. To group by a derived value, add a column with `with_columns`, then group on that name (pydantable `Expr` or PlanFrame expr):

```python
from planframe.expr import api as pf
from pydantable.expressions import col

out = (
    df.with_columns(dbl=col("x") * 2)
    .group_by("dbl")
    .agg(n=pf.agg_count(pf.col("y")))
)
```

## See also

- {doc}`PLAN_AND_PLUGINS` — plan materialization and observers on the core `DataFrame`.
- [PlanFrame: Creating an adapter](https://github.com/eddiethedean/planframe/blob/main/docs/guides/planframe/creating-an-adapter.md) — `BaseAdapter` contract pydantable implements.
