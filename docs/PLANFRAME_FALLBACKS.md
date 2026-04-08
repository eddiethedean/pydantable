# PlanFrame and `DataFrameModel`

`DataFrameModel` keeps a typed [PlanFrame](https://pypi.org/project/planframe/) `Frame` as `_pf` and executes it through `pydantable.planframe_adapter.PydantableAdapter` (Rust/native `DataFrame` backend).

**Requirement:** pydantable **1.16.x** depends on **PlanFrame ≥ 0.5.0**.

## PlanFrame–first core API

For the methods below, **there is no silent legacy path**: the operation is expressed in PlanFrame and executed via the adapter.

| Method | Behavior |
|--------|----------|
| `select(*cols: str)` | Plain projection only; at least one name. |
| `with_columns`, `filter` | Always PlanFrame `WithColumn` / `Filter`. |
| `drop(*columns: str, strict=…)` | PlanFrame `Drop` (no-op if no columns). |
| `sort(*by: str, …)` | PlanFrame `Sort`; supports `nulls_last` (per-key like PlanFrame). |
| `rename(..., strict=…)` | PlanFrame `Rename` supports `strict=True/False`. |
| `join` | PlanFrame `Join` supports string and expression keys; `how="cross"` with no keys; `JoinOptions` supported. `allow_parallel` / `force_parallel` remain pydantable-native concerns. |
| `group_by(...).agg(...)` | PlanFrame `GroupBy` + `Agg` (narrowed: key columns are `str` only). |
| `group_by_dynamic(...).agg(...)` | PlanFrame `DynamicGroupByAgg` via adapter; returns a dynamic grouped object whose `agg(...)` is PlanFrame-backed. |
| `rolling_agg(...)` | PlanFrame `RollingAgg` via adapter. |
| `unique`, `distinct`, `head`, `tail`, `slice` | PlanFrame nodes + `execute_frame`. |
| `with_row_count(name="row_nr", offset=0)` | PlanFrame `WithRowCount`. |
| `fill_null` | PlanFrame `FillNull` supports `value=` literals or expressions and `strategy=`. |
| `drop_nulls` | PlanFrame `DropNulls` supports `how="any"/"all"` and `threshold`. |
| `clip(lower=..., upper=..., subset=...)` | PlanFrame `clip` (note: `subset=None` clips **all numeric** columns). |
| `melt` | PlanFrame `Melt` (narrowed: `value_vars=` required; string names only; no `streaming=`). |
| `unpivot` | PlanFrame `unpivot` (lowered to `melt`). |
| `pivot` | PlanFrame `Pivot` (narrowed: string column names only; no `streaming=`). |
| `explode` | PlanFrame `Explode` (narrowed: string column names only; no `streaming=`). Supports `outer=`. |
| `explode_all` | PlanFrame-backed: expands to `explode(*schema_fields)`. |
| `unnest` | PlanFrame `Unnest` (narrowed: string column names only; expands struct fields from schema; no `streaming=`). |
| `unnest_all` | PlanFrame-backed: expands to `unnest(*schema_fields)`. |
| `concat` | PlanFrame `ConcatVertical` / `ConcatHorizontal` (narrowed: identical schemas for vertical; no overlaps for horizontal). |

Unsupported use cases (e.g. `select` with expressions, `join` on `Expr` keys) currently require the core **`DataFrame`**. There is **no stable public accessor** on `DataFrameModel` today—**backlog:** add something like `to_dataframe()` / `inner_frame()` if we want a supported escape hatch (see below).

## `_pf` always defined and consistent

Every instance from `_from_dataframe` or `as_model` gets `_pf = Frame.source(inner_df, …)` so `_pf` is never missing. After each PlanFrame-backed step, `_pf` holds the extended plan; after transforms that still delegate to `_df` only (see below), `_from_dataframe` **resets** `_pf` to a fresh `Source` for the new lazy frame so the plan never points at the wrong data.

## Operations that are intentionally unsupported on `DataFrameModel` for now

These either have no PlanFrame node yet, or require selector-driven behavior that `DataFrameModel` is avoiding in the PlanFrame-first surface. They raise `NotImplementedError` (with the old backend implementation kept in place after the `raise` for future work).

`select_schema`, `with_columns_cast`, `with_columns_fill_null`, `rename_upper` / `rename_lower` / `rename_title` / `rename_strip`, `pivot_longer`, `pivot_wider`, and similar selector-driven helpers.

Wiring more of these through PlanFrame plan nodes (and tightening types) is incremental work.

## Pydantable backlog (work still to do here)

| Area | What |
|------|------|
| **Improve reshape ergonomics** | Support richer `melt` / `pivot` kwargs (selectors, defaults) while keeping the PlanFrame-first typing ethos. |
| **Widen explode/unnest** | Multi-column explode/unnest, `outer=`, and schema-driven `*_all` variants need additional PlanFrame/pydantable surface design. |
| **Parity for `drop_nulls` / `fill_null`** | Now available via PlanFrame 0.3; ensure all `DataFrameModel` surface params are forwarded and covered by tests. |
| **Join + sort + group_by expr keys** | PlanFrame supports expression keys; pydantable adapter currently lowers expr keys only when they reference exactly one column (core engine limitation). Decide whether to extend the engine or constrain the model API. |
| **`planframe_adapter/expr.py`** | Lower remaining `planframe.expr.api` nodes (`StrLower`, `DtYear`, `Over`, …) so PlanFrame-native expr trees execute without `NotImplementedError`. |
| **Public escape hatch** | Documented way to get a `DataFrame` from a `DataFrameModel` for APIs we intentionally do not wrap (until PlanFrame catches up). |
| **Tests / typing artifacts** | Regenerate stubs and add tests when new PlanFrame-backed methods ship. |
| **`execute_frame` duplication** | PlanFrame 0.3 ships a public plan interpreter (`planframe.execution.execute_plan`); pydantable delegates to it. |

## Upstream PlanFrame

PlanFrame **0.5.0** adds several schema-driven conveniences (selectors, multi-cast/fill, rename case helpers, pivot_longer/pivot_wider) that allow pydantable to remove more `NotImplementedError` gaps in the PlanFrame-first `DataFrameModel` surface.

### PlanFrame `Expr` lowering in the pydantable adapter

See previous docs: `planframe_adapter/expr.py` covers a subset of `planframe.expr.api`; other nodes raise `NotImplementedError` when used inside a PlanFrame plan executed by this adapter.

### Async

PlanFrame is synchronous; pydantable `acollect` / `ato_dict` / … delegate to `DataFrame`. See [planframe#15](https://github.com/eddiethedean/planframe/issues/15).

## See also

- {doc}`PLAN_AND_PLUGINS` — plan materialization and observers on the core `DataFrame`.
- [PlanFrame: Creating an adapter](https://github.com/eddiethedean/planframe/blob/main/docs/guides/planframe/creating-an-adapter.md) — `BaseAdapter` contract pydantable implements.
