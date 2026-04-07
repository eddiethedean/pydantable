# PlanFrame and `DataFrameModel`

`DataFrameModel` keeps a typed [PlanFrame](https://pypi.org/project/planframe/) `Frame` as `_pf` and executes it through `pydantable.planframe_adapter.PydantableAdapter` (Rust/native `DataFrame` backend).

**Requirement:** pydantable **1.16.x** depends on **PlanFrame ≥ 0.2.0**.

## PlanFrame–first core API

For the methods below, **there is no silent legacy path**: only shapes PlanFrame can represent are accepted. Narrow **static types** (`str` column names, `strict=True` rename, etc.) match that contract.

| Method | Behavior |
|--------|----------|
| `select(*cols: str)` | Plain projection only; at least one name. |
| `with_columns`, `filter` | Always PlanFrame `WithColumn` / `Filter`. |
| `drop(*columns: str, strict=…)` | PlanFrame `Drop` (no-op if no columns). |
| `sort(*by: str, …)` | PlanFrame `Sort`; supports `nulls_last` (per-key like PlanFrame). |
| `rename(..., strict=True)` | PlanFrame `Rename`; `strict=False` raises until [planframe#7](https://github.com/eddiethedean/planframe/issues/7). |
| `join` | String `on` / `left_on` / `right_on` only; `how="cross"` with no keys; `JoinOptions` supported. Otherwise `TypeError`. `allow_parallel` / `force_parallel` → `NotImplementedError`. Typing uses `cast(Any, _pf)` until [planframe#18](https://github.com/eddiethedean/planframe/issues/18). Expr keys: [planframe#10](https://github.com/eddiethedean/planframe/issues/10). |
| `unique`, `distinct`, `head`, `tail`, `slice` | PlanFrame nodes + `execute_frame`. |
| `fill_null` | `value=` only (no `strategy=`); `subset` as `str` or `Sequence[str]` or `None`. Strategies / expr fill: [planframe#17](https://github.com/eddiethedean/planframe/issues/17). |
| `drop_nulls` | `how="any"`, `threshold=None` only; `subset` as `str` or `Sequence[str]` or `None`. Wider row-null semantics: [planframe#16](https://github.com/eddiethedean/planframe/issues/16). |

Unsupported use cases (e.g. `select` with expressions, `join` on `Expr` keys) currently require the core **`DataFrame`**. There is **no stable public accessor** on `DataFrameModel` today—**backlog:** add something like `to_dataframe()` / `inner_frame()` if we want a supported escape hatch (see below).

## `_pf` always defined and consistent

Every instance from `_from_dataframe` or `as_model` gets `_pf = Frame.source(inner_df, …)` so `_pf` is never missing. After each PlanFrame-backed step, `_pf` holds the extended plan; after transforms that still delegate to `_df` only (see below), `_from_dataframe` **resets** `_pf` to a fresh `Source` for the new lazy frame so the plan never points at the wrong data.

## Operations that still use `_df` (PlanFrame plan reset to `Source`)

These either have no `DataFrameModel` wrapper over PlanFrame yet, or need engine features PlanFrame does not model. They return a new model via `_from_dataframe`, which re-binds `_pf` to `Source` for the result:

`select_schema`, `with_columns_cast`, `with_columns_fill_null`, `with_row_count`, `rename_upper` / `rename_lower` / `rename_title` / `rename_strip`, `clip`, `melt` / `unpivot` / `pivot*` (rich kwargs), `explode` / `unnest` (multi-column, streaming, etc.), `group_by`, window/rolling helpers, I/O, `concat`, and similar.

Wiring more of these through PlanFrame plan nodes (and tightening types) is incremental work.

## Pydantable backlog (work still to do here)

| Area | What |
|------|------|
| **Wire `_pf` + `execute_frame`** | For ops that already exist in PlanFrame (`Melt`, `Pivot`, `Explode`, `Unnest`, `ConcatVertical` / `ConcatHorizontal`, `Sample`, `DropNullsAll`, `Cast`, …): add `DataFrameModel` methods that build the same plans instead of only `_df`, so history stays on the plan where possible. |
| **`DataFrameModel.concat`** | Today uses `DataFrame.concat` + `_from_dataframe` (plan resets to `Source`). Could use `Frame.concat_vertical` / `concat_horizontal` between two models’ `_pf` when schemas align. |
| **`group_by` / `GroupedDataFrameModel`** | Still native grouped `DataFrame` only. Needs `GroupBy` + `Agg` on `_pf` once keys/aggs match PlanFrame ([planframe#11](https://github.com/eddiethedean/planframe/issues/11), [planframe#12](https://github.com/eddiethedean/planframe/issues/12)). |
| **Parity for `drop_nulls` / `fill_null`** | After [planframe#16](https://github.com/eddiethedean/planframe/issues/16) / [planframe#17](https://github.com/eddiethedean/planframe/issues/17), extend `DataFrameModel` to forward `how` / `threshold` / `strategy`. |
| **`join` typing** | Remove `cast(Any, _pf)` once [planframe#18](https://github.com/eddiethedean/planframe/issues/18) lands. |
| **`planframe_adapter/expr.py`** | Lower remaining `planframe.expr.api` nodes (`StrLower`, `DtYear`, `Over`, …) so PlanFrame-native expr trees execute without `NotImplementedError`. |
| **Public escape hatch** | Documented way to get a `DataFrame` from a `DataFrameModel` for APIs we intentionally do not wrap (until PlanFrame catches up). |
| **Tests / typing artifacts** | Regenerate stubs and add tests when new PlanFrame-backed methods ship. |

## Upstream PlanFrame — tracker

**Shipped in 0.2.0:** asymmetric join keys + `JoinOptions`, per-key `Sort`, `Drop(strict=…)`. Issues [planframe#1](https://github.com/eddiethedean/planframe/issues/1)–[#3](https://github.com/eddiethedean/planframe/issues/3) (closed).

| # | Topic |
|---|--------|
| [7](https://github.com/eddiethedean/planframe/issues/7) | `Rename.strict=False` |
| [8](https://github.com/eddiethedean/planframe/issues/8) | Rich `select` / projection with `Expr` |
| [9](https://github.com/eddiethedean/planframe/issues/9) | Sort keys as expressions |
| [10](https://github.com/eddiethedean/planframe/issues/10) | Join keys as expressions |
| [11](https://github.com/eddiethedean/planframe/issues/11) | Group-by keys as expressions |
| [12](https://github.com/eddiethedean/planframe/issues/12) | Aggregations beyond `(op, column)` |
| [13](https://github.com/eddiethedean/planframe/issues/13) | `Unnest` IR must carry `fields` |
| [14](https://github.com/eddiethedean/planframe/issues/14) | Optional schema context for `compile_expr` |
| [15](https://github.com/eddiethedean/planframe/issues/15) | Async materialization / adapter hooks |
| [16](https://github.com/eddiethedean/planframe/issues/16) | `DropNulls`: `how=all`, `threshold` |
| [17](https://github.com/eddiethedean/planframe/issues/17) | `FillNull`: strategies / expr fill |
| [18](https://github.com/eddiethedean/planframe/issues/18) | Join typing for many key columns (no `Any` cast) |

**Str-only column lists in PlanFrame IR** (`Explode` / `Unnest` / subsets, etc.) — same theme as rich `select` ([planframe#8](https://github.com/eddiethedean/planframe/issues/8)).

### PlanFrame `Expr` lowering in the pydantable adapter

See previous docs: `planframe_adapter/expr.py` covers a subset of `planframe.expr.api`; other nodes raise `NotImplementedError` when used inside a PlanFrame plan executed by this adapter.

### Async

PlanFrame is synchronous; pydantable `acollect` / `ato_dict` / … delegate to `DataFrame`. See [planframe#15](https://github.com/eddiethedean/planframe/issues/15).

## See also

- {doc}`PLAN_AND_PLUGINS` — plan materialization and observers on the core `DataFrame`.
- [PlanFrame: Creating an adapter](https://github.com/eddiethedean/planframe/blob/main/docs/guides/planframe/creating-an-adapter.md) — `BaseAdapter` contract pydantable implements.
