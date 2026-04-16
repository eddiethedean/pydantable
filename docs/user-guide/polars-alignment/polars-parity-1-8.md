# Polars parity roadmap (1.8.0)

This page is the **implementation roadmap** for expanding Polars DataFrame parity in
PydanTable **1.8.0**, focused on the **core** API:

- `pydantable.DataFrame` (`python/pydantable/dataframe/_impl.py`)
- `pydantable.DataFrameModel` delegation (`python/pydantable/dataframe_model.py`)

It complements:

- The current-state table: [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md)
- The long-horizon parity history: [POLARS_TRANSFORMATIONS_ROADMAP](../../user-guide/polars-alignment/transformations-roadmap.md)
- The behavioral guarantees: [INTERFACE_CONTRACT](../../semantics/interface-contract.md)

## Scope and constraints

### In scope (1.8.0)

- “Most popular” Polars DataFrame **methods and arguments** that map cleanly to a
  **schema-first**, typed API.
- Argument-parity work that improves DX without changing the core semantics
  (for example: broadcast rules, validation errors, convenience overloads).

### Out of scope (1.8.0)

- Accepting arbitrary Polars dtypes that do not correspond to supported typed columns
  (the schema remains Pydantic-first; see [SUPPORTED_TYPES](../../user-guide/supported-types.md)).
- Selector DSL parity (e.g. full `pl.col("^re$")`, wildcard selectors) beyond explicit,
  schema-driven helpers.
- Index-like semantics (no implicit row index; no pandas-style alignment).
- A Python `polars.LazyFrame` escape hatch (see [INTERFACE_CONTRACT](../../semantics/interface-contract.md)).

## “Most popular” definition

We prioritize features by:

- Frequency in Polars tutorials/recipes: `select`, `with_columns`, `group_by`, `join`,
  `sort`, reshape (`melt`/`pivot`), sampling.
- Service impact: correctness and argument parity for joins/group-bys and predictable
  naming/schema propagation.
- Low-risk sequencing: **argument parity** and **convenience overloads** before new
  execution primitives.

## Target list (1.8.0)

The table below is the **deliverable checklist**. Each row should land with:

- API behavior in Python
- engine wiring (Rust) when required
- contract tests
- docs updates (this page + [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md) and sometimes [INTERFACE_CONTRACT](../../semantics/interface-contract.md))

| Area | Target | Arguments / details | Implementation notes |
|---|---|---|---|
| Core | `DataFrame.select` ergonomic overloads | allow expression aliasing; schema-driven helpers like `select_all()` / `select_prefix(...)` / `select_suffix(...)` | Avoid wildcard/regex selectors; expand using current schema fields. |
| Core | `DataFrame.with_columns` ergonomic overloads | accept positional aliased expressions in addition to kwargs | Must preserve deterministic schema order rules. |
| Core | `DataFrame.sort` argument parity | broadcast validation (`descending`, `nulls_last`), consistent errors; consider `maintain_order=` if engine supports | Must keep existing `engine_streaming` story. |
| Core | `drop` / `rename` missing-column behavior | add `strict=` / `errors=` behavior for missing columns | Must be consistent with typed contract; document defaults. |
| Core | `unique` / `distinct` options | clarify allowed `keep`; consider `maintain_order=` | If engine lacks support, document as unsupported. |
| GroupBy | `GroupedDataFrame` convenience methods | `sum/mean/min/max/count/len` style shortcuts + deterministic naming | Must not change all-null group semantics (see [INTERFACE_CONTRACT](../../semantics/interface-contract.md)). |
| GroupBy | Group-by arguments | `maintain_order=` / `drop_nulls=` where feasible | If not feasible, define explicit non-support. |
| Join | Join argument parity | `coalesce=` and stricter `on/left_on/right_on` validation; consider `validate=` checks | Prefer “documented constraints” over partial silent behavior. |
| Reshape | `pivot` argument parity | `sort_columns=`, `separator=`, etc. where feasible | Preserve deterministic output naming contract. |
| Utilities | High-use utilities | `sample`, `shift`, `null_count`, `is_empty`-style helpers | Decide per-method: plan step vs eager (document costs). |

## Testing strategy

- **Contract-first**: add tests that assert behavior against the stated contract, not row order.
- **Parity checks**: where possible, compare to Polars on the same inputs, but accept
  documented deviations when schema-first typing requires it.
- **Error paths**: every new argument should have at least one “bad input” test.

## Release documentation

When features land:

- Update [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md) (status + notes).
- Add or refresh examples in [POLARS_WORKFLOWS](../../user-guide/polars-alignment/workflows.md) for newly-added common patterns.

