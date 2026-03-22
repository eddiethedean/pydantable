# Pydantable Interface Contract (Polars-style)

This document records the behavior contract that the typed DataFrame API guarantees.
It is intended to be independent of Python import style (default vs `pandas` / `pyspark` UI)
at the *type/semantics* level, while allowing implementation-specific physical ordering
(e.g. row order from `to_dict()` / `collect(as_lists=True)`).

**Row models:** `collect()` with default arguments returns a **list of Pydantic models**;
order of that list follows the same non-guarantee as columnar materialization unless
documented otherwise for a specific op.

## Ordering

Columnar materialization (`to_dict()`, `collect(as_lists=True)`) output order is **not a stable API guarantee**.

When tests or user assertions need deterministic comparisons, compare on the
subset of columns that define identity (for example, join keys) rather than
row position. The project test-suite uses sorted comparisons to enforce this.

## Join semantics (collision + keys)

### Join keys
- `join(on=...)` requires at least one join key.
- `on` must reference a column that exists on both sides.
- `join(left_on=..., right_on=...)` supports column names or single-column expressions.
- `cross` joins do not accept `on`/`left_on`/`right_on`.

### Collision handling
- Column name collisions introduced by the right-hand side are resolved by
  renaming right-side non-key columns with the provided `suffix` (default:
  `"_right"`).
- Left-hand columns keep their original names.

In other words: collisions on non-key columns become `"<name><suffix>"` for the
right side, while join key columns remain singletons.

### Supported join kinds
- `inner`, `left`, `full`, `right`, `semi`, `anti`, `cross`
- `semi`/`anti` return only left-side columns.

## Null semantics

Null handling is SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the comparison result is `NULL`
- `filter(condition)`: retains rows where the condition evaluates to exactly
  `True`, and drops rows where it is `False` or `NULL`

These rules are enforced in the Rust core so that derived schemas and runtime
values remain aligned.

## Group-by aggregation semantics (all-null groups)

Supported aggregation operators:
- `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`

For groups where all input values for an aggregated column are `NULL`:
- `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, and `last` yield `None`
- `count` yields `0`
- `n_unique` yields `0` (SQL-like distinct-count behavior over non-null values)

The output field nullability is preserved/derived accordingly:
- nullable aggregates above are typed as `Optional[...]`
- `count` and `n_unique` outputs remain non-optional integers

## Reshaping semantics

Supported reshape methods:
- `melt` / `unpivot`
- `pivot`
- `explode` and `unnest` API entrypoints

`melt` / `unpivot`:
- `id_vars` are preserved as-is.
- `variable_name` is always non-nullable `str`.
- `value_name` requires all source `value_vars` to share a compatible scalar base dtype.
- `value_vars` cannot overlap with `id_vars`.

`pivot`:
- Requires `index`, `columns`, and at least one `values` column.
- Supports aggregate functions: `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`.
- Numeric aggregates (`sum`, `mean`, `median`, `std`, `var`) require numeric value dtypes.
- Generated output columns use deterministic names:
  - single value column: `<pivot_value>_<agg>`
  - multiple value columns: `<pivot_value>_<value_col>_<agg>`

`explode` / `unnest`:
- Only **scalar** column dtypes are modeled; see `SUPPORTED_TYPES.md` for the full list (`int`, `float`, `bool`, `str`, `datetime`, `date`, `timedelta`, each nullable).
- Because **list**/**struct** typed columns are not yet part of the schema system, both methods raise explicit `NotImplementedError` with guidance.

## Window and time-series semantics

Supported P6 API surface:
- `Expr.over(partition_by=..., order_by=...)` exists for API compatibility, but **window framing is not implemented yet**. Calling `.over()` with no arguments is silent; if you pass `partition_by` or `order_by`, pydantable emits a **runtime warning** and still evaluates the underlying expression as a non-window expression (no partition/order semantics).
- `rolling_agg(...)`
- `group_by_dynamic(...).agg(...)` requires **positive** `every` and `period` duration strings (e.g. `every="0s"` raises `ValueError` to avoid infinite loops in the reference dynamic implementation).

Temporal typing:
- Schema descriptors support `datetime`, `date`, and `duration` base types (including nullable variants).
- Temporal descriptors round-trip through Rust schema descriptors into derived Python schema types.

Rolling/dynamic contracts:
- Rolling windows support grouped trailing windows with deterministic ordering by `on` (and optional `by` keys).
- Dynamic windows support `every` / `period` with `s/m/h/d` suffixes and explicit aggregation contracts.
- Nulls are ignored for numeric aggregations; all-null windows yield `None` for nullable aggregates and `0` for `count`.

## Migration Notes (Polars -> Pydantable)

- Keep schema-first modeling: declare columns on `DataFrameModel` before transforms.
- Prefer `out_name=(op, column)` aggregation specs instead of ad-hoc expression maps.
- Treat row order as non-contractual unless explicitly sorted before assertions.
- Use typed nullability (`T | None`) consistently; output schema nullability is contract-driven.
- For reshape workflows, follow deterministic output naming contracts documented in this file.

